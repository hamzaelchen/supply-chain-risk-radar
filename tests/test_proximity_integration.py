"""Job de proximité de bout en bout (make up) : tables jetables, vrai cycle GDELT, oracle Python."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F

from portfolio import generate as pf
from ref import chokepoints as ref_chokepoints
from ref import cities as ref_cities
from silver import gdelt_events as events
from silver import proximity

from .test_geo import hav_py
from .test_silver_integration import append_bronze
from .test_silver_transform import LINES

pytestmark = [pytest.mark.spark, pytest.mark.integration]
T1 = datetime(2026, 10, 9, 8, 0, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 9, 8, 15, 0, tzinfo=UTC)
FIRST, SECOND = LINES[:1000], LINES[1000:]


@pytest.fixture
def world(spark, infra, sandbox):
    name = sandbox.name()
    ns = sandbox.namespace
    t = {
        "bronze": f"{ns}.bronze_{name}",
        "events": f"{ns}.events_{name}",
        "quarantine": f"{ns}.quarantine_{name}",
        "suppliers": f"{ns}.suppliers_{name}",
        "routes": f"{ns}.routes_{name}",
        "cities": f"{ns}.cities_{name}",
        "chokepoints": f"{ns}.chokepoints_{name}",
        "supplier_pairs": f"{ns}.supplier_pairs_{name}",
        "chokepoint_pairs": f"{ns}.chokepoint_pairs_{name}",
        "runs": f"{ns}.runs_{name}",
    }
    from streaming.bronze_gdelt_events import ensure_table

    ensure_table(spark, t["bronze"])
    ref_cities.load(spark, t["cities"])
    ref_chokepoints.load(spark, t["chokepoints"])
    pf.load(spark, t["suppliers"], t["routes"])
    return t


def load_events(spark, t, lines, at):
    append_bronze(spark, t["bronze"], lines, at)
    events.run(spark, bronze=t["bronze"], silver=t["events"], quarantine=t["quarantine"])


def proximity_run(spark, t):
    return proximity.run(
        spark,
        events_table=t["events"],
        suppliers_table=t["suppliers"],
        cities_table=t["cities"],
        chokepoints_table=t["chokepoints"],
        supplier_pairs=t["supplier_pairs"],
        chokepoint_pairs=t["chokepoint_pairs"],
    )


def oracle(spark, t):
    """Les paires attendues, calculées en Python pur sur les lignes de la silver."""
    rows = proximity.precise_events(spark.table(t["events"])).collect()
    cities = {r.city_id: r for r in spark.table(t["cities"]).collect()}
    suppliers = spark.table(t["suppliers"]).select("supplier_id", "city_id").collect()
    chokes = spark.table(t["chokepoints"]).collect()
    supplier_pairs, choke_pairs = {}, {}
    for e in rows:
        for s in suppliers:
            c = cities[s.city_id]
            km = hav_py(e.action_geo_lat, e.action_geo_long, c.lat, c.lon)
            if km <= proximity.SUPPLIER_RADIUS_KM:
                supplier_pairs[(e.global_event_id, s.supplier_id)] = km
        for k in chokes:
            km = hav_py(e.action_geo_lat, e.action_geo_long, k.lat, k.lon)
            if km <= k.radius_km:
                choke_pairs[(e.global_event_id, k.chokepoint_id)] = km
    return supplier_pairs, choke_pairs


def stored(spark, t):
    s = {
        (r.global_event_id, r.supplier_id): r.distance_km
        for r in spark.table(t["supplier_pairs"]).collect()
    }
    c = {
        (r.global_event_id, r.chokepoint_id): r.distance_km
        for r in spark.table(t["chokepoint_pairs"]).collect()
    }
    return s, c


def assert_same(got, expected):
    assert got.keys() == expected.keys()
    for key, km in expected.items():
        assert got[key] == pytest.approx(km, abs=1e-6)


def test_pairs_match_an_independent_python_oracle(spark, world):
    load_events(spark, world, FIRST, T1)
    stats = proximity_run(spark, world)

    expected_s, expected_c = oracle(spark, world)
    got_s, got_c = stored(spark, world)
    assert len(expected_s) > 10 and len(expected_c) > 5  # le cycle réel contient des cas
    assert_same(got_s, expected_s)
    assert_same(got_c, expected_c)
    assert (stats.supplier_pairs, stats.chokepoint_pairs) == (len(got_s), len(got_c))
    assert stats.events_read == spark.table(world["events"]).count()
    assert stats.events_precise < stats.events_read  # des centroïdes ont été écartés


def test_only_city_level_events_are_ever_paired(spark, world):
    load_events(spark, world, FIRST, T1)
    proximity_run(spark, world)
    types = {
        r.action_geo_type
        for r in spark.table(world["events"])
        .join(spark.table(world["supplier_pairs"]), "global_event_id")
        .select("action_geo_type")
        .collect()
    }
    assert types <= {3, 4}


def test_rerunning_is_a_no_op_and_the_cursor_is_the_newest_event_read(spark, world):
    load_events(spark, world, FIRST, T1)
    proximity_run(spark, world)
    before = stored(spark, world)
    cursor = events.read_cursor(spark, world["supplier_pairs"], proximity.CURSOR_PROPERTY)
    newest = spark.table(world["events"]).agg(F.max(F.unix_micros("bronze_ingested_at"))).first()[0]
    assert cursor == newest

    again = proximity_run(spark, world)
    assert again.events_read == 0 and stored(spark, world) == before


def test_incremental_run_handles_only_new_events(spark, world):
    load_events(spark, world, FIRST, T1)
    first = proximity_run(spark, world)
    load_events(spark, world, SECOND, T2)
    second = proximity_run(spark, world)

    assert second.events_read == len(SECOND) and second.events_read < first.events_read + 1000
    expected_s, expected_c = oracle(spark, world)
    got_s, got_c = stored(spark, world)
    assert_same(got_s, expected_s)
    assert_same(got_c, expected_c)


def test_replaying_after_a_lost_cursor_adds_nothing(spark, world):
    load_events(spark, world, FIRST, T1)
    proximity_run(spark, world)
    before = stored(spark, world)
    spark.sql(
        f"ALTER TABLE {world['supplier_pairs']} UNSET TBLPROPERTIES ('{proximity.CURSOR_PROPERTY}')"
    )
    replay = proximity_run(spark, world)
    assert replay.events_read > 0
    assert stored(spark, world) == before
    assert spark.table(world["supplier_pairs"]).count() == len(before[0])  # pas de doublon


def test_each_run_is_recorded_in_ops_pipeline_runs(spark, world):
    load_events(spark, world, FIRST, T1)
    tables = {
        "events_table": world["events"], "suppliers_table": world["suppliers"],
        "cities_table": world["cities"], "chokepoints_table": world["chokepoints"],
        "supplier_pairs": world["supplier_pairs"], "chokepoint_pairs": world["chokepoint_pairs"],
        "runs_table": world["runs"],
    }  # fmt: skip
    proximity.run_and_record(spark, **tables)
    proximity.run_and_record(spark, **tables)
    first, second = spark.table(world["runs"]).orderBy("started_at").collect()
    assert first.pipeline == "silver.proximity" and first.status == "succeeded"
    assert first.rows_read == spark.table(world["events"]).count() and first.rows_inserted > 0
    assert second.rows_read == 0 and second.rows_inserted == 0
