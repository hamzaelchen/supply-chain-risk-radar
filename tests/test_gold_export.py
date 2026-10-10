"""Export du contrat du site : fichiers, types et règles du contrat, sur une base DuckDB minimale."""

import json
from datetime import UTC, date, datetime

import duckdb
import pytest

from transform import export

ISO = [("S001", "Alpha", "auto", "Pièces automobiles", "Shenzhen", "China", "CN", "CHINA_SOUTH")]


def make_db(path, *, double_count=False, score=55.0, level="alerte", null_score=False):
    con = duckdb.connect(str(path))
    con.execute("""create table score_window as select date '2026-10-09' day_end, date '2026-10-07' day_start,
        timestamp '2026-10-07 00:00:00' window_start, timestamp '2026-10-10 00:00:00' window_end""")
    con.execute("""create table stg_coverage as select * from (values
        (date '2026-10-07', 96, 100000), (date '2026-10-08', 96, 110000), (date '2026-10-09', 96, 90000))
        t(day, batches, events)""")
    con.execute("""create table stg_events as select date '2026-10-09' batch_day,
        timestamp '2026-10-09 12:00:00' src_fetched_at, true is_recent""")
    rows = []
    for i in range(100):
        sid = f"S{i + 1:03d}"
        sc = None if (null_score and i == 0) else score
        rows.append(
            (
                sid,
                f"Fournisseur {i}",
                "auto",
                "Pièces automobiles",
                "Ville",
                "Pays",
                "XX",
                "R",
                1.0,
                2.0,
                False,
                1.0,
                "FICTIF - Entreprise démo",
                sc,
                level,
                10.0,
                5.0,
                1.0,
                20.0,
                10.0,
                1.0,
                3,
                1,
                "3 événements négatifs près de Ville sur les 3 derniers jours",
            )
        )
    con.execute(
        "create table supplier_scores (supplier_id varchar, name varchar, category varchar, "
        "category_label varchar, city varchar, country varchar, iso2 varchar, region varchar, "
        "lat double, lon double, sole_source boolean, purchase_share_pct double, label varchar, "
        "risk_score double, risk_level varchar, component_e double, component_t double, "
        "component_r double, criticality double, score_local double, score_route double, "
        "events_local integer, events_route integer, top_driver varchar)"
    )
    con.executemany("insert into supplier_scores values (" + ",".join("?" * 24) + ")", rows)
    con.execute(
        "create table supplier_routes as select * from (values ('S001', 1, 'hormuz')) t(supplier_id, position, chokepoint_id)"
    )
    ev = [
        (
            "S001",
            "city",
            None,
            12.0,
            1,
            date(2026, 10, 9),
            datetime(2026, 10, 9, 12, tzinfo=UTC),
            "Shenzhen",
            4,
            22.5,
            114.0,
            "190",
            "19",
            4,
            -10.0,
            -3.0,
            5,
            2,
            5,
            "http://a",
            "http://f",
            8.0,
            "violence_attaque",
            "Violence ou attaque",
            None,
            None,
        ),
        (
            "S001",
            "chokepoint",
            "hormuz",
            90.0,
            2,
            date(2026, 10, 9),
            datetime(2026, 10, 9, 12, tzinfo=UTC),
            "Dubaï",
            4,
            25.2,
            55.3,
            "180",
            "18",
            4,
            -9.0,
            -2.0,
            3,
            1,
            3,
            "http://b",
            "http://f",
            2.0,
            "violence_attaque",
            "Violence ou attaque",
            None,
            None,
        ),
    ]
    if double_count:
        ev.append(
            (
                "S001",
                "chokepoint",
                "hormuz",
                90.0,
                1,
                date(2026, 10, 9),
                datetime(2026, 10, 9, 12, tzinfo=UTC),
                "Shenzhen",
                4,
                22.5,
                114.0,
                "190",
                "19",
                4,
                -10.0,
                -3.0,
                5,
                2,
                5,
                "http://a",
                "http://f",
                1.0,
                "violence_attaque",
                "V",
                None,
                None,
            )
        )
    con.execute(
        "create table supplier_events (supplier_id varchar, link_type varchar, chokepoint_id varchar, "
        "distance_km double, global_event_id bigint, event_date date, fetched_at timestamptz, "
        "action_geo_name varchar, action_geo_type integer, lat double, lon double, event_code varchar, "
        "event_root_code varchar, quad_class integer, goldstein_scale double, avg_tone double, "
        "num_mentions integer, num_sources integer, num_articles integer, source_url varchar, "
        "src_file_url varchar, contribution double, event_family varchar, event_family_label_fr varchar, "
        "llm_category varchar, llm_summary varchar)"
    )
    con.executemany("insert into supplier_events values (" + ",".join("?" * 26) + ")", ev)
    con.execute(
        "create table chokepoint_scores as select * from (select 'hormuz' chokepoint_id, "
        "'Détroit d''Ormuz' name_fr, 'Strait of Hormuz' name_en, 26.6 lat, 56.5 lon, 200.0 radius_km, "
        "10.0 risk_score, 'surveillance' risk_level, 5 events_recent, 1 suppliers_exposed, "
        "1.0 purchase_share_exposed_pct, 0 sole_source_exposed)"
    )
    for i in range(15):
        con.execute(
            f"insert into chokepoint_scores values ('c{i}', 'n', 'n', 0.0, 0.0, 100.0, 0.0, "
            "'surveillance', 0, 0, 0.0, 0)"
        )
    con.close()


def test_export_writes_the_five_contract_files_with_typed_columns(tmp_path):
    make_db(tmp_path / "gold.duckdb")
    meta = export.export(
        tmp_path / "gold.duckdb", tmp_path / "out", datetime(2026, 10, 10, 8, tzinfo=UTC)
    )

    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == [
        "chokepoint_scores.parquet", "meta.json", "supplier_events.parquet",
        "supplier_routes.parquet", "supplier_scores.parquet",
    ]  # fmt: skip
    assert (
        meta["schema_version"] == 1 and meta["is_sample"] is False and meta["sample_notice"] is None
    )
    assert meta["generated_at"] == "2026-10-10T08:00:00Z"
    assert (meta["window_start"], meta["window_end"]) == (
        "2026-10-07T00:00:00Z",
        "2026-10-10T00:00:00Z",
    )
    assert meta["gdelt_batches"] == 288 and meta["events_total"] == 300000
    assert meta["latest_fetched_at"] == "2026-10-09T12:00:00Z"
    assert [c["id"] for c in meta["components"]] == [
        "component_e",
        "component_t",
        "component_r",
        "criticality",
    ]
    assert [(l["id"], l["min"], l["max"], l["alert"]) for l in meta["risk_levels"]] == [
        ("surveillance", 0, 40, False), ("alerte", 40, 70, True), ("critique", 70, 100, True),
    ]  # fmt: skip

    con = duckdb.connect()
    types = dict(
        con.execute(
            f"describe select * from read_parquet('{tmp_path / 'out/supplier_events.parquet'}')"
        ).fetchall()[i][:2]
        for i in range(26)
    )
    assert types["global_event_id"] == "BIGINT" and types["event_date"] == "DATE"
    assert types["fetched_at"] == "TIMESTAMP" and types["quad_class"] == "INTEGER"


def test_a_clean_export_has_no_contract_violation(tmp_path):
    make_db(tmp_path / "gold.duckdb")
    export.export(tmp_path / "gold.duckdb", tmp_path / "out")
    assert export.validate(tmp_path / "out") == []


def test_an_event_counted_in_the_city_and_the_chokepoint_is_refused(tmp_path):
    make_db(tmp_path / "gold.duckdb", double_count=True)
    with pytest.raises(SystemExit, match="à la fois ville et détroit"):
        export.export(tmp_path / "gold.duckdb", tmp_path / "out")


def test_levels_that_disagree_with_the_thresholds_are_refused(tmp_path):
    make_db(tmp_path / "gold.duckdb", score=72.0, level="alerte")
    with pytest.raises(SystemExit, match="niveaux incohérents"):
        export.export(tmp_path / "gold.duckdb", tmp_path / "out")


def test_a_null_risk_score_is_refused(tmp_path):
    make_db(tmp_path / "gold.duckdb", null_score=True)
    with pytest.raises(SystemExit, match="risk_score NULL"):
        export.export(tmp_path / "gold.duckdb", tmp_path / "out")


def test_boundaries_40_and_70_belong_to_alerte(tmp_path):
    for score in (40.0, 70.0):
        make_db(tmp_path / f"g{int(score)}.duckdb", score=score, level="alerte")
        export.export(tmp_path / f"g{int(score)}.duckdb", tmp_path / f"o{int(score)}")


def test_meta_is_valid_json_with_utc_timestamps(tmp_path):
    make_db(tmp_path / "gold.duckdb")
    export.export(tmp_path / "gold.duckdb", tmp_path / "out")
    meta = json.loads((tmp_path / "out/meta.json").read_text("utf-8"))
    for key in ("generated_at", "window_start", "window_end", "latest_fetched_at"):
        assert meta[key].endswith("Z")
