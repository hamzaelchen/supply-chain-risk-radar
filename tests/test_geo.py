"""Distance haversine et jointures « à moins de X km » : correction et équivalence."""

import math
import random

import pytest
from pyspark.sql import functions as F

from silver import geo
from silver.gdelt_events import parse_events
from silver.proximity import precise_events
from streaming.bronze_gdelt_events import to_bronze

from .test_bronze_transform import LINES, kafka_df

pytestmark = pytest.mark.spark


def hav_py(lat1, lon1, lat2, lon2):
    """Haversine en Python pur : l'oracle indépendant de Spark."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (
        math.sin((p2 - p1) / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * geo.EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def distances(spark, pairs):
    df = spark.createDataFrame(pairs, "a double, b double, c double, d double")
    out = df.select(geo.haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d")).alias("km"))
    return [r.km for r in out.collect()]


def test_haversine_known_distances(spark):
    one_degree, antipodes, paris_london = distances(
        spark, [(0.0, 0.0, 0.0, 1.0), (0.0, 0.0, 0.0, 180.0), (48.8566, 2.3522, 51.5074, -0.1278)]
    )
    assert one_degree == pytest.approx(geo.KM_PER_DEGREE, rel=1e-9)
    assert one_degree == pytest.approx(111.195, abs=0.01)
    assert antipodes == pytest.approx(math.pi * geo.EARTH_RADIUS_KM, rel=1e-9)
    assert paris_london == pytest.approx(343.5, abs=1.0)  # valeur usuelle publiée


def test_haversine_across_the_antimeridian_is_short(spark):
    (km,) = distances(spark, [(0.0, 179.95, 0.0, -179.95)])
    assert km == pytest.approx(0.1 * geo.KM_PER_DEGREE, rel=1e-6)


def test_haversine_matches_the_python_oracle(spark):
    rng = random.Random(7)
    pairs = [
        (rng.uniform(-90, 90), rng.uniform(-180, 180), rng.uniform(-90, 90), rng.uniform(-180, 180))
        for _ in range(200)
    ]
    for got, args in zip(distances(spark, pairs), pairs, strict=True):
        assert got == pytest.approx(hav_py(*args), abs=1e-6)


def test_a_small_circle_at_the_equator_covers_a_few_cells():
    cells = geo.covering_cells(0.5, 0.5, 50, cell_deg=1.0)
    assert (90, 180) in cells  # la cellule du centre
    assert 1 <= len(cells) <= 9


def test_a_circle_over_the_antimeridian_takes_cells_on_both_sides():
    columns = {c for _, c in geo.covering_cells(0.0, 179.95, 100, cell_deg=1.0)}
    assert 359 in columns and 0 in columns


def test_a_circle_over_a_pole_takes_every_longitude():
    columns = {c for _, c in geo.covering_cells(89.5, 10.0, 200, cell_deg=1.0)}
    assert len(columns) == 360


def random_scene(seed, n_events=400, n_points=25):
    """Événements et points aléatoires, dont beaucoup près de l'antiméridien et des pôles."""
    rng = random.Random(seed)

    def lat():
        return rng.choice([rng.uniform(-90, 90), rng.uniform(75, 90), rng.uniform(-90, -75)])

    def lon():
        return rng.choice([rng.uniform(-180, 180), rng.uniform(170, 180), rng.uniform(-180, -170)])

    events = [(i, lat(), lon()) for i in range(n_events)]
    points = [
        (f"p{i}", lat(), lon(), rng.choice([50.0, 100.0, 150.0, 300.0, 800.0]))
        for i in range(n_points)
    ]
    return events, points


def frames(spark, events, points):
    e = spark.createDataFrame(events, "global_event_id long, event_lat double, event_lon double")
    p = spark.createDataFrame(
        points, "point_id string, point_lat double, point_lon double, radius_km double"
    )
    return e, p


def pair_set(df):
    return {(r.global_event_id, r.point_id) for r in df.collect()}


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_every_method_finds_exactly_the_pairs_of_the_python_oracle(spark, seed):
    events, points = random_scene(seed)
    oracle = {
        (e[0], p[0]) for e in events for p in points if hav_py(e[1], e[2], p[1], p[2]) <= p[3]
    }
    assert len(oracle) > 20  # le scénario n'est pas vide
    e, p = frames(spark, events, points)
    assert pair_set(geo.within_radius_cross(e, p, prefilter=False)) == oracle
    assert pair_set(geo.within_radius_cross(e, p)) == oracle
    assert pair_set(geo.within_radius_grid(e, p, 1.0)) == oracle
    assert pair_set(geo.within_radius_grid(e, p, 0.5)) == oracle


def test_distance_is_returned_and_never_above_the_radius(spark):
    events, points = random_scene(11)
    e, p = frames(spark, events, points)
    rows = geo.within_radius_cross(e, p).collect()
    assert rows and all(0 <= r.distance_km <= r.radius_km for r in rows)


def test_real_gdelt_events_give_the_same_pairs_with_the_grid_and_the_cross_join(spark):
    from ref import chokepoints, cities

    valid, _ = parse_events(_as_table(spark, LINES))
    events = precise_events(valid).select(
        "global_event_id",
        F.col("action_geo_lat").alias("event_lat"),
        F.col("action_geo_long").alias("event_lon"),
    )
    rows = [(c["city_id"], float(c["lat"]), float(c["lon"]), 150.0) for c in cities.read_csv()] + [
        (c["chokepoint_id"], float(c["lat"]), float(c["lon"]), float(c["radius_km"]))
        for c in chokepoints.read_csv()
    ]
    points = spark.createDataFrame(
        rows, "point_id string, point_lat double, point_lon double, radius_km double"
    )
    assert events.count() > 100
    assert pair_set(geo.within_radius_grid(events, points)) == pair_set(
        geo.within_radius_cross(events, points, prefilter=False)
    )


def _as_table(spark, lines):
    bronze = to_bronze(kafka_df(spark, lines))
    return spark.createDataFrame(bronze.collect(), bronze.schema)


def test_precise_events_keep_only_city_level_places(spark):
    schema = "action_geo_lat double, action_geo_long double, action_geo_type int"
    df = spark.createDataFrame(
        [(1.0, 1.0, 1), (1.0, 1.0, 2), (1.0, 1.0, 3), (1.0, 1.0, 4), (1.0, 1.0, 5), (None, 1.0, 4)],
        schema,
    )
    kept = sorted(r.action_geo_type for r in precise_events(df).collect())
    assert kept == [3, 4]
