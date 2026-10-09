"""Mesure de trois jointures « événement à moins de X km d'un point » sur les vraies données.

Usage : python -m silver.proximity_bench [--repeat 3] [--scales 1 10 50]

Les événements sont les ActionGeo précis de silver.gdelt_events ; les points sont les villes des
fournisseurs (rayon 150 km) et les détroits (leur rayon). Pour mesurer le passage à l'échelle, les
événements réels sont recopiés N fois : les temps sont fiables, les résultats de ces recopies ne
sont JAMAIS écrits nulle part. Vérifie aussi que les méthodes rendent exactement les mêmes paires.

H3 (bibliothèque `h3`, optionnelle) n'est mesuré que si elle est installée.
"""

import argparse
import math
import statistics
import sys
import time
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver import geo
from silver.proximity import SUPPLIER_RADIUS_KM, precise_events

H3_RESOLUTION = 4


def load_points(spark: SparkSession) -> DataFrame:
    cities = (
        spark.table("lakehouse.silver.ref_suppliers")
        .select("city_id")
        .distinct()
        .join(spark.table("lakehouse.silver.ref_cities"), "city_id")
        .select(
            F.concat(F.lit("city:"), "city_id").alias("point_id"),
            F.col("lat").alias("point_lat"),
            F.col("lon").alias("point_lon"),
            F.lit(float(SUPPLIER_RADIUS_KM)).alias("radius_km"),
        )
    )
    chokepoints = spark.table("lakehouse.silver.ref_chokepoints").select(
        F.concat(F.lit("choke:"), "chokepoint_id").alias("point_id"),
        F.col("lat").alias("point_lat"),
        F.col("lon").alias("point_lon"),
        F.col("radius_km"),
    )
    return cities.unionByName(chokepoints)


def within_radius_h3(events: DataFrame, points: DataFrame) -> DataFrame:
    import h3
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("string")
    def event_cell(lat: pd.Series, lon: pd.Series) -> pd.Series:
        return pd.Series([h3.latlng_to_cell(a, b, H3_RESOLUTION) for a, b in zip(lat, lon)])

    edge = h3.average_hexagon_edge_length(H3_RESOLUTION, unit="km")
    cells = []
    for p in points.collect():
        # Distance entre centres de cellules voisines ~ racine(3) x arête ; +2 anneaux de marge
        # (position du point dans sa cellule, déformation de la grille).
        k = math.ceil(p.radius_km / (math.sqrt(3) * edge)) + 2
        center = h3.latlng_to_cell(p.point_lat, p.point_lon, H3_RESOLUTION)
        cells += [(p.point_id, c) for c in h3.grid_disk(center, k)]
    cell_df = events.sparkSession.createDataFrame(cells, "point_id STRING, h3_cell STRING")
    keyed = events.withColumn("h3_cell", event_cell("event_lat", "event_lon"))
    joined = keyed.join(F.broadcast(cell_df), "h3_cell").join(F.broadcast(points), "point_id")
    distance = geo.haversine_km(
        F.col("event_lat"), F.col("event_lon"), F.col("point_lat"), F.col("point_lon")
    )
    return (
        joined.withColumn("distance_km", distance)
        .filter(F.col("distance_km") <= F.col("radius_km"))
        .drop("h3_cell")
    )


def methods(points: DataFrame) -> dict:
    found = {
        "croisé + haversine": lambda e: geo.within_radius_cross(e, points, prefilter=False),
        "croisé + préfiltre latitude": lambda e: geo.within_radius_cross(e, points),
        "grille 1,0°": lambda e: geo.within_radius_grid(e, points, 1.0),
        "grille 0,5°": lambda e: geo.within_radius_grid(e, points, 0.5),
    }
    try:
        import h3  # noqa: F401

        found[f"H3 résolution {H3_RESOLUTION} (UDF pandas)"] = lambda e: within_radius_h3(e, points)
    except ImportError:
        print("h3 absent : méthode H3 non mesurée")
    return found


def pairs(df: DataFrame) -> DataFrame:
    return df.select("global_event_id", "point_id")


def timed(action, repeat: int) -> list[float]:
    action()  # tour de chauffe (compilation, premier plan)
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        action()
        times.append(time.perf_counter() - start)
    return times


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--scales", type=int, nargs="+", default=[1, 10, 50])
    args = parser.parse_args(argv)

    from lakehouse.spark_session import build_spark_session

    spark = build_spark_session("proximity-bench")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        base = (
            precise_events(spark.table("lakehouse.silver.gdelt_events"))
            .select(
                "global_event_id",
                F.col("action_geo_lat").alias("event_lat"),
                F.col("action_geo_long").alias("event_lon"),
            )
            .cache()
        )
        n_base = base.count()
        points = load_points(spark).cache()
        print(f"{n_base} événements précis, {points.count()} points\n")
        candidates = methods(points)

        reference = pairs(candidates["croisé + haversine"](base)).cache()
        n_pairs = reference.count()
        print(f"paires de référence (croisé + haversine) : {n_pairs}")
        for name, build in candidates.items():
            got = pairs(build(base))
            missing = reference.exceptAll(got).count()
            extra = got.exceptAll(reference).count()
            print(f"  {name:36s} manquantes {missing}, en trop {extra}")
        print()

        print(
            f"{'méthode':36s}"
            + "".join(f"{f'x{s} ({s * n_base:,} évts)':>24s}" for s in args.scales)
        )
        results: dict[str, list[str]] = {name: [] for name in candidates}
        for scale in args.scales:
            scaled = reduce(DataFrame.unionByName, [base] * scale).cache()
            scaled.count()
            for name, build in candidates.items():
                action = lambda b=build, s=scaled: b(s).count()
                times = timed(action, 1 if scale >= 50 else args.repeat)
                results[name].append(f"{statistics.median(times):6.2f} s")
            scaled.unpersist()
        for name, cols in results.items():
            print(f"{name:36s}" + "".join(f"{c:>24s}" for c in cols))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
