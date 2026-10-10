"""Silver -> proximité : événements GDELT proches des fournisseurs et des détroits.

Usage :
    python -m silver.proximity        # traite les événements silver arrivés depuis le dernier passage

Deux tables, une ligne par paire (événement, objet) à portée :
- silver.event_supplier_proximity : événement à moins de 150 km de la ville d'un fournisseur
  (une ligne par fournisseur de cette ville) ;
- silver.event_chokepoint_proximity : événement à moins du rayon d'un détroit du référentiel
  (tous les détroits ; le lien avec la route d'un fournisseur se fait au gold, par
  silver.ref_supplier_routes).

Quels événements : ActionGeo « précis » seulement, c'est-à-dire type de lieu 3 (ville des
États-Unis) ou 4 (ville du monde) avec latitude et longitude présentes. Les types 1 (pays),
2 (État américain) et 5 (région) donnent le centroïde du pays ou de la région, pas le lieu de
l'événement : un événement « Belgique » serait compté à Anvers (voir CLAUDE.md, décision du
2026-10-09). Le filtre temporel (événements rétrospectifs) n'est pas appliqué ici : voir
silver/recency.py.

Incrémental : même mécanisme que la silver (curseur = max(bronze_ingested_at) des événements
LUS, en propriété de la table). INSERT seulement : relancé, il n'ajoute rien de plus.
"""

import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ops import pipeline_runs
from silver import geo
from silver.gdelt_events import (
    SILVER,
    action_geo_usable,
    limit_to_next_batches,
    read_cursor,
    write_cursor,
)

PIPELINE = "silver.proximity"
SUPPLIER_RADIUS_KM = 150.0
PRECISE_GEO_TYPES = (3, 4)
SUPPLIER_PAIRS = "lakehouse.silver.event_supplier_proximity"
CHOKEPOINT_PAIRS = "lakehouse.silver.event_chokepoint_proximity"
SUPPLIERS = "lakehouse.silver.ref_suppliers"
CITIES = "lakehouse.silver.ref_cities"
CHOKEPOINTS = "lakehouse.silver.ref_chokepoints"
CURSOR_PROPERTY = "scrr.silver_events_ingested_at_cursor_us"
METHOD = "cross"  # voir docs/geo-join.md pour la mesure qui motive ce choix

SUPPLIER_PAIRS_SCHEMA = (
    "global_event_id BIGINT, supplier_id STRING, city_id STRING, distance_km DOUBLE, "
    "bronze_ingested_at TIMESTAMP, computed_at TIMESTAMP"
)
CHOKEPOINT_PAIRS_SCHEMA = (
    "global_event_id BIGINT, chokepoint_id STRING, distance_km DOUBLE, radius_km DOUBLE, "
    "bronze_ingested_at TIMESTAMP, computed_at TIMESTAMP"
)


def precise_events(events: DataFrame) -> DataFrame:
    """Événements dont ActionGeo désigne un lieu précis (pas un centroïde de pays ou de région)."""
    return events.filter(action_geo_usable() & F.col("action_geo_type").isin(*PRECISE_GEO_TYPES))


def _within(events: DataFrame, points: DataFrame, method: str) -> DataFrame:
    if method == "cross":
        return geo.within_radius_cross(events, points)
    if method == "grid":
        return geo.within_radius_grid(events, points)
    raise ValueError(f"méthode inconnue : {method}")


@dataclass
class ProximityStats:
    events_read: int = 0
    events_precise: int = 0
    supplier_pairs: int = 0
    chokepoint_pairs: int = 0
    cursor_before_us: int | None = None
    cursor_after_us: int | None = None

    @property
    def events_coarse_excluded(self) -> int:
        return self.events_read - self.events_precise


def ensure_tables(
    spark: SparkSession,
    supplier_pairs: str = SUPPLIER_PAIRS,
    chokepoint_pairs: str = CHOKEPOINT_PAIRS,
) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {supplier_pairs.rsplit('.', 1)[0]}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {supplier_pairs} ({SUPPLIER_PAIRS_SCHEMA}) USING iceberg"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {chokepoint_pairs} ({CHOKEPOINT_PAIRS_SCHEMA}) USING iceberg"
    )


def run(
    spark: SparkSession,
    *,
    events_table: str = SILVER,
    suppliers_table: str = SUPPLIERS,
    cities_table: str = CITIES,
    chokepoints_table: str = CHOKEPOINTS,
    supplier_pairs: str = SUPPLIER_PAIRS,
    chokepoint_pairs: str = CHOKEPOINT_PAIRS,
    method: str = METHOD,
    max_batches: int | None = None,
) -> ProximityStats:
    ensure_tables(spark, supplier_pairs, chokepoint_pairs)
    cursor = read_cursor(spark, supplier_pairs, CURSOR_PROPERTY)
    stats = ProximityStats(cursor_before_us=cursor, cursor_after_us=cursor)
    window = spark.table(events_table)
    if cursor is not None:
        window = window.filter(
            F.col("bronze_ingested_at") > F.timestamp_micros(F.lit(cursor))
        )  # comparaison directe : poussée vers les statistiques de fichiers
    window = limit_to_next_batches(window, "bronze_ingested_at", max_batches).cache()
    try:
        newest = window.agg(F.max(F.unix_micros("bronze_ingested_at"))).first()[0]
        if newest is None:
            return stats  # rien de nouveau

        precise = (
            precise_events(window)
            .select(
                "global_event_id",
                "bronze_ingested_at",
                F.col("action_geo_lat").alias("event_lat"),
                F.col("action_geo_long").alias("event_lon"),
            )
            .cache()
        )
        stats.events_read, stats.events_precise = window.count(), precise.count()

        # Fournisseurs : on joint par ville distincte (40 points pour 100 fournisseurs), puis
        # on retrouve les fournisseurs de chaque ville.
        city_points = (
            spark.table(suppliers_table)
            .select("city_id")
            .distinct()
            .join(spark.table(cities_table), "city_id")
            .select(
                F.col("city_id").alias("point_id"),
                F.col("lat").alias("point_lat"),
                F.col("lon").alias("point_lon"),
                F.lit(SUPPLIER_RADIUS_KM).alias("radius_km"),
            )
        )
        by_city = _within(precise, city_points, method)
        supplier_rows = (
            by_city.join(
                spark.table(suppliers_table).select("supplier_id", "city_id"),
                by_city.point_id == F.col("city_id"),
            )
            .select(
                "global_event_id", "supplier_id", "city_id", "distance_km", "bronze_ingested_at",
                F.current_timestamp().alias("computed_at"),
            )
        )  # fmt: skip
        choke_points = spark.table(chokepoints_table).select(
            F.col("chokepoint_id").alias("point_id"),
            F.col("lat").alias("point_lat"),
            F.col("lon").alias("point_lon"),
            "radius_km",
        )
        choke_rows = _within(precise, choke_points, method).select(
            "global_event_id",
            F.col("point_id").alias("chokepoint_id"),
            "distance_km",
            "radius_km",
            "bronze_ingested_at",
            F.current_timestamp().alias("computed_at"),
        )

        for rows, table, keys, view in [
            (choke_rows, chokepoint_pairs, ["global_event_id", "chokepoint_id"], "_scrr_choke"),
            (supplier_rows, supplier_pairs, ["global_event_id", "supplier_id"], "_scrr_supplier"),
        ]:
            rows = rows.cache()
            count = rows.count()
            if table == supplier_pairs:
                stats.supplier_pairs = count
            else:
                stats.chokepoint_pairs = count
            rows.createOrReplaceTempView(view)
            on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
            spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} WHEN NOT MATCHED THEN INSERT *")

        write_cursor(spark, supplier_pairs, newest, CURSOR_PROPERTY)
        stats.cursor_after_us = newest
        return stats
    finally:
        spark.catalog.clearCache()


def run_and_record(spark: SparkSession, **tables) -> ProximityStats:
    """`run` + une ligne dans ops.pipeline_runs (rows_read = événements lus,
    rows_inserted = paires fournisseur + paires détroit calculées, rows_quarantined = 0)."""
    runs_table = tables.pop("runs_table", pipeline_runs.TABLE)
    started, clock = datetime.now(UTC), time.monotonic()
    try:
        stats = run(spark, **tables)
    except Exception as exc:
        pipeline_runs.record(
            spark,
            pipeline_runs.PipelineRun(
                pipeline=PIPELINE, started_at=started,
                finished_at=started + timedelta(seconds=time.monotonic() - clock),
                status="failed", error=repr(exc)[:2000],
            ),
            runs_table,
        )  # fmt: skip
        raise
    pipeline_runs.record(
        spark,
        pipeline_runs.PipelineRun(
            pipeline=PIPELINE, started_at=started,
            finished_at=started + timedelta(seconds=time.monotonic() - clock),
            status="succeeded", rows_read=stats.events_read,
            rows_inserted=stats.supplier_pairs + stats.chokepoint_pairs, rows_quarantined=0,
            cursor_before=pipeline_runs.micros_to_datetime(stats.cursor_before_us),
            cursor_after=pipeline_runs.micros_to_datetime(stats.cursor_after_us),
        ),
        runs_table,
    )  # fmt: skip
    return stats


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--max-batches", type=int, metavar="N", help="N lots d'ingestion par passage"
    )
    parser.add_argument("--until-caught-up", action="store_true")
    args = parser.parse_args(argv)
    from lakehouse.spark_session import build_spark_session

    spark = build_spark_session("silver-proximity")
    spark.sparkContext.setLogLevel("WARN")
    try:
        while True:
            stats = run_and_record(spark, max_batches=args.max_batches)
            print(f"{PIPELINE} : {stats}", flush=True)
            if not (args.until_caught_up and stats.events_read):
                break
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
