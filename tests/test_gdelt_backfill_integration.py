"""Chargement historique de bout en bout (make up) : vrai bronze Iceberg jetable, vrai cycle GDELT
comme contenu des fichiers, téléchargement remplacé par une doublure."""

from datetime import UTC, date, datetime

import pytest
from pyspark.sql import functions as F

from ingestion import gdelt_backfill as bf
from silver import gdelt_events as silver
from silver import proximity
from silver.recency import is_recent_event

from .test_gdelt_backfill import FakeManifest, N, fetcher, short_days  # noqa: F401
from .test_gdelt_producer import FakeStore
from .test_proximity_integration import proximity_run, world  # noqa: F401
from .test_silver_integration import append_bronze
from .test_silver_transform import LINES

pytestmark = [pytest.mark.spark, pytest.mark.integration]
DAYS = [date(2026, 2, 3), date(2026, 2, 4), date(2026, 2, 5)]
STREAM_AT = datetime(2026, 10, 9, 8, 0, 0, tzinfo=UTC)


@pytest.fixture
def lake(spark, world):  # noqa: F811 - `world` apporte déjà les tables jetables et les référentiels
    return world


def backfill(spark, lake, days, *, fetch=None, manifest=None):
    writer = bf.SparkBronzeWriter(spark, lake["bronze"])
    manifest = manifest or FakeManifest()
    results = bf.run(
        days, manifest=manifest, store=FakeStore(), writer=writer, pause=0.5,
        fetch=fetch or fetcher(),
    )  # fmt: skip
    return results, manifest


def silver_run(spark, lake, **kwargs):
    return silver.run(
        spark, bronze=lake["bronze"], silver=lake["events"], quarantine=lake["quarantine"], **kwargs
    )


def test_a_day_written_twice_adds_nothing_the_second_time(spark, lake):
    writer = bf.SparkBronzeWriter(spark, lake["bronze"])
    rows = []

    class Capture:
        def write_day(self, day, r):
            rows.extend(r)
            return writer.write_day(day, r)

    manifest = FakeManifest()
    bf.process_day(DAYS[0], done=set(), fetch=fetcher(), store=FakeStore(), writer=Capture(),
                   manifest=manifest, sleep=lambda s: None)  # fmt: skip
    table = spark.table(lake["bronze"])
    assert table.count() == len(rows) == N * 1775
    assert table.filter("ingestion_mode = 'backfill'").count() == table.count()

    received, inserted = writer.write_day(DAYS[0], rows)  # le même jour, rejoué
    assert (received, inserted) == (len(rows), 0)
    assert spark.table(lake["bronze"]).count() == len(rows)


def test_backfill_and_stream_rows_live_in_the_same_table_without_clashing(spark, lake):
    append_bronze(spark, lake["bronze"], LINES[:50], STREAM_AT)
    backfill(spark, lake, DAYS[:1])
    modes = {r.ingestion_mode: r["count"] for r in
             spark.table(lake["bronze"]).groupBy("ingestion_mode").count().collect()}  # fmt: skip
    assert modes == {"stream": 50, "backfill": N * 1775}


def test_silver_loads_backfill_rows_and_keeps_the_stream_copy_of_a_duplicate_event(spark, lake):
    append_bronze(spark, lake["bronze"], LINES[:50], STREAM_AT)
    backfill(
        spark, lake, DAYS[:1]
    )  # mêmes événements que le flux : l'ingestion du flux est plus ancienne

    stats = silver_run(spark, lake)

    assert stats.rows_read == 50 + N * 1775
    ids = spark.table(lake["events"]).select("global_event_id").distinct().count()
    assert ids == 1775 == spark.table(lake["events"]).count()  # aucun doublon
    kept = {r.ingestion_mode: r["count"] for r in
            spark.table(lake["events"]).groupBy("ingestion_mode").count().collect()}  # fmt: skip
    assert kept == {"stream": 50, "backfill": 1725}  # le premier arrivé gagne : les 50 du flux
    assert stats.duplicates_eliminated == 50 + N * 1775 - 1775


def test_a_bad_backfill_line_is_quarantined_once_even_when_the_window_is_replayed(spark, lake):
    def fetch(url):
        zip_bytes = fetcher()(url)
        return zip_bytes

    class BadLine:
        """Abîme la première ligne de chaque jour avant l'écriture."""

        def __init__(self, inner):
            self.inner = inner

        def write_day(self, day, rows):
            first = list(rows[0])
            first[0] = "ligne" + "\t" + "trop courte"
            return self.inner.write_day(day, [tuple(first), *rows[1:]])

    manifest = FakeManifest()
    bf.process_day(DAYS[0], done=set(), fetch=fetcher(), store=FakeStore(),
                   writer=BadLine(bf.SparkBronzeWriter(spark, lake["bronze"])), manifest=manifest,
                   sleep=lambda s: None)  # fmt: skip
    silver_run(spark, lake)
    quarantined = spark.table(lake["quarantine"])
    assert quarantined.count() == 1
    row = quarantined.first()
    assert (row.ingestion_mode, row.src_line_no, row.reason) == (
        "backfill",
        1,
        "field_count=2 (attendu 61)",
    )

    spark.sql(f"ALTER TABLE {lake['events']} UNSET TBLPROPERTIES ('{silver.CURSOR_PROPERTY}')")
    silver_run(spark, lake)  # rejeu de toute la fenêtre
    assert spark.table(lake["quarantine"]).count() == 1  # identité null-safe : pas de doublon


def test_recency_of_loaded_history_is_measured_at_the_batch_date(spark, lake):
    # Les événements du cycle réel de la fixture sont datés du 2026-10-08 : on les charge dans les
    # lots du 2026-10-08, douze jours plus tard.
    loaded = datetime(2026, 10, 20, 9, 0, tzinfo=UTC)
    bf.process_day(
        date(2026, 10, 8), done=set(), fetch=fetcher(), store=FakeStore(), manifest=FakeManifest(),
        writer=bf.SparkBronzeWriter(spark, lake["bronze"]), sleep=lambda s: None,
        now=lambda: loaded,
    )  # fmt: skip
    silver_run(spark, lake)
    rows = spark.table(lake["events"])
    assert rows.filter(is_recent_event()).count() > 1000  # mesuré depuis l'étiquette du lot
    # Mesuré depuis la date de chargement, plus rien ne serait récent (écart de douze jours).
    from_load = rows.select(F.datediff(F.to_date("src_fetched_at"), F.col("sql_date")).alias("lag"))
    assert from_load.filter("lag <= 2").count() == 0


def test_silver_and_proximity_catch_up_a_few_ingestion_batches_at_a_time(spark, lake):
    results, _ = backfill(spark, lake, DAYS)
    per_day = [r.rows for r in results]
    assert len(per_day) == 3

    first = silver_run(spark, lake, max_batches=1)
    assert first.rows_read == per_day[0]
    second = silver_run(spark, lake, max_batches=2)
    assert second.rows_read == per_day[1] + per_day[2]
    assert silver_run(spark, lake, max_batches=2).rows_read == 0

    events_in_silver = spark.table(lake["events"]).count()
    p1 = proximity.run(
        spark, events_table=lake["events"], suppliers_table=lake["suppliers"],
        cities_table=lake["cities"], chokepoints_table=lake["chokepoints"],
        supplier_pairs=lake["supplier_pairs"], chokepoint_pairs=lake["chokepoint_pairs"],
        max_batches=1,
    )  # fmt: skip
    assert 0 < p1.events_read <= events_in_silver
    again = proximity_run(spark, lake)  # le reste, sans limite
    assert p1.events_read + again.events_read == events_in_silver
    assert proximity_run(spark, lake).events_read == 0
