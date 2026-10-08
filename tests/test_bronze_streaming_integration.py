"""Job de streaming de bout en bout sur l'infra locale (make up), dans un topic,
une table et un checkpoint jetables. Rejoue un vrai cycle GDELT."""

import uuid

import pytest
from confluent_kafka.admin import AdminClient

from ingestion import gdelt
from ingestion.kafka_io import EventPublisher, ensure_topic
from streaming.bronze_gdelt_events import run

from .conftest import FIXTURES

pytestmark = [pytest.mark.spark, pytest.mark.integration]

LINES = list(gdelt.iter_event_lines((FIXTURES / "gdelt/20261008171500.export.CSV.zip").read_bytes()))
HEADERS = {
    "source_url": "http://data.gdeltproject.org/gdeltv2/20261008171500.export.CSV.zip",
    "slot_ts": "2026-10-08T17:15:00+00:00",
    "fetched_at": "2026-10-08T17:15:33+00:00",
    "raw_object_uri": "s3://lakehouse/raw/gdelt/events/date=2026-10-08/20261008171500.export.CSV.zip",
}


def hadoop_path(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path


@pytest.fixture
def job(spark, infra):
    run_id = uuid.uuid4().hex[:8]
    topic = f"test.gdelt.events.{run_id}"
    table = f"lakehouse.test_tmp.gdelt_events_{run_id}"
    checkpoint = f"s3a://{infra.bucket}/checkpoints/test_tmp/{run_id}"
    admin = AdminClient({"bootstrap.servers": infra.kafka_bootstrap})
    ensure_topic(infra.kafka_bootstrap, topic)
    publisher = EventPublisher(infra.kafka_bootstrap, topic)

    def publish():
        publisher.publish(((gdelt.event_key(line), line) for line in LINES), HEADERS)

    def run_job():
        run(spark, bootstrap=infra.kafka_bootstrap, checkpoint=checkpoint, topic=topic, table=table)

    yield publish, run_job, table, checkpoint

    admin.delete_topics([topic])[topic].result(timeout=30)
    fs, path = hadoop_path(spark, checkpoint)
    fs.delete(path, True)
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql("DROP NAMESPACE IF EXISTS lakehouse.test_tmp")


def counts(spark, table):
    rows = spark.sql(
        f"SELECT count(*) AS n, count(DISTINCT kafka_partition, kafka_offset) AS offsets, "
        f"count(DISTINCT global_event_id) AS events FROM {table}"
    ).first()
    snapshots = spark.table(f"{table}.snapshots").count()
    return rows.n, rows.offsets, rows.events, snapshots


def test_restarting_the_job_creates_no_duplicates(spark, job):
    publish, run_job, table, _ = job

    publish()
    run_job()
    assert counts(spark, table) == (1775, 1775, 1775, 1)

    # Relance sans nouvelles données : rien à lire, aucun commit.
    run_job()
    assert counts(spark, table) == (1775, 1775, 1775, 1)


def test_restart_after_crash_following_the_iceberg_commit(spark, job):
    """Crash simulé entre le commit Iceberg et la fin du lot côté Spark.

    On efface la trace « lot 0 terminé » du checkpoint : Spark rejoue le lot 0
    avec les mêmes offsets, Iceberg reconnaît le lot déjà commité et n'écrit rien.
    Avec Spark 4.1.1 ce rejeu levait une NullPointerException (SPARK-57438,
    corrigé en 4.1.3) : run_job() ne rattrape rien, toute exception fait échouer
    le test.
    """
    publish, run_job, table, checkpoint = job
    publish()
    run_job()
    fs, commit_marker = hadoop_path(spark, f"{checkpoint}/commits/0")
    assert fs.delete(commit_marker, False)

    run_job()

    assert counts(spark, table) == (1775, 1775, 1775, 1)
    assert fs.exists(commit_marker)

    # Et le job reprend normalement ensuite : seul le nouvel envoi est lu.
    publish()
    run_job()
    assert counts(spark, table) == (3550, 3550, 1775, 2)


def test_bronze_keeps_duplicates_sent_by_the_producer(spark, job):
    publish, run_job, table, _ = job

    publish()
    run_job()
    # Le producteur est « au moins une fois » : il peut renvoyer un créneau entier.
    publish()
    run_job()

    # Seuls les nouveaux offsets sont lus, et le bronze garde les deux envois.
    assert counts(spark, table) == (3550, 3550, 1775, 2)

    partitions = spark.sql(f"SELECT DISTINCT partition.ingested_at_day FROM {table}.files")
    assert partitions.count() == 1
