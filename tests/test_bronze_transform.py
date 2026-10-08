"""to_bronze sur les lignes d'un vrai cycle GDELT, présentées comme les lit la source Kafka."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ingestion import gdelt
from streaming.bronze_gdelt_events import to_bronze
from streaming.gdelt_schema import BRONZE_COLUMNS, GDELT_EVENT_COLUMNS

from .conftest import FIXTURES

pytestmark = pytest.mark.spark

LINES = list(
    gdelt.iter_event_lines((FIXTURES / "gdelt/20261008171500.export.CSV.zip").read_bytes())
)
FILE_URL = "http://data.gdeltproject.org/gdeltv2/20261008171500.export.CSV.zip"
HEADERS = {
    "source_url": FILE_URL,
    "gdelt_batch_label_ts": "2026-10-08T17:15:00+00:00",
    "fetched_at": "2026-10-08T17:15:33.369978+00:00",
    "raw_object_uri": "s3://lakehouse/raw/gdelt/events/date=2026-10-08/20261008171500.export.CSV.zip",
}
KAFKA_TS = datetime(2026, 10, 8, 17, 15, 34, tzinfo=UTC)

# Schéma de la source Kafka de Spark avec includeHeaders=true.
KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("timestampType", T.IntegerType()),
        T.StructField(
            "headers",
            T.ArrayType(
                T.StructType(
                    [T.StructField("key", T.StringType()), T.StructField("value", T.BinaryType())]
                )
            ),
        ),
    ]
)


def kafka_df(spark, lines, headers=HEADERS):
    encoded = None if headers is None else [(k, v.encode()) for k, v in headers.items()]
    rows = [
        (gdelt.event_key(line), line, "gdelt.events", i % 3, i // 3, KAFKA_TS, 0, encoded)
        for i, line in enumerate(lines)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


@pytest.fixture(scope="module")
def bronze(spark):
    return to_bronze(kafka_df(spark, LINES)).cache()


def test_schema_has_61_gdelt_columns():
    assert len(GDELT_EVENT_COLUMNS) == 61
    assert len(set(GDELT_EVENT_COLUMNS)) == 61


def test_output_columns_match_table_definition(bronze):
    spark_types = {"STRING": "string", "INT": "int", "BIGINT": "bigint", "TIMESTAMP": "timestamp"}
    expected = [(name, spark_types[dtype]) for name, dtype in BRONZE_COLUMNS]
    assert bronze.dtypes == expected


def test_no_row_is_dropped_and_all_have_61_fields(bronze):
    assert bronze.count() == len(LINES) == 1775
    assert bronze.filter("field_count != 61").count() == 0


def test_raw_line_is_kept_unchanged(bronze):
    got = {r.kafka_offset * 3 + r.kafka_partition: r.raw_line for r in bronze.collect()}
    assert all(got[i] == line.decode() for i, line in enumerate(LINES))


def test_columns_are_mapped_by_position_and_stay_strings(bronze):
    first = LINES[0].decode().split("\t")
    row = bronze.filter(f"global_event_id = '{first[0]}'").first()
    assert [row[name] for name in GDELT_EVENT_COLUMNS] == first
    assert row.sql_date == "20251008"
    assert row.date_added == "20261008171500"
    assert row.source_url.startswith("http")
    # Les champs vides restent des chaînes vides : pas d'interprétation en bronze.
    assert row.actor1_code == ""


def test_date_added_is_the_batch_label_for_every_row(bronze):
    assert [r.date_added for r in bronze.select("date_added").distinct().collect()] == [
        "20261008171500"
    ]


def test_lineage_comes_from_kafka_headers_and_coordinates(bronze):
    # Horodatages rendus en texte par Spark (session en UTC) : indépendant du
    # fuseau de la machine qui lance les tests.
    timestamps = ["src_gdelt_batch_label_ts", "src_fetched_at", "kafka_timestamp"]
    row = (
        bronze.filter("kafka_partition = 1 AND kafka_offset = 2")
        .select("*", *[F.col(c).cast("string").alias(f"{c}_text") for c in timestamps])
        .first()
    )
    assert row.raw_line == LINES[7].decode()
    assert row.src_file_url == FILE_URL
    assert row.src_gdelt_batch_label_ts_text == "2026-10-08 17:15:00"
    assert row.src_fetched_at_text == "2026-10-08 17:15:33.369978"
    assert row.src_raw_object_uri == HEADERS["raw_object_uri"]
    assert row.kafka_topic == "gdelt.events"
    assert row.kafka_timestamp_text == "2026-10-08 17:15:34"
    assert row.ingested_at is not None


def test_ingested_at_is_constant_within_a_batch(bronze):
    assert bronze.select("ingested_at").distinct().count() == 1


def test_truncated_line_is_kept_and_flagged_by_field_count(spark):
    truncated = b"\t".join(LINES[0].split(b"\t")[:10])
    row = to_bronze(kafka_df(spark, [truncated])).first()
    assert row.field_count == 10
    assert row.raw_line == truncated.decode()
    assert row.global_event_id == LINES[0].split(b"\t")[0].decode()
    assert row.date_added is None and row.source_url is None


def test_missing_or_unreadable_headers_give_null_lineage(spark):
    no_headers = to_bronze(kafka_df(spark, LINES[:1], headers=None)).first()
    assert no_headers.src_file_url is None and no_headers.src_gdelt_batch_label_ts is None
    assert no_headers.field_count == 61

    bad_headers = {"gdelt_batch_label_ts": "pas une date"}
    bad = to_bronze(kafka_df(spark, LINES[:1], headers=bad_headers)).first()
    assert bad.src_gdelt_batch_label_ts is None


def test_legacy_slot_ts_header_still_fills_the_batch_label(spark):
    # Messages publiés avant le renommage, encore dans Kafka (rétention 7 jours).
    legacy = {k: v for k, v in HEADERS.items() if k != "gdelt_batch_label_ts"}
    legacy["slot_ts"] = HEADERS["gdelt_batch_label_ts"]
    df = to_bronze(kafka_df(spark, LINES[:1], headers=legacy))
    row = df.select(F.col("src_gdelt_batch_label_ts").cast("string").alias("text")).first()
    assert row.text == "2026-10-08 17:15:00"
