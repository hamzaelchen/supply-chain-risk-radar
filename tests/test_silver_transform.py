"""Typage, quarantaine et dédoublonnage de silver.gdelt_events (sans infra).

Les lignes viennent d'un vrai cycle GDELT (tests/fixtures/gdelt). Les cas d'erreur sont de
vraies lignes dont UN champ est abîmé par le test : on vérifie la raison du rejet.
"""

from datetime import UTC, date, datetime

import pytest
from pyspark.sql import functions as F

from silver.gdelt_events import (
    QUARANTINE_COLUMNS,
    SILVER_COLUMNS,
    action_geo_usable,
    keep_first_seen,
    parse_events,
)
from streaming.bronze_gdelt_events import to_bronze
from streaming.gdelt_schema import GDELT_EVENT_COLUMNS

from .test_bronze_transform import LINES, kafka_df

pytestmark = pytest.mark.spark
SPARK_TYPES = {"STRING": "string", "INT": "int", "BIGINT": "bigint", "TIMESTAMP": "timestamp",
               "DOUBLE": "double", "BOOLEAN": "boolean", "DATE": "date"}  # fmt: skip
INDEX = {name: i for i, name in enumerate(GDELT_EVENT_COLUMNS)}


def damaged(line: bytes, **fields: str) -> bytes:
    """La même ligne, avec certains champs remplacés."""
    parts = line.split(b"\t")
    for name, value in fields.items():
        parts[INDEX[name]] = value.encode()
    return b"\t".join(parts)


def bronze_table(spark, lines):
    """Lignes bronze « relues d'une table » : le plan de to_bronze n'est pas réinjecté dans la silver."""
    bronze = to_bronze(kafka_df(spark, lines))
    return spark.createDataFrame(bronze.collect(), bronze.schema)


def parse(spark, lines):
    valid, rejected = parse_events(bronze_table(spark, lines))
    return valid.collect(), rejected.collect()


@pytest.fixture(scope="module")
def real(spark):
    valid, rejected = parse_events(bronze_table(spark, LINES))
    return valid.cache(), rejected.cache()


def test_every_real_line_of_the_fixture_is_valid(real):
    valid, rejected = real
    assert valid.count() == len(LINES) == 1775
    assert rejected.count() == 0


def test_output_columns_match_table_definitions(real):
    valid, rejected = real
    assert valid.dtypes == [(n, SPARK_TYPES[t]) for n, t in SILVER_COLUMNS]
    assert rejected.dtypes == [(n, SPARK_TYPES[t]) for n, t in QUARANTINE_COLUMNS]


def test_values_are_typed_from_the_raw_line(real):
    valid, _ = real
    raw = LINES[0].decode().split("\t")
    row = valid.filter(F.col("global_event_id") == int(raw[INDEX["global_event_id"]])).first()
    sql_date = raw[INDEX["sql_date"]]
    assert row.sql_date == date(int(sql_date[:4]), int(sql_date[4:6]), int(sql_date[6:]))
    assert row.goldstein_scale == float(raw[INDEX["goldstein_scale"]])
    assert row.num_mentions == int(raw[INDEX["num_mentions"]])
    assert row.avg_tone == float(raw[INDEX["avg_tone"]])
    assert row.is_root_event is (raw[INDEX["is_root_event"]] == "1")
    assert row.event_code == raw[INDEX["event_code"]]
    assert row.source_url == raw[INDEX["source_url"]]


def test_date_added_is_a_utc_timestamp(real):
    valid, _ = real
    texts = {r.t for r in valid.select(F.col("date_added").cast("string").alias("t")).collect()}
    assert texts == {"2026-10-08 17:15:00"}


def test_cameo_codes_keep_their_leading_zero(real):
    valid, _ = real
    assert valid.filter("event_root_code = '01'").count() > 0
    assert valid.filter("length(event_root_code) = 2").count() == valid.count()


def test_empty_optional_values_become_null_not_empty_strings(real):
    valid, _ = real
    assert valid.filter("actor1_code = '' OR action_geo_full_name = ''").count() == 0
    assert valid.filter("actor1_code IS NULL").count() > 0


def test_lineage_and_hash_are_kept(real):
    valid, _ = real
    row = valid.first()
    assert row.kafka_topic == "gdelt.events" and row.kafka_offset is not None
    assert row.src_file_url.endswith("20261008171500.export.CSV.zip")
    assert len(row.row_hash) == 64


BAD_VALUES = [
    ({"goldstein_scale": "abc"}, "goldstein_scale='abc' : double invalide"),
    ({"goldstein_scale": "10.5"}, "goldstein_scale='10.5' : hors plage"),
    ({"avg_tone": "1e3"}, "avg_tone='1e3' : double invalide"),
    ({"num_mentions": "-3"}, "num_mentions='-3' : int invalide"),
    ({"num_mentions": "12.5"}, "num_mentions='12.5' : int invalide"),
    ({"num_articles": "99999999999"}, "num_articles='99999999999' : int invalide"),
    ({"quad_class": "5"}, "quad_class='5' : hors plage"),
    ({"is_root_event": "2"}, "is_root_event='2' : boolean invalide"),
    ({"sql_date": "20261340"}, "sql_date='20261340' : date invalide"),
    ({"sql_date": "2026-10-08"}, "sql_date='2026-10-08' : date invalide"),
    ({"date_added": "20261008251500"}, "date_added='20261008251500' : timestamp invalide"),
    ({"action_geo_lat": "91.2"}, "action_geo_lat='91.2' : hors plage"),
    ({"action_geo_long": "-181"}, "action_geo_long='-181' : hors plage"),
    ({"action_geo_lat": "N48.8"}, "action_geo_lat='N48.8' : double invalide"),
    ({"action_geo_type": "9"}, "action_geo_type='9' : hors plage"),
    ({"event_code": "ABC"}, "event_code='ABC' : format inattendu"),
    ({"global_event_id": "12x"}, "global_event_id='12x' : bigint invalide"),
    ({"global_event_id": ""}, "global_event_id : valeur obligatoire absente"),
    ({"goldstein_scale": ""}, "goldstein_scale : valeur obligatoire absente"),
    ({"source_url": ""}, "source_url : valeur obligatoire absente"),
]
TRUNCATED = [10, 60, 62]


@pytest.fixture(scope="module")
def outcomes(spark):
    """Tous les cas d'erreur passent par Spark en une seule fois (une requête coûte ~3 s)."""
    lines = {}
    for fields, _ in BAD_VALUES:
        lines[tuple(sorted(fields.items()))] = damaged(LINES[0], **fields)
    for count in TRUNCATED:
        parts = LINES[0].split(b"\t")
        lines[count] = b"\t".join((parts + [b"x"] * 5)[:count])
    valid, rejected = parse(spark, list(lines.values()))
    by_line = {r.raw_line: r for r in rejected}
    assert valid == []  # chaque ligne abîmée doit être rejetée
    return {key: by_line[line.decode()] for key, line in lines.items()}


@pytest.mark.parametrize(("fields", "expected"), BAD_VALUES, ids=[e for _, e in BAD_VALUES])
def test_an_impossible_value_sends_the_line_to_quarantine_with_its_reason(
    outcomes, fields, expected
):
    assert outcomes[tuple(sorted(fields.items()))].reason == expected


def test_a_leap_day_is_a_valid_date(spark):
    valid, rejected = parse(spark, [damaged(LINES[0], sql_date="20240229")])
    assert rejected == [] and valid[0].sql_date == date(2024, 2, 29)


def test_several_problems_are_all_reported(spark):
    _, rejected = parse(spark, [damaged(LINES[0], goldstein_scale="x", quad_class="9")])
    assert rejected[0].reason.count("; ") == 1
    assert "goldstein_scale" in rejected[0].reason and "quad_class" in rejected[0].reason


@pytest.mark.parametrize("count", TRUNCATED)
def test_a_line_without_61_fields_is_quarantined_without_typing(outcomes, count):
    assert outcomes[count].reason == f"field_count={count} (attendu 61)"
    assert outcomes[count].field_count == count


def test_no_line_is_lost_between_valid_and_quarantine(spark):
    lines = [
        LINES[0],
        damaged(LINES[1], goldstein_scale="abc"),
        LINES[2][:50],
        LINES[3],
    ]
    valid, rejected = parse(spark, lines)
    assert len(valid) + len(rejected) == len(lines) == 4
    assert all(r.kafka_topic == "gdelt.events" for r in valid + rejected)  # lignage des deux côtés


def test_half_coordinate_pair_is_kept_as_is_but_not_usable(spark):
    # Cas réel observé : latitude vide, longitude « 42 », pays GG, type 0.
    line = damaged(LINES[0], action_geo_type="0", action_geo_lat="", action_geo_long="42")
    valid, rejected = parse(spark, [line])
    assert rejected == []
    assert valid[0].action_geo_lat is None and valid[0].action_geo_long == 42.0
    df = spark.createDataFrame(valid, parse_events(bronze_table(spark, [line]))[0].schema)
    assert df.select(action_geo_usable().alias("u")).first().u is False


def test_usable_geo_needs_both_coordinates_a_place_and_not_null_island(spark):
    schema = "action_geo_lat double, action_geo_long double, action_geo_type int"
    df = spark.createDataFrame(
        [(48.8, 2.3, 4), (48.8, None, 4), (None, 2.3, 4), (48.8, 2.3, 0), (0.0, 0.0, 4)], schema
    )
    flags = [r.u for r in df.select(action_geo_usable().alias("u")).collect()]
    assert flags == [True, False, False, False, False]


def test_duplicates_keep_the_first_arrived_row(spark):
    original_mentions = int(LINES[0].decode().split("\t")[INDEX["num_mentions"]])
    later_copy = damaged(LINES[0], num_mentions="999")
    # Même instant d'ingestion : l'ordre Kafka (partition, offset) départage. La copie
    # modifiée est à l'offset 1 de la partition 1, après l'originale (partition 0).
    valid, _ = parse_events(bronze_table(spark, [LINES[0], later_copy, LINES[1]]))
    kept = keep_first_seen(valid).collect()
    assert len(kept) == 2
    by_id = {r.global_event_id: r for r in kept}
    assert by_id[int(LINES[0].split(b"\t")[0])].num_mentions == original_mentions
    assert valid.select("row_hash").distinct().count() == 3  # la copie a bien un autre contenu


def test_limit_to_next_batches_keeps_the_oldest_ingestion_instants(spark):
    from silver.gdelt_events import limit_to_next_batches

    rows = [
        (i, datetime(2026, 10, 9, 8, minute, tzinfo=UTC)) for i in range(6) for minute in [i // 2]
    ]
    df = spark.createDataFrame(rows, "id int, ingested_at timestamp")  # 3 instants, 2 lignes chacun
    assert df.select("ingested_at").distinct().count() == 3
    assert sorted(r.id for r in limit_to_next_batches(df, "ingested_at", 2).collect()) == [
        0,
        1,
        2,
        3,
    ]
    assert limit_to_next_batches(df, "ingested_at", None).count() == 6
    assert limit_to_next_batches(df, "ingested_at", 10).count() == 6
    assert limit_to_next_batches(df.limit(0), "ingested_at", 2).count() == 0


def test_a_backfill_row_keeps_its_identity_and_mode_in_silver_and_quarantine(spark):
    from .test_bronze_transform import backfill_df

    lines = [LINES[0], damaged(LINES[1], goldstein_scale="abc")]
    bronze = backfill_df(spark, lines)
    bronze = spark.createDataFrame(bronze.collect(), bronze.schema)
    valid, rejected = parse_events(bronze)
    (ok,), (bad,) = valid.collect(), rejected.collect()
    assert (ok.ingestion_mode, ok.src_line_no, ok.kafka_offset) == ("backfill", 1, None)
    assert (bad.ingestion_mode, bad.src_line_no, bad.src_file_url) == (
        "backfill",
        2,
        ok.src_file_url,
    )


def test_stream_rows_without_the_mode_column_value_become_stream_in_silver(spark):
    bronze = bronze_table(spark, LINES[:2])
    bronze = bronze.withColumn("ingestion_mode", F.lit(None).cast("string"))  # ligne ancienne
    valid, _ = parse_events(bronze)
    assert {r.ingestion_mode for r in valid.collect()} == {"stream"}
