"""Streaming Kafka (gdelt.events) -> Iceberg (bronze.gdelt_events).

Usage :
    python -m streaming.bronze_gdelt_events                  # traite ce qui est en attente, puis s'arrête
    python -m streaming.bronze_gdelt_events --every 60       # tourne en continu, un lot par minute

Pas de dédoublonnage ici : le producteur est « au moins une fois », le bronze
garde tout, la silver dédoublonnera sur global_event_id. En revanche ce job
n'ajoute lui-même aucun doublon, même relancé après un crash :
- le checkpoint enregistre la plage d'offsets Kafka d'un lot AVANT de l'écrire ;
  au redémarrage, Spark rejoue exactement la même plage (il n'utilise pas les
  offsets de groupe de consommateurs de Kafka) ;
- le commit Iceberg porte l'identifiant de la requête et le numéro du lot ; si
  ce lot a déjà été commité, Iceberg l'ignore.
Perdre le checkpoint, c'est tout relire depuis le début du topic : il vit donc
dans MinIO, à côté des données.
"""

import argparse
import sys

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.gdelt_schema import BRONZE_COLUMNS, GDELT_EVENT_COLUMNS

TOPIC = "gdelt.events"
TABLE = "lakehouse.bronze.gdelt_events"


def _header(name: str) -> Column:
    # get(..., 0) rend NULL si l'en-tête est absent (pas d'erreur en mode ANSI).
    return F.expr(f"CAST(get(filter(headers, h -> h.key = '{name}'), 0).value AS STRING)")


def event_columns(raw_line: Column) -> list[Column]:
    """`raw_line`, `field_count` et les 61 colonnes STRING d'une ligne brute GDELT.

    Partagé par le flux Kafka et le chargement historique : une même ligne donne les mêmes
    colonnes quelle que soit son origine.
    """
    # Limite -1 : on garde les champs vides en fin de ligne.
    fields = F.split(raw_line, "\t", -1)
    return [
        raw_line.alias("raw_line"),
        F.size(fields).alias("field_count"),
        *[F.get(fields, F.lit(i)).alias(name) for i, name in enumerate(GDELT_EVENT_COLUMNS)],
    ]


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Enregistrements Kafka (avec en-têtes) -> lignes de bronze.gdelt_events."""
    return kafka_df.select(
        *event_columns(F.col("value").cast("string")),
        _header("source_url").alias("src_file_url"),
        # try_ : un en-tête illisible donne NULL, il n'arrête pas le flux.
        # Les messages publiés avant le renommage portent encore l'ancien en-tête
        # « slot_ts » (rétention 7 jours) : à retirer à partir du 2026-10-16.
        F.try_to_timestamp(F.coalesce(_header("gdelt_batch_label_ts"), _header("slot_ts"))).alias(
            "src_gdelt_batch_label_ts"
        ),
        F.try_to_timestamp(_header("fetched_at")).alias("src_fetched_at"),
        _header("raw_object_uri").alias("src_raw_object_uri"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        # Constant dans un micro-lot : un lot écrit dans une seule partition.
        F.current_timestamp().alias("ingested_at"),
        F.lit("stream").alias("ingestion_mode"),
        F.lit(None).cast("int").alias("src_line_no"),
    )


def to_bronze_backfill(lines: DataFrame) -> DataFrame:
    """Lignes d'un fichier GDELT lu hors Kafka -> lignes de bronze.gdelt_events.

    Entrée : `raw_line`, `src_file_url`, `src_gdelt_batch_label_ts`, `src_fetched_at`,
    `src_raw_object_uri`, `src_line_no`. Les colonnes Kafka restent NULL.
    """
    return lines.select(
        *event_columns(F.col("raw_line")),
        "src_file_url",
        "src_gdelt_batch_label_ts",
        "src_fetched_at",
        "src_raw_object_uri",
        F.lit(None).cast("string").alias("kafka_topic"),
        F.lit(None).cast("int").alias("kafka_partition"),
        F.lit(None).cast("bigint").alias("kafka_offset"),
        F.lit(None).cast("timestamp").alias("kafka_timestamp"),
        # Constant dans un jour chargé : un jour écrit dans une seule partition.
        F.current_timestamp().alias("ingested_at"),
        F.lit("backfill").alias("ingestion_mode"),
        F.col("src_line_no").cast("int").alias("src_line_no"),
    )


def ensure_table(spark: SparkSession, table: str = TABLE) -> None:
    columns = ",\n  ".join(f"{name} {dtype}" for name, dtype in BRONZE_COLUMNS)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    # Partition par jour d'ingestion (UTC), pas par date d'événement : un lot
    # n'écrit que dans la partition du jour, alors que sql_date s'étale sur des
    # mois (un même fichier contient des événements datés d'un an plus tôt).
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {columns}\n) "
        "USING iceberg PARTITIONED BY (days(ingested_at))"
    )


def run(
    spark: SparkSession,
    *,
    bootstrap: str,
    checkpoint: str,
    topic: str = TOPIC,
    table: str = TABLE,
    every_seconds: int | None = None,
) -> None:
    ensure_table(spark, table)
    _start(spark, bootstrap, checkpoint, topic, table, every_seconds).awaitTermination()


def _start(spark, bootstrap, checkpoint, topic, table, every_seconds):
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        # Ne vaut qu'au tout premier démarrage ; ensuite le checkpoint fait foi.
        .option("startingOffsets", "earliest")
        .option("includeHeaders", "true")
        .load()
    )
    writer = (
        to_bronze(source)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
    )
    if every_seconds:
        writer = writer.trigger(processingTime=f"{every_seconds} seconds")
    else:
        writer = writer.trigger(availableNow=True)
    return writer.toTable(table)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--every", type=int, metavar="SECONDES", help="tourner en continu (défaut : availableNow)"
    )
    args = parser.parse_args(argv)

    from ingestion.config import Settings
    from lakehouse.spark_session import build_spark_session

    settings = Settings.from_env()
    spark = build_spark_session("bronze-gdelt-events")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(
            spark,
            bootstrap=settings.kafka_bootstrap,
            checkpoint=f"s3a://{settings.bucket}/checkpoints/bronze_gdelt_events",
            every_seconds=args.every,
        )
        rows = spark.table(TABLE).count()
        print(f"{TABLE} : {rows} lignes")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
