"""Bronze -> silver : bronze.gdelt_events -> silver.gdelt_events (+ quarantaine).

Usage :
    python -m silver.gdelt_events        # traite les lignes bronze arrivées depuis le dernier passage

Ce que fait la silver, et que le bronze ne fait pas :
- typage strict : une valeur qui n'est pas du type attendu, ou hors de sa plage, envoie la
  ligne en quarantaine avec la raison. Jamais de NULL à la place d'une conversion ratée.
  Seule l'absence est normalisée : en GDELT une chaîne vide veut dire « non renseigné », elle
  devient NULL pour les colonnes facultatives (et motive une quarantaine pour les obligatoires) ;
- une ligne qui n'a pas 61 champs est mise en quarantaine, sans tentative de typage ;
- dédoublonnage sur global_event_id : le premier arrivé gagne (un événement GDELT est un fait,
  le producteur « au moins une fois » ne fait que le répéter). MERGE INTO sans clause UPDATE.

Lecture incrémentale : filtre `ingested_at > curseur`, le curseur étant stocké en propriété de
la table silver (voir CLAUDE.md, décision du 2026-10-09). Le job est idempotent : relancé, il ne
trouve rien de nouveau ; interrompu entre le MERGE et la mise à jour du curseur, il rejoue la même
fenêtre et le MERGE n'insère rien de plus.
"""

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ops import pipeline_runs
from streaming.gdelt_schema import GDELT_EVENT_COLUMNS

BRONZE = "lakehouse.bronze.gdelt_events"
SILVER = "lakehouse.silver.gdelt_events"
QUARANTINE = "lakehouse.silver.gdelt_events_quarantine"
CURSOR_PROPERTY = "scrr.bronze_ingested_at_cursor_us"  # microsecondes depuis l'epoch (UTC)
EXPECTED_FIELDS = 61
PIPELINE = "silver.gdelt_events"


@dataclass(frozen=True)
class Rule:
    """Type attendu d'une colonne GDELT. Sans règle, la colonne reste STRING facultative."""

    dtype: str  # bigint, int, double, boolean, date, timestamp, string
    required: bool = False
    lo: float | None = None
    hi: float | None = None


_GEO = [f"{p}_geo" for p in ("actor1", "actor2", "action")]
RULES: dict[str, Rule] = {
    "global_event_id": Rule("bigint", required=True),
    "sql_date": Rule("date", required=True),
    "month_year": Rule("int", required=True),
    "year": Rule("int", required=True),
    "fraction_date": Rule("double", required=True),
    "is_root_event": Rule("boolean", required=True),
    # Codes CAMEO : texte, car « 010 » et « 10 » sont deux codes différents.
    "event_code": Rule("string", required=True),
    "event_base_code": Rule("string", required=True),
    "event_root_code": Rule("string", required=True),
    "quad_class": Rule("int", required=True, lo=1, hi=4),
    "goldstein_scale": Rule("double", required=True, lo=-10, hi=10),
    "num_mentions": Rule("int", required=True, lo=0),
    "num_sources": Rule("int", required=True, lo=0),
    "num_articles": Rule("int", required=True, lo=0),
    "avg_tone": Rule("double", required=True, lo=-100, hi=100),
    # 0 = aucun lieu, 1 pays, 2 État US, 3 ville US, 4 ville du monde, 5 région du monde
    **{f"{g}_type": Rule("int", required=True, lo=0, hi=5) for g in _GEO},
    **{f"{g}_lat": Rule("double", lo=-90, hi=90) for g in _GEO},
    **{f"{g}_long": Rule("double", lo=-180, hi=180) for g in _GEO},
    "date_added": Rule("timestamp", required=True),
    "source_url": Rule("string", required=True),
}
_PATTERNS = {
    "bigint": r"^\d{1,18}$",
    "int": r"^\d{1,9}$",
    "double": r"^-?\d+(\.\d+)?$",
    "boolean": r"^[01]$",
    "date": r"^\d{8}$",
    "timestamp": r"^\d{14}$",
}
_FORMATS = {"date": "yyyyMMdd", "timestamp": "yyyyMMddHHmmss"}
# Formats plus stricts que le typage seul : un code d'événement a 2 à 4 chiffres.
_STRING_PATTERNS = {
    "event_code": r"^\d{2,4}$",
    "event_base_code": r"^\d{2,4}$",
    "event_root_code": r"^\d{2}$",
}
_SPARK_TYPES = {
    "bigint": "BIGINT", "int": "INT", "double": "DOUBLE", "boolean": "BOOLEAN",
    "date": "DATE", "timestamp": "TIMESTAMP", "string": "STRING",
}  # fmt: skip

# Lignage conservé tel quel depuis le bronze (nom silver, nom bronze, type).
LINEAGE = [
    ("src_file_url", "src_file_url", "STRING"),
    ("src_gdelt_batch_label_ts", "src_gdelt_batch_label_ts", "TIMESTAMP"),
    ("src_fetched_at", "src_fetched_at", "TIMESTAMP"),
    ("kafka_topic", "kafka_topic", "STRING"),
    ("kafka_partition", "kafka_partition", "INT"),
    ("kafka_offset", "kafka_offset", "BIGINT"),
    ("bronze_ingested_at", "ingested_at", "TIMESTAMP"),
]
# Ajoutées en fin de table (voir streaming/gdelt_schema.py) : mode d'ingestion et numéro de ligne.
# Une ligne silver écrite avant leur ajout les a à NULL ; ingestion_mode NULL se lit « stream ».
INGESTION_COLUMNS = [("ingestion_mode", "STRING"), ("src_line_no", "INT")]
SILVER_COLUMNS = [
    *[(c, _SPARK_TYPES[RULES.get(c, Rule("string")).dtype]) for c in GDELT_EVENT_COLUMNS],
    *[(silver, dtype) for silver, _, dtype in LINEAGE],
    (
        "row_hash",
        "STRING",
    ),  # sha256 de la ligne brute : détecte un « doublon » au contenu différent
    ("silver_loaded_at", "TIMESTAMP"),
    *INGESTION_COLUMNS,
]
QUARANTINE_COLUMNS = [
    ("raw_line", "STRING"),
    ("field_count", "INT"),
    ("reason", "STRING"),
    *[(silver, dtype) for silver, _, dtype in LINEAGE],
    ("quarantined_at", "TIMESTAMP"),
    *INGESTION_COLUMNS,
]


def _shorten(value: Column) -> Column:
    return F.when(F.length(value) > 40, F.concat(F.substring(value, 1, 40), F.lit("…"))).otherwise(
        value
    )


def _parse(column: str, rule: Rule) -> tuple[Column, Column | None]:
    """Valeur typée (NULL si absente ou invalide) et message d'erreur (NULL si tout va bien).

    La valeur typée ne dépend pas de l'erreur : seules les lignes sans aucune erreur arrivent
    en silver, une valeur hors plage n'y figure donc jamais. Garder les deux indépendantes
    évite d'imbriquer les expressions (61 colonnes), ce qui ralentit et fait déborder Catalyst.
    """
    raw = F.col(column)
    empty = raw == ""
    shown = F.concat(F.lit(f"{column}='"), _shorten(raw), F.lit("'"))

    if rule.dtype == "string":
        pattern = _STRING_PATTERNS.get(column)
        typed = F.when(~empty, raw)
        error = (
            None
            if pattern is None
            else F.when(~empty & ~raw.rlike(pattern), F.concat(shown, F.lit(" : format inattendu")))
        )
    else:
        if rule.dtype in _FORMATS:
            parsed = F.try_to_timestamp(raw, F.lit(_FORMATS[rule.dtype]))
            parsed = parsed.cast("date") if rule.dtype == "date" else parsed
        elif rule.dtype == "boolean":
            parsed = raw == "1"
        else:
            parsed = raw.try_cast(rule.dtype)
        typed = F.when(raw.rlike(_PATTERNS[rule.dtype]), parsed)
        error = F.when(~empty & typed.isNull(), F.concat(shown, F.lit(f" : {rule.dtype} invalide")))
        if rule.lo is not None or rule.hi is not None:
            lo = F.lit(rule.lo if rule.lo is not None else float("-inf"))
            hi = F.lit(rule.hi if rule.hi is not None else float("inf"))
            error = error.when(
                typed.isNotNull() & ((typed < lo) | (typed > hi)),
                F.concat(shown, F.lit(" : hors plage")),
            )
    if rule.required:
        missing = F.when(empty, F.lit(f"{column} : valeur obligatoire absente"))
        error = missing if error is None else F.coalesce(error, missing)
    return typed, error


def parse_events(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Lignes bronze -> (lignes silver typées, lignes en quarantaine). Aucune ligne perdue."""
    typed_cols, errors = {}, []
    for column in GDELT_EVENT_COLUMNS:
        typed, error = _parse(column, RULES.get(column, Rule("string")))
        typed_cols[column] = typed
        if error is not None:
            errors.append(error)

    shape_ok = F.col("field_count") == EXPECTED_FIELDS
    problems = F.array_compact(F.array(*errors))
    reason = (
        F.when(
            ~shape_ok,
            F.concat(F.lit("field_count="), F.col("field_count").cast("string"), F.lit(f" (attendu {EXPECTED_FIELDS})")),
        )
        .when(F.size(problems) > 0, F.array_join(problems, "; "))
    )  # fmt: skip
    lineage = [F.col(b).alias(s) for s, b, _ in LINEAGE]
    # NULL = ligne écrite avant l'ajout de la colonne, donc issue du flux.
    ingestion = [
        F.coalesce(F.col("ingestion_mode"), F.lit("stream")).alias("ingestion_mode"),
        F.col("src_line_no"),
    ]

    flagged = bronze.withColumn("_reason", reason)
    valid = flagged.filter(F.col("_reason").isNull()).select(
        *[typed_cols[c].alias(c) for c in GDELT_EVENT_COLUMNS],
        *lineage,
        F.sha2(F.col("raw_line"), 256).alias("row_hash"),
        F.current_timestamp().alias("silver_loaded_at"),
        *ingestion,
    )
    quarantine = flagged.filter(F.col("_reason").isNotNull()).select(
        "raw_line",
        "field_count",
        F.col("_reason").alias("reason"),
        *lineage,
        F.current_timestamp().alias("quarantined_at"),
        *ingestion,
    )
    return valid, quarantine


def keep_first_seen(valid: DataFrame) -> DataFrame:
    """Un seul exemplaire par global_event_id : le premier arrivé dans le bronze.

    Départage : instant d'ingestion, puis coordonnées Kafka (flux), puis fichier et numéro de ligne
    (backfill, qui n'a pas de coordonnées Kafka).
    """
    from pyspark.sql import Window

    order = Window.partitionBy("global_event_id").orderBy(
        "bronze_ingested_at", "kafka_partition", "kafka_offset", "src_file_url", "src_line_no"
    )
    return valid.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").drop("_rank")


def action_geo_usable() -> Column:
    """ActionGeo exploitable pour une carte ou un calcul de distance.

    Latitude ET longitude présentes (une paire incomplète existe dans les données réelles),
    un lieu déclaré (type 0 = aucun) et pas le point (0, 0). Les plages sont déjà garanties
    par le typage.
    """
    lat, lon = F.col("action_geo_lat"), F.col("action_geo_long")
    return (
        lat.isNotNull()
        & lon.isNotNull()
        & (F.col("action_geo_type") > 0)
        & ~((lat == 0) & (lon == 0))
    )


def ensure_tables(spark: SparkSession, silver: str = SILVER, quarantine: str = QUARANTINE) -> None:
    def ddl(table, columns, partition):
        body = ",\n  ".join(f"{name} {dtype}" for name, dtype in columns)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} (\n  {body}\n) USING iceberg "
            f"PARTITIONED BY ({partition})"
        )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {silver.rsplit('.', 1)[0]}")
    # Jour du lot GDELT pour la silver : c'est ce que le gold lira (« derniers jours »).
    ddl(silver, SILVER_COLUMNS, "days(date_added)")
    ddl(quarantine, QUARANTINE_COLUMNS, "days(quarantined_at)")


def read_cursor(spark: SparkSession, table: str, prop: str = CURSOR_PROPERTY) -> int | None:
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    value = props.get(prop)
    return int(value) if value is not None else None


def write_cursor(spark: SparkSession, table: str, micros: int, prop: str = CURSOR_PROPERTY) -> None:
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{prop}' = '{micros}')")


@dataclass
class RunStats:
    rows_read: int = 0
    quarantined: int = 0
    quarantine_reasons: dict[str, int] = field(default_factory=dict)
    valid: int = 0
    duplicates_in_batch: int = 0
    already_in_silver: int = 0
    conflicting_duplicates: int = 0
    inserted: int = 0
    action_geo_usable: int = 0  # parmi les événements valides lus (hors doublons)
    action_geo_total: int = 0
    cursor_before_us: int | None = None
    cursor_after_us: int | None = None

    @property
    def duplicates_eliminated(self) -> int:
        return self.duplicates_in_batch + self.already_in_silver


def limit_to_next_batches(df: DataFrame, ts_column: str, max_batches: int | None) -> DataFrame:
    """Ne garde que les `max_batches` plus anciennes valeurs distinctes de `ts_column`.

    Un « lot » est un instant d'ingestion : un micro-lot du flux, ou un jour de backfill. Le
    curseur avance donc de N lots par passage, ce qui borne la mémoire d'un gros rattrapage.
    """
    if not max_batches:
        return df
    micros = F.unix_micros(ts_column)
    upper = df.select(micros.alias("t")).distinct().orderBy("t").limit(max_batches)
    newest = upper.agg(F.max("t")).first()[0]
    return df if newest is None else df.filter(micros <= newest)


def run(
    spark: SparkSession,
    *,
    bronze: str = BRONZE,
    silver: str = SILVER,
    quarantine: str = QUARANTINE,
    max_batches: int | None = None,
) -> RunStats:
    ensure_tables(spark, silver, quarantine)
    cursor = read_cursor(spark, silver)
    window = spark.table(bronze)
    if cursor is not None:
        window = window.filter(F.unix_micros("ingested_at") > cursor)
    window = limit_to_next_batches(window, "ingested_at", max_batches).cache()
    stats = RunStats(cursor_before_us=cursor, cursor_after_us=cursor)
    try:
        # Le curseur est le plus récent ingested_at des lignes LUES, jamais l'heure d'exécution :
        # un lot bronze commité pendant le passage (donc plus récent) sera lu au suivant.
        newest = window.agg(F.max(F.unix_micros("ingested_at"))).first()[0]
        if newest is None:
            return stats  # rien de nouveau : le job est un no-op

        valid, rejected = parse_events(window)
        valid, rejected = valid.cache(), rejected.cache()
        stats.rows_read = window.count()
        stats.quarantined = rejected.count()
        stats.valid = valid.count()
        stats.quarantine_reasons = {
            r.kind: r["count"]
            for r in rejected.groupBy(
                F.regexp_extract("reason", r"^([a-z_0-9]+)[=' :]", 1).alias("kind")
            ).count().collect()
        }  # fmt: skip

        ids = valid.select("global_event_id").distinct().count()
        stats.duplicates_in_batch = stats.valid - ids
        in_batch = (
            valid.groupBy("global_event_id").agg(F.countDistinct("row_hash").alias("n"))
            .filter("n > 1").count()
        )  # fmt: skip
        first = keep_first_seen(valid).cache()
        geo = first.agg(
            F.count("*").alias("total"), F.sum(action_geo_usable().cast("int")).alias("usable")
        ).first()
        stats.action_geo_total, stats.action_geo_usable = geo.total, geo.usable or 0
        known = spark.table(silver).select("global_event_id", F.col("row_hash").alias("known_hash"))
        overlap = first.join(known, "global_event_id")
        stats.already_in_silver = overlap.count()
        vs_silver = overlap.filter("row_hash <> known_hash").count()
        stats.conflicting_duplicates = in_batch + vs_silver
        stats.inserted = ids - stats.already_in_silver

        # Quarantaine d'abord : si la suite échoue, la relance rejoue la même fenêtre
        # et chaque MERGE n'insère que ce qui manque.
        rejected.createOrReplaceTempView("_scrr_rejected")
        spark.sql(
            f"MERGE INTO {quarantine} t USING _scrr_rejected s "
            # Identité d'une ligne : coordonnées Kafka (flux) OU fichier + numéro de ligne
            # (backfill). <=> : égalité qui traite NULL = NULL comme vrai.
            "ON t.kafka_topic <=> s.kafka_topic AND t.kafka_partition <=> s.kafka_partition "
            "AND t.kafka_offset <=> s.kafka_offset AND t.src_file_url <=> s.src_file_url "
            "AND t.src_line_no <=> s.src_line_no WHEN NOT MATCHED THEN INSERT *"
        )
        first.createOrReplaceTempView("_scrr_first")
        spark.sql(
            f"MERGE INTO {silver} t USING _scrr_first s "
            "ON t.global_event_id = s.global_event_id WHEN NOT MATCHED THEN INSERT *"
        )
        write_cursor(spark, silver, newest)
        stats.cursor_after_us = newest
        return stats
    finally:
        spark.catalog.clearCache()


def run_and_record(
    spark: SparkSession,
    *,
    bronze: str = BRONZE,
    silver: str = SILVER,
    quarantine: str = QUARANTINE,
    runs_table: str = pipeline_runs.TABLE,
    max_batches: int | None = None,
) -> RunStats:
    """`run` + une ligne dans ops.pipeline_runs, que l'exécution réussisse ou échoue."""
    ensure_tables(spark, silver, quarantine)
    started = datetime.now(UTC)
    clock = time.monotonic()
    cursor_before = read_cursor(spark, silver)

    def finished() -> datetime:
        # Durée mesurée par l'horloge monotone, pas par la différence de deux heures murales.
        return started + timedelta(seconds=time.monotonic() - clock)

    try:
        stats = run(
            spark, bronze=bronze, silver=silver, quarantine=quarantine, max_batches=max_batches
        )
    except Exception as exc:
        pipeline_runs.record(
            spark,
            pipeline_runs.PipelineRun(
                pipeline=PIPELINE, started_at=started, finished_at=finished(), status="failed",
                error=repr(exc)[:2000],
                cursor_before=pipeline_runs.micros_to_datetime(cursor_before),
                cursor_after=pipeline_runs.micros_to_datetime(read_cursor(spark, silver)),
            ),
            runs_table,
        )  # fmt: skip
        raise
    usable_pct = (
        100 * stats.action_geo_usable / stats.action_geo_total if stats.action_geo_total else None
    )
    pipeline_runs.record(
        spark,
        pipeline_runs.PipelineRun(
            pipeline=PIPELINE, started_at=started, finished_at=finished(), status="succeeded",
            rows_read=stats.rows_read, rows_inserted=stats.inserted,
            rows_quarantined=stats.quarantined,
            duplicates_eliminated=stats.duplicates_eliminated,
            conflicting_duplicates=stats.conflicting_duplicates,
            action_geo_usable_pct=usable_pct,
            cursor_before=pipeline_runs.micros_to_datetime(stats.cursor_before_us),
            cursor_after=pipeline_runs.micros_to_datetime(stats.cursor_after_us),
        ),
        runs_table,
    )  # fmt: skip
    return stats


def geo_report(spark: SparkSession, silver: str = SILVER) -> dict[str, int]:
    row = spark.table(silver).agg(
        F.count("*").alias("total"),
        F.sum(action_geo_usable().cast("int")).alias("usable"),
        F.sum((F.col("action_geo_lat").isNull() != F.col("action_geo_long").isNull()).cast("int")).alias("half_pair"),
    ).first()  # fmt: skip
    return {"total": row.total, "usable": row.usable or 0, "half_pair": row.half_pair or 0}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--max-batches", type=int, metavar="N",
        help="ne traiter que les N plus anciens lots d'ingestion (un jour de backfill = un lot)",
    )  # fmt: skip
    parser.add_argument(
        "--until-caught-up", action="store_true",
        help="repasser jusqu'à ce qu'il n'y ait plus rien de nouveau (avec --max-batches)",
    )  # fmt: skip
    args = parser.parse_args(argv)
    from lakehouse.spark_session import build_spark_session

    spark = build_spark_session("silver-gdelt-events")
    spark.sparkContext.setLogLevel("WARN")
    try:
        started = datetime.now(UTC)
        while True:
            stats = run_and_record(spark, max_batches=args.max_batches)
            print(f"{SILVER} : {stats}", flush=True)
            if not (args.until_caught_up and stats.rows_read):
                break
        geo = geo_report(spark)
        pct = 100 * geo["usable"] / geo["total"] if geo["total"] else 0
        print(
            f"ActionGeo exploitable : {geo['usable']}/{geo['total']} ({pct:.2f} %), paires incomplètes : {geo['half_pair']}"
        )
        print(f"durée : {(datetime.now(UTC) - started).total_seconds():.0f} s")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
