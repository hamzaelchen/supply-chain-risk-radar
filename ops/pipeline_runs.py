"""ops.pipeline_runs : une ligne par exécution d'un job du pipeline (réussie ou non).

Sert à répondre, avec une requête SQL, à « le silver a-t-il tourné, combien a-t-il lu, que
vaut le taux de coordonnées exploitables aujourd'hui par rapport à hier ? ». Les colonnes sont
génériques : les prochains jobs (PortWatch, gold) y écriront aussi, en laissant NULL ce qui ne
les concerne pas.
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

TABLE = "lakehouse.ops.pipeline_runs"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

COLUMNS = [
    ("run_id", "STRING"),
    ("pipeline", "STRING"),
    ("started_at", "TIMESTAMP"),
    ("finished_at", "TIMESTAMP"),
    ("duration_seconds", "DOUBLE"),
    ("status", "STRING"),  # succeeded | failed
    ("error", "STRING"),
    ("rows_read", "BIGINT"),
    ("rows_inserted", "BIGINT"),
    ("rows_quarantined", "BIGINT"),
    ("duplicates_eliminated", "BIGINT"),
    ("conflicting_duplicates", "BIGINT"),  # même identifiant, contenu différent
    # Part des lignes lues pendant cette exécution dont ActionGeo est exploitable ; NULL si rien lu.
    ("action_geo_usable_pct", "DOUBLE"),
    ("cursor_before", "TIMESTAMP"),  # max(ingested_at) bronze déjà traité avant / après
    ("cursor_after", "TIMESTAMP"),
]


@dataclass(frozen=True)
class PipelineRun:
    pipeline: str
    started_at: datetime
    finished_at: datetime
    status: str
    error: str | None = None
    rows_read: int | None = None
    rows_inserted: int | None = None
    rows_quarantined: int | None = None
    duplicates_eliminated: int | None = None
    conflicting_duplicates: int | None = None
    action_geo_usable_pct: float | None = None
    cursor_before: datetime | None = None
    cursor_after: datetime | None = None
    run_id: str = ""

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()


def micros_to_datetime(micros: int | None) -> datetime | None:
    """Microsecondes depuis l'epoch -> datetime UTC, sans passer par un flottant."""
    return None if micros is None else _EPOCH + timedelta(microseconds=micros)


def ensure_table(spark: SparkSession, table: str = TABLE) -> None:
    body = ",\n  ".join(f"{name} {dtype}" for name, dtype in COLUMNS)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (\n  {body}\n) USING iceberg")


def record(spark: SparkSession, run: PipelineRun, table: str = TABLE) -> str:
    """Ajoute la ligne d'une exécution. Rend son run_id."""
    ensure_table(spark, table)
    run_id = run.run_id or uuid.uuid4().hex
    row = (
        run_id, run.pipeline, run.started_at, run.finished_at, run.duration_seconds, run.status,
        run.error, run.rows_read, run.rows_inserted, run.rows_quarantined,
        run.duplicates_eliminated, run.conflicting_duplicates, run.action_geo_usable_pct,
        run.cursor_before, run.cursor_after,
    )  # fmt: skip
    schema = ", ".join(f"{name} {dtype}" for name, dtype in COLUMNS)
    spark.createDataFrame([row], schema).writeTo(table).append()
    return run_id
