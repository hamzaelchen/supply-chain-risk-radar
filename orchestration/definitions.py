"""Orchestration minimale (Dagster) : un job qui enchaîne tout le pipeline.

    producteur -> bronze -> silver -> proximité -> gold (dbt) -> export

Lancer l'interface :
    DAGSTER_HOME=$PWD/orchestration/.dagster .venv/bin/dagster dev -f orchestration/definitions.py

Le job fait exactement ce que font les commandes `make` : chaque étape est un sous-processus qui
appelle le même module que la ligne de commande, sans logique dupliquée. Dagster apporte l'ordre,
l'arrêt à la première erreur, l'historique des exécutions et le planning.

Planning : toutes les 15 minutes (un lot GDELT), **NON ACTIVÉ par défaut** (STOPPED). Pour l'activer,
dans l'interface (Automation) ou : `dagster schedule start pipeline_every_15_minutes`.
Une seule exécution à la fois (voir orchestration/dagster.yaml) : une exécution plus lente que
15 minutes fait attendre la suivante, elle ne la double pas. Ne JAMAIS lancer en parallèle de
`make backfill` (curseur de la silver sur `ingested_at`).
"""

import subprocess
import sys
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    Failure,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ScheduleDefinition,
    job,
    op,
)

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
TAIL_LINES = 25  # lignes de sortie gardées dans le message d'erreur


def run_command(context: OpExecutionContext, argv: list[str], cwd: Path = ROOT) -> None:
    """Exécute une commande, journalise sa sortie, lève Failure si elle échoue."""
    context.log.info("$ %s", " ".join(argv))
    result = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, check=False)
    output = (result.stdout or "") + (result.stderr or "")
    for line in output.splitlines()[-TAIL_LINES:]:
        context.log.info(line)
    if result.returncode != 0:
        tail = "\n".join(output.splitlines()[-TAIL_LINES:])
        raise Failure(
            description=f"{' '.join(argv)} a échoué (code {result.returncode})",
            metadata={"sortie": tail},
        )


@op(
    out=Out(Nothing),
    description="Télécharge les lots GDELT manquants, archive les zips, publie dans Kafka.",
)
def produce(context: OpExecutionContext):
    run_command(context, [PYTHON, "-m", "ingestion.gdelt_producer"])


@op(
    ins={"after": In(Nothing)},
    out=Out(Nothing),
    description="Kafka -> bronze.gdelt_events (Spark Structured Streaming, availableNow).",
)
def bronze(context: OpExecutionContext):
    run_command(context, [PYTHON, "-m", "streaming.bronze_gdelt_events"])


@op(
    ins={"after": In(Nothing)},
    out=Out(Nothing),
    description="bronze -> silver.gdelt_events (typage, quarantaine, dédoublonnage), incrémental.",
)
def silver(context: OpExecutionContext):
    run_command(
        context, [PYTHON, "-m", "silver.gdelt_events", "--max-batches", "20", "--until-caught-up"]
    )


@op(
    ins={"after": In(Nothing)},
    out=Out(Nothing),
    description="Événements proches des fournisseurs et des détroits, incrémental.",
)
def proximity(context: OpExecutionContext):
    run_command(
        context, [PYTHON, "-m", "silver.proximity", "--max-batches", "40", "--until-caught-up"]
    )


@op(
    ins={"after": In(Nothing)},
    out=Out(Nothing),
    description="Score v1 : dbt seed, run et tests sur la silver (DuckDB lit Iceberg).",
)
def gold(context: OpExecutionContext):
    dbt = str(Path(PYTHON).parent / "dbt")
    for step in ("seed", "run", "test"):
        run_command(context, [dbt, step, "--profiles-dir", "."], cwd=ROOT / "transform")


@op(
    ins={"after": In(Nothing)},
    out=Out(Nothing),
    description="Export du contrat du site (meta.json + 4 Parquet), validé.",
)
def export(context: OpExecutionContext):
    run_command(
        context,
        [
            PYTHON,
            "transform/export.py",
            "--db",
            "transform/target/gold.duckdb",
            "--out",
            "build/gold/data",
        ],
    )


@job(description="Pipeline complet GDELT -> score -> export, un lot de 15 minutes à la fois.")
def pipeline():
    export(gold(proximity(silver(bronze(produce())))))


pipeline_every_15_minutes = ScheduleDefinition(
    name="pipeline_every_15_minutes",
    job=pipeline,
    cron_schedule="*/15 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,  # non activé par défaut
    description="Un lot GDELT toutes les 15 minutes. Désactivé tant qu'on ne le démarre pas.",
)

defs = Definitions(jobs=[pipeline], schedules=[pipeline_every_15_minutes])
