"""Migration unique : ajoute `ingestion_mode` et `src_line_no` aux tables existantes.

Usage : make up && python -m lakehouse.add_ingestion_mode

ALTER TABLE ... ADD COLUMN d'Iceberg ne touche qu'aux métadonnées : aucun fichier de données n'est
réécrit, les lignes existantes ont NULL dans les nouvelles colonnes (NULL = « stream » pour
ingestion_mode). Le manifeste (écrit par PyIceberg) reçoit sa colonne au premier accès d'
IcebergManifest. Idempotent. Le script vérifie qu'aucune ligne ni fichier n'a bougé.
"""

import sys

from ingestion.config import Settings
from ingestion.manifest import IcebergManifest
from lakehouse.spark_session import CATALOG, build_spark_session
from silver.gdelt_events import INGESTION_COLUMNS

TABLES = [
    f"{CATALOG}.bronze.gdelt_events",
    f"{CATALOG}.silver.gdelt_events",
    f"{CATALOG}.silver.gdelt_events_quarantine",
]


def migrate(spark, table: str) -> bool:
    before_files = sorted(r.file_path for r in spark.table(f"{table}.files").collect())
    before_rows = spark.table(table).count()
    columns = [f.name for f in spark.table(table).schema.fields]
    for name, dtype in INGESTION_COLUMNS:
        if name not in columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {name} {dtype}")
            print(f"{table} : colonne {name} {dtype} ajoutée")
    after = [f.name for f in spark.table(table).schema.fields]
    ok = (
        after[-2:] == [n for n, _ in INGESTION_COLUMNS]
        and spark.table(table).count() == before_rows
        and sorted(r.file_path for r in spark.table(f"{table}.files").collect()) == before_files
    )
    print(
        f"{table} : {before_rows} lignes, {len(before_files)} fichiers inchangés -> {'OK' if ok else 'ÉCHEC'}"
    )
    return ok


def main() -> int:
    manifest = IcebergManifest(Settings.from_env())  # ajoute la colonne du manifeste si absente
    print(
        "bronze.ingestion_manifest : colonnes",
        [f.name for f in manifest._table.schema().fields][-2:],
    )
    spark = build_spark_session("add-ingestion-mode")
    spark.sparkContext.setLogLevel("WARN")
    try:
        results = [migrate(spark, t) for t in TABLES]
    finally:
        spark.stop()
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
