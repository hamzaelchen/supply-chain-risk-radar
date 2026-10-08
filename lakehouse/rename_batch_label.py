"""Migration unique : slot_ts -> gdelt_batch_label_ts sur les tables bronze existantes.

Usage : make up && python -m lakehouse.rename_batch_label

ALTER TABLE ... RENAME COLUMN d'Iceberg ne touche qu'aux métadonnées (le schéma
est identifié par des numéros de colonne, pas par des noms) : aucun fichier de
données n'est réécrit, les anciens snapshots restent lisibles. Le script le
prouve : mêmes fichiers de données avant/après, mêmes comptes à chaque snapshot.
Idempotent : une table déjà migrée est seulement vérifiée.
"""

import sys

from lakehouse.spark_session import CATALOG, build_spark_session

# (table, ancien nom, nouveau nom)
RENAMES = [
    (f"{CATALOG}.bronze.ingestion_manifest", "slot_ts", "gdelt_batch_label_ts"),
    (f"{CATALOG}.bronze.gdelt_events", "src_slot_ts", "src_gdelt_batch_label_ts"),
]


def _state(spark, table: str) -> dict:
    snapshots = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at, snapshot_id"
    ).collect()
    return {
        "columns": [f.name for f in spark.table(table).schema.fields],
        "count": spark.table(table).count(),
        "files": sorted(r.file_path for r in spark.table(f"{table}.files").collect()),
        "snapshots": [r.snapshot_id for r in snapshots],
        "counts": {
            r.snapshot_id: spark.sql(f"SELECT * FROM {table} VERSION AS OF {r.snapshot_id}").count()
            for r in snapshots
        },
    }


def migrate(spark, table: str, old: str, new: str) -> bool:
    before = _state(spark, table)
    print(f"\n== {table}")
    print(
        f"avant : {before['count']} lignes, {len(before['snapshots'])} snapshots, "
        f"{len(before['files'])} fichiers de données"
    )
    if old in before["columns"]:
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")
        print(f"ALTER TABLE ... RENAME COLUMN {old} TO {new} : fait")
    elif new in before["columns"]:
        print(f"{new} existe déjà : rien à renommer")
    else:
        print(f"ERREUR : ni {old} ni {new} dans {table}")
        return False

    after = _state(spark, table)
    print(
        f"après : {after['count']} lignes, {len(after['snapshots'])} snapshots, "
        f"{len(after['files'])} fichiers de données"
    )
    # Lecture d'un ancien snapshot : Iceberg lit avec le schéma de CE snapshot.
    oldest = before["snapshots"][0]
    cols_then = spark.sql(f"SELECT * FROM {table} VERSION AS OF {oldest}").columns
    print(
        f"time travel, 1er snapshot {oldest} : {after['counts'][oldest]} lignes, "
        f"colonne lue sous le nom {[c for c in cols_then if new in c or old in c]}"
    )

    checks = {
        "nombre de lignes inchangé": before["count"] == after["count"],
        "mêmes fichiers de données (aucune réécriture)": before["files"] == after["files"],
        "anciens snapshots présents": after["snapshots"][: len(before["snapshots"])]
        == before["snapshots"],
        "comptes identiques à chaque ancien snapshot": all(
            after["counts"][s] == before["counts"][s] for s in before["snapshots"]
        ),
        f"{new} présente, {old} absente": new in after["columns"] and old not in after["columns"],
    }
    for name, ok in checks.items():
        print(f"  [{'OK' if ok else 'ÉCHEC'}] {name}")
    return all(checks.values())


def main() -> int:
    spark = build_spark_session("scrr-rename-batch-label")
    spark.sparkContext.setLogLevel("WARN")
    try:
        results = [migrate(spark, *r) for r in RENAMES]
    finally:
        spark.stop()
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
