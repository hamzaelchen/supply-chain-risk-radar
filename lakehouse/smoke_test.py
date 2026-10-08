"""Smoke test de l'infra : Spark -> catalogue REST -> MinIO, aller-retour complet.

Usage : make up && make smoke

La ligne écrite est un marqueur technique dans un namespace jetable (`_smoke`),
supprimé à la fin : ce n'est pas une donnée du projet (règle 1 de CLAUDE.md).
"""

import sys

from lakehouse.spark_session import CATALOG, build_spark_session

NAMESPACE = f"{CATALOG}._smoke"
TABLE = f"{NAMESPACE}.roundtrip"


def main() -> int:
    spark = build_spark_session("scrr-smoke-test")
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
        spark.sql(f"CREATE TABLE {TABLE} (id INT, label STRING) USING iceberg")
        spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'smoke')")

        rows = spark.table(TABLE).collect()
        files = [r.file_path for r in spark.table(f"{TABLE}.files").collect()]
        snapshots = spark.table(f"{TABLE}.snapshots").count()

        print(f"Spark {spark.version} | lignes lues : {rows}")
        print(f"snapshots : {snapshots} | fichiers de données : {files}")

        ok = (
            [(r.id, r.label) for r in rows] == [(1, "smoke")]
            and snapshots == 1
            and len(files) == 1
            and files[0].startswith("s3://")
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {NAMESPACE}")
        spark.stop()

    print("SMOKE TEST OK" if ok else "SMOKE TEST KO")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
