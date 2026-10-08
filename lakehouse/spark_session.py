"""Session Spark configurée pour le catalogue Iceberg REST + MinIO locaux.

Versions vérifiées sur Maven Central le 2026-10-08 (règle 2 de CLAUDE.md) :
Iceberg 1.11.0 est compilé contre Spark 4.1.1 (gradle/libs.versions.toml du tag
apache-iceberg-1.11.0). Monter l'un sans l'autre demande de refaire la vérification.
"""

import os
from collections.abc import Mapping

SPARK_VERSION = "4.1.1"
SCALA_BINARY = "2.13"
ICEBERG_VERSION = "1.11.0"

CATALOG = "lakehouse"

SPARK_PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_{SCALA_BINARY}:{ICEBERG_VERSION}",
    # SDK AWS v2 embarqué, requis par S3FileIO (absent du runtime Spark).
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
]


def iceberg_spark_conf(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Configuration Spark du catalogue, vue depuis l'hôte (localhost).

    Les valeurs par défaut sont celles de docker-compose.yml.
    """
    env = os.environ if env is None else env
    rest_port = env.get("ICEBERG_REST_PORT", "8181")
    minio_port = env.get("MINIO_API_PORT", "19000")
    prefix = f"spark.sql.catalog.{CATALOG}"
    return {
        "spark.jars.packages": ",".join(SPARK_PACKAGES),
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": f"http://localhost:{rest_port}",
        # Pas de « warehouse » ici : avec un catalogue REST, c'est le serveur qui
        # décide de l'emplacement des tables.
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": f"http://localhost:{minio_port}",
        # MinIO n'a pas de DNS par bucket : http://hôte/bucket/clé.
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": env.get("MINIO_ROOT_USER", "minioadmin"),
        f"{prefix}.s3.secret-access-key": env.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        f"{prefix}.client.region": "us-east-1",
        "spark.sql.defaultCatalog": CATALOG,
        # Horodatages GDELT en UTC : on ne dépend pas du fuseau de la machine.
        "spark.sql.session.timeZone": "UTC",
    }


def build_spark_session(app_name: str, master: str = "local[*]"):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in iceberg_spark_conf().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
