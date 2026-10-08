"""Session Spark configurée pour le catalogue Iceberg REST, MinIO et Kafka locaux.

Versions vérifiées sur Maven Central le 2026-10-08 (règle 2 de CLAUDE.md) :
- Iceberg 1.12.0 est compilé contre Spark 4.1.3 (gradle/libs.versions.toml du tag
  apache-iceberg-1.12.0).
- spark-sql-kafka-0-10_2.13 4.1.3 fait partie de la même release que Spark.
- hadoop-aws doit avoir exactement la version des jars Hadoop embarqués par
  pyspark 4.1.3 (hadoop-client-api-3.4.2).
Monter l'un sans les autres demande de refaire la vérification.
"""

import os
import sys
from collections.abc import Mapping
from pathlib import Path

SPARK_VERSION = "4.1.3"
SCALA_BINARY = "2.13"
ICEBERG_VERSION = "1.12.0"
HADOOP_VERSION = "3.4.2"
# SDK AWS v2 : version contre laquelle Iceberg 1.12.0 est compilé (awssdk-bom dans
# gradle/libs.versions.toml du tag apache-iceberg-1.12.0).
AWS_SDK_VERSION = "2.54.17"

CATALOG = "lakehouse"

SPARK_PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_{SCALA_BINARY}:{ICEBERG_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{SPARK_VERSION}",
    # Système de fichiers s3a:// pour le checkpoint du streaming : Spark écrit
    # ses checkpoints via l'API Hadoop, pas via le S3FileIO d'Iceberg.
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
    # Un seul SDK AWS pour Iceberg (S3FileIO) et pour s3a : le bundle complet.
    # Il remplace iceberg-aws-bundle, qui depuis la 1.12 n'embarque plus le client
    # HTTP Apache 4 dont hadoop-aws 3.4.2 a besoin. Déclaré ici, il l'emporte sur
    # la version 2.29.52 que hadoop-aws demande (Ivy garde la plus récente).
    f"software.amazon.awssdk:bundle:{AWS_SDK_VERSION}",
]


def iceberg_spark_conf(
    env: Mapping[str, str] | None = None, local_jars: Path | None = None
) -> dict[str, str]:
    """Configuration Spark du catalogue, vue depuis l'hôte (localhost).

    Les valeurs par défaut sont celles de docker-compose.yml. Avec `local_jars`
    (dossier rempli par `make jars`), les dépendances sont mises directement sur
    le classpath du driver ; sinon Spark les résout lui-même, et les recopie à
    chaque session.
    """
    env = os.environ if env is None else env
    rest_port = env.get("ICEBERG_REST_PORT", "8181")
    minio_endpoint = f"http://localhost:{env.get('MINIO_API_PORT', '19000')}"
    access_key = env.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = env.get("MINIO_ROOT_PASSWORD", "minioadmin")
    prefix = f"spark.sql.catalog.{CATALOG}"
    if local_jars is not None:
        # Le joker est développé par la JVM : tous les jars du dossier.
        dependencies = {"spark.driver.extraClassPath": f"{local_jars}/*"}
    else:
        dependencies = {"spark.jars.packages": ",".join(SPARK_PACKAGES)}
    return {
        **dependencies,
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": f"http://localhost:{rest_port}",
        # Pas de « warehouse » ici : avec un catalogue REST, c'est le serveur qui
        # décide de l'emplacement des tables.
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": minio_endpoint,
        # MinIO n'a pas de DNS par bucket : http://hôte/bucket/clé.
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.access-key-id": access_key,
        f"{prefix}.s3.secret-access-key": secret_key,
        f"{prefix}.client.region": "us-east-1",
        "spark.sql.defaultCatalog": CATALOG,
        # Horodatages GDELT en UTC : on ne dépend pas du fuseau de la machine.
        "spark.sql.session.timeZone": "UTC",
        # s3a:// (checkpoints) : mêmes accès MinIO, côté Hadoop.
        "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }


def build_spark_session(app_name: str, master: str = "local[*]"):
    from pyspark.sql import SparkSession

    # Les workers Python doivent utiliser le Python du venv (3.11) : sans ça Spark
    # lance le `python3` du système (3.12 sur Ubuntu 24.04) et refuse de tourner.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # En local, le driver est la seule JVM : son classpath suffit. Sur un cluster,
    # les exécuteurs auraient besoin des jars eux aussi, d'où spark.jars.packages.
    local_jars = None
    if master.startswith("local"):
        from lakehouse.jars import require_local_jars

        local_jars = require_local_jars()
    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in iceberg_spark_conf(local_jars=local_jars).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
