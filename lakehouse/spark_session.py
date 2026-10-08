"""Session Spark configurée pour le catalogue Iceberg REST, MinIO et Kafka locaux.

Versions vérifiées sur Maven Central le 2026-10-08 (règle 2 de CLAUDE.md) :
- Iceberg 1.11.0 est compilé contre Spark 4.1.1 (gradle/libs.versions.toml du tag
  apache-iceberg-1.11.0).
- spark-sql-kafka-0-10_2.13 4.1.1 fait partie de la même release que Spark.
- hadoop-aws doit avoir exactement la version des jars Hadoop embarqués par
  pyspark 4.1.1 (hadoop-client-api-3.4.2).
Monter l'un sans les autres demande de refaire la vérification.
"""

import os
import sys
from collections.abc import Mapping

SPARK_VERSION = "4.1.1"
SCALA_BINARY = "2.13"
ICEBERG_VERSION = "1.11.0"
HADOOP_VERSION = "3.4.2"
# Version du SDK AWS v2 embarqué (non relocalisé) dans iceberg-aws-bundle 1.11.0.
AWS_SDK_VERSION = "2.44.4"

CATALOG = "lakehouse"

SPARK_PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_{SCALA_BINARY}:{ICEBERG_VERSION}",
    # SDK AWS v2 embarqué, requis par S3FileIO (absent du runtime Spark).
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{SPARK_VERSION}",
    # Système de fichiers s3a:// pour le checkpoint du streaming : Spark écrit
    # ses checkpoints via l'API Hadoop, pas via le S3FileIO d'Iceberg.
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
]

# hadoop-aws tire software.amazon.awssdk:bundle (641 Mo), qui dupliquerait dans
# une autre version les classes du SDK déjà présentes dans iceberg-aws-bundle.
# On l'exclut : un seul SDK sur le classpath, celui d'Iceberg.
SPARK_EXCLUDES = ["software.amazon.awssdk:bundle"]

# Seul module du SDK dont s3a a besoin et qu'iceberg-aws-bundle n'embarque pas.
# Passé en URL directe (et non en package) pour ne pas résoudre ses dépendances,
# toutes déjà fournies par iceberg-aws-bundle.
SPARK_EXTRA_JARS = [
    (
        "https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/"
        f"{AWS_SDK_VERSION}/s3-transfer-manager-{AWS_SDK_VERSION}.jar"
    ),
]


def iceberg_spark_conf(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Configuration Spark du catalogue, vue depuis l'hôte (localhost).

    Les valeurs par défaut sont celles de docker-compose.yml.
    """
    env = os.environ if env is None else env
    rest_port = env.get("ICEBERG_REST_PORT", "8181")
    minio_endpoint = f"http://localhost:{env.get('MINIO_API_PORT', '19000')}"
    access_key = env.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = env.get("MINIO_ROOT_PASSWORD", "minioadmin")
    prefix = f"spark.sql.catalog.{CATALOG}"
    return {
        "spark.jars.packages": ",".join(SPARK_PACKAGES),
        "spark.jars.excludes": ",".join(SPARK_EXCLUDES),
        "spark.jars": ",".join(SPARK_EXTRA_JARS),
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
    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in iceberg_spark_conf().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
