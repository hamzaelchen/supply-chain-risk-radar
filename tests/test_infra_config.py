import re
from pathlib import Path

from lakehouse.spark_session import (
    AWS_SDK_VERSION,
    CATALOG,
    ICEBERG_VERSION,
    SPARK_PACKAGES,
    SPARK_VERSION,
    iceberg_spark_conf,
)

ROOT = Path(__file__).resolve().parent.parent
PREFIX = f"spark.sql.catalog.{CATALOG}"


def test_iceberg_runtime_matches_spark_minor_version():
    spark_minor = ".".join(SPARK_VERSION.split(".")[:2])
    runtime = next(p for p in SPARK_PACKAGES if "iceberg-spark-runtime" in p)
    assert f"iceberg-spark-runtime-{spark_minor}_2.13:{ICEBERG_VERSION}" in runtime


def test_aws_sdk_comes_from_the_full_bundle_only():
    # Un seul fournisseur du SDK AWS : ni iceberg-aws-bundle, ni module isolé.
    aws = [p for p in SPARK_PACKAGES if "awssdk" in p or "aws-bundle" in p]
    assert aws == [f"software.amazon.awssdk:bundle:{AWS_SDK_VERSION}"]


def test_kafka_connector_has_exactly_the_spark_version():
    assert f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}" in SPARK_PACKAGES


def test_without_local_jars_spark_resolves_packages_itself():
    conf = iceberg_spark_conf(env={})
    assert conf["spark.jars.packages"] == ",".join(SPARK_PACKAGES)
    assert "spark.driver.extraClassPath" not in conf


def test_with_local_jars_only_the_driver_classpath_is_set(tmp_path):
    conf = iceberg_spark_conf(env={}, local_jars=tmp_path)
    assert conf["spark.driver.extraClassPath"] == f"{tmp_path}/*"
    # spark.jars.packages ferait recopier tous les jars à chaque session.
    assert not any(key.startswith("spark.jars") for key in conf)


def test_s3a_points_to_minio():
    conf = iceberg_spark_conf(env={})
    assert conf["spark.hadoop.fs.s3a.endpoint"] == conf[f"{PREFIX}.s3.endpoint"]
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"


def test_pyspark_pin_matches_spark_version():
    pyproject = (ROOT / "pyproject.toml").read_text()
    assert f'"pyspark=={SPARK_VERSION}"' in pyproject


def test_conf_defaults_match_docker_compose():
    conf = iceberg_spark_conf(env={})
    assert conf[f"{PREFIX}.type"] == "rest"
    assert conf[f"{PREFIX}.uri"] == "http://localhost:18181"
    assert conf[f"{PREFIX}.s3.endpoint"] == "http://localhost:19000"
    assert conf[f"{PREFIX}.s3.path-style-access"] == "true"
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert f"{PREFIX}.warehouse" not in conf


def test_conf_follows_env_overrides():
    conf = iceberg_spark_conf(
        env={"ICEBERG_REST_PORT": "9999", "MINIO_API_PORT": "9998", "MINIO_ROOT_USER": "u"}
    )
    assert conf[f"{PREFIX}.uri"] == "http://localhost:9999"
    assert conf[f"{PREFIX}.s3.endpoint"] == "http://localhost:9998"
    assert conf[f"{PREFIX}.s3.access-key-id"] == "u"


def test_compose_images_are_pinned():
    compose = (ROOT / "docker-compose.yml").read_text()
    images = re.findall(r"^\s+image:\s*(\S+)$", compose, flags=re.MULTILINE)
    assert len(images) == 4
    for image in images:
        pinned_by_digest = "@sha256:" in image
        tag = image.rsplit(":", 1)[-1] if ":" in image else ""
        assert pinned_by_digest or (tag and tag != "latest"), f"image non épinglée : {image}"


def test_driver_memory_defaults_to_3g_and_can_be_overridden():
    assert iceberg_spark_conf(env={})["spark.driver.memory"] == "3g"
    assert iceberg_spark_conf(env={"SPARK_DRIVER_MEMORY": "5g"})["spark.driver.memory"] == "5g"


def test_jvm_and_session_time_zones_are_utc():
    conf = iceberg_spark_conf(env={})
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.driver.extraJavaOptions"] == "-Duser.timezone=UTC"
    assert conf["spark.executor.extraJavaOptions"] == "-Duser.timezone=UTC"


def test_shuffle_partitions_are_sized_for_one_machine():
    assert iceberg_spark_conf(env={})["spark.sql.shuffle.partitions"] == "16"
    assert (
        iceberg_spark_conf(env={"SPARK_SHUFFLE_PARTITIONS": "4"})["spark.sql.shuffle.partitions"]
        == "4"
    )
