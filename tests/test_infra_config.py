import re
from pathlib import Path

from lakehouse.spark_session import (
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


def test_all_iceberg_packages_share_one_version():
    assert all(p.endswith(f":{ICEBERG_VERSION}") for p in SPARK_PACKAGES)


def test_pyspark_pin_matches_spark_version():
    pyproject = (ROOT / "pyproject.toml").read_text()
    assert f'"pyspark=={SPARK_VERSION}"' in pyproject


def test_conf_defaults_match_docker_compose():
    conf = iceberg_spark_conf(env={})
    assert conf[f"{PREFIX}.type"] == "rest"
    assert conf[f"{PREFIX}.uri"] == "http://localhost:8181"
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
