"""Résolution locale des jars Spark (make jars)."""

import re
from pathlib import Path

import pytest

from lakehouse import jars
from lakehouse.spark_session import SPARK_PACKAGES


def test_hash_is_stable_and_follows_versions_and_exclusions():
    reference = jars.jars_hash()
    assert reference == jars.jars_hash(list(reversed(SPARK_PACKAGES)))
    assert len(reference) == 12

    bumped = [
        p.replace("iceberg-spark-runtime-4.1_2.13:", "iceberg-spark-runtime-4.1_2.13:9")
        for p in SPARK_PACKAGES
    ]
    assert jars.jars_hash(bumped) != reference
    assert jars.jars_hash(excluded=[]) != reference


def test_missing_directory_gives_an_error_that_says_what_to_do(tmp_path):
    with pytest.raises(jars.LocalJarsMissing, match="make jars"):
        jars.require_local_jars(tmp_path)


def test_incomplete_directory_is_not_accepted(tmp_path):
    # Dossier présent mais sans marqueur : résolution interrompue.
    directory = tmp_path / jars.jars_hash()
    directory.mkdir()
    (directory / "bundle-2.54.17.jar").write_bytes(b"")
    with pytest.raises(jars.LocalJarsMissing):
        jars.require_local_jars(tmp_path)

    (directory / jars.COMPLETE_MARKER).write_text("bundle-2.54.17.jar\n")
    assert jars.require_local_jars(tmp_path) == directory


def test_ivy_descriptor_lists_every_package_and_exclusion():
    xml = jars.ivy_xml(SPARK_PACKAGES, jars.PROVIDED_BY_SPARK)
    for package in SPARK_PACKAGES:
        org, name, rev = package.split(":")
        assert f'org="{org}" name="{name}" rev="{rev}"' in xml
    for item in jars.PROVIDED_BY_SPARK:
        org, module = item.split(":")
        assert f'<exclude org="{org}" module="{module}"/>' in xml


def artifact(jar_name: str) -> str:
    return re.sub(r"-\d[^-]*(-[A-Za-z0-9.]+)?\.jar$", "", jar_name)


def test_resolved_jars_do_not_shadow_what_spark_ships():
    # Le classpath du driver passe avant les jars de Spark : un doublon y
    # remplacerait silencieusement la version livrée avec Spark.
    import pyspark

    shipped = {artifact(p.name) for p in (Path(pyspark.__file__).parent / "jars").glob("*.jar")}
    resolved = {artifact(p.name) for p in jars.require_local_jars().glob("*.jar")}
    assert resolved, "aucun jar résolu"
    assert resolved.isdisjoint(shipped), sorted(resolved & shipped)
