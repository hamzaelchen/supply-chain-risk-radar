"""Le nettoyage des tests d'intégration : une exécution cassée ne gêne pas la suivante."""

import pytest
from confluent_kafka.admin import AdminClient

from ingestion.kafka_io import ensure_topic

from .conftest import Sandbox, sweep

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def leave_a_mess(spark, infra, box):
    """Ce qu'une exécution interrompue laisse derrière elle : table, topic, fichiers."""
    table = f"{box.namespace}.orphan_{box.name()}"
    topic = f"{box.topic_prefix}orphan"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {box.namespace}")
    spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    ensure_topic(infra.kafka_bootstrap, topic)
    spark.range(3).write.mode("overwrite").json(f"{box.storage_root}/orphan")
    return topic


def remains(spark, infra, box):
    topics = AdminClient({"bootstrap.servers": infra.kafka_bootstrap}).list_topics(timeout=15).topics
    short = box.namespace.split(".", 1)[1]
    path = spark._jvm.org.apache.hadoop.fs.Path(box.storage_root)
    return {
        "topics": [t for t in topics if t.startswith(box.topic_prefix)],
        "namespace": spark.sql("SHOW NAMESPACES IN lakehouse").filter(f"namespace = '{short}'").count(),
        "storage": path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(path),
    }


def test_sandboxes_of_two_runs_share_nothing(infra):
    one, other = Sandbox("aaaaaaaa", infra.bucket), Sandbox("bbbbbbbb", infra.bucket)
    assert one.namespace != other.namespace
    assert not other.topic_prefix.startswith(one.topic_prefix)
    assert not other.storage_root.startswith(one.storage_root)


def test_sweep_removes_everything_and_ignores_other_runs(spark, infra, sandbox):
    # Deux exécutions « cassées » : chacune a laissé une table, un topic, des fichiers.
    broken = Sandbox(f"{sandbox.run_id}x1", infra.bucket)
    neighbour = Sandbox(f"{sandbox.run_id}x2", infra.bucket)
    try:
        leave_a_mess(spark, infra, broken)
        leave_a_mess(spark, infra, neighbour)

        assert sweep(spark, infra, broken) == []

        assert remains(spark, infra, broken) == {"topics": [], "namespace": 0, "storage": False}
        # Le nettoyage d'une exécution ne touche pas à celle d'à côté.
        left = remains(spark, infra, neighbour)
        assert (len(left["topics"]), left["namespace"], left["storage"]) == (1, 1, True)
        # Rejouable sans erreur : rien à supprimer n'est pas un échec.
        assert sweep(spark, infra, broken) == []
    finally:
        sweep(spark, infra, broken)
        sweep(spark, infra, neighbour)


def test_sweep_reports_errors_but_tries_every_step(spark, infra):
    # Kafka injoignable : l'erreur est rendue, les autres étapes sont tentées.
    from dataclasses import replace

    box = Sandbox("cccccccc", infra.bucket)
    unreachable = replace(infra, kafka_bootstrap="localhost:1")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {box.namespace}")
    try:
        errors = sweep(spark, unreachable, box)
        assert len(errors) == 1 and errors[0].startswith("topics Kafka")
        short = box.namespace.split(".", 1)[1]
        assert spark.sql("SHOW NAMESPACES IN lakehouse").filter(f"namespace = '{short}'").count() == 0
    finally:
        sweep(spark, infra, box)
