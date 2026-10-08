"""Politique de relance du job bronze : une fois, et seulement sur SPARK-57438."""

import pytest
from pyspark.errors import StreamingQueryException

from streaming import bronze_gdelt_events as job
from streaming.bronze_gdelt_events import is_kafka_metrics_npe

from .conftest import FIXTURES

# Trace JVM réelle, capturée avec Spark 4.1.1 (voir tests/fixtures/spark/README.md).
SPARK_57438_TRACE = (FIXTURES / "spark/spark_57438_stacktrace.txt").read_text()


def test_real_spark_57438_trace_is_recognised():
    assert is_kafka_metrics_npe(SPARK_57438_TRACE)


@pytest.mark.parametrize(
    "trace",
    [
        None,
        "",
        # Même NPE, mais ailleurs que dans les métriques de la source Kafka.
        SPARK_57438_TRACE.replace("KafkaMicroBatchStream$.metrics(", "KafkaMicroBatchStream$.other("),
        # Même endroit, mais ce n'est pas une NullPointerException.
        SPARK_57438_TRACE.replace("java.lang.NullPointerException", "java.lang.IllegalStateException"),
    ],
)
def test_other_errors_are_not_recognised(trace):
    assert not is_kafka_metrics_npe(trace)


class FakeQuery:
    def __init__(self, error):
        self.error = error

    def awaitTermination(self):
        if self.error:
            raise self.error


def failure(trace):
    # PySpark range la trace JVM dans `_stackTrace` (la classe réelle ne se
    # construit pas sans JVM ; le test d'intégration vérifie la vraie exception).
    exc = StreamingQueryException("requête arrêtée")
    exc._stackTrace = trace
    return exc


@pytest.fixture
def starts(monkeypatch):
    """Remplace le démarrage de la requête : chaque appel consomme une issue prévue."""
    outcomes, calls = [], []

    def fake_start(*args):
        calls.append(args)
        return FakeQuery(outcomes.pop(0))

    monkeypatch.setattr(job, "ensure_table", lambda spark, table: None)
    monkeypatch.setattr(job, "_start", fake_start)
    return outcomes, calls


def run():
    job.run(None, bootstrap="localhost:19092", checkpoint="s3a://bucket/checkpoint")


def test_restarts_once_on_spark_57438_and_logs_a_warning(starts, caplog):
    outcomes, calls = starts
    outcomes += [failure(SPARK_57438_TRACE), None]

    with caplog.at_level("WARNING"):
        run()

    assert len(calls) == 2
    assert [r.levelname for r in caplog.records] == ["WARNING"]
    assert "SPARK-57438" in caplog.text


def test_second_spark_57438_in_a_row_stops_the_job(starts):
    outcomes, calls = starts
    outcomes += [failure(SPARK_57438_TRACE), failure(SPARK_57438_TRACE), None]

    with pytest.raises(StreamingQueryException):
        run()
    assert len(calls) == 2


def test_any_other_error_stops_the_job_without_restart(starts, caplog):
    outcomes, calls = starts
    outcomes += [failure("org.apache.kafka.common.errors.TimeoutException: broker injoignable")]

    with caplog.at_level("WARNING"), pytest.raises(StreamingQueryException):
        run()
    assert len(calls) == 1
    assert caplog.records == []


def test_restart_can_be_disabled(starts):
    outcomes, calls = starts
    outcomes += [failure(SPARK_57438_TRACE)]

    with pytest.raises(StreamingQueryException):
        job.run(None, bootstrap="b", checkpoint="c", restart_on_kafka_metrics_npe=False)
    assert len(calls) == 1
