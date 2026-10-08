import socket
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    # Une seule session par JVM : la même sert aux tests de transformation
    # (sans infra) et aux tests d'intégration (Kafka, MinIO, catalogue).
    from lakehouse.spark_session import build_spark_session

    session = build_spark_session("tests", master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def infra():
    """Paramètres de l'infra locale ; saute le test si elle n'est pas démarrée."""
    from ingestion.config import Settings

    settings = Settings.from_env()
    for name, address in [
        ("Kafka", settings.kafka_bootstrap),
        ("MinIO", settings.s3_endpoint.removeprefix("http://")),
        ("catalogue REST", settings.rest_uri.removeprefix("http://")),
    ]:
        host, port = address.rsplit(":", 1)
        try:
            socket.create_connection((host, int(port)), timeout=2).close()
        except OSError:
            pytest.skip(f"{name} injoignable ({address}) : lancer `make up`")
    return settings
