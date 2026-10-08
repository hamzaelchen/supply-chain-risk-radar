import socket
import uuid
import warnings
from dataclasses import dataclass
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


@dataclass(frozen=True)
class Sandbox:
    """Espace jetable d'une exécution de tests : tout ce qu'elle crée porte son identifiant.

    Deux exécutions ne partagent rien. Les restes d'une exécution interrompue ne
    peuvent donc pas faire échouer la suivante.
    """

    run_id: str
    bucket: str

    @property
    def namespace(self) -> str:
        return f"lakehouse.test_{self.run_id}"

    @property
    def topic_prefix(self) -> str:
        return f"test.{self.run_id}."

    @property
    def storage_root(self) -> str:
        return f"s3a://{self.bucket}/{self.storage_prefix}"

    @property
    def storage_prefix(self) -> str:
        return f"checkpoints/test_{self.run_id}"

    def name(self) -> str:
        """Suffixe unique pour une table, un topic ou un dossier dans ce bac à sable."""
        return uuid.uuid4().hex[:8]


def sweep(spark, settings, box: Sandbox) -> list[str]:
    """Supprime tout ce que le bac à sable contient. Rend les erreurs rencontrées.

    Chaque étape est indépendante : un échec sur l'une n'empêche pas les autres.
    """
    from confluent_kafka.admin import AdminClient

    errors = []

    def step(label, action):
        try:
            action()
        except Exception as exc:  # noqa: BLE001 - on veut tenter toutes les étapes
            errors.append(f"{label} : {exc!r}"[:300])

    def drop_topics():
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
        topics = [t for t in admin.list_topics(timeout=15).topics if t.startswith(box.topic_prefix)]
        for future in admin.delete_topics(topics).values() if topics else []:
            future.result(timeout=30)

    def drop_tables():
        short = box.namespace.split(".", 1)[1]
        if not spark.sql("SHOW NAMESPACES IN lakehouse").filter(f"namespace = '{short}'").count():
            return
        for row in spark.sql(f"SHOW TABLES IN {box.namespace}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {box.namespace}.{row.tableName} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {box.namespace}")

    def drop_storage():
        path = spark._jvm.org.apache.hadoop.fs.Path(box.storage_root)
        path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(path, True)

    step("topics Kafka", drop_topics)
    step("tables Iceberg", drop_tables)
    step("stockage MinIO", drop_storage)
    return errors


@pytest.fixture(scope="session")
def sandbox(spark, infra):
    box = Sandbox(run_id=uuid.uuid4().hex[:8], bucket=infra.bucket)
    try:
        yield box
    finally:
        # Exécuté que les tests aient réussi ou non. Un nettoyage raté est
        # signalé, mais ne transforme pas des tests réussis en échec.
        for error in sweep(spark, infra, box):
            warnings.warn(f"nettoyage du bac à sable {box.run_id} : {error}", stacklevel=1)
