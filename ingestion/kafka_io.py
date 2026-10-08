"""Topic gdelt.events et publication des événements."""

import logging
from collections.abc import Iterable

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)

TOPIC = "gdelt.events"

# 3 partitions. Le débit n'en demande qu'une (environ 2 000 événements de 400 octets
# par quart d'heure), mais plusieurs partitions permettent à Spark de lire en
# parallèle et de rattraper vite après une panne. La clé GLOBALEVENTID envoie
# toutes les versions d'un même événement dans la même partition. On reste bas
# car le nombre de partitions ne se réduit pas, et l'augmenter change la
# répartition des clés.
PARTITIONS = 3
# Broker unique en local.
REPLICATION_FACTOR = 1
TOPIC_CONFIG = {
    # 7 jours. Kafka n'est qu'un tampon : la copie durable est le zip dans MinIO.
    # Une semaine laisse au job de streaming le temps de survivre à un week-end
    # d'arrêt sans rejouer depuis les zips, pour environ 0,5 Go non compressé.
    "retention.ms": str(7 * 24 * 60 * 60 * 1000),
    # Événements immuables : on supprime par âge, on ne compacte pas par clé
    # (le bronze veut chaque occurrence, pas seulement la dernière).
    "cleanup.policy": "delete",
    # Le broker garde la compression choisie par le producteur (zstd).
    "compression.type": "producer",
}


def ensure_topic(bootstrap: str, topic: str = TOPIC) -> bool:
    """Crée le topic s'il n'existe pas. Rend True s'il vient d'être créé."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=15).topics
    if topic in existing:
        partitions = len(existing[topic].partitions)
        if partitions != PARTITIONS:
            log.warning("topic %s : %d partitions, %d attendues", topic, partitions, PARTITIONS)
        return False
    new_topic = NewTopic(
        topic,
        num_partitions=PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        config=TOPIC_CONFIG,
    )
    admin.create_topics([new_topic])[topic].result(timeout=30)
    log.info("topic %s créé (%d partitions, rétention 7 jours)", topic, PARTITIONS)
    return True


class EventPublisher:
    def __init__(self, bootstrap: str, topic: str = TOPIC):
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                # Pas de doublon ni de désordre en cas de nouvel essai réseau.
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )

    def publish(self, records: Iterable[tuple[bytes, bytes]], headers: dict[str, str]) -> int:
        """Envoie (clé, valeur) et ne rend la main qu'une fois tout acquitté."""
        errors: list[str] = []

        def on_delivery(err, _msg):
            if err is not None:
                errors.append(str(err))

        kafka_headers = [(k, v.encode()) for k, v in headers.items()]
        count = 0
        for key, value in records:
            while True:
                try:
                    self._producer.produce(
                        self._topic,
                        key=key,
                        value=value,
                        headers=kafka_headers,
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    # File locale pleine : on laisse partir des messages.
                    self._producer.poll(1)
            self._producer.poll(0)
            count += 1

        pending = self._producer.flush(60)
        if pending or errors:
            raise KafkaException(
                f"{pending} message(s) non acquitté(s), {len(errors)} erreur(s) : {errors[:3]}"
            )
        return count
