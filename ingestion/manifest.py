"""Manifeste d'ingestion dans Iceberg : bronze.ingestion_manifest.

Une ligne par tentative sur un créneau. C'est la seule mémoire du producteur :
la supprimer du disque local ne change rien, et n'importe quel moteur (Spark,
dbt, DuckDB) peut l'interroger pour auditer l'ingestion.
"""

from dataclasses import asdict, dataclass
from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

from ingestion.config import Settings

NAMESPACE = "bronze"
TABLE = f"{NAMESPACE}.ingestion_manifest"

PUBLISHED = "published"  # zip archivé et événements envoyés à Kafka
MISSING = "missing"  # créneau absent chez GDELT (404) : définitif, non retenté
FAILED = "failed"  # erreur (réseau, MD5...) : retenté au cycle suivant

_TS = pa.timestamp("us", tz="UTC")
SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("slot_ts", _TS, nullable=False),
        pa.field("source_url", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("fetched_at", _TS, nullable=False),
        pa.field("raw_object_uri", pa.string()),
        pa.field("zip_bytes", pa.int64()),
        pa.field("zip_md5", pa.string()),
        pa.field("md5_verified", pa.bool_()),
        pa.field("record_count", pa.int64()),
        pa.field("kafka_topic", pa.string()),
        pa.field("detail", pa.string()),
    ]
)


@dataclass(frozen=True)
class ManifestEntry:
    source: str
    slot_ts: datetime
    source_url: str
    status: str
    fetched_at: datetime
    raw_object_uri: str | None = None
    zip_bytes: int | None = None
    zip_md5: str | None = None
    md5_verified: bool | None = None
    record_count: int | None = None
    kafka_topic: str | None = None
    detail: str | None = None


class IcebergManifest:
    def __init__(self, settings: Settings):
        self._catalog = load_catalog(
            "lakehouse",
            **{
                "type": "rest",
                "uri": settings.rest_uri,
                "s3.endpoint": settings.s3_endpoint,
                "s3.access-key-id": settings.s3_access_key,
                "s3.secret-access-key": settings.s3_secret_key,
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
            },
        )
        self._catalog.create_namespace_if_not_exists(NAMESPACE)
        self._table = self._catalog.create_table_if_not_exists(TABLE, schema=SCHEMA)

    def statuses(self, source: str) -> dict[datetime, set[str]]:
        """Tous les statuts enregistrés par créneau pour une source."""
        rows = (
            self._table.refresh()
            .scan(row_filter=EqualTo("source", source), selected_fields=("slot_ts", "status"))
            .to_arrow()
            .to_pylist()
        )
        result: dict[datetime, set[str]] = {}
        for row in rows:
            result.setdefault(row["slot_ts"], set()).add(row["status"])
        return result

    def record(self, entry: ManifestEntry) -> None:
        self._table.append(pa.Table.from_pylist([asdict(entry)], schema=SCHEMA))
