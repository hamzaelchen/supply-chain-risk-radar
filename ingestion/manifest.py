"""Manifeste d'ingestion dans Iceberg : bronze.ingestion_manifest.

Une ligne par tentative sur un lot GDELT. C'est la seule mémoire du producteur :
la supprimer du disque local ne change rien, et n'importe quel moteur (Spark,
dbt, DuckDB) peut l'interroger pour auditer l'ingestion.
"""

from dataclasses import asdict, dataclass
from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.types import StringType

from ingestion.config import Settings

NAMESPACE = "bronze"
TABLE = f"{NAMESPACE}.ingestion_manifest"

PUBLISHED = "published"  # zip archivé et événements envoyés à Kafka
MISSING = "missing"  # absent chez GDELT (404) : provisoire, retenté
MISSING_FINAL = "missing_final"  # toujours absent après les nouvelles tentatives : définitif
FAILED = "failed"  # erreur (réseau, MD5...) : retenté au cycle suivant

_TS = pa.timestamp("us", tz="UTC")
SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("gdelt_batch_label_ts", _TS, nullable=False),
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
        # « stream » (producteur Kafka) ou « backfill » (chargement historique). Ajoutée en fin :
        # sur la table existante, elle est créée par IcebergManifest au premier accès.
        pa.field("ingestion_mode", pa.string()),
    ]
)


@dataclass(frozen=True)
class ManifestEntry:
    source: str
    gdelt_batch_label_ts: datetime
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
    ingestion_mode: str = "stream"


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
        self._ensure_columns()

    def attempts(
        self, source: str, modes: tuple[str, ...] | None = None
    ) -> dict[datetime, list[tuple[str, datetime]]]:
        """Par lot GDELT, toutes les tentatives enregistrées : (statut, fetched_at).

        `modes` : ne garder que ces modes d'ingestion (NULL = ligne antérieure à la colonne, donc
        « stream »). Par défaut, tous.
        """
        rows = (
            self._table.refresh()
            .scan(
                row_filter=EqualTo("source", source),
                selected_fields=("gdelt_batch_label_ts", "status", "fetched_at", "ingestion_mode"),
            )
            .to_arrow()
            .to_pylist()
        )
        result: dict[datetime, list[tuple[str, datetime]]] = {}
        for row in rows:
            if modes is not None and (row["ingestion_mode"] or "stream") not in modes:
                continue
            result.setdefault(row["gdelt_batch_label_ts"], []).append(
                (row["status"], row["fetched_at"])
            )
        return result

    def _ensure_columns(self) -> None:
        """Table créée avant l'ajout d'une colonne : on l'ajoute (métadonnées seules)."""
        existing = {f.name for f in self._table.schema().fields}
        missing = [f for f in SCHEMA if f.name not in existing]
        if missing:
            with self._table.update_schema() as update:
                for field in missing:
                    update.add_column(field.name, StringType())  # seules des chaînes à ce jour
            self._table = self._table.refresh()

    def record(self, entry: ManifestEntry) -> None:
        self.record_many([entry])

    def record_many(self, entries: list[ManifestEntry]) -> None:
        """Plusieurs tentatives en un seul commit (un jour de backfill = un snapshot)."""
        if entries:
            self._table.append(pa.Table.from_pylist([asdict(e) for e in entries], schema=SCHEMA))
