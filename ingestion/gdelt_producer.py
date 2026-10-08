"""Producteur GDELT : lastupdate.txt -> zip brut dans MinIO -> topic gdelt.events.

Usage :
    python -m ingestion.gdelt_producer            # un cycle
    python -m ingestion.gdelt_producer --loop     # interroge GDELT chaque minute

Garantie : au moins une fois. Le manifeste n'est écrit qu'après l'acquittement
Kafka ; un arrêt entre les deux rejoue le créneau au cycle suivant. Les doublons
éventuels sont éliminés en silver sur GLOBALEVENTID.
"""

import argparse
import hashlib
import logging
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from ingestion import gdelt
from ingestion.manifest import FAILED, MISSING, PUBLISHED, ManifestEntry

log = logging.getLogger("gdelt_producer")

SOURCE = "gdelt.events"
TOPIC = "gdelt.events"


class SlotNotFound(Exception):
    """Le fichier du créneau n'existe pas chez GDELT (HTTP 404)."""


class Manifest(Protocol):
    def statuses(self, source: str) -> dict[datetime, set[str]]: ...
    def record(self, entry: ManifestEntry) -> None: ...


class Store(Protocol):
    def put(
        self, key: str, body: bytes, *, source_url: str, fetched_at: datetime, md5: str
    ) -> str: ...


class Publisher(Protocol):
    def publish(self, records: Iterable[tuple[bytes, bytes]], headers: dict[str, str]) -> int: ...


@dataclass(frozen=True)
class SlotResult:
    slot_ts: datetime
    status: str
    record_count: int = 0


def http_fetch(url: str, timeout: float = 60, attempts: int = 3) -> bytes:
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise SlotNotFound(url) from exc
            if exc.code < 500 or attempt == attempts:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == attempts:
                raise
        time.sleep(2**attempt)
    raise AssertionError("inatteignable")


def process_slot(
    file: gdelt.GdeltFile,
    fetch: Callable[[str], bytes],
    store: Store,
    publisher: Publisher,
    now: Callable[[], datetime],
) -> ManifestEntry:
    """Traite un créneau et rend la ligne de manifeste. Ne lève jamais."""
    fetched_at = now()
    base = {"source": SOURCE, "slot_ts": file.slot_ts, "source_url": file.url}
    try:
        zip_bytes = fetch(file.url)
        fetched_at = now()
    except SlotNotFound:
        log.warning("créneau %s absent chez GDELT (404) : %s", file.slot_ts.isoformat(), file.url)
        return ManifestEntry(**base, status=MISSING, fetched_at=fetched_at, detail="HTTP 404")
    except Exception as exc:  # noqa: BLE001 - un créneau en échec ne doit pas arrêter le cycle
        log.error("créneau %s : téléchargement impossible : %s", file.slot_ts.isoformat(), exc)
        return ManifestEntry(**base, status=FAILED, fetched_at=fetched_at, detail=repr(exc))

    md5 = hashlib.md5(zip_bytes).hexdigest()
    sizes = {"zip_bytes": len(zip_bytes), "zip_md5": md5}
    if file.md5 is not None and md5 != file.md5:
        log.error("créneau %s : MD5 %s, attendu %s", file.slot_ts.isoformat(), md5, file.md5)
        return ManifestEntry(
            **base, **sizes, status=FAILED, fetched_at=fetched_at, md5_verified=False,
            detail=f"MD5 attendu {file.md5}",
        )

    try:
        # Lecture complète avant tout envoi : un zip corrompu ne publie rien.
        lines = list(gdelt.iter_event_lines(zip_bytes))
        uri = store.put(
            gdelt.raw_object_key(file), zip_bytes,
            source_url=file.url, fetched_at=fetched_at, md5=md5,
        )
        headers = {
            "source_url": file.url,
            "slot_ts": file.slot_ts.isoformat(),
            "fetched_at": fetched_at.isoformat(),
            "raw_object_uri": uri,
        }
        count = publisher.publish(((gdelt.event_key(line), line) for line in lines), headers)
    except Exception as exc:  # noqa: BLE001 - idem, l'échec est tracé dans le manifeste
        log.error("créneau %s : échec archivage/publication : %s", file.slot_ts.isoformat(), exc)
        return ManifestEntry(
            **base, **sizes, status=FAILED, fetched_at=fetched_at, detail=repr(exc)
        )

    log.info("créneau %s : %d événements publiés, zip %s", file.slot_ts.isoformat(), count, uri)
    return ManifestEntry(
        **base, **sizes, status=PUBLISHED, fetched_at=fetched_at, raw_object_uri=uri,
        # None quand GDELT ne fournit pas de MD5 (créneaux rattrapés).
        md5_verified=True if file.md5 is not None else None,
        record_count=count, kafka_topic=TOPIC,
    )


def run_cycle(
    fetch: Callable[[str], bytes],
    manifest: Manifest,
    store: Store,
    publisher: Publisher,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    max_backfill: int = 96,
) -> list[SlotResult]:
    latest = gdelt.latest_export(fetch(gdelt.LASTUPDATE_URL).decode())
    statuses = manifest.statuses(SOURCE)
    # Terminé = publié, ou absent chez GDELT. Un échec sera retenté.
    done = {slot for slot, seen in statuses.items() if seen & {PUBLISHED, MISSING}}
    first_known = min(statuses) if statuses else None

    slots, out_of_window = gdelt.slots_to_process(latest.slot_ts, first_known, done, max_backfill)
    if out_of_window:
        log.warning(
            "%d créneau(x) non traité(s) au-delà de la fenêtre de rattrapage (%d créneaux)",
            out_of_window, max_backfill,
        )
    if not slots:
        log.info("rien à faire : créneau %s déjà traité", latest.slot_ts.isoformat())

    results = []
    for slot in slots:
        # MD5 et taille ne sont connus que pour le créneau de lastupdate.txt.
        file = latest if slot == latest.slot_ts else gdelt.export_file_for_slot(slot)
        entry = process_slot(file, fetch, store, publisher, now)
        manifest.record(entry)
        results.append(SlotResult(slot, entry.status, entry.record_count or 0))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--loop", action="store_true", help="tourner en continu")
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--max-backfill", type=int, default=96, help="créneaux (96 = 24 h)")
    args = parser.parse_args(argv)

    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s %(message)s"
    )

    # Imports tardifs : les tests de run_cycle n'ont besoin ni de Kafka ni d'Iceberg.
    from ingestion.config import Settings
    from ingestion.kafka_io import EventPublisher, ensure_topic
    from ingestion.manifest import IcebergManifest
    from ingestion.raw_store import RawStore

    settings = Settings.from_env()
    ensure_topic(settings.kafka_bootstrap)
    manifest = IcebergManifest(settings)
    store = RawStore(settings)
    publisher = EventPublisher(settings.kafka_bootstrap)

    while True:
        try:
            results = run_cycle(
                http_fetch, manifest, store, publisher, max_backfill=args.max_backfill
            )
        except Exception:
            # Une panne passagère (GDELT, catalogue) ne doit pas tuer la boucle.
            log.exception("cycle en échec")
            results = [SlotResult(datetime.now(UTC), FAILED)]
        if not args.loop:
            return 1 if any(r.status == FAILED for r in results) else 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    sys.exit(main())
