"""Producteur GDELT : lastupdate.txt -> zip brut dans MinIO -> topic gdelt.events.

Usage :
    python -m ingestion.gdelt_producer            # un cycle
    python -m ingestion.gdelt_producer --loop     # interroge GDELT chaque minute

Garantie : au moins une fois. Le manifeste n'est écrit qu'après l'acquittement
Kafka ; un arrêt entre les deux rejoue le lot GDELT au cycle suivant. Les doublons
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
from ingestion.manifest import FAILED, MISSING, MISSING_FINAL, PUBLISHED, ManifestEntry

log = logging.getLogger("gdelt_producer")

SOURCE = "gdelt.events"
TOPIC = "gdelt.events"
# Un User-Agent qui dit qui télécharge : c'est la moindre politesse envers GDELT.
USER_AGENT = "supply-chain-risk-radar/0.1 (projet pédagogique; [adresse e-mail retirée])"

# GDELT publie parfois un fichier en retard. Un lot GDELT en 404 est retenté 4 fois,
# à 15 minutes d'intervalle au moins (soit 1 h), avant d'être déclaré définitivement
# absent. L'espacement évite qu'une boucle à la minute épuise les essais en 4 minutes.
MISSING_RETRIES = 4
RETRY_SPACING = gdelt.BATCH_INTERVAL


class BatchNotFound(Exception):
    """Le fichier du lot GDELT n'existe pas chez GDELT (HTTP 404)."""


class Manifest(Protocol):
    def attempts(
        self, source: str, modes: tuple[str, ...] | None = None
    ) -> dict[datetime, list[tuple[str, datetime]]]: ...
    def record(self, entry: ManifestEntry) -> None: ...


class Store(Protocol):
    def put(
        self, key: str, body: bytes, *, source_url: str, fetched_at: datetime, md5: str
    ) -> str: ...


class Publisher(Protocol):
    def publish(self, records: Iterable[tuple[bytes, bytes]], headers: dict[str, str]) -> int: ...


@dataclass(frozen=True)
class BatchResult:
    gdelt_batch_label_ts: datetime
    status: str
    record_count: int = 0


def http_fetch(url: str, timeout: float = 60, attempts: int = 3) -> bytes:
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise BatchNotFound(url) from exc
            if exc.code < 500 or attempt == attempts:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == attempts:
                raise
        time.sleep(2**attempt)
    raise AssertionError("inatteignable")


def process_batch(
    file: gdelt.GdeltFile,
    fetch: Callable[[str], bytes],
    store: Store,
    publisher: Publisher,
    now: Callable[[], datetime],
    previous_misses: int = 0,
) -> ManifestEntry:
    """Traite un lot GDELT et rend la ligne de manifeste. Ne lève jamais.

    `previous_misses` : nombre de 404 déjà enregistrés pour ce lot GDELT.
    """
    fetched_at = now()
    label = file.gdelt_batch_label_ts.isoformat()
    base = {
        "source": SOURCE,
        "gdelt_batch_label_ts": file.gdelt_batch_label_ts,
        "source_url": file.url,
    }
    try:
        zip_bytes = fetch(file.url)
        fetched_at = now()
    except BatchNotFound:
        final = previous_misses >= MISSING_RETRIES
        log.warning(
            "lot GDELT %s absent chez GDELT (404), essai %d/%d%s : %s",
            label,
            previous_misses + 1,
            MISSING_RETRIES + 1,
            " : abandon définitif" if final else "",
            file.url,
        )
        return ManifestEntry(
            **base,
            status=MISSING_FINAL if final else MISSING,
            fetched_at=fetched_at,
            detail=f"HTTP 404, essai {previous_misses + 1}/{MISSING_RETRIES + 1}",
        )
    except Exception as exc:  # noqa: BLE001 - un lot GDELT en échec ne doit pas arrêter le cycle
        log.error("lot GDELT %s : téléchargement impossible : %s", label, exc)
        return ManifestEntry(**base, status=FAILED, fetched_at=fetched_at, detail=repr(exc))

    md5 = hashlib.md5(zip_bytes).hexdigest()
    sizes = {"zip_bytes": len(zip_bytes), "zip_md5": md5}
    if file.md5 is not None and md5 != file.md5:
        log.error("lot GDELT %s : MD5 %s, attendu %s", label, md5, file.md5)
        return ManifestEntry(
            **base,
            **sizes,
            status=FAILED,
            fetched_at=fetched_at,
            md5_verified=False,
            detail=f"MD5 attendu {file.md5}",
        )

    try:
        # Lecture complète avant tout envoi : un zip corrompu ne publie rien.
        lines = list(gdelt.iter_event_lines(zip_bytes))
        uri = store.put(
            gdelt.raw_object_key(file),
            zip_bytes,
            source_url=file.url,
            fetched_at=fetched_at,
            md5=md5,
        )
        headers = {
            "source_url": file.url,
            "gdelt_batch_label_ts": label,
            "fetched_at": fetched_at.isoformat(),
            "raw_object_uri": uri,
        }
        count = publisher.publish(((gdelt.event_key(line), line) for line in lines), headers)
    except Exception as exc:  # noqa: BLE001 - idem, l'échec est tracé dans le manifeste
        log.error("lot GDELT %s : échec archivage/publication : %s", label, exc)
        return ManifestEntry(
            **base, **sizes, status=FAILED, fetched_at=fetched_at, detail=repr(exc)
        )

    log.info("lot GDELT %s : %d événements publiés, zip %s", label, count, uri)
    return ManifestEntry(
        **base,
        **sizes,
        status=PUBLISHED,
        fetched_at=fetched_at,
        raw_object_uri=uri,
        # None quand GDELT ne fournit pas de MD5 (lots GDELT rattrapés).
        md5_verified=True if file.md5 is not None else None,
        record_count=count,
        kafka_topic=TOPIC,
    )


def run_cycle(
    fetch: Callable[[str], bytes],
    manifest: Manifest,
    store: Store,
    publisher: Publisher,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    max_backfill: int = 96,
) -> list[BatchResult]:
    latest = gdelt.latest_export(fetch(gdelt.LASTUPDATE_URL).decode())
    attempts = manifest.attempts(SOURCE)
    # Terminé = publié, ou définitivement absent. Un échec ou un 404 récent sera retenté.
    done = {
        label
        for label, rows in attempts.items()
        if any(status in (PUBLISHED, MISSING_FINAL) for status, _ in rows)
    }
    # Le chargement historique (mode « backfill ») remplit le même manifeste : seul le flux
    # définit « le premier lot connu », sinon chaque trou entre deux périodes chargées serait
    # compté comme un lot manqué hors fenêtre.
    stream_attempts = manifest.attempts(SOURCE, modes=("stream",))
    first_known = min(stream_attempts) if stream_attempts else None

    labels, out_of_window = gdelt.batches_to_process(
        latest.gdelt_batch_label_ts, first_known, done, max_backfill
    )
    if out_of_window:
        log.warning(
            "%d lot(s) GDELT non traité(s) au-delà de la fenêtre de rattrapage (%d lots GDELT)",
            out_of_window,
            max_backfill,
        )
    results = []
    for label in labels:
        misses = sorted(at for status, at in attempts.get(label, []) if status == MISSING)
        if misses and now() - misses[-1] < RETRY_SPACING:
            continue  # 404 trop récent : on réessaiera au prochain quart d'heure
        # MD5 et taille ne sont connus que pour le lot GDELT de lastupdate.txt.
        file = (
            latest if label == latest.gdelt_batch_label_ts else gdelt.export_file_for_batch(label)
        )
        entry = process_batch(file, fetch, store, publisher, now, previous_misses=len(misses))
        manifest.record(entry)
        results.append(BatchResult(label, entry.status, entry.record_count or 0))
    if not results:
        log.info("rien à faire : lot GDELT %s déjà traité", latest.gdelt_batch_label_ts.isoformat())
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--loop", action="store_true", help="tourner en continu")
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--max-backfill", type=int, default=96, help="lots GDELT (96 = 24 h)")
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
            results = [BatchResult(datetime.now(UTC), FAILED)]
        if not args.loop:
            return 1 if any(r.status == FAILED for r in results) else 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    sys.exit(main())
