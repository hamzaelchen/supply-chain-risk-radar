"""Chargement historique GDELT : fichiers 15 minutes -> bronze.gdelt_events, en lot (sans Kafka).

Usage :
    python -m ingestion.gdelt_backfill --range 2026-01-04:2026-03-31 --range 2026-07-13:2026-10-08
    python -m ingestion.gdelt_backfill --range ... --dry-run         # plan seulement
    python -m ingestion.gdelt_backfill --range ... --max-days 1      # essai sur un jour

Pourquoi pas Kafka : la rétention du topic est de 7 jours et un historique n'est pas un flux.
Les lignes sont écrites dans le MÊME bronze.gdelt_events (ingestion_mode = 'backfill', colonnes
Kafka à NULL, identité = fichier + numéro de ligne) et les tentatives dans le MÊME manifeste.

Idempotent et reprenable :
- un lot GDELT déjà `published` ou `missing_final` dans le manifeste (flux ou backfill) est sauté ;
- le bronze est écrit par MERGE INTO sans UPDATE sur (fichier, numéro de ligne) : rejouer un jour
  n'ajoute rien ;
- ordre : zips archivés dans MinIO -> bronze (un commit par jour) -> manifeste (un commit par
  jour). Un arrêt entre les deux rejoue le jour : les lignes sont déjà là, le MERGE les ignore.

Respect de GDELT : téléchargements séquentiels, pause fixe après chaque fichier (1 s par défaut),
User-Agent explicite, attente croissante sur HTTP 429.

À NE PAS lancer en même temps que `make silver` ou `make proximity` : le curseur de ces jobs est
le plus récent `ingested_at` lu ; un jour de backfill encore en cours d'écriture a un `ingested_at`
fixé au début de son écriture, donc un passage de la silver entre-temps pourrait le dépasser.
"""

import argparse
import hashlib
import logging
import sys
import time
import urllib.error
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

from ingestion import gdelt
from ingestion.gdelt_producer import (
    SOURCE,
    BatchNotFound,
    http_fetch,
)
from ingestion.manifest import FAILED, MISSING_FINAL, PUBLISHED, ManifestEntry

log = logging.getLogger("gdelt_backfill")

MODE = "backfill"
DEFAULT_PAUSE_SECONDS = 1.0
RATE_LIMIT_WAITS = (30, 60, 120, 240, 480)  # secondes, sur HTTP 429
LABELS_PER_DAY = 96
SHUFFLE_PARTITIONS = 4

# (ligne brute, URL du fichier, étiquette du lot, heure de téléchargement, URI du zip, n° de ligne)
Row = tuple[str, str, datetime, datetime, str, int]


class Manifest(Protocol):
    def attempts(
        self, source: str, modes: tuple[str, ...] | None = None
    ) -> dict[datetime, list[tuple[str, datetime]]]: ...
    def record_many(self, entries: list[ManifestEntry]) -> None: ...


class Store(Protocol):
    def put(
        self, key: str, body: bytes, *, source_url: str, fetched_at: datetime, md5: str
    ) -> str: ...


class BronzeWriter(Protocol):
    def write_day(self, day: date, rows: list[Row]) -> tuple[int, int]:
        """(lignes reçues, lignes réellement insérées)."""


@dataclass
class DayResult:
    day: date
    planned: int = 0
    published: int = 0
    missing: int = 0
    failed: int = 0
    rows: int = 0
    rows_inserted: int = 0
    download_seconds: float = 0.0
    write_seconds: float = 0.0
    zip_bytes: int = 0
    files: list[str] = field(default_factory=list)

    @property
    def seconds_per_file(self) -> float:
        return self.download_seconds / self.planned if self.planned else 0.0


def parse_range(text: str) -> tuple[date, date]:
    start, _, end = text.partition(":")
    first, last = date.fromisoformat(start), date.fromisoformat(end or start)
    if last < first:
        raise ValueError(f"période à l'envers : {text}")
    return first, last


def days_in(ranges: list[tuple[date, date]]) -> list[date]:
    """Jours distincts des périodes (bornes incluses), du plus ancien au plus récent."""
    days: set[date] = set()
    for first, last in ranges:
        days.update(first + timedelta(d) for d in range((last - first).days + 1))
    return sorted(days)


def labels_of(day: date) -> list[datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return [start + gdelt.BATCH_INTERVAL * i for i in range(LABELS_PER_DAY)]


def finished_labels(manifest: Manifest) -> set[datetime]:
    """Lots GDELT terminés (publiés ou définitivement absents), quel que soit le mode."""
    return {
        label
        for label, rows in manifest.attempts(SOURCE).items()
        if any(status in (PUBLISHED, MISSING_FINAL) for status, _ in rows)
    }


def polite_fetch(
    url: str,
    *,
    fetch: Callable[[str], bytes] = http_fetch,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Téléchargement avec attente croissante si GDELT répond 429 (trop de requêtes)."""
    for wait in (*RATE_LIMIT_WAITS, None):
        try:
            return fetch(url)
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or wait is None:
                raise
            log.warning("HTTP 429 sur %s : attente de %d s", url, wait)
            sleep(wait)
    raise AssertionError("inatteignable")


def process_day(
    day: date,
    *,
    done: set[datetime],
    fetch: Callable[[str], bytes],
    store: Store,
    writer: BronzeWriter,
    manifest: Manifest,
    pause: float = DEFAULT_PAUSE_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    clock: Callable[[], float] = time.monotonic,
) -> DayResult:
    """Télécharge, archive et charge les lots d'un jour. Ne lève que si l'écriture échoue."""
    result = DayResult(day)
    todo = [label for label in labels_of(day) if label not in done]
    result.planned = len(todo)
    rows: list[Row] = []
    entries: list[ManifestEntry] = []
    started = clock()
    for index, label in enumerate(todo):
        file = gdelt.export_file_for_batch(label)
        base = {
            "source": SOURCE, "gdelt_batch_label_ts": label, "source_url": file.url,
            "ingestion_mode": MODE,
        }  # fmt: skip
        fetched_at = now()
        try:
            body = fetch(file.url)
            fetched_at = now()
        except BatchNotFound:
            # Fichier absent d'un lot ancien : définitif, il n'y a plus de « retard » à attendre.
            result.missing += 1
            entries.append(
                ManifestEntry(
                    **base, status=MISSING_FINAL, fetched_at=fetched_at, detail="HTTP 404"
                )
            )
        except Exception as exc:  # noqa: BLE001 - un fichier en échec ne doit pas arrêter le jour
            log.error("lot GDELT %s : téléchargement impossible : %s", label.isoformat(), exc)
            result.failed += 1
            entries.append(
                ManifestEntry(**base, status=FAILED, fetched_at=fetched_at, detail=repr(exc))
            )
        else:
            md5 = hashlib.md5(body).hexdigest()
            try:
                lines = list(gdelt.iter_event_lines(body))
                uri = store.put(
                    gdelt.raw_object_key(file), body,
                    source_url=file.url, fetched_at=fetched_at, md5=md5,
                )  # fmt: skip
            except Exception as exc:  # noqa: BLE001 - zip illisible ou MinIO en panne
                log.error("lot GDELT %s : archivage impossible : %s", label.isoformat(), exc)
                result.failed += 1
                entries.append(
                    ManifestEntry(**base, status=FAILED, fetched_at=fetched_at, detail=repr(exc))
                )
            else:
                rows.extend(
                    (line.decode("utf-8", errors="replace"), file.url, label, fetched_at, uri, n)
                    for n, line in enumerate(lines, start=1)
                )
                result.published += 1
                result.zip_bytes += len(body)
                entries.append(
                    ManifestEntry(
                        **base, status=PUBLISHED, fetched_at=fetched_at, raw_object_uri=uri,
                        zip_bytes=len(body), zip_md5=md5, record_count=len(lines),
                    )
                )  # fmt: skip
        if index < len(todo) - 1:
            sleep(pause)  # pause entre deux fichiers, jamais en rafale
    result.download_seconds = clock() - started

    started = clock()
    if rows:
        result.rows, result.rows_inserted = writer.write_day(day, rows)
    manifest.record_many(entries)  # APRÈS le bronze : un arrêt entre les deux rejoue le jour
    result.write_seconds = clock() - started
    done.update(e.gdelt_batch_label_ts for e in entries if e.status in (PUBLISHED, MISSING_FINAL))
    return result


class SparkBronzeWriter:
    """Écrit un jour de lignes dans bronze.gdelt_events : un commit, MERGE sans UPDATE."""

    def __init__(self, spark, table: str = "lakehouse.bronze.gdelt_events"):
        from streaming.bronze_gdelt_events import ensure_table

        self._spark, self._table = spark, table
        # Un jour = environ 125 000 lignes, soit 15 Mo : quelques fichiers, pas les 200 partitions
        # de shuffle par défaut (une centaine de jours donneraient des dizaines de milliers de
        # petits fichiers).
        spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        ensure_table(spark, table)

    def write_day(self, day: date, rows: list[Row]) -> tuple[int, int]:
        from pyspark.sql import types as T

        from streaming.bronze_gdelt_events import to_bronze_backfill

        schema = T.StructType(
            [
                T.StructField("raw_line", T.StringType()),
                T.StructField("src_file_url", T.StringType()),
                T.StructField("src_gdelt_batch_label_ts", T.TimestampType()),
                T.StructField("src_fetched_at", T.TimestampType()),
                T.StructField("src_raw_object_uri", T.StringType()),
                T.StructField("src_line_no", T.IntegerType()),
            ]
        )
        spark = self._spark
        before = self._last_snapshot()
        view = f"_scrr_backfill_{day:%Y%m%d}"
        to_bronze_backfill(spark.createDataFrame(rows, schema)).createOrReplaceTempView(view)
        lo = datetime(day.year, day.month, day.day, tzinfo=UTC)
        hi = lo + timedelta(days=1)
        # Les deux bornes ne portent que sur la table cible : Iceberg écarte les fichiers des
        # autres jours au lieu de comparer le jour à tout le bronze.
        spark.sql(
            f"MERGE INTO {self._table} t USING {view} s "
            "ON t.src_file_url = s.src_file_url AND t.src_line_no = s.src_line_no "
            f"AND t.src_gdelt_batch_label_ts >= TIMESTAMP '{lo:%Y-%m-%d %H:%M:%S}' "
            f"AND t.src_gdelt_batch_label_ts < TIMESTAMP '{hi:%Y-%m-%d %H:%M:%S}' "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        spark.catalog.dropTempView(view)
        after = self._last_snapshot()
        inserted = 0
        if after and after[0] != (before[0] if before else None):
            inserted = int(after[1] or 0)
        return len(rows), inserted

    def _last_snapshot(self):
        row = self._spark.sql(
            f"SELECT snapshot_id, summary['added-records'] AS added "
            f"FROM {self._table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        return (row[0].snapshot_id, row[0].added) if row else None


def run(
    days: list[date],
    *,
    manifest: Manifest,
    store: Store,
    writer: BronzeWriter,
    pause: float = DEFAULT_PAUSE_SECONDS,
    max_days: int | None = None,
    fetch: Callable[[str], bytes] | None = None,
    on_day: Callable[[DayResult], None] = lambda r: None,
) -> list[DayResult]:
    done = finished_labels(manifest)
    results = []
    fetch = fetch or polite_fetch
    for day in days:
        if max_days is not None and len(results) >= max_days:
            break
        if all(label in done for label in labels_of(day)):
            continue  # jour entièrement chargé : ne compte pas dans max_days
        results.append(
            process_day(
                day, done=done, fetch=fetch, store=store, writer=writer, manifest=manifest,
                pause=pause,
            )
        )  # fmt: skip
        on_day(results[-1])
    return results


def plan(days: list[date], manifest: Manifest) -> tuple[int, int, int]:
    """(jours à traiter, fichiers à télécharger, fichiers déjà terminés) pour un --dry-run."""
    done = finished_labels(manifest)
    todo_days = [d for d in days if not all(label in done for label in labels_of(d))]
    files = sum(1 for d in todo_days for label in labels_of(d) if label not in done)
    return len(todo_days), files, len(days) * LABELS_PER_DAY - files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--range", action="append", required=True, metavar="DEBUT:FIN",
                        help="période, bornes incluses (AAAA-MM-JJ:AAAA-MM-JJ) ; répétable")  # fmt: skip
    parser.add_argument("--pause", type=float, default=DEFAULT_PAUSE_SECONDS,
                        help="secondes d'attente après chaque fichier")  # fmt: skip
    parser.add_argument("--max-days", type=int, help="s'arrêter après N jours traités")
    parser.add_argument(
        "--dry-run", action="store_true", help="afficher le plan sans rien télécharger"
    )
    args = parser.parse_args(argv)
    if args.pause < 0.5:
        parser.error("--pause < 0,5 s refusée : GDELT doit être ménagé")

    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s %(message)s"
    )

    from ingestion.config import Settings
    from ingestion.manifest import IcebergManifest
    from ingestion.raw_store import RawStore

    settings = Settings.from_env()
    manifest = IcebergManifest(settings)
    days = days_in([parse_range(r) for r in args.range])
    todo_days, files, done_files = plan(days, manifest)
    log.info("%d jours demandés, %d à traiter, %d fichiers à télécharger, %d déjà terminés",
             len(days), todo_days, files, done_files)  # fmt: skip
    if args.dry_run or not files:
        return 0

    from lakehouse.spark_session import build_spark_session
    from ops.memory import peak_memory

    spark = build_spark_session("gdelt-backfill")
    spark.sparkContext.setLogLevel("WARN")
    exit_code = 0
    try:
        writer = SparkBronzeWriter(spark)

        def report(r: DayResult) -> None:
            mem = peak_memory(spark)
            log.info(
                "%s : %d/%d fichiers publiés (%d absents, %d en échec), %d lignes (%d insérées), "
                "%.2f s/fichier téléchargé, écriture %.1f s, zips %.1f Mo, pic JVM %.0f Mo (tas %.0f)",
                r.day, r.published, r.planned, r.missing, r.failed, r.rows, r.rows_inserted,
                r.seconds_per_file, r.write_seconds, r.zip_bytes / 1e6,
                mem["jvm_rss_peak_mb"] or 0, mem["jvm_heap_peak_mb"],
            )  # fmt: skip

        results = run(days, manifest=manifest, store=RawStore(settings), writer=writer,
                      pause=args.pause, max_days=args.max_days, on_day=report)  # fmt: skip
        failed = sum(r.failed for r in results)
        if failed:
            log.warning("%d fichier(s) en échec : relancer la même commande les reprendra", failed)
            exit_code = 1
        log.info("mémoire (pics) : %s", peak_memory(spark))
    finally:
        spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
