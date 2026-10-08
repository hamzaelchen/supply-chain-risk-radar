"""Logique du cycle, avec des doublures à la place de GDELT, MinIO, Kafka et Iceberg."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion import gdelt
from ingestion.gdelt_producer import MISSING_RETRIES, SOURCE, SlotNotFound, run_cycle
from ingestion.manifest import FAILED, MISSING, MISSING_FINAL, PUBLISHED

FIXTURES = Path(__file__).parent / "fixtures" / "gdelt"
LASTUPDATE = (FIXTURES / "lastupdate.txt").read_bytes()
ZIP = (FIXTURES / "20261008171500.export.CSV.zip").read_bytes()
LATEST = datetime(2026, 10, 8, 17, 15, tzinfo=UTC)
NOW = datetime(2026, 10, 8, 17, 20, tzinfo=UTC)


def slot(n: int) -> datetime:
    return LATEST + n * gdelt.SLOT


def url(n: int) -> str:
    return gdelt.export_file_for_slot(slot(n)).url


class FakeManifest:
    def __init__(self, entries=()):
        self.entries = list(entries)

    def attempts(self, source):
        result = {}
        for e in self.entries:
            if e.source == source:
                result.setdefault(e.slot_ts, []).append((e.status, e.fetched_at))
        return result

    def record(self, entry):
        self.entries.append(entry)


class FakeStore:
    def __init__(self):
        self.objects = {}

    def put(self, key, body, *, source_url, fetched_at, md5):
        self.objects[key] = (body, source_url, fetched_at)
        return f"s3://lakehouse/{key}"


class FakePublisher:
    def __init__(self, fail=False):
        self.messages = []
        self.fail = fail

    def publish(self, records, headers):
        if self.fail:
            raise RuntimeError("broker injoignable")
        batch = [(key, value, headers) for key, value in records]
        self.messages.extend(batch)
        return len(batch)


def fetcher(pages: dict[str, bytes]):
    def fetch(u):
        if u not in pages:
            raise SlotNotFound(u)
        return pages[u]

    return fetch


def cycle(pages, manifest, store=None, publisher=None, now=NOW, **kwargs):
    store = store or FakeStore()
    publisher = publisher or FakePublisher()
    results = run_cycle(fetcher(pages), manifest, store, publisher, now=lambda: now, **kwargs)
    return results, store, publisher


def manifest_with_gap():
    """Manifeste qui connaît le créneau -2 : le créneau -1 est donc attendu."""
    manifest = FakeManifest()
    cycle(PAGES, manifest)
    manifest.entries[0] = replace(manifest.entries[0], slot_ts=slot(-2), source_url=url(-2))
    return manifest


PAGES = {gdelt.LASTUPDATE_URL: LASTUPDATE, url(0): ZIP}


def test_first_cycle_archives_publishes_and_records():
    manifest = FakeManifest()
    results, store, publisher = cycle(PAGES, manifest)

    assert [(r.slot_ts, r.status, r.record_count) for r in results] == [(LATEST, PUBLISHED, 1775)]
    key = "raw/gdelt/events/date=2026-10-08/20261008171500.export.CSV.zip"
    assert store.objects[key] == (ZIP, url(0), NOW)

    assert len(publisher.messages) == 1775
    msg_key, value, headers = publisher.messages[0]
    assert value.startswith(msg_key + b"\t")
    assert headers == {
        "source_url": url(0),
        "slot_ts": "2026-10-08T17:15:00+00:00",
        "fetched_at": "2026-10-08T17:20:00+00:00",
        "raw_object_uri": f"s3://lakehouse/{key}",
    }

    (entry,) = manifest.entries
    assert (entry.source, entry.status, entry.record_count) == (SOURCE, PUBLISHED, 1775)
    assert entry.md5_verified is True
    assert entry.zip_bytes == len(ZIP)


def test_second_cycle_on_same_slot_is_a_no_op():
    manifest = FakeManifest()
    cycle(PAGES, manifest)
    results, store, publisher = cycle(PAGES, manifest)

    assert results == []
    assert publisher.messages == []
    assert store.objects == {}
    assert len(manifest.entries) == 1


def test_missing_slot_is_logged_recorded_and_does_not_stop_the_cycle(caplog):
    manifest = manifest_with_gap()

    with caplog.at_level("WARNING"):
        results, _, publisher = cycle(PAGES, manifest)

    assert [(r.slot_ts, r.status) for r in results] == [(slot(-1), MISSING), (LATEST, PUBLISHED)]
    assert len(publisher.messages) == 1775
    assert "absent chez GDELT" in caplog.text


def test_missing_slot_is_not_retried_before_fifteen_minutes():
    manifest = manifest_with_gap()
    cycle(PAGES, manifest)

    results, _, _ = cycle(PAGES, manifest, now=NOW + timedelta(minutes=14))
    assert results == []


def test_missing_slot_is_retried_for_one_hour_then_becomes_final():
    manifest = manifest_with_gap()
    cycle(PAGES, manifest)  # premier 404

    statuses = []
    for n in range(1, MISSING_RETRIES + 1):
        results, _, _ = cycle(PAGES, manifest, now=NOW + n * gdelt.SLOT)
        statuses.append([(r.slot_ts, r.status) for r in results])

    assert MISSING_RETRIES == 4
    assert statuses == [[(slot(-1), MISSING)]] * 3 + [[(slot(-1), MISSING_FINAL)]]
    history = [e.status for e in manifest.entries if e.slot_ts == slot(-1)]
    assert history == [MISSING] * 4 + [MISSING_FINAL]

    # Définitif : plus aucune tentative, même bien plus tard.
    results, _, _ = cycle(PAGES, manifest, now=NOW + timedelta(days=1) - gdelt.SLOT)
    assert results == []


def test_missing_slot_published_late_by_gdelt_is_recovered_on_retry():
    manifest = manifest_with_gap()
    cycle(PAGES, manifest)  # premier 404

    late = {**PAGES, url(-1): ZIP}
    results, _, publisher = cycle(late, manifest, now=NOW + gdelt.SLOT)

    assert [(r.slot_ts, r.status) for r in results] == [(slot(-1), PUBLISHED)]
    assert len(publisher.messages) == 1775
    results, _, _ = cycle(late, manifest, now=NOW + 2 * gdelt.SLOT)
    assert results == []


def test_md5_mismatch_publishes_nothing_and_is_retried():
    manifest = FakeManifest()
    corrupted = {**PAGES, url(0): ZIP[:-1] + bytes([ZIP[-1] ^ 0xFF])}
    results, store, publisher = cycle(corrupted, manifest)

    assert [r.status for r in results] == [FAILED]
    assert publisher.messages == [] and store.objects == {}
    assert manifest.entries[0].md5_verified is False

    results, _, publisher = cycle(PAGES, manifest)
    assert [r.status for r in results] == [PUBLISHED]
    assert len(publisher.messages) == 1775


def test_kafka_failure_is_recorded_as_failed_and_retried():
    manifest = FakeManifest()
    results, _, _ = cycle(PAGES, manifest, publisher=FakePublisher(fail=True))
    assert [r.status for r in results] == [FAILED]
    assert manifest.entries[0].record_count is None

    results, _, _ = cycle(PAGES, manifest)
    assert [r.status for r in results] == [PUBLISHED]


def test_backfilled_slot_has_no_md5_to_verify():
    manifest = manifest_with_gap()
    cycle({**PAGES, url(-1): ZIP}, manifest)

    backfilled = next(e for e in manifest.entries if e.slot_ts == slot(-1))
    assert backfilled.status == PUBLISHED
    assert backfilled.md5_verified is None
