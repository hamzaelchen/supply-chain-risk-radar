"""Chargement historique : planification, reprise, politesse, ordre des écritures (sans réseau)."""

import urllib.error
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise

import pytest

from ingestion import gdelt
from ingestion import gdelt_backfill as bf
from ingestion.gdelt_producer import BatchNotFound
from ingestion.manifest import FAILED, MISSING_FINAL, PUBLISHED, ManifestEntry

from .test_gdelt_producer import ZIP, FakeStore

DAY = date(2026, 2, 3)
LINES_IN_ZIP = 1775
N = 6  # lots par jour dans ces tests : 96 fichiers de 1 775 lignes par test rendraient la suite très lente


@pytest.fixture(autouse=True)
def short_days(monkeypatch):
    monkeypatch.setattr(bf, "LABELS_PER_DAY", N)


class FakeManifest:
    def __init__(self, log=None):
        self.entries: list[ManifestEntry] = []
        self.log = log if log is not None else []

    def attempts(self, source, modes=None):
        out = {}
        for e in self.entries:
            if e.source == source and (modes is None or e.ingestion_mode in modes):
                out.setdefault(e.gdelt_batch_label_ts, []).append((e.status, e.fetched_at))
        return out

    def record_many(self, entries):
        self.log.append(("manifest", len(entries)))
        self.entries.extend(entries)


class FakeWriter:
    def __init__(self, log=None, fail=False):
        self.days, self.log, self.fail = [], log if log is not None else [], fail

    def write_day(self, day, rows):
        self.log.append(("bronze", len(rows)))
        if self.fail:
            raise RuntimeError("disque plein")
        self.days.append((day, rows))
        return len(rows), len(rows)


def fetcher(missing=(), broken=()):
    calls = []

    def fetch(url):
        calls.append(url)
        if any(url.endswith(f"{m}.export.CSV.zip") for m in missing):
            raise BatchNotFound(url)
        if any(url.endswith(f"{b}.export.CSV.zip") for b in broken):
            raise urllib.error.URLError("réseau coupé")
        return ZIP

    fetch.calls = calls
    return fetch


def one_day(**kwargs):
    log = []
    manifest, writer, store = FakeManifest(log), FakeWriter(log), FakeStore()
    sleeps = []
    fetch = kwargs.pop("fetch", fetcher())
    result = bf.process_day(
        DAY, done=kwargs.pop("done", set()), fetch=fetch, store=store, writer=writer,
        manifest=manifest, pause=kwargs.pop("pause", 1.0), sleep=sleeps.append,
        now=lambda: datetime(2026, 10, 9, 13, 0, tzinfo=UTC), **kwargs,
    )  # fmt: skip
    return result, manifest, writer, store, sleeps, log, fetch


def test_parse_range_and_days_are_inclusive_sorted_and_merged():
    assert bf.parse_range("2026-02-01:2026-02-03") == (date(2026, 2, 1), date(2026, 2, 3))
    assert bf.parse_range("2026-02-01") == (date(2026, 2, 1), date(2026, 2, 1))
    with pytest.raises(ValueError):
        bf.parse_range("2026-02-03:2026-02-01")
    days = bf.days_in([(date(2026, 2, 3), date(2026, 2, 4)), (date(2026, 2, 1), date(2026, 2, 3))])
    assert days == [date(2026, 2, d) for d in (1, 2, 3, 4)]


def test_a_day_has_96_utc_labels_every_15_minutes(monkeypatch):
    monkeypatch.setattr(bf, "LABELS_PER_DAY", 96)
    labels = bf.labels_of(DAY)
    assert len(labels) == 96 and labels[0] == datetime(2026, 2, 3, tzinfo=UTC)
    assert labels[-1] == datetime(2026, 2, 3, 23, 45, tzinfo=UTC)
    assert {b - a for a, b in pairwise(labels)} == {timedelta(minutes=15)}


def test_a_day_is_downloaded_archived_written_once_then_recorded_once():
    result, manifest, writer, store, _sleeps, log, fetch = one_day()

    assert (result.planned, result.published, result.missing, result.failed) == (N, N, 0, 0)
    assert len(fetch.calls) == N and len(store.objects) == N
    assert result.rows == N * LINES_IN_ZIP and len(writer.days) == 1
    # Ordre : le bronze AVANT le manifeste (un arrêt entre les deux rejoue le jour sans perte).
    assert log == [("bronze", N * LINES_IN_ZIP), ("manifest", N)]
    assert {e.ingestion_mode for e in manifest.entries} == {"backfill"}
    assert {e.status for e in manifest.entries} == {PUBLISHED}
    assert all(e.md5_verified is None and e.kafka_topic is None for e in manifest.entries)


def test_rows_carry_file_url_label_download_time_archive_uri_and_line_numbers():
    _, _, writer, _, _, _, _ = one_day()
    _, rows = writer.days[0]
    first = rows[0]
    raw_line, url, label, fetched_at, uri, number = first
    assert raw_line.startswith(raw_line.split("\t")[0] + "\t") and number == 1
    assert url == gdelt.export_file_for_batch(datetime(2026, 2, 3, tzinfo=UTC)).url
    assert label == datetime(2026, 2, 3, tzinfo=UTC) and fetched_at.year == 2026
    assert uri.startswith("s3://lakehouse/raw/gdelt/events/date=2026-02-03/")
    numbers = [r[5] for r in rows[:LINES_IN_ZIP]]
    assert numbers == list(range(1, LINES_IN_ZIP + 1))  # recommence à 1 pour chaque fichier
    assert rows[LINES_IN_ZIP][5] == 1


def test_there_is_a_pause_between_files_but_not_after_the_last_one():
    _, _, _, _, sleeps, _, fetch = one_day(pause=1.5)
    assert sleeps == [1.5] * (N - 1) and len(fetch.calls) == N


def test_downloads_are_strictly_sequential_and_in_label_order():
    _, _, _, _, _, _, fetch = one_day()
    assert fetch.calls == [gdelt.export_file_for_batch(label).url for label in bf.labels_of(DAY)]


def test_a_missing_old_file_is_final_and_a_failing_one_is_recorded_without_stopping_the_day():
    result, manifest, _, _, _, _, _ = one_day(
        fetch=fetcher(missing=["20260203001500"], broken=["20260203003000"])
    )
    assert (result.published, result.missing, result.failed) == (N - 2, 1, 1)
    by_label = {e.gdelt_batch_label_ts.strftime("%H%M"): e for e in manifest.entries}
    assert by_label["0015"].status == MISSING_FINAL and by_label["0030"].status == FAILED
    assert "URLError" in by_label["0030"].detail


def test_nothing_is_recorded_when_the_bronze_write_fails_so_the_day_is_replayed():
    log = []
    manifest, writer = FakeManifest(log), FakeWriter(log, fail=True)
    with pytest.raises(RuntimeError, match="disque plein"):
        bf.process_day(DAY, done=set(), fetch=fetcher(), store=FakeStore(), writer=writer,
                       manifest=manifest, sleep=lambda s: None)  # fmt: skip
    assert manifest.entries == []


def test_finished_labels_are_skipped_whatever_their_mode():
    done = set(bf.labels_of(DAY)[: N - 2])
    result, manifest, _, _, _, _, fetch = one_day(done=done)
    assert result.planned == 2 and len(fetch.calls) == 2 and len(manifest.entries) == 2


def test_run_resumes_skips_loaded_days_and_honours_max_days():
    manifest, writer, store = FakeManifest(), FakeWriter(), FakeStore()
    days = [date(2026, 2, d) for d in (1, 2, 3)]
    common = {"manifest": manifest, "store": store, "writer": writer, "pause": 0.5}

    first = bf.run(days, fetch=fetcher(), max_days=2, **common)
    assert [r.day for r in first] == days[:2]

    second = bf.run(days, fetch=fetcher(), **common)  # reprend au 3e jour
    assert [r.day for r in second] == days[2:]
    assert bf.run(days, fetch=fetcher(), **common) == []  # tout est chargé : rien à faire


def test_a_loaded_day_does_not_count_against_max_days():
    manifest, writer, store = FakeManifest(), FakeWriter(), FakeStore()
    days = [date(2026, 2, 1), date(2026, 2, 2)]
    common = {"manifest": manifest, "store": store, "writer": writer, "pause": 0.5}
    bf.run(days[:1], fetch=fetcher(), **common)
    results = bf.run(days, fetch=fetcher(), max_days=1, **common)
    assert [r.day for r in results] == [days[1]]


def test_plan_counts_days_and_files_left():
    manifest = FakeManifest()
    days = [date(2026, 2, 1), date(2026, 2, 2)]
    assert bf.plan(days, manifest) == (2, 2 * N, 0)
    bf.run(
        days[:1],
        fetch=fetcher(),
        manifest=manifest,
        store=FakeStore(),
        writer=FakeWriter(),
        pause=0.5,
    )
    assert bf.plan(days, manifest) == (1, N, N)


def http_error(code):
    return urllib.error.HTTPError("http://x", code, "x", {}, None)


def test_rate_limiting_is_waited_out_with_growing_delays_then_succeeds():
    attempts, waits = [], []

    def fetch(url):
        attempts.append(url)
        if len(attempts) <= 2:
            raise http_error(429)
        return b"ok"

    assert bf.polite_fetch("http://x", fetch=fetch, sleep=waits.append) == b"ok"
    assert waits == list(bf.RATE_LIMIT_WAITS[:2]) and waits[1] > waits[0]


def test_rate_limiting_that_never_ends_gives_up():
    waits = []
    with pytest.raises(urllib.error.HTTPError):
        bf.polite_fetch(
            "http://x", fetch=lambda u: (_ for _ in ()).throw(http_error(429)), sleep=waits.append
        )
    assert waits == list(bf.RATE_LIMIT_WAITS)


def test_other_http_errors_are_not_retried_here():
    waits = []
    with pytest.raises(urllib.error.HTTPError):
        bf.polite_fetch(
            "http://x", fetch=lambda u: (_ for _ in ()).throw(http_error(403)), sleep=waits.append
        )
    assert waits == []


def test_a_pause_shorter_than_half_a_second_is_refused():
    with pytest.raises(SystemExit):
        bf.main(["--range", "2026-02-01", "--pause", "0.1"])
