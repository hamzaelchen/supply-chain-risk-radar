"""Fonctions pures GDELT, testées sur un vrai cycle (voir tests/fixtures/gdelt/README.md)."""

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ingestion import gdelt

FIXTURES = Path(__file__).parent / "fixtures" / "gdelt"
SLOT = datetime(2026, 10, 8, 17, 15, tzinfo=UTC)
ZIP = FIXTURES / "20261008171500.export.CSV.zip"


def slot(n: int) -> datetime:
    return SLOT + n * gdelt.SLOT


def test_parse_lastupdate_reads_three_files_in_utc():
    files = gdelt.parse_lastupdate((FIXTURES / "lastupdate.txt").read_text())
    assert [f.url.rsplit(".", 3)[1] for f in files] == ["export", "mentions", "gkg"]
    assert all(f.slot_ts == SLOT and f.slot_ts.utcoffset() == timedelta(0) for f in files)


def test_latest_export_matches_downloaded_zip():
    export = gdelt.latest_export((FIXTURES / "lastupdate.txt").read_text())
    body = ZIP.read_bytes()
    assert export.size_bytes == len(body)
    assert export.md5 == hashlib.md5(body).hexdigest()


def test_latest_export_rejects_file_without_export():
    with pytest.raises(ValueError):
        gdelt.latest_export("")


def test_slot_from_url_rejects_unknown_format():
    with pytest.raises(ValueError):
        gdelt.slot_from_url("http://data.gdeltproject.org/gdeltv2/lastupdate.txt")


def test_export_file_for_slot_round_trips():
    file = gdelt.export_file_for_slot(SLOT)
    assert file.url == gdelt.latest_export((FIXTURES / "lastupdate.txt").read_text()).url
    assert gdelt.slot_from_url(file.url) == SLOT
    assert file.md5 is None


def test_iter_event_lines_keeps_raw_tsv_lines():
    lines = list(gdelt.iter_event_lines(ZIP.read_bytes()))
    assert len(lines) == 1775
    assert all(line.count(b"\t") == 60 for line in lines)
    assert not any(line.endswith((b"\n", b"\r")) for line in lines)


def test_event_key_is_unique_global_event_id():
    keys = [gdelt.event_key(line) for line in gdelt.iter_event_lines(ZIP.read_bytes())]
    assert all(key.isdigit() for key in keys)
    assert len(set(keys)) == len(keys)


def test_raw_object_key_is_partitioned_by_utc_date():
    key = gdelt.raw_object_key(gdelt.export_file_for_slot(SLOT))
    assert key == "raw/gdelt/events/date=2026-10-08/20261008171500.export.CSV.zip"


def test_slots_first_run_takes_only_latest():
    assert gdelt.slots_to_process(SLOT, None, set(), 96) == ([SLOT], 0)


def test_slots_nothing_to_do_when_latest_done():
    assert gdelt.slots_to_process(SLOT, SLOT, {SLOT}, 96) == ([], 0)


def test_slots_fill_gap_oldest_first_and_skip_done():
    todo, skipped = gdelt.slots_to_process(slot(4), slot(0), {slot(0), slot(2)}, 96)
    assert todo == [slot(1), slot(3), slot(4)]
    assert skipped == 0


def test_slots_beyond_backfill_window_are_counted_not_processed():
    todo, skipped = gdelt.slots_to_process(slot(10), slot(0), {slot(0)}, max_backfill=2)
    assert todo == [slot(8), slot(9), slot(10)]
    assert skipped == 7
