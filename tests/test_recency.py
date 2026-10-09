"""Fenêtre temporelle : événements récents et rétrospectifs."""

from datetime import UTC, date, datetime, timedelta

import pytest

from silver.recency import RECENT_MAX_LAG_DAYS, is_recent_event, lag_days, observed_at

pytestmark = pytest.mark.spark
FETCHED = datetime(2026, 10, 9, 10, 0, 0, tzinfo=UTC)
TODAY = date(2026, 10, 9)


def evaluate(spark, rows):
    """rows : (sql_date, src_fetched_at) pour des lignes du flux."""
    return evaluate_full(spark, [(d, f, "stream", f) for d, f in rows])


def evaluate_full(spark, rows):
    """rows : (sql_date, src_fetched_at, ingestion_mode, src_gdelt_batch_label_ts)."""
    df = spark.createDataFrame(
        rows,
        "sql_date date, src_fetched_at timestamp, ingestion_mode string, "
        "src_gdelt_batch_label_ts timestamp",
    )
    out = df.select(lag_days().alias("lag"), is_recent_event().alias("recent"))
    return [(r.lag, r.recent) for r in out.collect()]


def test_lag_and_window(spark):
    event_dates = [TODAY - timedelta(days=d) for d in (0, 1, 2, 3, 30, -1, -2)]
    got = evaluate(spark, [(d, FETCHED) for d in event_dates])
    assert got == [
        (0, True), (1, True), (2, True), (3, False), (30, False),
        (-1, True),   # daté du lendemain (date locale en avance sur l'UTC) : toléré
        (-2, False),  # deux jours dans le futur : incohérent
    ]  # fmt: skip


def test_the_lag_uses_the_download_date_not_the_batch_label(spark):
    # Lot étiqueté le 10-08 à 23:45 mais téléchargé le 10-09 (rattrapage) : l'écart part du 10-09.
    fetched = datetime(2026, 10, 9, 9, 16, 0, tzinfo=UTC)
    assert evaluate(spark, [(date(2026, 10, 8), fetched)]) == [(1, True)]


def test_a_ten_year_old_event_is_retrospective(spark):
    old = date(2016, 10, 10)
    assert evaluate(spark, [(old, FETCHED)]) == [((TODAY - old).days, False)]


def test_the_threshold_is_two_days():
    assert RECENT_MAX_LAG_DAYS == 2


LOADED = datetime(2026, 10, 9, 13, 0, 0, tzinfo=UTC)  # le jour où l'historique a été chargé


def test_a_backfill_row_is_measured_from_the_batch_label_not_from_the_load_time(spark):
    label = datetime(2026, 2, 3, 10, 0, 0, tzinfo=UTC)
    rows = [
        (date(2026, 2, 3), LOADED, "backfill", label),  # même jour que son lot : récent
        (date(2026, 2, 2), LOADED, "backfill", label),  # veille : récent
        (date(2026, 1, 20), LOADED, "backfill", label),  # 14 jours avant son lot : rétrospectif
    ]
    assert evaluate_full(spark, rows) == [(0, True), (1, True), (14, False)]


def test_the_same_dates_would_all_be_retrospective_if_measured_from_the_load_time(spark):
    label = datetime(2026, 2, 3, 10, 0, 0, tzinfo=UTC)
    rows = [
        (date(2026, 2, 3), LOADED, "stream", label)
    ]  # mode flux : l'heure de chargement fait foi
    ((lag, recent),) = evaluate_full(spark, rows)
    assert lag == 248 and recent is False


def test_a_row_without_ingestion_mode_is_a_stream_row(spark):
    label = datetime(2026, 10, 9, 9, 0, 0, tzinfo=UTC)
    fetched = datetime(2026, 10, 9, 9, 10, 0, tzinfo=UTC)
    assert evaluate_full(spark, [(date(2026, 10, 9), fetched, None, label)]) == [(0, True)]
    df = spark.createDataFrame(
        [(fetched, None, label)],
        "src_fetched_at timestamp, ingestion_mode string, src_gdelt_batch_label_ts timestamp",
    )
    assert df.select(observed_at().alias("t")).first().t.replace(tzinfo=UTC) == fetched
