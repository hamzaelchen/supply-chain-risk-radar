"""Fenêtre temporelle : événements récents et rétrospectifs."""

from datetime import UTC, date, datetime, timedelta

import pytest

from silver.recency import RECENT_MAX_LAG_DAYS, is_recent_event, lag_days

pytestmark = pytest.mark.spark
FETCHED = datetime(2026, 10, 9, 10, 0, 0, tzinfo=UTC)
TODAY = date(2026, 10, 9)


def evaluate(spark, rows):
    df = spark.createDataFrame(rows, "sql_date date, src_fetched_at timestamp")
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
