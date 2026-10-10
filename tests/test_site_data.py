"""Chiffres de la page d'accueil : lus dans la base et le référentiel, jamais écrits à la main."""

import duckdb
import pytest

from transform import site_data


def test_lake_stats_are_read_from_the_database(tmp_path):
    db = tmp_path / "g.duckdb"
    con = duckdb.connect(str(db))
    con.execute("""create table stg_coverage as select * from (values
        (date '2026-01-04', 96, 70000, true), (date '2026-01-05', 90, 65000, true),
        (date '2026-01-07', 12, 1000, false)) t(day, batches, events, covered)""")
    con.execute("create table supplier_scores as select range i from range(100)")
    con.execute("create table chokepoint_scores as select range i from range(16)")
    con.close()
    assert site_data.lake_stats(db) == {
        "events_total": 136000, "gdelt_batches": 198, "days_covered": 2,
        "first_day": "2026-01-04", "last_day": "2026-01-07", "suppliers": 100, "chokepoints": 16,
    }  # fmt: skip


def test_hub_comes_from_the_reference_with_its_source():
    hub = site_data.hub()
    assert hub["name"] == "Casablanca"
    assert 33.0 < hub["lat"] < 34.0 and -8.0 < hub["lon"] < -7.0
    assert "wikidata.org" in hub["source_url"]


def test_unknown_hub_is_an_error():
    with pytest.raises(KeyError):
        site_data.hub(city_id="atlantide")
