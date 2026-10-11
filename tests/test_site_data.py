"""Chiffres de la page d'accueil : lus dans la base et le référentiel, jamais écrits à la main."""

from datetime import UTC, datetime

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


def _stats(**over):
    def ts(hour):
        return datetime(2026, 1, 4, hour, tzinfo=UTC)

    manifest = [
        # lot 1 : un échec puis une publication (la dernière tentative compte)
        {"gdelt_batch_label_ts": ts(0), "status": "failed", "fetched_at": ts(1), "ingestion_mode": "backfill"},
        {"gdelt_batch_label_ts": ts(0), "status": "published", "fetched_at": ts(2), "ingestion_mode": "backfill"},
        {"gdelt_batch_label_ts": ts(1), "status": "published", "fetched_at": ts(2), "ingestion_mode": None},
        {"gdelt_batch_label_ts": ts(2), "status": "missing_final", "fetched_at": ts(3), "ingestion_mode": None},
    ]  # fmt: skip
    run = {"status": "succeeded", "duration_seconds": 10.0, "duplicates_eliminated": 2,
           "conflicting_duplicates": 0, "pipeline": "silver.gdelt_events"}  # fmt: skip
    totals = {
        "bronze.gdelt_events": 100,
        "silver.gdelt_events": 97,
        "silver.gdelt_events_quarantine": 3,
        "silver.event_supplier_proximity": 40,
        "silver.event_chokepoint_proximity": 5,
    }
    kw = {
        "manifest_rows": manifest,
        "runs": [run, {**run, "status": "failed", "duplicates_eliminated": None}],
        "totals": totals,
        "gold_counts": {"supplier_scores": 100, "chokepoint_scores": 16},
        "dbt": None,
        "exported": {"snapshots": 2, "parquet_files": 8, "parquet_bytes": 1000},
        "generated_at": datetime(2026, 10, 11, tzinfo=UTC),
    }
    kw.update(over)
    stats = site_data.compute_pipeline_stats(**kw)
    return {s["id"]: {m["id"]: m["value"] for m in s["metrics"]} for s in stats["stages"]}


def test_pipeline_stats_count_one_outcome_per_batch():
    bronze = _stats()["bronze"]
    assert (
        bronze["batches"] == 2 and bronze["batches_backfill"] == 1 and bronze["batches_stream"] == 1
    )
    assert bronze["batches_missing"] == 1 and bronze["batches_failed"] == 0
    assert bronze["days_loaded"] == 1 and bronze["first_day"] == "2026-01-04"


def test_pipeline_stats_silver_balance_comes_from_totals():
    silver = _stats()["silver"]
    assert (silver["rows"], silver["quarantine"], silver["unaccounted"]) == (97, 3, 0)
    assert (
        silver["duplicates"] == 2
        and silver["silver_runs"] == 1
        and silver["silver_runs_failed"] == 1
    )


def test_pipeline_stats_never_invent_a_missing_value():
    gold = _stats()["gold"]
    assert "dbt_tests_passed" not in gold  # pas de run_results.json : la mesure est absente
