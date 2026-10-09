"""Silver de bout en bout sur le catalogue local (make up), dans des tables jetables :
lecture incrémentale, MERGE idempotent, quarantaine, doublons, rejeu après interruption."""

from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F

from silver import gdelt_events as silver
from streaming.bronze_gdelt_events import ensure_table, to_bronze

from .test_bronze_transform import LINES, kafka_df
from .test_silver_transform import damaged

pytestmark = [pytest.mark.spark, pytest.mark.integration]


@pytest.fixture
def tables(spark, infra, sandbox):
    name = sandbox.name()
    t = {
        "bronze": f"{sandbox.namespace}.bronze_{name}",
        "silver": f"{sandbox.namespace}.silver_{name}",
        "quarantine": f"{sandbox.namespace}.quarantine_{name}",
        "runs": f"{sandbox.namespace}.pipeline_runs_{name}",
    }
    ensure_table(spark, t["bronze"])
    return t


def append_bronze(spark, table, lines, ingested_at):
    """Un lot bronze arrivé à `ingested_at` (comme un micro-lot du streaming)."""
    df = to_bronze(kafka_df(spark, lines)).withColumn(
        "ingested_at", F.lit(ingested_at).cast("timestamp")
    )
    df.writeTo(table).append()


def run(spark, t):
    return silver.run(spark, bronze=t["bronze"], silver=t["silver"], quarantine=t["quarantine"])


def count(spark, table):
    return spark.table(table).count()


T1 = datetime(2026, 10, 9, 8, 0, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 9, 8, 15, 0, tzinfo=UTC)


def test_first_run_splits_valid_and_quarantined_rows(spark, tables):
    broken = [damaged(LINES[100], goldstein_scale="abc"), b"\t".join(LINES[101].split(b"\t")[:10])]
    append_bronze(spark, tables["bronze"], LINES[:50] + broken, T1)

    stats = run(spark, tables)

    assert (stats.rows_read, stats.valid, stats.quarantined, stats.inserted) == (52, 50, 2, 50)
    assert count(spark, tables["silver"]) == 50
    reasons = {r.reason for r in spark.table(tables["quarantine"]).collect()}
    assert reasons == {"goldstein_scale='abc' : double invalide", "field_count=10 (attendu 61)"}
    assert stats.quarantine_reasons == {"goldstein_scale": 1, "field_count": 1}


def test_rerunning_finds_nothing_new_and_writes_nothing(spark, tables):
    append_bronze(spark, tables["bronze"], LINES[:30], T1)
    run(spark, tables)
    snapshots = spark.table(f"{tables['silver']}.snapshots").count()

    stats = run(spark, tables)

    assert stats.rows_read == 0
    assert count(spark, tables["silver"]) == 30
    assert spark.table(f"{tables['silver']}.snapshots").count() == snapshots


def test_incremental_run_reads_only_new_bronze_rows_and_drops_duplicates(spark, tables):
    append_bronze(spark, tables["bronze"], LINES[:40], T1)
    run(spark, tables)

    # Lot suivant : 10 événements déjà vus (rejeu du producteur), 1 « doublon » au contenu
    # différent, 20 nouveaux, et 1 doublon à l'intérieur du lot.
    changed = damaged(LINES[0], num_mentions="999")
    new = LINES[40:60]
    append_bronze(spark, tables["bronze"], LINES[30:40] + [changed] + new + [new[0]], T2)
    stats = run(spark, tables)

    assert stats.rows_read == 32  # le premier lot n'est pas relu
    assert stats.duplicates_in_batch == 1  # new[0] deux fois dans le même lot
    assert stats.already_in_silver == 11  # 10 rejoués + la copie modifiée de LINES[0]
    assert stats.conflicting_duplicates == 1
    assert stats.inserted == 20
    assert stats.duplicates_eliminated == 12
    assert count(spark, tables["silver"]) == 60
    ids = spark.table(tables["silver"]).select("global_event_id").distinct().count()
    assert ids == 60


def test_first_seen_wins_and_a_conflicting_copy_is_counted(spark, tables):
    original = int(LINES[0].decode().split("\t")[silver.GDELT_EVENT_COLUMNS.index("num_mentions")])
    append_bronze(spark, tables["bronze"], LINES[:5], T1)
    run(spark, tables)

    append_bronze(spark, tables["bronze"], [damaged(LINES[0], num_mentions="999")], T2)
    stats = run(spark, tables)

    assert stats.already_in_silver == 1 and stats.conflicting_duplicates == 1
    assert stats.inserted == 0
    kept = spark.table(tables["silver"]).filter("kafka_offset = 0 AND kafka_partition = 0").first()
    assert kept.num_mentions == original


def test_replaying_the_same_window_after_an_interruption_adds_nothing(spark, tables):
    append_bronze(spark, tables["bronze"], LINES[:40] + [LINES[50][:30]], T1)
    run(spark, tables)
    before = (count(spark, tables["silver"]), count(spark, tables["quarantine"]))

    # Simule un arrêt entre le MERGE et la mise à jour du curseur : il est perdu.
    spark.sql(f"ALTER TABLE {tables['silver']} UNSET TBLPROPERTIES ('{silver.CURSOR_PROPERTY}')")
    stats = run(spark, tables)

    assert stats.rows_read == 41 and stats.inserted == 0  # tout est déjà là
    assert (count(spark, tables["silver"]), count(spark, tables["quarantine"])) == before


def test_every_bronze_row_is_accounted_for(spark, tables):
    broken = [damaged(LINES[100], quad_class="7"), LINES[101][:80]]
    append_bronze(spark, tables["bronze"], LINES[:40] + broken, T1)
    append_bronze(spark, tables["bronze"], LINES[20:50], T2)
    stats = run(spark, tables)

    bronze = count(spark, tables["bronze"])
    assert stats.rows_read == bronze == 72
    assert stats.valid + stats.quarantined == bronze
    assert count(spark, tables["silver"]) + stats.duplicates_eliminated == stats.valid


def micros(moment):
    return int(moment.timestamp() * 1_000_000)


def test_cursor_is_the_newest_ingested_at_actually_read_not_the_clock(spark, tables):
    append_bronze(spark, tables["bronze"], LINES[:20], T1)
    append_bronze(spark, tables["bronze"], LINES[20:30], T2)

    run(spark, tables)

    # T1 et T2 sont dans le passé : si le curseur était l'heure d'exécution, il serait bien plus grand.
    assert silver.read_cursor(spark, tables["silver"]) == micros(T2)


def test_a_bronze_batch_committed_while_silver_runs_is_never_skipped(spark, tables, monkeypatch):
    T3 = datetime(2026, 10, 9, 8, 30, 0, tzinfo=UTC)
    append_bronze(spark, tables["bronze"], LINES[:20], T1)
    late_batch = LINES[20:35]
    real_keep_first_seen = silver.keep_first_seen

    def keep_first_seen_while_streaming_commits(valid):
        # Le job de streaming commite un nouveau lot au milieu du passage de la silver :
        # après la lecture de la fenêtre, avant la mise à jour du curseur.
        append_bronze(spark, tables["bronze"], late_batch, T3)
        return real_keep_first_seen(valid)

    monkeypatch.setattr(silver, "keep_first_seen", keep_first_seen_while_streaming_commits)
    first = run(spark, tables)
    monkeypatch.undo()

    assert first.rows_read == 20 and count(spark, tables["bronze"]) == 35
    assert silver.read_cursor(spark, tables["silver"]) == micros(T1)  # pas T3, pas l'heure
    second = run(spark, tables)
    # Le lot tardif est relu au passage suivant : le curseur n'a jamais dépassé T1. Il peut déjà
    # être en silver (ici le test écrit dans la même session Spark, qui invalide son cache et
    # relit le bronze) : le MERGE ne l'insère alors pas une seconde fois. En production, le
    # streaming est un autre processus et le lot n'arrive qu'à ce second passage.
    assert second.rows_read == 15
    assert count(spark, tables["silver"]) == 35
    ids = spark.table(tables["silver"]).select("global_event_id").distinct().count()
    assert ids == 35
    assert silver.read_cursor(spark, tables["silver"]) == micros(T3)


def run_and_record(spark, t):
    return silver.run_and_record(
        spark,
        bronze=t["bronze"],
        silver=t["silver"],
        quarantine=t["quarantine"],
        runs_table=t["runs"],
    )


def utc(moment):
    """Spark rend des datetimes sans fuseau ; la session est en UTC (spark.sql.session.timeZone)."""
    return None if moment is None else moment.replace(tzinfo=UTC)


def runs(spark, t):
    return spark.table(t["runs"]).orderBy("started_at").collect()


def test_each_run_leaves_one_row_in_ops_pipeline_runs(spark, tables):
    broken = [damaged(LINES[100], goldstein_scale="abc")]
    append_bronze(spark, tables["bronze"], LINES[:30] + broken, T1)
    append_bronze(spark, tables["bronze"], LINES[20:40], T2)  # 10 déjà vus

    run_and_record(spark, tables)
    run_and_record(spark, tables)  # rien de nouveau

    first, second = runs(spark, tables)
    assert first.pipeline == "silver.gdelt_events" and first.status == "succeeded"
    assert (first.rows_read, first.rows_inserted, first.rows_quarantined) == (51, 40, 1)
    assert first.duplicates_eliminated == 10 and first.conflicting_duplicates == 0
    assert first.cursor_before is None and utc(first.cursor_after) == T2
    assert first.duration_seconds > 0 and first.finished_at >= first.started_at
    assert 0 < first.action_geo_usable_pct <= 100
    assert first.error is None and len(first.run_id) == 32

    assert (second.rows_read, second.rows_inserted, second.rows_quarantined) == (0, 0, 0)
    assert second.action_geo_usable_pct is None  # rien lu : pas de pourcentage à inventer
    assert utc(second.cursor_before) == T2 == utc(second.cursor_after)
    assert first.run_id != second.run_id


def test_a_failed_run_is_recorded_with_its_error_and_the_cursor_unchanged(
    spark, tables, monkeypatch
):
    append_bronze(spark, tables["bronze"], LINES[:10], T1)
    run_and_record(spark, tables)
    append_bronze(spark, tables["bronze"], LINES[10:20], T2)

    def boom(*_args, **_kwargs):
        raise RuntimeError("panne simulée")

    monkeypatch.setattr(silver, "keep_first_seen", boom)
    with pytest.raises(RuntimeError, match="panne simulée"):
        run_and_record(spark, tables)
    monkeypatch.undo()

    ok, failed = runs(spark, tables)
    assert failed.status == "failed" and "panne simulée" in failed.error
    assert failed.rows_read is None
    assert utc(failed.cursor_before) == T1 == utc(failed.cursor_after)
    # Le passage suivant reprend où le dernier succès s'était arrêté.
    assert run_and_record(spark, tables).rows_read == 10
    assert ok.status == "succeeded"
