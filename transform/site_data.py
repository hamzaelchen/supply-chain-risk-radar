"""Données du site : un dossier par instantané + index.json (voir web/CONTRACT.md, « Instantanés »).

Usage :
    python transform/site_data.py --out web/public/data

Instantanés produits (chacun = les cinq fichiers du contrat, validés par export.py) :
- `today` : le dernier jour couvert (transform/target/gold.duckdb) ;
- `ormuz-2026-03-02` : le score tel qu'il aurait été le 2 mars 2026 (transform/target/ormuz.duckdb,
  `dbt build --vars '{as_of_day: 2026-03-02}'`).
Le texte affiché sous le sélecteur est composé de chiffres lus dans la base : jamais écrit à la main,
et il décrit ce que les données contiennent sans en donner la cause.
"""

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(1, str(Path(__file__).resolve().parent.parent))  # ingestion.* en mode script
import export

HERE = Path(__file__).resolve().parent
CITIES_CSV = HERE.parent / "ref" / "cities.csv"
HUB_CITY_ID = "casablanca"  # siège de l'Entreprise démo : point d'arrivée de toutes les routes
LIVE_DB = HERE / "target" / "gold.duckdb"
EPISODE_DB = HERE / "target" / "ormuz.duckdb"
EPISODE_ID = "ormuz-2026-03-02"
DBT_RUN_RESULTS = HERE / "target" / "run_results.json"
PIPELINE_STATS_VERSION = 1
MONTHS = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
          "octobre", "novembre", "décembre"]  # fmt: skip


def french_day(day: date) -> str:
    return f"{day.day} {MONTHS[day.month - 1]} {day.year}"


def n(value: int) -> str:
    return f"{value:,}".replace(",", " ")  # espace fine insécable serait mieux, mais restons simple


def as_of(db: Path) -> date:
    con = duckdb.connect(str(db), read_only=True)
    return con.execute("select day_end from score_window").fetchone()[0]


def live_description(db: Path) -> str:
    con = duckdb.connect(str(db), read_only=True)
    (day_end,) = con.execute("select day_end from score_window").fetchone()
    alerts, total = con.execute(
        "select count(*) filter (where risk_level != 'surveillance'), count(*) from supplier_scores"
    ).fetchone()
    who = (
        "Aucun fournisseur n'est en alerte ni critique."
        if alerts == 0
        else f"{alerts} fournisseur{'s' if alerts > 1 else ''} sur {total} en alerte ou critique."
    )
    return (
        f"Score du dernier jour entièrement couvert par les fichiers GDELT, le {french_day(day_end)} "
        f"(le jour en cours n'est pas noté). {who}"
    )


def episode_description(db: Path, chokepoint: str = "hormuz") -> str:
    con = duckdb.connect(str(db), read_only=True)
    (day_end,) = con.execute("select day_end from score_window").fetchone()
    name, radius = con.execute(
        "select name_fr, radius_km from chokepoint_scores where chokepoint_id = ?", [chokepoint]
    ).fetchone()

    def week_events(start: str) -> int:
        return int(
            con.execute(
                "select coalesce(sum(n_all), 0) from int_chokepoint_day where chokepoint_id = ? "
                "and day between cast(? as date) and cast(? as date) + 6",
                [chokepoint, start, start],
            ).fetchone()[0]
        )

    w1, w2, w3 = week_events("2026-02-09"), week_events("2026-02-23"), week_events("2026-03-02")
    exposed, alerts = con.execute(
        "select count(*), count(*) filter (where s.risk_level != 'surveillance') "
        "from supplier_scores s join supplier_routes r using (supplier_id) where r.chokepoint_id = ?",
        [chokepoint],
    ).fetchone()
    all_alerts, total = con.execute(
        "select count(*) filter (where risk_level != 'surveillance'), count(*) from supplier_scores"
    ).fetchone()
    return (
        f"Score tel qu'il aurait été calculé le {french_day(day_end)}. "
        f"Dans le rayon de {radius:.0f} km du {name[0].lower() + name[1:]}, GDELT localise {n(w1)} événements "
        f"la semaine du 9 février, {n(w2)} celle du 23 février et {n(w3)} celle du 2 mars. "
        f"{exposed} fournisseurs fictifs ont ce détroit sur leur route ; {alerts} d'entre eux sont en "
        f"alerte ou critiques ce jour-là ({all_alerts} fournisseurs sur {total} dans tout le portefeuille). "
        "Le site montre ce que les articles recensent ; il n'établit pas la cause."
    )


def lake_stats(db: Path) -> dict:
    """Chiffres de la page d'accueil, lus dans la base (jamais écrits à la main).

    `days_covered` ne compte que les jours entièrement couverts (assez de lots GDELT) ;
    `events_total` compte tous les événements lus, y compris ceux d'un jour partiel.
    """
    con = duckdb.connect(str(db), read_only=True)
    events, batches, days, first_day, last_day = con.execute(
        "select sum(events), sum(batches), count(*) filter (where covered), min(day), max(day) from stg_coverage"
    ).fetchone()
    (suppliers,) = con.execute("select count(*) from supplier_scores").fetchone()
    (chokepoints,) = con.execute("select count(*) from chokepoint_scores").fetchone()
    return {
        "events_total": int(events), "gdelt_batches": int(batches), "days_covered": int(days),
        "first_day": first_day.isoformat(), "last_day": last_day.isoformat(),
        "suppliers": int(suppliers), "chokepoints": int(chokepoints),
    }  # fmt: skip


def hub(csv_path: Path = CITIES_CSV, city_id: str = HUB_CITY_ID) -> dict:
    """Ville d'arrivée des routes, avec la source de ses coordonnées (référentiel Wikidata CC0)."""
    with csv_path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row["city_id"] == city_id:
                return {
                    "name": row["name"], "country": row["country"],
                    "lat": float(row["lat"]), "lon": float(row["lon"]),
                    "source": row["coord_source"], "source_url": row["coord_source_url"],
                }  # fmt: skip
    raise KeyError(f"ville « {city_id} » absente de {csv_path}")


def build(out: Path, live_db: Path = LIVE_DB, episode_db: Path = EPISODE_DB) -> dict:
    snapshots = [
        {
            "id": "today", "label_fr": "Aujourd'hui", "kind": "live",
            "as_of": as_of(live_db).isoformat(), "description_fr": live_description(live_db),
            "_db": live_db,
        },
        {
            "id": EPISODE_ID, "label_fr": "Épisode d'Ormuz (2 mars 2026)", "kind": "episode",
            "as_of": as_of(episode_db).isoformat(), "description_fr": episode_description(episode_db),
            "_db": episode_db,
        },
    ]  # fmt: skip
    for snap in snapshots:
        export.export(snap.pop("_db"), out / snap["id"])
    (out / "preview.json").write_text(
        json.dumps(episode_preview(out), ensure_ascii=False, separators=(",", ":")) + "\n", "utf-8"
    )
    index = {
        "schema_version": 1, "default": EPISODE_ID,
        "stats": lake_stats(live_db), "hub": hub(), "snapshots": snapshots,
    }  # fmt: skip
    (out / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", "utf-8")
    return index


# --- Aperçu de la carte pour la page d'accueil (preview.json) --------------------------------------
# La page d'accueil ne charge pas DuckDB-WASM : elle lit ce petit JSON, tiré des Parquet de
# l'instantané « épisode » déjà exportés (donc les mêmes chiffres que le radar).


def episode_preview(out: Path, snapshot_id: str = EPISODE_ID, chokepoint: str = "hormuz") -> dict:
    folder = out / snapshot_id

    def rows(sql: str, name: str) -> list[dict]:
        con = duckdb.connect()
        cur = con.execute(sql.format(f=(folder / name).as_posix()))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    suppliers = rows(
        "select supplier_id, name, city, country, lat, lon, risk_score, risk_level, "
        "sole_source, purchase_share_pct from read_parquet('{f}') order by supplier_id",
        "supplier_scores.parquet",
    )
    chokepoints = rows(
        "select chokepoint_id, name_fr, name_en, lat, lon, radius_km, risk_score, risk_level, "
        "events_recent, suppliers_exposed, purchase_share_exposed_pct "
        "from read_parquet('{f}') order by chokepoint_id",
        "chokepoint_scores.parquet",
    )
    routes = rows(
        "select supplier_id, \"position\", chokepoint_id from read_parquet('{f}') "
        'order by supplier_id, "position"',
        "supplier_routes.parquet",
    )
    meta = json.loads((folder / "meta.json").read_text("utf-8"))
    alert_ids = {lv["id"] for lv in meta["risk_levels"] if lv["alert"]}
    alerts = [s for s in suppliers if s["risk_level"] in alert_ids]
    focus = next((c for c in chokepoints if c["chokepoint_id"] == chokepoint), None)
    return {
        "schema_version": 1,
        "snapshot_id": snapshot_id,
        "window_end": meta["window_end"],
        "risk_levels": meta["risk_levels"],
        "suppliers": suppliers,
        "chokepoints": chokepoints,
        "routes": routes,
        "kpis": {
            "suppliers": len(suppliers),
            "alerts": len(alerts),
            "alert_purchase_share_pct": round(sum(s["purchase_share_pct"] for s in alerts), 2),
            "sole_source_alerts": sum(1 for s in alerts if s["sole_source"]),
            "events_total": meta["events_total"],
            "focus_chokepoint": focus,
        },
    }


# --- Chiffres du pipeline (pipeline_stats.json) ---------------------------------------------------
# Lus dans le manifeste d'ingestion, les totaux des snapshots Iceberg, ops.pipeline_runs, la base
# gold, les résultats du dernier `dbt build` et les fichiers exportés. Rien n'est écrit à la main :
# une valeur introuvable est absente du fichier, jamais remplacée par une valeur plausible.


def metric(metric_id: str, label_fr: str, value, kind: str = "count") -> dict:
    return {"id": metric_id, "label_fr": label_fr, "value": value, "kind": kind}


def batch_outcomes(manifest_rows: list[dict]) -> dict:
    """Une issue par lot GDELT : celle de la dernière tentative (le manifeste garde chaque essai)."""
    last: dict = {}
    for row in manifest_rows:
        key = row["gdelt_batch_label_ts"]
        if key not in last or row["fetched_at"] >= last[key]["fetched_at"]:
            last[key] = row
    published = [r for r in last.values() if r["status"] == "published"]
    return {
        "status": Counter(r["status"] for r in last.values()),
        "mode": Counter((r.get("ingestion_mode") or "stream") for r in published),
        "days": sorted({r["gdelt_batch_label_ts"].astimezone(UTC).date() for r in published}),
    }


def compute_pipeline_stats(
    *,
    manifest_rows: list[dict],
    runs: list[dict],
    totals: dict[str, int],
    gold_counts: dict[str, int],
    dbt: dict | None,
    exported: dict,
    generated_at: datetime,
) -> dict:
    """Assemble pipeline_stats.json à partir de ce qui a été lu ; fonction pure, testée."""
    out = batch_outcomes(manifest_rows)
    status, mode, days = out["status"], out["mode"], out["days"]

    def runs_of(pipeline: str) -> list[dict]:
        return [r for r in runs if r["pipeline"] == pipeline]

    silver_runs, prox_runs = runs_of("silver.gdelt_events"), runs_of("silver.proximity")

    def run_metrics(prefix: str, rs: list[dict]) -> list[dict]:
        ok = [r for r in rs if r["status"] == "succeeded"]
        return [
            metric(f"{prefix}_runs", "Passages réussis", len(ok)),
            metric(f"{prefix}_runs_failed", "Passages en échec", len(rs) - len(ok)),
            metric(
                f"{prefix}_duration",
                "Durée cumulée des passages",
                round(sum(r["duration_seconds"] for r in ok), 1),
                "seconds",
            ),
        ]

    bronze = [
        metric("rows", "Lignes brutes en bronze", totals["bronze.gdelt_events"]),
        metric("batches", "Lots GDELT de 15 min publiés", status.get("published", 0)),
        metric("batches_stream", "dont via Kafka (flux)", mode.get("stream", 0)),
        metric("batches_backfill", "dont en chargement historique", mode.get("backfill", 0)),
        metric("days_loaded", "Jours chargés", len(days)),
        metric("first_day", "Premier jour", days[0].isoformat() if days else None, "date"),
        metric("last_day", "Dernier jour", days[-1].isoformat() if days else None, "date"),
        metric(
            "batches_missing",
            "Lots absents chez GDELT",
            status.get("missing_final", 0) + status.get("missing", 0),
        ),
        metric("batches_failed", "Lots en échec", status.get("failed", 0)),
    ]
    silver = [
        metric("rows", "Lignes en silver", totals["silver.gdelt_events"]),
        metric("quarantine", "Lignes en quarantaine", totals["silver.gdelt_events_quarantine"]),
        metric(
            "duplicates",
            "Doublons éliminés",
            sum(r["duplicates_eliminated"] or 0 for r in silver_runs),
        ),
        metric(
            "conflicting_duplicates",
            "Doublons au contenu différent",
            sum(r["conflicting_duplicates"] or 0 for r in silver_runs),
        ),
        metric(
            "unaccounted",
            "Lignes du bronze sans destination (ni silver ni quarantaine)",
            totals["bronze.gdelt_events"]
            - totals["silver.gdelt_events"]
            - totals["silver.gdelt_events_quarantine"],
        ),
        *run_metrics("silver", silver_runs),
    ]
    proximity = [
        metric(
            "supplier_pairs",
            "Paires événement-fournisseur (150 km)",
            totals["silver.event_supplier_proximity"],
        ),
        metric(
            "chokepoint_pairs",
            "Paires événement-détroit",
            totals["silver.event_chokepoint_proximity"],
        ),
        *run_metrics("proximity", prox_runs),
    ]
    gold = [
        metric("suppliers", "Fournisseurs notés", gold_counts["supplier_scores"]),
        metric("chokepoints", "Détroits notés", gold_counts["chokepoint_scores"]),
    ]
    if dbt:
        gold += [
            metric("dbt_tests_passed", "Tests dbt réussis", dbt["tests_passed"]),
            metric("dbt_tests_total", "Tests dbt exécutés", dbt["tests_total"]),
            metric("dbt_duration", "Durée du dernier dbt build", dbt["elapsed"], "seconds"),
        ]
    export_ = [
        metric("snapshots", "Instantanés publiés", exported["snapshots"]),
        metric("files", "Fichiers Parquet", exported["parquet_files"]),
        metric("bytes", "Poids des Parquet", exported["parquet_bytes"], "bytes"),
    ]
    stages = {
        "bronze": bronze,
        "silver": silver,
        "proximity": proximity,
        "gold": gold,
        "export": export_,
    }
    return {
        "schema_version": PIPELINE_STATS_VERSION,
        "generated_at": generated_at.astimezone(UTC).isoformat(timespec="seconds"),
        "stages": [
            {"id": k, "metrics": [m for m in v if m["value"] is not None]}
            for k, v in stages.items()
        ],
    }


def read_dbt_results(path: Path = DBT_RUN_RESULTS) -> dict | None:
    if not path.exists():
        return None
    data = json.loads(path.read_text("utf-8"))
    tests = [r for r in data["results"] if r["unique_id"].startswith("test.")]
    return {
        "tests_total": len(tests),
        "tests_passed": sum(1 for r in tests if r["status"] == "pass"),
        "elapsed": round(float(data["elapsed_time"]), 1),
    }


def read_exported(out: Path, snapshot_ids: list[str]) -> dict:
    files = [p for sid in snapshot_ids for p in (out / sid).glob("*.parquet")]
    return {
        "snapshots": len(snapshot_ids),
        "parquet_files": len(files),
        "parquet_bytes": sum(p.stat().st_size for p in files),
    }


def read_lake() -> tuple[list[dict], list[dict], dict[str, int]]:
    """Manifeste, ops.pipeline_runs et totaux des snapshots courants. Demande `make up`."""
    from pyiceberg.catalog import load_catalog

    from ingestion.config import Settings

    s = Settings.from_env()
    catalog = load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": s.rest_uri,
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.s3_access_key,
            "s3.secret-access-key": s.s3_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
    manifest = (
        catalog.load_table("bronze.ingestion_manifest")
        .scan(selected_fields=("gdelt_batch_label_ts", "status", "fetched_at", "ingestion_mode"))
        .to_arrow()
        .to_pylist()
    )
    runs = catalog.load_table("ops.pipeline_runs").scan().to_arrow().to_pylist()
    totals = {}
    for name in (
        "bronze.gdelt_events",
        "silver.gdelt_events",
        "silver.gdelt_events_quarantine",
        "silver.event_supplier_proximity",
        "silver.event_chokepoint_proximity",
    ):
        snap = catalog.load_table(name).current_snapshot()
        totals[name] = int(snap.summary.additional_properties["total-records"])
    return manifest, runs, totals


def pipeline_stats(out: Path, snapshot_ids: list[str], live_db: Path = LIVE_DB) -> dict:
    manifest, runs, totals = read_lake()
    con = duckdb.connect(str(live_db), read_only=True)
    gold_counts = {
        t: int(con.execute(f"select count(*) from {t}").fetchone()[0])
        for t in ("supplier_scores", "chokepoint_scores")
    }
    return compute_pipeline_stats(
        manifest_rows=manifest,
        runs=runs,
        totals=totals,
        gold_counts=gold_counts,
        dbt=read_dbt_results(),
        exported=read_exported(out, snapshot_ids),
        generated_at=datetime.now(UTC),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("web/public/data"))
    parser.add_argument(
        "--skip-pipeline-stats",
        action="store_true",
        help="ne pas relire le lac (sans `make up`) : pipeline_stats.json existant conservé",
    )
    args = parser.parse_args(argv)
    index = build(args.out)
    if not args.skip_pipeline_stats:
        stats = pipeline_stats(args.out, [s["id"] for s in index["snapshots"]])
        (args.out / "pipeline_stats.json").write_text(
            json.dumps(stats, ensure_ascii=False, indent=2) + "\n", "utf-8"
        )
        print(f"pipeline_stats.json : {sum(len(s['metrics']) for s in stats['stages'])} mesures")
    for snap in index["snapshots"]:
        print(f"{snap['id']} (au {snap['as_of']}) : {snap['description_fr'][:110]}…")
    print(f"index.json : défaut = {index['default']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
