"""Export gold -> fichiers du contrat du site (meta.json + 4 Parquet).

Usage :
    python transform/export.py --db transform/target/gold.duckdb --out build/gold/data

Lit les tables construites par dbt (marts) et écrit exactement le contrat de web/CONTRACT.md :
`meta.json`, `supplier_scores.parquet`, `supplier_events.parquet`, `chokepoint_scores.parquet`,
`supplier_routes.parquet`. Valide les règles du contrat avant de rendre la main (échec bruyant
plutôt qu'un export silencieusement faux).
"""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

SCORE_VERSION = "v1"
PORTFOLIO_LABEL = "FICTIF - Entreprise démo"

RISK_LEVELS = [
    {"id": "surveillance", "label_fr": "Surveillance", "min": 0, "max": 40, "alert": False},
    {"id": "alerte", "label_fr": "Alerte", "min": 40, "max": 70, "alert": True},
    {"id": "critique", "label_fr": "Critique", "min": 70, "max": 100, "alert": True},
]
COMPONENTS = [
    {
        "id": "component_e",
        "label_fr": "Événements locaux",
        "description_fr": (
            "Événements négatifs (grèves et blocages, embargos et saisies, violences et attaques) "
            "à moins de 150 km de la ville du fournisseur, au-delà de ce qui est normal pour cette "
            "ville sur les 28 jours précédents."
        ),
    },
    {
        "id": "component_t",
        "label_fr": "Ton des médias",
        "description_fr": (
            "Ton des articles près de la ville plus négatif que d'habitude (28 jours précédents)."
        ),
    },
    {
        "id": "component_r",
        "label_fr": "Événements sur la route",
        "description_fr": (
            "Événements négatifs dans le rayon d'un détroit de la route d'approvisionnement, au-delà "
            "de la normale, hors événements déjà comptés près de la ville. Poids plus faible que les "
            "événements locaux : les grandes villes voisines d'un détroit font du bruit."
        ),
    },
    {
        "id": "criticality",
        "label_fr": "Criticité",
        "description_fr": (
            "Fournisseur unique et part des achats. Elle amplifie le score (de 60 % à 100 % de "
            "l'aléa), elle ne le déclenche pas."
        ),
    },
]

SUPPLIER_SCORES_SQL = """
select supplier_id, name, category, category_label, city, country, iso2, region,
       cast(lat as double) lat, cast(lon as double) lon, sole_source,
       cast(purchase_share_pct as double) purchase_share_pct, label,
       cast(risk_score as double) risk_score, risk_level,
       cast(component_e as double) component_e, cast(component_t as double) component_t,
       cast(component_r as double) component_r, cast(criticality as double) criticality,
       cast(score_local as double) score_local, cast(score_route as double) score_route,
       cast(events_local as integer) events_local, cast(events_route as integer) events_route,
       top_driver
from supplier_scores order by supplier_id
"""
SUPPLIER_EVENTS_SQL = """
select supplier_id, link_type, chokepoint_id, cast(distance_km as double) distance_km,
       cast(global_event_id as bigint) global_event_id, cast(event_date as date) event_date,
       cast(fetched_at as timestamp) fetched_at, action_geo_name,
       cast(action_geo_type as integer) action_geo_type, cast(lat as double) lat,
       cast(lon as double) lon, event_code, event_root_code, cast(quad_class as integer) quad_class,
       cast(goldstein_scale as double) goldstein_scale, cast(avg_tone as double) avg_tone,
       cast(num_mentions as integer) num_mentions, cast(num_sources as integer) num_sources,
       cast(num_articles as integer) num_articles, source_url, src_file_url,
       cast(contribution as double) contribution, event_family, event_family_label_fr,
       cast(llm_category as varchar) llm_category, cast(llm_summary as varchar) llm_summary
from supplier_events order by supplier_id, contribution desc nulls last, global_event_id
"""
CHOKEPOINT_SCORES_SQL = """
select chokepoint_id, name_fr, name_en, cast(lat as double) lat, cast(lon as double) lon,
       cast(radius_km as double) radius_km, cast(risk_score as double) risk_score, risk_level,
       cast(events_recent as integer) events_recent,
       cast(suppliers_exposed as integer) suppliers_exposed,
       cast(purchase_share_exposed_pct as double) purchase_share_exposed_pct,
       cast(sole_source_exposed as integer) sole_source_exposed
from chokepoint_scores order by chokepoint_id
"""
SUPPLIER_ROUTES_SQL = """
select supplier_id, cast("position" as integer) as "position", chokepoint_id
from supplier_routes order by supplier_id, "position"
"""
PARQUET_FILES = {
    "supplier_scores.parquet": SUPPLIER_SCORES_SQL,
    "supplier_events.parquet": SUPPLIER_EVENTS_SQL,
    "chokepoint_scores.parquet": CHOKEPOINT_SCORES_SQL,
    "supplier_routes.parquet": SUPPLIER_ROUTES_SQL,
}


def iso(value) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_meta(con: duckdb.DuckDBPyConnection, generated_at: datetime) -> dict:
    day_start, day_end, window_start, window_end = con.execute(
        "select day_start, day_end, window_start, window_end from score_window"
    ).fetchone()
    batches, events = con.execute(
        "select coalesce(sum(batches), 0), coalesce(sum(events), 0) from stg_coverage "
        "where day between ? and ?",
        [day_start, day_end],
    ).fetchone()
    (latest_fetched,) = con.execute(
        "select max(src_fetched_at) from stg_events where batch_day between ? and ? and is_recent",
        [day_start, day_end],
    ).fetchone()
    return {
        "schema_version": 1,
        "is_sample": False,
        "sample_notice": None,
        "generated_at": iso(generated_at),
        # Fenêtre en heure d'OBSERVATION : téléchargement pour le flux, étiquette du lot GDELT
        # pour l'historique chargé en lot (exception du 2026-10-09, voir CLAUDE.md).
        "window_start": iso(window_start),
        "window_end": iso(window_end),
        "latest_fetched_at": iso(latest_fetched),
        "gdelt_batches": int(batches),
        "events_total": int(events),
        "score_version": SCORE_VERSION,
        "risk_levels": RISK_LEVELS,
        "components": COMPONENTS,
        "portfolio_label": PORTFOLIO_LABEL,
    }


def validate(out: Path) -> list[str]:
    """Règles du contrat (web/CONTRACT.md). Rend la liste des violations, vide si tout va bien."""
    problems: list[str] = []
    con = duckdb.connect()
    read = lambda name: f"read_parquet('{out / name}')"
    one = lambda sql: con.execute(sql).fetchone()[0]

    meta = json.loads((out / "meta.json").read_text(encoding="utf-8"))
    if meta["schema_version"] != 1:
        problems.append("meta.schema_version != 1")
    if meta["is_sample"] is not False:
        problems.append("meta.is_sample doit être false pour un export réel")
    if len(meta["components"]) != 4:
        problems.append("meta.components doit avoir 4 entrées")

    n = one(f"select count(*) from {read('supplier_scores.parquet')}")
    if n != 100:
        problems.append(f"supplier_scores : {n} lignes, 100 attendues")
    if one(f"select count(*) from {read('supplier_scores.parquet')} where risk_score is null") > 0:
        problems.append("supplier_scores : risk_score NULL interdit")
    if one(
        f"select count(*) from {read('supplier_scores.parquet')} where label != '{PORTFOLIO_LABEL}'"
    ):
        problems.append("supplier_scores : label FICTIF manquant")
    bad = one(
        f"select count(*) from {read('supplier_scores.parquet')} where not ("
        "(risk_score > 70 and risk_level = 'critique') or "
        "(risk_score >= 40 and risk_score <= 70 and risk_level = 'alerte') or "
        "(risk_score < 40 and risk_level = 'surveillance'))"
    )
    if bad:
        problems.append(f"supplier_scores : {bad} niveaux incohérents avec les seuils 40/70")
    total = one(f"select round(sum(purchase_share_pct), 2) from {read('supplier_scores.parquet')}")
    if total != 100.0:
        problems.append(f"supplier_scores : parts des achats = {total}, 100,00 attendu")

    ev = read("supplier_events.parquet")
    both = one(
        f"select count(*) from {ev} a join {ev} b on a.supplier_id = b.supplier_id and "
        "a.global_event_id = b.global_event_id and a.link_type = 'city' and b.link_type = 'chokepoint'"
    )
    if both:
        problems.append(f"supplier_events : {both} événements comptés à la fois ville et détroit")
    dup = one(
        f"select count(*) - count(distinct (supplier_id, global_event_id, link_type, "
        f"coalesce(chokepoint_id, ''))) from {ev}"
    )
    if dup:
        problems.append(f"supplier_events : {dup} lignes en double sur la clé du contrat")
    if one(
        f"select count(*) from {ev} where (link_type = 'chokepoint') != (chokepoint_id is not null)"
    ):
        problems.append(
            "supplier_events : chokepoint_id renseigné si et seulement si link_type = chokepoint"
        )
    off_route = one(
        f"select count(*) from {ev} e where e.link_type = 'chokepoint' and not exists "
        f"(select 1 from {read('supplier_routes.parquet')} r "
        "where r.supplier_id = e.supplier_id and r.chokepoint_id = e.chokepoint_id)"
    )
    if off_route:
        problems.append(f"supplier_events : {off_route} détroits hors de la route du fournisseur")
    if one(f"select count(*) from {ev} where action_geo_type not in (3, 4)"):
        problems.append("supplier_events : action_geo_type hors 3 et 4")
    if one(f"select count(*) from {ev} where quad_class not in (3, 4)"):
        problems.append("supplier_events : événement non négatif exporté")

    ck = read("chokepoint_scores.parquet")
    if one(f"select count(*) from {ck}") != 16:
        problems.append("chokepoint_scores : 16 lignes attendues")
    return problems


def export(db: Path, out: Path, generated_at: datetime | None = None) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    meta = build_meta(con, generated_at or datetime.now(UTC))
    for name, sql in PARQUET_FILES.items():
        con.execute(f"copy ({sql}) to '{out / name}' (format parquet, compression snappy)")
    (out / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", "utf-8")
    problems = validate(out)
    if problems:
        raise SystemExit("Export invalide :\n- " + "\n- ".join(problems))
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, default=Path("transform/target/gold.duckdb"))
    parser.add_argument("--out", type=Path, default=Path("build/gold/data"))
    args = parser.parse_args(argv)
    meta = export(args.db, args.out)
    print(
        f"export OK -> {args.out} (fenêtre {meta['window_start']} -> {meta['window_end']}, "
        f"{meta['events_total']} événements, {meta['gdelt_batches']} lots)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
