"""Génère web/public/data/ : fichiers d'EXEMPLE conformes à web/CONTRACT.md.

Ce qui est réel : le portefeuille (fictif mais généré), les détroits et villes
(Wikidata), et les événements (un vrai cycle GDELT, fixture du dépôt, avec son URL
source). Ce qui ne l'est pas : le score, qui n'existe pas encore côté backend. La
formule ci-dessous est une illustration, déclarée comme telle dans meta.json.

Usage, depuis la racine du dépôt (pyarrow requis) :
    PYTHONPATH=. python web/scripts/make_sample.py
"""

from __future__ import annotations

import csv
import io
import json
import math
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from streaming.gdelt_schema import GDELT_EVENT_COLUMNS

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/gdelt"
OUT = ROOT / "web/public/data"

BATCH_ZIP = "20261008171500.export.CSV.zip"
# README des fixtures : téléchargé le 2026-10-08 à 17:11 UTC.
FETCHED_AT = datetime(2026, 10, 8, 17, 11, tzinfo=UTC)
FILE_URL = f"http://data.gdeltproject.org/gdeltv2/{BATCH_ZIP}"

CITY_RADIUS_KM = 150.0  # docs/geo-join.md
PRECISE_GEO_TYPES = {"3", "4"}  # villes seulement, pas les centroïdes
RECENT_GAP_DAYS = (-1, 2)  # silver/recency.py

# Illustration seulement. Voir SAMPLE_NOTICE.
LEVELS = [
    {"id": "low", "label_fr": "Faible", "min": 0, "max": 25},
    {"id": "medium", "label_fr": "Modéré", "min": 25, "max": 50},
    {"id": "high", "label_fr": "Élevé", "min": 50, "max": 75},
    {"id": "critical", "label_fr": "Critique", "min": 75, "max": 100},
]
SAMPLE_NOTICE = (
    "EXEMPLE : portefeuille fictif et événements GDELT réels d'un seul lot de 15 minutes "
    "(fixture du dépôt), mais le score est une formule d'illustration, pas le score v1."
)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(a))


def level_of(score: float) -> str:
    for lv in LEVELS:
        if score < lv["max"]:
            return lv["id"]
    return LEVELS[-1]["id"]


def illustrative_component(n: int) -> float:
    return round(100 * (1 - math.exp(-n / 3)), 1)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_events() -> list[dict]:
    with zipfile.ZipFile(FIXTURE / BATCH_ZIP) as z:
        raw = z.read(z.namelist()[0]).decode("utf-8")
    events = []
    for line in raw.splitlines():
        parts = line.split("\t")
        if len(parts) != len(GDELT_EVENT_COLUMNS):
            continue  # la silver mettrait la ligne en quarantaine
        e = dict(zip(GDELT_EVENT_COLUMNS, parts, strict=True))
        if e["action_geo_type"] not in PRECISE_GEO_TYPES:
            continue
        try:
            e["_lat"], e["_lon"] = float(e["action_geo_lat"]), float(e["action_geo_long"])
            day = datetime.strptime(e["sql_date"], "%Y%m%d").date()
        except ValueError:
            continue
        if (0.0, 0.0) == (e["_lat"], e["_lon"]):
            continue
        gap = (FETCHED_AT.date() - day).days
        if not RECENT_GAP_DAYS[0] <= gap <= RECENT_GAP_DAYS[1]:
            continue
        e["_day"] = day
        events.append(e)
    return events


def int_or_none(s: str) -> int | None:
    return int(s) if s.strip() else None


def float_or_none(s: str) -> float | None:
    return float(s) if s.strip() else None


def main() -> None:
    suppliers = read_csv(ROOT / "portfolio/suppliers.csv")
    routes = read_csv(ROOT / "portfolio/supplier_routes.csv")
    chokepoints = read_csv(ROOT / "ref/chokepoints.csv")
    events = load_events()
    total_lines = sum(1 for _ in zipfile.ZipFile(FIXTURE / BATCH_ZIP).open(f"{BATCH_ZIP[:-4]}", "r"))

    route_of: dict[str, list[str]] = {}
    for r in sorted(routes, key=lambda r: (r["supplier_id"], int(r["position"]))):
        route_of.setdefault(r["supplier_id"], []).append(r["chokepoint_id"])

    def event_row(e: dict, supplier_id: str, link: str, cp: str | None, dist: float) -> dict:
        return {
            "supplier_id": supplier_id,
            "link_type": link,
            "chokepoint_id": cp,
            "distance_km": round(dist, 1),
            "global_event_id": int(e["global_event_id"]),
            "event_date": e["_day"],
            "fetched_at": FETCHED_AT.replace(tzinfo=None),
            "action_geo_name": e["action_geo_full_name"] or None,
            "action_geo_type": int(e["action_geo_type"]),
            "lat": e["_lat"],
            "lon": e["_lon"],
            "event_code": e["event_code"] or None,
            "event_root_code": e["event_root_code"] or None,
            "quad_class": int_or_none(e["quad_class"]),
            "goldstein_scale": float_or_none(e["goldstein_scale"]),
            "avg_tone": float_or_none(e["avg_tone"]),
            "num_mentions": int_or_none(e["num_mentions"]),
            "num_sources": int_or_none(e["num_sources"]),
            "num_articles": int_or_none(e["num_articles"]),
            "source_url": e["source_url"] or None,
            "src_file_url": FILE_URL,
            "contribution": None,
            "llm_category": None,
            "llm_summary": None,
        }

    event_rows: list[dict] = []
    local_count: dict[str, int] = {}
    route_count: dict[str, int] = {}
    cp_events: dict[str, int] = {c["chokepoint_id"]: 0 for c in chokepoints}
    cp_by_id = {c["chokepoint_id"]: c for c in chokepoints}

    for s in suppliers:
        slat, slon = float(s["lat"]), float(s["lon"])
        for e in events:
            d = haversine_km(slat, slon, e["_lat"], e["_lon"])
            if d <= CITY_RADIUS_KM:
                event_rows.append(event_row(e, s["supplier_id"], "city", None, d))
                local_count[s["supplier_id"]] = local_count.get(s["supplier_id"], 0) + 1
    for c in chokepoints:
        clat, clon, rad = float(c["lat"]), float(c["lon"]), float(c["radius_km"])
        for e in events:
            d = haversine_km(clat, clon, e["_lat"], e["_lon"])
            if d > rad:
                continue
            cp_events[c["chokepoint_id"]] += 1
            for s in suppliers:
                if c["chokepoint_id"] in route_of.get(s["supplier_id"], []):
                    event_rows.append(event_row(e, s["supplier_id"], "chokepoint", c["chokepoint_id"], d))
                    route_count[s["supplier_id"]] = route_count.get(s["supplier_id"], 0) + 1

    score_rows = []
    for s in suppliers:
        sid = s["supplier_id"]
        nl, nr = local_count.get(sid, 0), route_count.get(sid, 0)
        sl, sr = illustrative_component(nl), illustrative_component(nr)
        score = round(min(100.0, 0.5 * sl + 0.5 * sr), 1)
        names = sorted({cp_by_id[x]["name_fr"] for x in route_of.get(sid, []) if any(
            r["supplier_id"] == sid and r["chokepoint_id"] == x for r in event_rows)})
        parts = []
        if nl:
            parts.append(f"{nl} événement(s) à moins de {CITY_RADIUS_KM:.0f} km de {s['city']}")
        if nr:
            parts.append(f"{nr} événement(s) près de la route ({', '.join(names)})")
        score_rows.append({
            "supplier_id": sid, "name": s["name"], "category": s["category"],
            "category_label": s["category_label"], "city": s["city"], "country": s["country"],
            "iso2": s["iso2"], "region": s["region"], "lat": float(s["lat"]), "lon": float(s["lon"]),
            "sole_source": s["sole_source"] == "true", "purchase_share_pct": float(s["purchase_share_pct"]),
            "label": s["label"], "risk_score": score, "risk_level": level_of(score),
            "score_local": sl, "score_route": sr, "events_local": nl, "events_route": nr,
            "top_driver": "; ".join(parts) or None,
        })

    share = {s["supplier_id"]: float(s["purchase_share_pct"]) for s in suppliers}
    sole = {s["supplier_id"]: s["sole_source"] == "true" for s in suppliers}
    cp_rows = []
    for c in chokepoints:
        cid = c["chokepoint_id"]
        exposed = [sid for sid, r in route_of.items() if cid in r]
        n = cp_events[cid]
        cp_rows.append({
            "chokepoint_id": cid, "name_fr": c["name_fr"], "name_en": c["name_en"],
            "lat": float(c["lat"]), "lon": float(c["lon"]), "radius_km": float(c["radius_km"]),
            "risk_score": illustrative_component(n),
            "risk_level": level_of(illustrative_component(n)),
            "events_recent": n, "suppliers_exposed": len(exposed),
            "purchase_share_exposed_pct": round(sum(share[x] for x in exposed), 2),
            "sole_source_exposed": sum(sole[x] for x in exposed),
        })

    route_rows = [
        {"supplier_id": r["supplier_id"], "position": int(r["position"]), "chokepoint_id": r["chokepoint_id"]}
        for r in routes
    ]

    OUT.mkdir(parents=True, exist_ok=True)
    for name, rows in [
        ("supplier_scores", score_rows), ("supplier_events", event_rows),
        ("chokepoint_scores", cp_rows), ("supplier_routes", route_rows),
    ]:
        pq.write_table(pa.Table.from_pylist(rows), OUT / f"{name}.parquet", compression="zstd")

    meta = {
        "schema_version": 1,
        "is_sample": True,
        "sample_notice": SAMPLE_NOTICE,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_start": FETCHED_AT.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": FETCHED_AT.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_fetched_at": FETCHED_AT.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "gdelt_batches": 1,
        "events_total": total_lines,
        "score_version": "exemple-0",
        "risk_levels": LEVELS,
        "portfolio_label": "FICTIF - Entreprise démo",
    }
    (OUT / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    scores = sorted(r["risk_score"] for r in score_rows)
    print(f"événements retenus (types 3/4, récents) : {len(events)} sur {total_lines}")
    print(f"lignes explicatives : {len(event_rows)}")
    print(f"fournisseurs avec score > 0 : {sum(s > 0 for s in scores)} / {len(scores)} ; max {scores[-1]}")
    print("par niveau :", {lv['id']: sum(r['risk_level'] == lv['id'] for r in score_rows) for lv in LEVELS})
    print("détroits avec événements :", {k: v for k, v in cp_events.items() if v})


if __name__ == "__main__":
    main()
