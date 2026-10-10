"""Lac de test pour la CI : un fichier DuckDB qui imite les tables Iceberg lues par dbt.

Usage :
    python transform/ci_fixture.py                      # écrit transform/target/ci_lake.duckdb
    cd transform && dbt build --target ci --vars '{...}' # voir .github/workflows/ci.yml

JEU DE TEST, pas une donnée du pipeline : les 1 775 événements du vrai cycle GDELT de
tests/fixtures sont rejoués sur 8 jours consécutifs (identifiants, dates et horodatages décalés).
Les 7 premiers jours en gardent un sur trois, le dernier les garde tous : une anomalie nette,
pour que les composantes du score ne soient pas toutes nulles. Le portefeuille et les référentiels
sont les vrais fichiers du dépôt ; les paires de proximité sont calculées ici en Python pur.
"""

import math
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ingestion import gdelt
from portfolio import generate as portfolio
from ref import chokepoints
from streaming.gdelt_schema import GDELT_EVENT_COLUMNS

FIXTURE = ROOT / "tests/fixtures/gdelt/20261008171500.export.CSV.zip"
OUT = ROOT / "transform/target/ci_lake.duckdb"
DAYS = 8
SUPPLIER_RADIUS_KM = 150.0
EARTH_RADIUS_KM = 6371.0088
INDEX = {name: i for i, name in enumerate(GDELT_EVENT_COLUMNS)}
BASE_LABEL = datetime(2026, 10, 8, 17, 15, tzinfo=UTC)


def haversine(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (
        math.sin((p2 - p1) / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def load_events() -> list[dict]:
    lines = list(gdelt.iter_event_lines(FIXTURE.read_bytes()))
    events = []
    for line in lines:
        f = line.decode("utf-8", errors="replace").split("\t")
        try:
            events.append(
                {
                    "global_event_id": int(f[INDEX["global_event_id"]]),
                    "sql_date": datetime.strptime(f[INDEX["sql_date"]], "%Y%m%d")
                    .replace(tzinfo=UTC)
                    .date(),
                    "event_code": f[INDEX["event_code"]],
                    "event_root_code": f[INDEX["event_root_code"]],
                    "quad_class": int(f[INDEX["quad_class"]]),
                    "goldstein_scale": float(f[INDEX["goldstein_scale"]]),
                    "avg_tone": float(f[INDEX["avg_tone"]]),
                    "num_mentions": int(f[INDEX["num_mentions"]]),
                    "num_sources": int(f[INDEX["num_sources"]]),
                    "num_articles": int(f[INDEX["num_articles"]]),
                    "source_url": f[INDEX["source_url"]],
                    "action_geo_full_name": f[INDEX["action_geo_full_name"]] or None,
                    "action_geo_type": int(f[INDEX["action_geo_type"]]),
                    "action_geo_lat": float(f[INDEX["action_geo_lat"]])
                    if f[INDEX["action_geo_lat"]]
                    else None,
                    "action_geo_long": float(f[INDEX["action_geo_long"]])
                    if f[INDEX["action_geo_long"]]
                    else None,
                }
            )
        except (ValueError, IndexError):
            continue  # ligne inexploitable : la vraie silver la mettrait en quarantaine
    return events


def replay(events: list[dict]) -> list[dict]:
    out = []
    for k in range(DAYS):
        label = BASE_LABEL + timedelta(days=k)
        file_url = gdelt.export_file_for_batch(label).url
        for i, e in enumerate(events):
            if k < DAYS - 1 and i % 3:
                continue
            out.append(
                {
                    **e,
                    "global_event_id": e["global_event_id"] + k * 10_000_000,
                    "sql_date": e["sql_date"] + timedelta(days=k),
                    "src_fetched_at": label + timedelta(minutes=2),
                    "src_gdelt_batch_label_ts": label,
                    "src_file_url": file_url,
                    "ingestion_mode": "stream",
                }
            )
    return out


def build(path: Path = OUT) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    events = replay(load_events())
    suppliers = portfolio.read_csv(portfolio.SUPPLIERS_CSV)
    routes = portfolio.read_csv(portfolio.ROUTES_CSV)
    chokes = chokepoints.read_csv()

    usable = [e for e in events if e["action_geo_type"] in (3, 4) and e["action_geo_lat"] is not None
              and e["action_geo_long"] is not None]  # fmt: skip
    supplier_pairs = [
        (e["global_event_id"], s["supplier_id"], s["city_id"], d)
        for e in usable
        for s in suppliers
        if (
            d := haversine(
                e["action_geo_lat"], e["action_geo_long"], float(s["lat"]), float(s["lon"])
            )
        )
        <= SUPPLIER_RADIUS_KM
    ]
    choke_pairs = [
        (e["global_event_id"], c["chokepoint_id"], d, float(c["radius_km"]))
        for e in usable
        for c in chokes
        if (
            d := haversine(
                e["action_geo_lat"], e["action_geo_long"], float(c["lat"]), float(c["lon"])
            )
        )
        <= float(c["radius_km"])
    ]

    con = duckdb.connect(str(path))
    con.execute("create schema silver")
    con.execute("""create table silver.gdelt_events (
        global_event_id bigint, sql_date date, event_code varchar, event_root_code varchar,
        quad_class integer, goldstein_scale double, avg_tone double, num_mentions integer,
        num_sources integer, num_articles integer, source_url varchar, src_file_url varchar,
        action_geo_full_name varchar, action_geo_type integer, action_geo_lat double,
        action_geo_long double, src_fetched_at timestamptz, src_gdelt_batch_label_ts timestamptz,
        ingestion_mode varchar)""")
    cols = ["global_event_id", "sql_date", "event_code", "event_root_code", "quad_class", "goldstein_scale",
            "avg_tone", "num_mentions", "num_sources", "num_articles", "source_url", "src_file_url",
            "action_geo_full_name", "action_geo_type", "action_geo_lat", "action_geo_long",
            "src_fetched_at", "src_gdelt_batch_label_ts", "ingestion_mode"]  # fmt: skip
    con.executemany(f"insert into silver.gdelt_events values ({','.join('?' * len(cols))})",
                    [[e[c] for c in cols] for e in events])  # fmt: skip
    con.execute(
        "create table silver.event_supplier_proximity (global_event_id bigint, supplier_id varchar, "
        "city_id varchar, distance_km double)"
    )
    con.executemany("insert into silver.event_supplier_proximity values (?,?,?,?)", supplier_pairs)
    con.execute(
        "create table silver.event_chokepoint_proximity (global_event_id bigint, chokepoint_id varchar, "
        "distance_km double, radius_km double)"
    )
    con.executemany("insert into silver.event_chokepoint_proximity values (?,?,?,?)", choke_pairs)
    con.execute("""create table silver.ref_suppliers (supplier_id varchar, name varchar, legal_form varchar,
        label varchar, buyer varchar, category varchar, category_label varchar, city_id varchar, city varchar,
        country varchar, iso2 varchar, region varchar, lat double, lon double, coord_source_url varchar,
        sole_source boolean, purchase_share_pct decimal(5,2))""")
    con.executemany(
        "insert into silver.ref_suppliers values (" + ",".join("?" * 17) + ")",
        [
            [s[k] for k in portfolio.SUPPLIER_COLUMNS[:15]]
            + [s["sole_source"] == "true", s["purchase_share_pct"]]
            for s in suppliers
        ],
    )
    con.execute(
        "create table silver.ref_supplier_routes (supplier_id varchar, position integer, chokepoint_id varchar)"
    )
    con.executemany(
        "insert into silver.ref_supplier_routes values (?,?,?)",
        [(r["supplier_id"], int(r["position"]), r["chokepoint_id"]) for r in routes],
    )
    con.execute(
        "create table silver.ref_chokepoints (chokepoint_id varchar, name_fr varchar, name_en varchar, "
        "kind varchar, lat double, lon double, radius_km double)"
    )
    con.executemany(
        "insert into silver.ref_chokepoints values (?,?,?,?,?,?,?)",
        [
            (
                c["chokepoint_id"],
                c["name_fr"],
                c["name_en"],
                c["kind"],
                float(c["lat"]),
                float(c["lon"]),
                float(c["radius_km"]),
            )
            for c in chokes
        ],
    )
    con.close()
    return {
        "events": len(events),
        "supplier_pairs": len(supplier_pairs),
        "chokepoint_pairs": len(choke_pairs),
    }


if __name__ == "__main__":
    print(build())
