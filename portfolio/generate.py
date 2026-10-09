"""Portefeuille fournisseurs de l'« Entreprise démo » (Casablanca) : FICTIF, graine fixe.

Usage :
    python -m portfolio.generate generate   # régénère portfolio/*.csv (déterministe)
    python -m portfolio.generate summary    # répartition par pays, catégorie, route
    python -m portfolio.generate load       # charge dans silver.ref_suppliers / ref_supplier_routes

Ce qui est FICTIF : les 100 fournisseurs (noms inventés par assemblage de syllabes, toute
ressemblance avec une société réelle est fortuite), leur catégorie, leur criticité, leur part
des achats. Ce qui est RÉEL : les villes et leurs coordonnées (ref/cities.csv, Wikidata CC0)
et les détroits (ref/chokepoints.csv). Toute ligne porte l'étiquette `FICTIF - Entreprise démo`.

Reproductibilité : mêmes graine, même code, mêmes CSV d'entrée => mêmes 100 fournisseurs,
octet pour octet (un test le vérifie). Les effectifs par région sont des quotas exacts : le
hasard ne décide que de la ville, du nom, de la catégorie et de la criticité.
"""

import csv
import random
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from portfolio.routing import REGION_ROUTES, route_for
from ref import cities as ref_cities

HERE = Path(__file__).parent
SUPPLIERS_CSV = HERE / "suppliers.csv"
ROUTES_CSV = HERE / "supplier_routes.csv"
SUPPLIERS_TABLE = "lakehouse.silver.ref_suppliers"
ROUTES_TABLE = "lakehouse.silver.ref_supplier_routes"

SEED = 20261009  # date de création ; valeur arbitraire mais FIGÉE
LABEL = "FICTIF - Entreprise démo"
BUYER = "Entreprise démo"
N_SUPPLIERS = 100

# Nombre de fournisseurs par région d'origine (somme = 100).
REGION_QUOTAS = {
    "MA": 12,
    "EU_ATLANTIC": 7,
    "EU_NORTH_SEA": 10,
    "EU_BALTIC": 2,
    "EU_MEDITERRANEAN": 10,
    "TR_MARMARA": 5,
    "TR_AEGEAN": 2,
    "BLACK_SEA": 2,
    "EGYPT_MEDITERRANEAN": 2,
    "RED_SEA": 1,
    "PERSIAN_GULF": 11,
    "SOUTH_ASIA": 9,
    "SOUTHEAST_ASIA": 5,
    "CHINA_NORTH": 7,
    "CHINA_SOUTH": 7,
    "KOREA": 2,
    "SOUTHERN_AFRICA": 1,
    "AMERICAS_ATLANTIC": 3,
    "AMERICAS_PACIFIC": 2,
}

CATEGORIES = {
    "electronics": ("Composants électroniques", "Electronics", 0.25),
    "auto": ("Pièces automobiles", "Automotive", 0.15),
    "textile": ("Textile et matières premières", "Textiles", 0.08),
    "packaging": ("Emballage", "Packaging", 0.08),
    "chemicals": ("Produits chimiques", "Chemicals", 0.15),
    "metals": ("Acier et métaux", "Metals", 0.08),
    "machinery": ("Machines et équipements", "Machinery", 0.25),
    "agro": ("Agroalimentaire (intrants)", "Agro", 0.08),
}  # clé : (libellé, mot du nom, probabilité de fournisseur unique)

# Poids relatifs des catégories selon la région (les absentes valent 0,3).
_EU = {"machinery": 3, "auto": 3, "chemicals": 2, "metals": 2, "packaging": 1.5, "electronics": 1.5}
_MED_EAST = {"agro": 3, "chemicals": 2, "metals": 2, "packaging": 1, "textile": 1}
_CHINA = {"electronics": 4, "machinery": 3, "metals": 2, "packaging": 2, "textile": 2, "auto": 2,
          "chemicals": 1.5}  # fmt: skip
CATEGORY_WEIGHTS = {
    "MA": {"auto": 3, "textile": 3, "packaging": 3, "agro": 2, "chemicals": 1, "metals": 1},
    "EU_ATLANTIC": _EU,
    "EU_NORTH_SEA": _EU,
    "EU_BALTIC": _EU,
    "EU_MEDITERRANEAN": {
        "agro": 2.5,
        "auto": 2,
        "machinery": 2,
        "textile": 1.5,
        "packaging": 1.5,
        "chemicals": 1.5,
        "metals": 1,
        "electronics": 1,
    },
    "TR_MARMARA": {"textile": 4, "auto": 3, "metals": 2, "agro": 1.5, "packaging": 1},
    "TR_AEGEAN": {"textile": 4, "auto": 3, "metals": 2, "agro": 1.5, "packaging": 1},
    "BLACK_SEA": _MED_EAST,
    "EGYPT_MEDITERRANEAN": _MED_EAST,
    "RED_SEA": _MED_EAST,
    "PERSIAN_GULF": {"chemicals": 5, "metals": 3, "packaging": 1},
    "SOUTH_ASIA": {
        "textile": 5,
        "chemicals": 2,
        "agro": 2,
        "metals": 1.5,
        "packaging": 1,
        "machinery": 1,
        "electronics": 1,
        "auto": 1,
    },
    "SOUTHEAST_ASIA": {
        "electronics": 4,
        "textile": 3,
        "packaging": 1.5,
        "agro": 1.5,
        "machinery": 1,
        "auto": 1,
        "chemicals": 1,
    },
    "CHINA_NORTH": _CHINA,
    "CHINA_SOUTH": _CHINA,
    "KOREA": {"electronics": 4, "auto": 3, "machinery": 2, "chemicals": 2, "metals": 2},
    "SOUTHERN_AFRICA": {"metals": 4, "agro": 2, "chemicals": 1},
    "AMERICAS_ATLANTIC": {"agro": 4, "chemicals": 2, "machinery": 2, "auto": 1, "metals": 1},
    "AMERICAS_PACIFIC": {"metals": 4, "agro": 3, "chemicals": 1},
}

LEGAL_FORMS = {
    "MA": "SARL",
    "PT": "Lda",
    "ES": "S.L.",
    "FR": "SAS",
    "NL": "B.V.",
    "DE": "GmbH",
    "BE": "NV",
    "PL": "Sp. z o.o.",
    "IT": "S.r.l.",
    "TR": "A.Ş.",
    "RO": "S.R.L.",
    "EG": "S.A.E.",
    "SA": "Co. Ltd",
    "AE": "LLC",
    "IN": "Pvt Ltd",
    "PK": "(Pvt) Ltd",
    "BD": "Ltd",
    "SG": "Pte Ltd",
    "VN": "Co., Ltd",
    "TH": "Co., Ltd",
    "CN": "Co., Ltd",
    "KR": "Co., Ltd",
    "ZA": "(Pty) Ltd",
    "US": "Inc.",
    "BR": "Ltda",
    "CL": "SpA",
}

_ONSETS = ["Val", "Nor", "Tek", "Kol", "Mar", "Sel", "Dar", "Fen", "Gal", "Hel", "Jun", "Kor",
           "Lum", "Mer", "Nav", "Ori", "Pal", "Quin", "Rav", "Sor", "Tal", "Uri", "Vex", "Zan",
           "Bel", "Cav", "Dor"]  # fmt: skip
_MIDS = ["a", "e", "i", "o", "an", "or", "el", "ar", "in", "ex", "ol", "um"]
_ENDS = ["ora", "ex", "is", "on", "ar", "ico", "ana", "um", "ova", "ys", "ent", "aro"]

SUPPLIER_COLUMNS = [
    "supplier_id", "name", "legal_form", "label", "buyer", "category", "category_label",
    "city_id", "city", "country", "iso2", "region", "lat", "lon", "coord_source_url",
    "sole_source", "purchase_share_pct",
]  # fmt: skip
ROUTE_COLUMNS = ["supplier_id", "position", "chokepoint_id", "label"]


def _name(rng: random.Random, taken: set[str]) -> str:
    while True:
        stem = rng.choice(_ONSETS) + rng.choice(_MIDS) + rng.choice(_ENDS)
        if stem not in taken:
            taken.add(stem)
            return stem


def _shares(rng: random.Random, n: int) -> list[float]:
    """Parts des achats : queue longue (quelques gros fournisseurs), somme exactement 100,00."""
    raw = [rng.lognormvariate(0, 0.9) for _ in range(n)]
    total = sum(raw)
    cents = [max(1, round(100 * 100 * r / total)) for r in raw]
    cents[cents.index(max(cents))] += 100 * 100 - sum(cents)  # le reste va au plus gros
    return [c / 100 for c in cents]


def generate(seed: int = SEED, cities: list[dict] | None = None) -> tuple[list[dict], list[dict]]:
    """(fournisseurs, étapes de route). Pur et déterministe pour une graine et des villes donnés."""
    assert sum(REGION_QUOTAS.values()) == N_SUPPLIERS
    assert set(REGION_QUOTAS) == set(REGION_ROUTES)
    rng = random.Random(seed)
    cities = cities if cities is not None else ref_cities.read_csv()
    by_region: dict[str, list[dict]] = {}
    for city in cities:
        by_region.setdefault(city["region"], []).append(city)

    shares = _shares(rng, N_SUPPLIERS)
    taken: set[str] = set()
    drafts = []
    for region, quota in REGION_QUOTAS.items():  # ordre fixe du dictionnaire
        weights = CATEGORY_WEIGHTS[region]
        keys = list(CATEGORIES)
        for _ in range(quota):
            city = rng.choice(by_region[region])
            category = rng.choices(keys, [weights.get(k, 0.3) for k in keys])[0]
            label, word, p_sole = CATEGORIES[category]
            drafts.append(
                {
                    "name": f"{_name(rng, taken)} {word} {LEGAL_FORMS[city['iso2']]}",
                    "legal_form": LEGAL_FORMS[city["iso2"]],
                    "category": category,
                    "category_label": label,
                    "city": city,
                    "sole_source": rng.random() < p_sole,
                }
            )
    rng.shuffle(drafts)  # les identifiants ne doivent pas regrouper les régions

    suppliers, routes = [], []
    for i, (draft, share) in enumerate(zip(drafts, shares, strict=True), start=1):
        city, supplier_id = draft["city"], f"S{i:03d}"
        suppliers.append(
            {
                "supplier_id": supplier_id,
                "name": draft["name"],
                "legal_form": draft["legal_form"],
                "label": LABEL,
                "buyer": BUYER,
                "category": draft["category"],
                "category_label": draft["category_label"],
                "city_id": city["city_id"],
                "city": city["name"],
                "country": city["country"],
                "iso2": city["iso2"],
                "region": city["region"],
                "lat": city["lat"],
                "lon": city["lon"],
                "coord_source_url": city["coord_source_url"],
                "sole_source": str(draft["sole_source"]).lower(),
                "purchase_share_pct": f"{share:.2f}",
            }
        )
        for position, chokepoint in enumerate(route_for(city["region"]), start=1):
            routes.append(
                {
                    "supplier_id": supplier_id,
                    "position": position,
                    "chokepoint_id": chokepoint,
                    "label": LABEL,
                }
            )
    return suppliers, routes


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def summary(suppliers: list[dict], routes: list[dict]) -> str:
    """Répartition lisible du portefeuille."""
    path_of: dict[str, list[str]] = {s["supplier_id"]: [] for s in suppliers}
    for r in sorted(routes, key=lambda r: (r["supplier_id"], int(r["position"]))):
        path_of[r["supplier_id"]].append(r["chokepoint_id"])

    def share(rows):
        return round(sum(float(s["purchase_share_pct"]) for s in rows), 2)

    lines = [f"{len(suppliers)} fournisseurs ({LABEL}), acheteur : {BUYER}, Casablanca\n"]

    def block(title, key):
        lines.append(f"-- {title}")
        groups: dict[str, list[dict]] = {}
        for s in suppliers:
            groups.setdefault(key(s), []).append(s)
        for name, rows in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            lines.append(f"  {len(rows):3d}  {share(rows):6.2f} % des achats  {name}")
        lines.append("")

    block("par pays", lambda s: f"{s['country']} ({s['iso2']})")
    block("par catégorie", lambda s: s["category_label"])
    block("par région d'origine", lambda s: s["region"])
    block("par route (détroits, de l'origine vers Casablanca)",
          lambda s: " > ".join(path_of[s["supplier_id"]]) or "(aucun détroit)")  # fmt: skip
    exposure = Counter(c for path in path_of.values() for c in set(path))
    by_id = {s["supplier_id"]: s for s in suppliers}
    lines.append("-- exposition par détroit (fournisseurs dont la route le traverse)")
    for choke, n in exposure.most_common():
        exposed = [by_id[i] for i, p in path_of.items() if choke in p]
        sole = sum(s["sole_source"] == "true" for s in exposed)
        lines.append(
            f"  {n:3d}  {share(exposed):6.2f} % des achats  dont {sole} fournisseur(s) unique(s)  {choke}"
        )
    sole_all = [s for s in suppliers if s["sole_source"] == "true"]
    lines.append(f"\nfournisseurs uniques : {len(sole_all)} ({share(sole_all):.2f} % des achats)")
    top = sorted(suppliers, key=lambda s: -float(s["purchase_share_pct"]))[:5]
    lines.append(
        "plus gros : " + ", ".join(f"{s['supplier_id']} {s['purchase_share_pct']} %" for s in top)
    )
    return "\n".join(lines)


SUPPLIERS_SCHEMA = (
    "supplier_id STRING, name STRING, legal_form STRING, label STRING, buyer STRING, "
    "category STRING, category_label STRING, city_id STRING, city STRING, country STRING, "
    "iso2 STRING, region STRING, lat DOUBLE, lon DOUBLE, coord_source_url STRING, "
    "sole_source BOOLEAN, purchase_share_pct DECIMAL(5,2), loaded_at TIMESTAMP"
)
ROUTES_SCHEMA = (
    "supplier_id STRING, position INT, chokepoint_id STRING, label STRING, loaded_at TIMESTAMP"
)


def to_dataframes(spark, suppliers: list[dict], routes: list[dict]):
    from decimal import Decimal

    loaded_at = datetime.now(UTC)
    rows = [
        (
            s["supplier_id"], s["name"], s["legal_form"], s["label"], s["buyer"], s["category"],
            s["category_label"], s["city_id"], s["city"], s["country"], s["iso2"], s["region"],
            float(s["lat"]), float(s["lon"]), s["coord_source_url"], s["sole_source"] == "true",
            Decimal(s["purchase_share_pct"]), loaded_at,
        )
        for s in suppliers
    ]  # fmt: skip
    steps = [
        (r["supplier_id"], int(r["position"]), r["chokepoint_id"], r["label"], loaded_at)
        for r in routes
    ]
    return spark.createDataFrame(rows, SUPPLIERS_SCHEMA), spark.createDataFrame(
        steps, ROUTES_SCHEMA
    )


def load(
    spark, suppliers_table: str = SUPPLIERS_TABLE, routes_table: str = ROUTES_TABLE
) -> tuple[int, int]:
    from pyspark.sql import functions as F

    df_suppliers, df_routes = to_dataframes(spark, read_csv(SUPPLIERS_CSV), read_csv(ROUTES_CSV))
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {suppliers_table.rsplit('.', 1)[0]}")
    for table, schema, df in [
        (suppliers_table, SUPPLIERS_SCHEMA, df_suppliers),
        (routes_table, ROUTES_SCHEMA, df_routes),
    ]:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({schema}) USING iceberg")
        df.writeTo(table).overwrite(F.lit(True))
    return spark.table(suppliers_table).count(), spark.table(routes_table).count()


def main(argv: list[str] | None = None) -> int:
    command = (argv if argv is not None else sys.argv[1:] or [""])[0]
    if command == "generate":
        suppliers, routes = generate()
        write_csv(SUPPLIERS_CSV, SUPPLIER_COLUMNS, suppliers)
        write_csv(ROUTES_CSV, ROUTE_COLUMNS, routes)
        print(f"{len(suppliers)} fournisseurs, {len(routes)} étapes de route (graine {SEED})")
        return 0
    if command == "summary":
        print(summary(read_csv(SUPPLIERS_CSV), read_csv(ROUTES_CSV)))
        return 0
    if command == "load":
        from lakehouse.spark_session import build_spark_session

        spark = build_spark_session("portfolio")
        spark.sparkContext.setLogLevel("WARN")
        try:
            print("lignes chargées (fournisseurs, étapes) :", load(spark))
        finally:
            spark.stop()
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
