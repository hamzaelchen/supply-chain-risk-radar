"""Référentiel des grands détroits et canaux maritimes -> silver.ref_chokepoints.

Usage :
    python -m ref.chokepoints fetch    # (re)télécharge les coordonnées depuis Wikidata -> ref/
    python -m ref.chokepoints load     # charge ref/chokepoints.csv dans silver.ref_chokepoints

D'où vient chaque champ :
- nom, type : choix du projet (liste ci-dessous, établie à la main) ;
- latitude, longitude : revendication « coordinate location » (P625) de l'entité Wikidata,
  données sous licence CC0. La réponse brute de l'API, son URL et l'heure de téléchargement
  sont archivées dans ref/sources/wikidata/ ; chaque ligne du CSV cite un lien permanent vers
  la révision exacte de l'entité (?oldid=...) ;
- rayon de surveillance : CHOIX DE CONCEPTION, pas une donnée de la source (voir RADIUS_TIERS).
  Il est à valider sur des mesures (combien d'événements GDELT tombent dans chaque rayon).

Une coordonnée Wikidata de détroit ou de canal est un point représentatif, pas une ligne :
elle sert de centre à un rayon de recherche, pas à situer un navire.
"""

import csv
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ref import wikidata

HERE = Path(__file__).parent
CSV_PATH = HERE / "chokepoints.csv"
SOURCES_DIR = HERE / "sources" / "wikidata"
LICENSE = wikidata.LICENSE
SOURCE_NAME = "Wikidata (P625, coordinate location)"

TABLE = "lakehouse.silver.ref_chokepoints"

# Rayon de surveillance, par gabarit du passage. Choix de conception : un passage étroit ou un
# canal a une zone d'influence réduite, un détroit large ou un golfe voisin en a une plus
# grande, un cap en pleine mer encore plus.
RADIUS_TIERS = {
    "narrow": (100.0, "choix de conception : passage étroit ou canal, 100 km"),
    "wide": (200.0, "choix de conception : détroit large ou golfe adjacent, 200 km"),
    "open_sea": (300.0, "choix de conception : cap en mer ouverte, 300 km"),
}


@dataclass(frozen=True)
class Spec:
    chokepoint_id: str
    name_fr: str
    name_en: str
    kind: str  # strait | canal | cape
    wikidata_id: str
    tier: str


SPECS = [
    Spec("hormuz", "Détroit d'Ormuz", "Strait of Hormuz", "strait", "Q79883", "wide"),
    Spec("suez", "Canal de Suez", "Suez Canal", "canal", "Q899", "narrow"),
    Spec("bab_el_mandeb", "Bab-el-Mandeb", "Bab-el-Mandeb", "strait", "Q83318", "wide"),
    Spec("malacca", "Détroit de Malacca", "Strait of Malacca", "strait", "Q48359", "wide"),
    Spec("panama", "Canal de Panama", "Panama Canal", "canal", "Q7350", "narrow"),
    Spec("gibraltar", "Détroit de Gibraltar", "Strait of Gibraltar", "strait", "Q36124", "narrow"),
    Spec("bosphorus", "Bosphore", "Bosporus Strait", "strait", "Q35958", "narrow"),
    Spec("dardanelles", "Dardanelles", "Dardanelles", "strait", "Q6514", "narrow"),
    Spec("dover", "Pas de Calais", "Strait of Dover", "strait", "Q159898", "narrow"),
    Spec("oresund", "Øresund", "Øresund", "strait", "Q104662", "narrow"),
    Spec("cape_of_good_hope", "Cap de Bonne-Espérance", "Cape of Good Hope", "cape", "Q4092", "open_sea"),
    Spec("taiwan", "Détroit de Taïwan", "Taiwan Strait", "strait", "Q127031", "wide"),
    Spec("korea", "Détroit de Corée", "Korea Strait", "strait", "Q52052", "wide"),
    Spec("luzon", "Détroit de Luçon", "Luzon Strait", "strait", "Q908741", "wide"),
    Spec("lombok", "Détroit de Lombok", "Lombok Strait", "strait", "Q745025", "wide"),
    Spec("sunda", "Détroit de la Sonde", "Sunda Strait", "strait", "Q192790", "wide"),
]  # fmt: skip

COLUMNS = [
    "chokepoint_id", "name_fr", "name_en", "kind", "lat", "lon", "coord_precision_deg", "radius_km",
    "radius_basis", "wikidata_id", "wikidata_label", "coord_source", "coord_source_url", "coord_license",
    "coord_retrieved_at",
]  # fmt: skip


def parse_entity(archive: dict) -> dict:
    """Archive brute (voir fetch_one) -> champs utiles. Pure : testable sans réseau."""
    wid = archive["wikidata_id"]
    entity = archive["requests"]["entity"]["response"]["entities"][wid]
    coordinate = wikidata.best_coordinate(archive["requests"]["coordinates"]["response"], wid)
    return {
        **coordinate,
        "wikidata_label": entity["labels"]["en"]["value"],
        "revision": entity["lastrevid"],
        "retrieved_at": archive["requests"]["coordinates"]["fetched_at"],
    }


def fetch_one(spec: Spec) -> dict:
    wid = spec.wikidata_id
    return {
        "wikidata_id": wid,
        "requests": {
            "entity": wikidata.request_record(
                {"action": "wbgetentities", "ids": wid, "props": "labels|info"}
            ),
            "coordinates": wikidata.request_record(
                {"action": "wbgetclaims", "entity": wid, "property": "P625"}
            ),
        },
    }


def to_row(spec: Spec, archive: dict) -> dict:
    found = parse_entity(archive)
    radius, basis = RADIUS_TIERS[spec.tier]
    return {
        "chokepoint_id": spec.chokepoint_id, "name_fr": spec.name_fr, "name_en": spec.name_en,
        "kind": spec.kind, "lat": found["lat"], "lon": found["lon"],
        "coord_precision_deg": "" if found["precision_deg"] is None else found["precision_deg"],
        "radius_km": radius,
        "radius_basis": basis, "wikidata_id": spec.wikidata_id,
        "wikidata_label": found["wikidata_label"], "coord_source": SOURCE_NAME,
        "coord_source_url": (
            f"https://www.wikidata.org/w/index.php?title={spec.wikidata_id}"
            f"&oldid={found['revision']}"
        ),
        "coord_license": LICENSE, "coord_retrieved_at": found["retrieved_at"],
    }  # fmt: skip


def fetch_all() -> list[dict]:
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for spec in SPECS:
        archive = fetch_one(spec)
        (SOURCES_DIR / f"{spec.wikidata_id}.json").write_text(
            json.dumps(archive, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        rows.append(to_row(spec, archive))
        time.sleep(0.5)  # courtoisie envers l'API
    with CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def read_csv(path: Path = CSV_PATH) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


SILVER_SCHEMA = (
    "chokepoint_id STRING, name_fr STRING, name_en STRING, kind STRING, lat DOUBLE, lon DOUBLE, "
    "coord_precision_deg DOUBLE, radius_km DOUBLE, radius_basis STRING, wikidata_id STRING, wikidata_label STRING, "
    "coord_source STRING, coord_source_url STRING, coord_license STRING, "
    "coord_retrieved_at TIMESTAMP, loaded_at TIMESTAMP"
)


def to_dataframe(spark, rows: list[dict]):
    """Lignes du CSV -> DataFrame typé (latitude, longitude, rayon en DOUBLE, dates en TIMESTAMP)."""
    loaded_at = datetime.now(UTC)
    typed = [
        (
            r["chokepoint_id"], r["name_fr"], r["name_en"], r["kind"], float(r["lat"]),
            float(r["lon"]),
            float(r["coord_precision_deg"]) if r["coord_precision_deg"] else None,
            float(r["radius_km"]), r["radius_basis"], r["wikidata_id"],
            r["wikidata_label"], r["coord_source"], r["coord_source_url"], r["coord_license"],
            datetime.fromisoformat(r["coord_retrieved_at"]), loaded_at,
        )
        for r in rows
    ]  # fmt: skip
    return spark.createDataFrame(typed, SILVER_SCHEMA)


def load(spark, table: str = TABLE, path: Path = CSV_PATH) -> int:
    """Remplace le contenu de la table par le CSV (référentiel : petit, rechargé en entier)."""
    from pyspark.sql import functions as F

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    df = to_dataframe(spark, read_csv(path))
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({SILVER_SCHEMA}) USING iceberg")
    df.writeTo(table).overwrite(F.lit(True))
    return spark.table(table).count()


def main(argv: list[str] | None = None) -> int:
    command = (argv if argv is not None else sys.argv[1:] or [""])[0]
    if command == "fetch":
        rows = fetch_all()
        print(f"{len(rows)} lignes -> {CSV_PATH}")
        return 0
    if command == "load":
        from lakehouse.spark_session import build_spark_session

        spark = build_spark_session("ref-chokepoints")
        spark.sparkContext.setLogLevel("WARN")
        try:
            print(f"{TABLE} : {load(spark)} lignes")
        finally:
            spark.stop()
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
