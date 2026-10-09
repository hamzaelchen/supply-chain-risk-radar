"""Référentiel des villes d'expédition (et de l'Entreprise démo) -> silver.ref_cities.

Usage :
    python -m ref.cities fetch    # (re)télécharge les coordonnées depuis Wikidata -> ref/
    python -m ref.cities load     # charge ref/cities.csv dans silver.ref_cities

Mêmes règles que pour les détroits (voir ref/chokepoints.py) : coordonnées = revendication P625
de Wikidata (CC0), réponse brute archivée dans ref/sources/wikidata/, lien permanent vers la
révision exacte dans le CSV. Les VILLES sont réelles ; les fournisseurs qui y sont placés sont
fictifs (voir portfolio/).

Contrôle supplémentaire : le pays déclaré par Wikidata (P17) doit être celui que nous avons
indiqué, ce qui attrape une homonymie (Alexandria, Valencia, Santos...).

`region` est notre classement des villes pour le routage maritime (portfolio/routing.py) :
un choix de conception, pas une donnée de la source.
"""

import csv
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ref import wikidata
from ref.chokepoints import HERE, SOURCES_DIR

CSV_PATH = HERE / "cities.csv"
TABLE = "lakehouse.silver.ref_cities"
SOURCE_NAME = "Wikidata (P625, coordinate location)"


@dataclass(frozen=True)
class City:
    city_id: str
    name: str
    country: str  # nom anglais, tel que libellé par Wikidata
    iso2: str
    region: str
    wikidata_id: str


CITIES = [
    City("casablanca", "Casablanca", "Morocco", "MA", "MA", "Q7903"),
    City("tangier", "Tanger", "Morocco", "MA", "MA", "Q126148"),
    City("fez", "Fès", "Morocco", "MA", "MA", "Q80985"),
    City("marrakesh", "Marrakech", "Morocco", "MA", "MA", "Q101625"),
    City("kenitra", "Kénitra", "Morocco", "MA", "MA", "Q478207"),
    City("porto", "Porto", "Portugal", "PT", "EU_ATLANTIC", "Q36433"),
    City("vigo", "Vigo", "Spain", "ES", "EU_ATLANTIC", "Q8745"),
    City("le_havre", "Le Havre", "France", "FR", "EU_ATLANTIC", "Q42810"),
    City("rotterdam", "Rotterdam", "Netherlands", "NL", "EU_NORTH_SEA", "Q34370"),
    City("hamburg", "Hambourg", "Germany", "DE", "EU_NORTH_SEA", "Q1055"),
    City("antwerp", "Anvers", "Belgium", "BE", "EU_NORTH_SEA", "Q12892"),
    City("gdansk", "Gdańsk", "Poland", "PL", "EU_BALTIC", "Q1792"),
    City("barcelona", "Barcelone", "Spain", "ES", "EU_MEDITERRANEAN", "Q1492"),
    City("valencia", "Valence", "Spain", "ES", "EU_MEDITERRANEAN", "Q8818"),
    City("marseille", "Marseille", "France", "FR", "EU_MEDITERRANEAN", "Q23482"),
    City("genoa", "Gênes", "Italy", "IT", "EU_MEDITERRANEAN", "Q1449"),
    City("istanbul", "Istanbul", "Turkey", "TR", "TR_MARMARA", "Q406"),
    City("izmir", "Izmir", "Turkey", "TR", "TR_AEGEAN", "Q35997"),
    City("constanta", "Constanța", "Romania", "RO", "BLACK_SEA", "Q79808"),
    City("alexandria", "Alexandrie", "Egypt", "EG", "EGYPT_MEDITERRANEAN", "Q87"),
    City("jeddah", "Djeddah", "Saudi Arabia", "SA", "RED_SEA", "Q374365"),
    City("dubai", "Dubaï", "United Arab Emirates", "AE", "PERSIAN_GULF", "Q612"),
    City("dammam", "Dammam", "Saudi Arabia", "SA", "PERSIAN_GULF", "Q160320"),
    City("mumbai", "Bombay", "India", "IN", "SOUTH_ASIA", "Q1156"),
    City("chennai", "Chennai", "India", "IN", "SOUTH_ASIA", "Q1352"),
    City("karachi", "Karachi", "Pakistan", "PK", "SOUTH_ASIA", "Q8660"),
    City("chittagong", "Chittagong", "Bangladesh", "BD", "SOUTH_ASIA", "Q376749"),
    City("singapore", "Singapour", "Singapore", "SG", "SOUTHEAST_ASIA", "Q334"),
    City("ho_chi_minh_city", "Hô Chi Minh-Ville", "Vietnam", "VN", "SOUTHEAST_ASIA", "Q1854"),
    City("bangkok", "Bangkok", "Thailand", "TH", "SOUTHEAST_ASIA", "Q1861"),
    City("shanghai", "Shanghai", "China", "CN", "CHINA_NORTH", "Q8686"),
    City("qingdao", "Qingdao", "China", "CN", "CHINA_NORTH", "Q170322"),
    City("shenzhen", "Shenzhen", "China", "CN", "CHINA_SOUTH", "Q15174"),
    City("guangzhou", "Canton", "China", "CN", "CHINA_SOUTH", "Q16572"),
    City("busan", "Busan", "South Korea", "KR", "KOREA", "Q16520"),
    City("durban", "Durban", "South Africa", "ZA", "SOUTHERN_AFRICA", "Q5468"),
    City("new_york", "New York", "United States", "US", "AMERICAS_ATLANTIC", "Q60"),
    City("santos", "Santos", "Brazil", "BR", "AMERICAS_ATLANTIC", "Q81882"),
    City("los_angeles", "Los Angeles", "United States", "US", "AMERICAS_PACIFIC", "Q65"),
    City("valparaiso", "Valparaíso", "Chile", "CL", "AMERICAS_PACIFIC", "Q33986"),
]  # fmt: skip
HUB_ID = "casablanca"
# Libellé Wikidata du pays quand il diffère du nom courant utilisé ci-dessus.
COUNTRY_ALIASES = {"China": "People's Republic of China"}

COLUMNS = [
    "city_id", "name", "country", "iso2", "region", "lat", "lon", "coord_precision_deg",
    "wikidata_id", "wikidata_label", "wikidata_country", "coord_source", "coord_source_url",
    "coord_license", "coord_retrieved_at",
]  # fmt: skip


def parse_city(archive: dict) -> dict:
    """Archive brute -> champs utiles. Pure : testable sans réseau."""
    wid = archive["wikidata_id"]
    requests = archive["requests"]
    entity = requests["entity"]["response"]["entities"][wid]
    coordinate = wikidata.best_coordinate(requests["coordinates"]["response"], wid)
    country_ids = wikidata.claim_entity_ids(requests["country"]["response"], "P17")
    labels = requests["country_labels"]["response"]["entities"]
    countries = sorted({labels[c]["labels"]["en"]["value"] for c in country_ids if c in labels})
    return {
        **coordinate,
        "wikidata_label": entity["labels"]["en"]["value"],
        "wikidata_countries": countries,
        "revision": entity["lastrevid"],
        "retrieved_at": requests["coordinates"]["fetched_at"],
    }


def fetch_one(city: City) -> dict:
    wid = city.wikidata_id
    requests = {
        "entity": wikidata.request_record(
            {"action": "wbgetentities", "ids": wid, "props": "labels|info"}
        ),
        "coordinates": wikidata.request_record(
            {"action": "wbgetclaims", "entity": wid, "property": "P625"}
        ),
        "country": wikidata.request_record(
            {"action": "wbgetclaims", "entity": wid, "property": "P17"}
        ),
    }
    ids = wikidata.claim_entity_ids(requests["country"]["response"], "P17")
    requests["country_labels"] = wikidata.request_record(
        {"action": "wbgetentities", "ids": "|".join(ids), "props": "labels"}
    )
    return {"wikidata_id": wid, "requests": requests}


def to_row(city: City, archive: dict) -> dict:
    found = parse_city(archive)
    accepted = {city.country, COUNTRY_ALIASES.get(city.country, city.country)}
    if not accepted & set(found["wikidata_countries"]):
        raise ValueError(
            f"{city.city_id} ({city.wikidata_id}) : pays attendu {city.country}, "
            f"Wikidata dit {found['wikidata_countries']} : mauvaise entité ?"
        )
    return {
        "city_id": city.city_id, "name": city.name, "country": city.country, "iso2": city.iso2,
        "region": city.region, "lat": found["lat"], "lon": found["lon"],
        "coord_precision_deg": "" if found["precision_deg"] is None else found["precision_deg"],
        "wikidata_id": city.wikidata_id, "wikidata_label": found["wikidata_label"],
        "wikidata_country": city.country, "coord_source": SOURCE_NAME,
        "coord_source_url": (
            f"https://www.wikidata.org/w/index.php?title={city.wikidata_id}"
            f"&oldid={found['revision']}"
        ),
        "coord_license": wikidata.LICENSE, "coord_retrieved_at": found["retrieved_at"],
    }  # fmt: skip


def fetch_all() -> list[dict]:
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for city in CITIES:
        archive = fetch_one(city)
        (SOURCES_DIR / f"{city.wikidata_id}.json").write_text(
            json.dumps(archive, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        rows.append(to_row(city, archive))
        time.sleep(0.5)
    with CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return rows


def read_csv(path: Path = CSV_PATH) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


SILVER_SCHEMA = (
    "city_id STRING, name STRING, country STRING, iso2 STRING, region STRING, lat DOUBLE, "
    "lon DOUBLE, coord_precision_deg DOUBLE, wikidata_id STRING, wikidata_label STRING, "
    "wikidata_country STRING, coord_source STRING, coord_source_url STRING, "
    "coord_license STRING, coord_retrieved_at TIMESTAMP, loaded_at TIMESTAMP"
)


def to_dataframe(spark, rows: list[dict]):
    loaded_at = datetime.now(UTC)
    typed = [
        (
            r["city_id"], r["name"], r["country"], r["iso2"], r["region"], float(r["lat"]),
            float(r["lon"]),
            float(r["coord_precision_deg"]) if r["coord_precision_deg"] else None,
            r["wikidata_id"], r["wikidata_label"], r["wikidata_country"], r["coord_source"],
            r["coord_source_url"], r["coord_license"],
            datetime.fromisoformat(r["coord_retrieved_at"]), loaded_at,
        )
        for r in rows
    ]  # fmt: skip
    return spark.createDataFrame(typed, SILVER_SCHEMA)


def load(spark, table: str = TABLE, path: Path = CSV_PATH) -> int:
    from pyspark.sql import functions as F

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({SILVER_SCHEMA}) USING iceberg")
    to_dataframe(spark, read_csv(path)).writeTo(table).overwrite(F.lit(True))
    return spark.table(table).count()


def main(argv: list[str] | None = None) -> int:
    command = (argv if argv is not None else sys.argv[1:] or [""])[0]
    if command == "fetch":
        print(f"{len(fetch_all())} lignes -> {CSV_PATH}")
        return 0
    if command == "load":
        from lakehouse.spark_session import build_spark_session

        spark = build_spark_session("ref-cities")
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
