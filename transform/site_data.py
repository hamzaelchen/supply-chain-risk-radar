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
from datetime import date
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import export

HERE = Path(__file__).resolve().parent
CITIES_CSV = HERE.parent / "ref" / "cities.csv"
HUB_CITY_ID = "casablanca"  # siège de l'Entreprise démo : point d'arrivée de toutes les routes
LIVE_DB = HERE / "target" / "gold.duckdb"
EPISODE_DB = HERE / "target" / "ormuz.duckdb"
EPISODE_ID = "ormuz-2026-03-02"
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
    index = {
        "schema_version": 1, "default": EPISODE_ID,
        "stats": lake_stats(live_db), "hub": hub(), "snapshots": snapshots,
    }  # fmt: skip
    (out / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", "utf-8")
    return index


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("web/public/data"))
    args = parser.parse_args(argv)
    index = build(args.out)
    for snap in index["snapshots"]:
        print(f"{snap['id']} (au {snap['as_of']}) : {snap['description_fr'][:110]}…")
    print(f"index.json : défaut = {index['default']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
