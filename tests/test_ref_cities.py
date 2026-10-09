"""Référentiel des villes : provenance, cohérence avec les archives, régions de routage."""

import json
from datetime import UTC, datetime

import pytest

from portfolio.routing import REGION_ROUTES
from ref import cities as ref
from ref.chokepoints import SOURCES_DIR

ROWS = ref.read_csv()
BY_ID = {r["city_id"]: r for r in ROWS}


def test_every_city_is_listed_once_and_belongs_to_a_routing_region():
    ids = [r["city_id"] for r in ROWS]
    assert len(ids) == len(set(ids)) == len(ref.CITIES) == 40
    assert {r["region"] for r in ROWS} == set(REGION_ROUTES)  # aucune région sans ville
    assert ref.HUB_ID in BY_ID and BY_ID[ref.HUB_ID]["country"] == "Morocco"


def test_every_row_cites_a_wikidata_revision_and_a_retrieval_time():
    for r in ROWS:
        assert r["coord_source_url"].startswith("https://www.wikidata.org/w/index.php?title=Q")
        assert "&oldid=" in r["coord_source_url"] and r["wikidata_id"] in r["coord_source_url"]
        assert r["coord_license"] == "CC0 1.0"
        retrieved = datetime.fromisoformat(r["coord_retrieved_at"])
        assert retrieved.tzinfo is not None and retrieved <= datetime.now(UTC)
        assert -90 <= float(r["lat"]) <= 90 and -180 <= float(r["lon"]) <= 180


def test_casablanca_is_where_it_should_be():
    hub = BY_ID["casablanca"]
    assert 33.4 <= float(hub["lat"]) <= 33.8 and -7.8 <= float(hub["lon"]) <= -7.4


@pytest.mark.parametrize("city", ref.CITIES, ids=lambda c: c.city_id)
def test_csv_matches_the_archived_raw_response(city):
    archive = json.loads((SOURCES_DIR / f"{city.wikidata_id}.json").read_text("utf-8"))
    assert {k: str(v) for k, v in ref.to_row(city, archive).items()} == BY_ID[city.city_id]


def test_wrong_country_in_wikidata_is_refused():
    city = next(c for c in ref.CITIES if c.city_id == "porto")
    archive = json.loads((SOURCES_DIR / f"{city.wikidata_id}.json").read_text("utf-8"))
    wrong = ref.City(city.city_id, city.name, "Brazil", "BR", city.region, city.wikidata_id)
    with pytest.raises(ValueError, match="mauvaise entité"):
        ref.to_row(wrong, archive)


@pytest.mark.spark
def test_dataframe_is_typed(spark):
    df = ref.to_dataframe(spark, ROWS)
    types = dict(df.dtypes)
    assert df.count() == 40 and types["lat"] == types["lon"] == "double"
    assert types["coord_retrieved_at"] == "timestamp"
