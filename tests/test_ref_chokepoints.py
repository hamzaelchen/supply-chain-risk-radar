"""Référentiel des détroits et canaux : contenu du CSV, provenance, typage, chargement."""

import json
from datetime import UTC, datetime

import pytest

from ref import chokepoints as ref

ROWS = ref.read_csv()
BY_ID = {r["chokepoint_id"]: r for r in ROWS}

# Garde-fou grossier, établi à la main : (lat min, lat max, lon min, lon max) de la région où
# se trouve chaque passage. Il attrape une mauvaise entité Wikidata, pas une erreur de quelques km.
REGION = {
    "hormuz": (25, 27.5, 55, 58), "suez": (29.5, 31.5, 31.8, 33), "bab_el_mandeb": (11.5, 13.5, 42.5, 44),
    "malacca": (1, 6.5, 99, 104), "panama": (8.5, 9.7, -80.2, -79.3),
    "gibraltar": (35.7, 36.2, -5.9, -5.2), "bosphorus": (40.9, 41.3, 28.9, 29.3),
    "dardanelles": (39.9, 40.5, 26, 26.8), "dover": (50.7, 51.3, 1, 2), "oresund": (55.3, 56.2, 12.4, 13),
    "cape_of_good_hope": (-35, -34, 18, 19.2), "taiwan": (22, 26, 117, 121),
    "korea": (33, 35.5, 128, 130.5), "luzon": (18, 21.5, 119.5, 122.5),
    "lombok": (-9, -8, 115.3, 116.5), "sunda": (-6.5, -5.5, 105.3, 106.2),
}  # fmt: skip
REQUESTED = {"hormuz", "suez", "bab_el_mandeb", "malacca", "panama", "gibraltar", "bosphorus",
             "dover", "cape_of_good_hope", "taiwan"}  # fmt: skip


def test_the_requested_chokepoints_are_all_present_and_ids_are_unique():
    ids = [r["chokepoint_id"] for r in ROWS]
    assert len(ids) == len(set(ids)) == len(ref.SPECS) == len(REGION) == 16
    assert REQUESTED <= set(ids)


def test_every_row_has_a_cited_public_source_and_a_retrieval_time():
    for r in ROWS:
        assert r["coord_source_url"].startswith("https://www.wikidata.org/w/index.php?title=Q")
        assert "&oldid=" in r["coord_source_url"], r["chokepoint_id"]  # révision exacte
        assert r["coord_license"] == "CC0 1.0"
        assert r["wikidata_id"] in r["coord_source_url"]
        retrieved = datetime.fromisoformat(r["coord_retrieved_at"])
        assert retrieved.tzinfo is not None and retrieved <= datetime.now(UTC)
        assert r["name_fr"] and r["name_en"] and r["kind"] in {"strait", "canal", "cape"}


def test_coordinates_and_radius_are_valid():
    for r in ROWS:
        assert -90 <= float(r["lat"]) <= 90 and -180 <= float(r["lon"]) <= 180
        assert float(r["radius_km"]) > 0
        assert r["radius_basis"].startswith("choix de conception")  # jamais présenté comme sourcé


@pytest.mark.parametrize("chokepoint_id", sorted(REGION))
def test_each_point_lies_in_its_expected_region(chokepoint_id):
    lat_min, lat_max, lon_min, lon_max = REGION[chokepoint_id]
    row = BY_ID[chokepoint_id]
    assert lat_min <= float(row["lat"]) <= lat_max
    assert lon_min <= float(row["lon"]) <= lon_max


@pytest.mark.parametrize("spec", ref.SPECS, ids=lambda s: s.chokepoint_id)
def test_csv_matches_the_archived_raw_response(spec):
    archive = json.loads((ref.SOURCES_DIR / f"{spec.wikidata_id}.json").read_text("utf-8"))
    assert archive["requests"]["coordinates"]["url"].startswith(
        "https://www.wikidata.org/w/api.php"
    )
    expected = ref.to_row(spec, archive)
    actual = BY_ID[spec.chokepoint_id]
    assert {k: str(v) for k, v in expected.items()} == actual


def test_parse_entity_refuses_an_entity_without_coordinates():
    archive = {
        "wikidata_id": "Q1",
        "requests": {
            "entity": {"response": {"entities": {"Q1": {"labels": {"en": {"value": "X"}}, "lastrevid": 1}}}},
            "coordinates": {"response": {"claims": {}}, "fetched_at": "2026-10-09T10:00:00+00:00"},
        },
    }  # fmt: skip
    with pytest.raises(ValueError, match="aucune coordonnée"):
        ref.parse_entity(archive)


@pytest.mark.spark
def test_dataframe_is_typed(spark):
    df = ref.to_dataframe(spark, ROWS)
    types = dict(df.dtypes)
    assert df.count() == 16
    assert types["lat"] == types["lon"] == types["radius_km"] == "double"
    assert types["coord_precision_deg"] == "double"
    assert types["coord_retrieved_at"] == types["loaded_at"] == "timestamp"
    assert df.filter("coord_precision_deg IS NULL").count() > 0  # précision absente = NULL, pas 0


@pytest.mark.spark
@pytest.mark.integration
def test_loading_twice_leaves_one_copy_of_each_row(spark, infra, sandbox):
    table = f"{sandbox.namespace}.ref_chokepoints_{sandbox.name()}"
    assert ref.load(spark, table) == 16
    assert ref.load(spark, table) == 16
    assert spark.table(table).select("chokepoint_id").distinct().count() == 16
