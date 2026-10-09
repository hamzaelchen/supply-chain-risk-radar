"""Portefeuille fictif : reproductibilité, quotas, étiquetage, routes, chargement."""

from collections import Counter
from decimal import Decimal

import pytest

from portfolio import generate as pf
from portfolio.routing import REGION_ROUTES, route_for
from ref import chokepoints, cities

SUPPLIERS, ROUTES = pf.generate()
CHOKEPOINT_IDS = {r["chokepoint_id"] for r in chokepoints.read_csv()}
CITY_BY_ID = {r["city_id"]: r for r in cities.read_csv()}


def test_the_same_seed_gives_the_same_portfolio_and_another_seed_a_different_one():
    assert pf.generate() == (SUPPLIERS, ROUTES)
    other, _ = pf.generate(seed=pf.SEED + 1)
    assert [s["name"] for s in other] != [s["name"] for s in SUPPLIERS]


def test_the_committed_csv_files_are_exactly_what_the_generator_produces():
    assert pf.read_csv(pf.SUPPLIERS_CSV) == SUPPLIERS
    as_text = [{k: str(v) for k, v in r.items()} for r in ROUTES]  # le CSV ne porte que du texte
    assert pf.read_csv(pf.ROUTES_CSV) == as_text


def test_golden_values_catch_an_accidental_change_of_the_generator():
    # À mettre à jour volontairement si le générateur change : c'est tout l'intérêt.
    assert (SUPPLIERS[0]["supplier_id"], SUPPLIERS[0]["name"]) == ("S001", "Feneon Chemicals A.Ş.")
    assert Counter(s["sole_source"] for s in SUPPLIERS)["true"] == 10
    assert len(ROUTES) == 200


def test_one_hundred_distinct_suppliers_whose_purchase_shares_sum_to_100():
    assert len(SUPPLIERS) == pf.N_SUPPLIERS == 100
    assert [s["supplier_id"] for s in SUPPLIERS] == [f"S{i:03d}" for i in range(1, 101)]
    assert len({s["name"] for s in SUPPLIERS}) == 100
    shares = [Decimal(s["purchase_share_pct"]) for s in SUPPLIERS]
    assert sum(shares) == Decimal("100.00") and min(shares) > 0


def test_everything_is_labelled_fictional():
    assert {s["label"] for s in SUPPLIERS} == {pf.LABEL} and "FICTIF" in pf.LABEL
    assert {s["buyer"] for s in SUPPLIERS} == {"Entreprise démo"}
    assert {r["label"] for r in ROUTES} == {pf.LABEL}


def test_region_quotas_are_exact_and_cities_are_real_and_sourced():
    assert Counter(s["region"] for s in SUPPLIERS) == Counter(pf.REGION_QUOTAS)
    for s in SUPPLIERS:
        city = CITY_BY_ID[s["city_id"]]
        assert (s["lat"], s["lon"], s["country"], s["region"]) == (
            city["lat"], city["lon"], city["country"], city["region"],
        )  # fmt: skip
        assert "wikidata.org" in s["coord_source_url"] and "&oldid=" in s["coord_source_url"]


def test_routes_follow_the_documented_rule_and_use_known_chokepoints():
    by_supplier: dict[str, list[dict]] = {}
    for r in ROUTES:
        by_supplier.setdefault(r["supplier_id"], []).append(r)
    for s in SUPPLIERS:
        steps = sorted(by_supplier.get(s["supplier_id"], []), key=lambda r: int(r["position"]))
        assert [int(r["position"]) for r in steps] == list(range(1, len(steps) + 1))
        assert [r["chokepoint_id"] for r in steps] == route_for(s["region"])


def test_every_routed_chokepoint_exists_in_the_reference_and_is_not_repeated():
    for region, route in REGION_ROUTES.items():
        assert set(route) <= CHOKEPOINT_IDS, region
        assert len(route) == len(set(route)), region


def test_route_order_makes_geographic_sense():
    chain = ["hormuz", "bab_el_mandeb", "suez", "gibraltar"]
    for route in REGION_ROUTES.values():
        present = [c for c in chain if c in route]
        assert present == [c for c in route if c in chain]  # même ordre relatif
    assert REGION_ROUTES["MA"] == [] and REGION_ROUTES["EU_ATLANTIC"] == []
    assert [r for r, route in REGION_ROUTES.items() if "panama" in route] == ["AMERICAS_PACIFIC"]
    assert [r for r, route in REGION_ROUTES.items() if "hormuz" in route] == ["PERSIAN_GULF"]


def test_summary_mentions_every_country_and_the_fictional_label():
    text = pf.summary(SUPPLIERS, ROUTES)
    assert pf.LABEL in text and "gibraltar" in text
    assert all(f"({iso})" in text for iso in {s["iso2"] for s in SUPPLIERS})


@pytest.mark.spark
def test_dataframes_are_typed(spark):
    suppliers, routes = pf.to_dataframes(spark, SUPPLIERS, ROUTES)
    types = dict(suppliers.dtypes)
    assert suppliers.count() == 100 and routes.count() == 200
    assert types["purchase_share_pct"] == "decimal(5,2)" and types["sole_source"] == "boolean"
    assert dict(routes.dtypes)["position"] == "int"


@pytest.mark.spark
@pytest.mark.integration
def test_loading_twice_leaves_one_copy_of_each_row(spark, infra, sandbox):
    name = sandbox.name()
    tables = (f"{sandbox.namespace}.suppliers_{name}", f"{sandbox.namespace}.routes_{name}")
    assert pf.load(spark, *tables) == (100, 200)
    assert pf.load(spark, *tables) == (100, 200)
    total = spark.sql(f"SELECT sum(purchase_share_pct) AS s FROM {tables[0]}").first().s
    assert total == Decimal("100.00")
