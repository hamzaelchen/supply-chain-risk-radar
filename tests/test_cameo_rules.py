"""Règles de classification CAMEO : cohérence avec la table officielle de GDELT."""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RULES = list(csv.DictReader((ROOT / "transform/seeds/cameo_rules.csv").open(encoding="utf-8")))
OFFICIAL = {}
for line in (ROOT / "ref/sources/cameo/CAMEO.eventcodes.txt").read_text("utf-8").splitlines()[1:]:
    code, _, label = line.partition("\t")
    OFFICIAL[code.strip()] = label.strip()


def test_every_rule_prefix_is_an_official_cameo_code_with_the_same_label():
    for rule in RULES:
        official = OFFICIAL.get(rule["code_prefix"])
        assert official, f"{rule['code_prefix']} absent de la table officielle"
        assert official.lower().startswith(rule["cameo_label_en"].lower()[:25]), rule["code_prefix"]


def test_prefixes_are_unique_and_every_rule_has_a_rationale():
    prefixes = [r["code_prefix"] for r in RULES]
    assert len(prefixes) == len(set(prefixes))
    assert all(r["rationale_fr"].strip() for r in RULES)


def test_only_conflict_codes_are_classified():
    # QuadClass 3 et 4 = racines 10 à 20 : aucune règle sur la coopération (01 à 09).
    assert all(int(r["code_prefix"][:2]) >= 13 for r in RULES)


def test_family_weights_are_in_unit_interval_and_threats_weigh_less_than_attacks():
    weights = {r["family"]: float(r["family_weight"]) for r in RULES}
    assert all(0 < w <= 1 for w in weights.values())
    assert weights["menace"] < weights["violence_attaque"]
    assert weights["posture_militaire"] < weights["violence_attaque"]


def test_each_family_has_a_single_label_and_weight():
    by_family = {}
    for r in RULES:
        by_family.setdefault(r["family"], set()).add((r["family_label_fr"], r["family_weight"]))
    assert all(len(v) == 1 for v in by_family.values())


def test_the_rules_discard_the_noisy_generic_families():
    prefixes = {r["code_prefix"] for r in RULES}
    # Décision documentée : manifestations pacifiques, arrestations individuelles, assassinats,
    # agressions physiques et répression : écartés (voir CLAUDE.md).
    for noisy in ("141", "173", "186", "182", "175", "172"):
        assert noisy not in prefixes, noisy  # (1723 et 1724 sont gardés : curfew, état d'urgence)
    # et aucune règle ne couvre une racine entière de bruit
    assert not {"14", "17", "18", "13", "15", "16"} & prefixes
