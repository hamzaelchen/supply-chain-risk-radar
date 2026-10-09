"""Accès minimal à l'API Wikidata (CC0), partagé par les référentiels de ref/.

Chaque appel rend l'URL, la réponse brute et l'heure de téléchargement : les appelants
archivent les trois (règle 1 de CLAUDE.md : URL source et horodatage pour toute donnée).
"""

import json
import urllib.parse
import urllib.request
from datetime import UTC, datetime

API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "supply-chain-risk-radar/0.1 (projet pédagogique; [adresse e-mail retirée])"
LICENSE = "CC0 1.0"
EARTH = "http://www.wikidata.org/entity/Q2"


def get(params: dict) -> tuple[str, dict, str]:
    """GET sur l'API : (URL, réponse JSON, horodatage UTC du téléchargement)."""
    url = f"{API}?{urllib.parse.urlencode({**params, 'format': 'json'})}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.load(response)
    return url, body, datetime.now(UTC).isoformat()


def request_record(params: dict) -> dict:
    url, body, fetched_at = get(params)
    return {"url": url, "fetched_at": fetched_at, "response": body}


def best_coordinate(claims_response: dict, wikidata_id: str) -> dict:
    """Meilleure revendication « coordinate location » (P625) d'une réponse wbgetclaims."""
    claims = claims_response["claims"].get("P625", [])
    usable = [c for c in claims if c.get("rank") != "deprecated" and "datavalue" in c["mainsnak"]]
    usable.sort(key=lambda c: c.get("rank") != "preferred")  # « preferred » d'abord
    if not usable:
        raise ValueError(f"{wikidata_id} : aucune coordonnée P625 exploitable")
    value = usable[0]["mainsnak"]["datavalue"]["value"]
    if value.get("globe") != EARTH:
        raise ValueError(f"{wikidata_id} : coordonnée non terrestre ({value.get('globe')})")
    return {
        "lat": float(value["latitude"]),
        "lon": float(value["longitude"]),
        # Précision déclarée, en degrés (1 = 111 km environ) ; None si non donnée.
        "precision_deg": value.get("precision"),
        "coordinate_claims": len(claims),
    }


def claim_entity_ids(claims_response: dict, prop: str) -> list[str]:
    """Identifiants Q des valeurs EN COURS d'une propriété (ex. P17, pays).

    Wikidata garde les valeurs historiques avec une date de fin (qualificatif P582) : on les
    écarte. Si une valeur est au rang « preferred », elle seule est retenue.
    """
    current = []
    for claim in claims_response["claims"].get(prop, []):
        if claim.get("rank") == "deprecated" or "datavalue" not in claim["mainsnak"]:
            continue
        if "P582" in claim.get("qualifiers", {}):
            continue
        current.append(claim)
    preferred = [c for c in current if c.get("rank") == "preferred"]
    return [c["mainsnak"]["datavalue"]["value"]["id"] for c in (preferred or current)]
