"""Règle de routage : région d'origine -> détroits traversés pour atteindre Casablanca.

Règle (volontairement simple, FICTIVE comme le portefeuille, ce n'est pas un trajet réel) :
- on suit le chemin maritime habituel le plus court en nombre de détroits ; entre la mer
  Rouge et la Méditerranée on passe par Suez, jamais par le cap de Bonne-Espérance ;
- Casablanca est sur l'Atlantique : tout ce qui vient de Méditerranée traverse Gibraltar,
  tout ce qui vient d'Europe du Nord traverse le Pas de Calais, et rien ne traverse de détroit
  pour l'Europe atlantique, l'Afrique de l'Ouest ou la côte atlantique des Amériques ;
- la liste est ordonnée de l'origine vers Casablanca ;
- seuls comptent les détroits du référentiel `silver.ref_chokepoints` ; un passage absent du
  référentiel (détroit de Floride, canal de Corinthe...) n'apparaît pas ;
- les villes sont des centres-villes ; la route part du port de la région, pas de la ville ;
- la marchandise peut en réalité arriver par Tanger Med (dans la zone du détroit de Gibraltar)
  plutôt que par le port de Casablanca : pour la surveillance c'est équivalent, Gibraltar est dans
  les deux cas dans le périmètre (Tanger Med est à moins de 20 km de son point de référence).

Simplifications connues : le détroit de Taïwan est compté pour tout le nord de la Chine et pour
la Corée (une route à l'est de Taïwan existe) ; Luçon, Lombok, Sonde et Dardanelles-sans-Marmara
ne sont utilisés par aucune région ; les déroutages (cap de Bonne-Espérance en cas de crise en
mer Rouge) ne sont pas modélisés dans la route de base.
"""

REGION_ROUTES: dict[str, list[str]] = {
    "MA": [],  # fournisseur marocain : pas de détroit
    "EU_ATLANTIC": [],
    "EU_NORTH_SEA": ["dover"],
    "EU_BALTIC": ["oresund", "dover"],
    "EU_MEDITERRANEAN": ["gibraltar"],
    "TR_MARMARA": ["dardanelles", "gibraltar"],  # mer de Marmara : Dardanelles, pas Bosphore
    "TR_AEGEAN": ["gibraltar"],
    "BLACK_SEA": ["bosphorus", "dardanelles", "gibraltar"],
    "EGYPT_MEDITERRANEAN": ["gibraltar"],
    "RED_SEA": ["suez", "gibraltar"],
    "PERSIAN_GULF": ["hormuz", "bab_el_mandeb", "suez", "gibraltar"],
    "SOUTH_ASIA": ["bab_el_mandeb", "suez", "gibraltar"],
    "SOUTHEAST_ASIA": ["malacca", "bab_el_mandeb", "suez", "gibraltar"],
    "CHINA_NORTH": ["taiwan", "malacca", "bab_el_mandeb", "suez", "gibraltar"],
    "CHINA_SOUTH": ["malacca", "bab_el_mandeb", "suez", "gibraltar"],
    "KOREA": ["korea", "taiwan", "malacca", "bab_el_mandeb", "suez", "gibraltar"],
    "SOUTHERN_AFRICA": ["cape_of_good_hope"],
    "AMERICAS_ATLANTIC": [],
    "AMERICAS_PACIFIC": ["panama"],
}


def route_for(region: str) -> list[str]:
    """Détroits traversés, de l'origine vers Casablanca. Lève KeyError si la région est inconnue."""
    return list(REGION_ROUTES[region])
