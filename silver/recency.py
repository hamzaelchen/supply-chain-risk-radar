"""Fenêtre temporelle : écarter les événements rétrospectifs.

Un événement GDELT porte sa date (SQLDATE) ; l'article qui l'a produit peut être publié des jours,
voire des années plus tard (mesuré le 2026-10-09 : jusqu'à 10 ans). Pour un score de risque
« maintenant », un tel événement n'est pas un signal frais.

Écart = date UTC de téléchargement (`src_fetched_at`, jamais l'étiquette du lot, règle du
2026-10-08) moins SQLDATE, en jours. Règle : récent si l'écart est compris entre -1 et 2 jours.
- 2 jours : sur les lots téléchargés en direct, 97,97 % des événements ont un écart de 0, 0,33 %
  de 1, aucun de 2 ; la queue commence à 3 jours. 2 est donc dans le « vide » de la distribution ;
- le jour de marge couvre un rattrapage après panne : un lot téléchargé jusqu'à 24 h après son
  étiquette (limite du producteur) décale l'écart d'un jour (15,17 % d'écart à 1 sur les lots
  rattrapés le 2026-10-09, contre 0,33 % en direct) ;
- -1 jour : une date d'événement locale peut précéder d'un jour la date UTC (aucun cas observé).
Voir docs/geo-join.md pour la distribution complète.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

RECENT_MIN_LAG_DAYS = -1
RECENT_MAX_LAG_DAYS = 2


def lag_days() -> Column:
    return F.datediff(F.to_date(F.col("src_fetched_at")), F.col("sql_date"))


def is_recent_event() -> Column:
    return lag_days().between(RECENT_MIN_LAG_DAYS, RECENT_MAX_LAG_DAYS)
