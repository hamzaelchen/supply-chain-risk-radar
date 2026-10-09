# Jointure géographique et fenêtre temporelle (phase 2, étape 2)

Mesures du 2026-10-09 sur les vraies données (`silver.gdelt_events`, portefeuille fictif, détroits et villes Wikidata). Code : `silver/geo.py`, `silver/proximity.py`, `silver/recency.py`, `silver/proximity_bench.py`.

## Quels événements ont une position fiable

GDELT donne à chaque événement un type de lieu (`action_geo_type`). Les types 1 (pays), 2 (État américain) et 5 (région) donnent le **centroïde** du pays ou de la région, pas le lieu de l'événement. Un événement « Belgique » est placé au centre de la Belgique, donc compté « près » d'Anvers.

Mesuré sur 78 518 événements géolocalisés : parmi les paires (événement, ville de fournisseur) à moins de 150 km, 1 032 sur 5 667 (18 %) venaient d'un centroïde (782 pays, 242 États, 8 régions). Près des détroits : 105 paires sur 1 119 (83 pays, 22 régions).

**Règle :** la proximité n'utilise que les types 3 (ville des États-Unis) et 4 (ville du monde). Sur 88 699 événements, 53 034 (59,8 %) sont retenus. Conséquence assumée : un événement « Singapour » (type pays) n'est pas rattaché à un fournisseur de Singapour. Un lien par pays (code pays GDELT, qui est un code FIPS, pas ISO) est au backlog.

## Comparaison des jointures

Pour chaque événement précis, trouver les points à portée : 37 villes distinctes de fournisseurs (150 km) + 16 détroits (rayon propre) = 53 points. Résultat de référence : 6 195 paires, **identiques pour les cinq méthodes** (aucune paire manquante ni en trop, vérifié par `exceptAll`).

Temps pour compter le résultat, médiane de 3 exécutions après un tour de chauffe (une seule à x50), Spark local. Au-delà de x1, les événements réels sont recopiés pour mesurer le passage à l'échelle ; ces copies ne sont écrites nulle part.

| Méthode | x1 (53 034 évts) | x10 (530 340) | x50 (2 651 700) |
|---|---|---|---|
| Croisé + haversine | 0,98 s | 1,19 s | 4,50 s |
| **Croisé + préfiltre latitude** | **0,33 s** | **0,37 s** | **0,87 s** |
| Grille 1,0° | 0,82 s | 0,86 s | 1,22 s |
| Grille 0,5° | 0,82 s | 0,82 s | 1,53 s |
| H3 résolution 4 (UDF pandas) | 1,06 s | 2,05 s | 101,03 s |

Une première série de mesures avait donné, pour H3, 1,02 / 31,13 / 13,85 s : très irrégulier d'une exécution à l'autre (démarrage des processus Python de l'UDF), alors que les autres méthodes varient de moins de 10 %.

**Choix : produit croisé avec points diffusés (broadcast) et préfiltre sur la latitude.**
- Exact : l'écart de latitude en degrés ne peut pas dépasser la distance angulaire, donc aucune paire à portée n'est écartée. Ne dépend pas de la longitude, donc pas de cas particulier à l'antiméridien ni aux pôles.
- Le plus rapide à tous les volumes mesurés, 5 fois plus que le croisé simple à x50.
- Le plus simple : pas d'index à construire ni à prouver.
- Le coût croît avec événements x points. Avec 53 points c'est négligeable ; avec des milliers de points (tous les ports du monde), la grille (coût indépendant du nombre de points, écart de 1,2 s contre 0,87 s à x50 aujourd'hui) devient préférable. Elle est écrite, testée et équivalente (`within_radius_grid`) : le passage de l'une à l'autre tient en un paramètre.
- H3 écarté : de 3 fois (x1) à plus de 100 fois (x50) plus lent que le préfiltre, irrégulier, et demande deux dépendances (`h3`, `pandas`) pour un gain nul. Reste mesurable : `pip install -e ".[bench]"`.
- Au rythme réel (un lot GDELT toutes les 15 minutes, environ 1 000 à 2 000 événements), toutes les méthodes sont dominées par le démarrage de Spark.

Dépendances : aucune ajoutée au projet (règle 2 sans objet, rien de Spark ni d'Iceberg). `pandas` et `h3` ne sont que dans l'extra `bench`.

## Fenêtre temporelle : exclure les événements rétrospectifs

Écart = date UTC de `src_fetched_at` moins `sql_date`, en jours (jamais l'étiquette du lot GDELT, décision du 2026-10-08). Sur 80 792 événements :

| Écart (jours) | Événements | Part |
|---|---|---|
| < 0 (daté dans le futur) | 0 | 0 % |
| 0 | 71 083 | 87,98 % |
| 1 | 8 526 | 10,55 % |
| 2 | 14 | 0,02 % |
| 3 à 7 | 304 | 0,38 % |
| 8 à 30 | 376 | 0,47 % |
| 31 à 365 | 450 | 0,56 % |
| plus d'un an | 39 | 0,05 % |

Médiane 0, 90e et 95e percentiles 1, 99e 30, 99,9e 365. Le plus ancien : 2016-10-10.

**Anomalie expliquée.** Les 10,55 % à 1 jour viennent surtout de notre panne : lots téléchargés en direct (moins d'une heure après leur étiquette), 97,97 % à 0 jour et 0,33 % à 1 ; lots rattrapés (jusqu'à 11 h de retard cette nuit), 83,47 % à 0 et 15,17 % à 1. Le décalage d'un jour est un artefact de notre ingestion, pas du comportement de GDELT.

**Règle proposée :** récent si l'écart est compris entre -1 et 2 jours (`silver/recency.py`). 2 jours tombe dans le vide de la distribution (aucun événement à 2 jours en direct, la queue commence à 3) et couvre un rattrapage allant jusqu'à 24 h, la limite du producteur. -1 tolère une date locale en avance sur l'UTC (aucun cas observé). Sont exclus 1 169 événements sur 80 792 (1,45 %). Les événements rétrospectifs ressemblent aux autres (mentions moyennes 4,3 contre 4,7 ; sources 1,04 contre 1,05 ; même racines CAMEO dominantes) : rien ne permet de les repérer autrement que par la date.

La règle n'est pas appliquée dans les tables de proximité (elles restent purement géographiques) : le gold l'applique.

## Résultats sur les données réelles (88 699 événements)

- `silver.event_supplier_proximity` : 10 176 paires (événement, fournisseur) à moins de 150 km, dont 9 992 récentes, 4 783 événements distincts.
- `silver.event_chokepoint_proximity` : 1 158 paires (événement, détroit), dont 1 136 récentes.
- Aucun fournisseur sans événement récent à 150 km. Par fournisseur : minimum 1, médiane 56, maximum 1 015.
- **Très inégal :** Los Angeles 1 015, New York 893, Bombay 518, contre 1 pour le moins couvert. Le nombre brut d'événements mesure surtout la couverture médiatique de la ville. Le score devra le normaliser (étape 3).
- Événements récents par détroit : Malacca 177, Taïwan 177, cap de Bonne-Espérance 165, Ormuz 162, Bab-el-Mandeb 142, Øresund 83, Sonde 70, Pas de Calais 50, Bosphore 38, Gibraltar 26, Corée 16, Panama 12, Lombok 12, Suez 6. Gibraltar, le détroit le plus exposé du portefeuille (63 % des fournisseurs), n'a que 26 événements ; Suez en a 6. Échantillon : environ 18,5 h de GDELT (74 lots), trop court pour conclure.
- Beaucoup d'événements « près d'un détroit » sont en fait des grandes villes voisines (Taipei pour Taïwan, Kuala Lumpur pour Malacca, Dubaï pour Ormuz). Ils comptent comme proches du détroit, ce qui n'est pas la même chose qu'un événement dans le détroit.

## Limites

- Seul ActionGeo est utilisé (le lieu de l'action), pas les lieux des acteurs.
- Si le portefeuille ou un référentiel change (par exemple un nouveau tirage), les tables de proximité sont périmées : supprimer le curseur de `silver.event_supplier_proximity` (propriété `scrr.silver_events_ingested_at_cursor_us`) et relancer ; l'insertion est idempotente mais ne retire pas les anciennes lignes (à vider avant).
