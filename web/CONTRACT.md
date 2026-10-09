# Contrat de données du site (phase 5)

Ce que le site attend des exports gold. **Proposition côté web** : le backend (phase 2 étape 3, puis export de la phase 5) n'existe pas encore. Tout ce qui est marqué *à valider* doit être confronté au score réel avant le premier export ; si le backend diffère, on change ce fichier **et** le code, jamais l'un sans l'autre.

Le site lit des fichiers statiques avec DuckDB-WASM (HTTP range sur du Parquet). Aucune API, aucun calcul métier côté navigateur : le site affiche, filtre et trie, il ne recalcule pas un score.

## Fichiers

Dossier publié : `data/` à la racine du site. Cinq fichiers, tous obligatoires.

| Fichier | Grain | Contenu |
|---|---|---|
| `meta.json` | 1 objet | Fraîcheur, fenêtre, version du score, drapeau exemple |
| `supplier_scores.parquet` | 1 ligne par fournisseur (100) | Score et composantes |
| `supplier_events.parquet` | 1 ligne par (fournisseur, événement, lien) | Événements explicatifs |
| `chokepoint_scores.parquet` | 1 ligne par détroit (16) | Score et exposition du portefeuille |
| `supplier_routes.parquet` | 1 ligne par (fournisseur, étape) | Détroits de la route, dans l'ordre |

Parquet : compression zstd ou snappy, horodatages en `TIMESTAMP` UTC, pas de types imbriqués (DuckDB-WASM les lit, mais cela garde les requêtes du site simples).

## `meta.json`

| Clé | Type | Règle |
|---|---|---|
| `schema_version` | entier | `1` pour ce contrat. Le site refuse une autre valeur et l'affiche. |
| `is_sample` | booléen | `true` : données d'exemple (voir ci-dessous). Le site affiche alors un bandeau « EXEMPLE » non masquable. **Un export réel porte `false`.** |
| `sample_notice` | texte \| null | Obligatoire si `is_sample`. Dit en une phrase ce qui est réel et ce qui ne l'est pas. |
| `generated_at` | ISO 8601 UTC | Fin de l'export. C'est la « date de mise à jour » affichée. |
| `window_start`, `window_end` | ISO 8601 UTC | Fenêtre des événements pris en compte, en heure de **téléchargement** (`src_fetched_at`), jamais l'étiquette du lot GDELT (décision du 2026-10-08). |
| `latest_fetched_at` | ISO 8601 UTC | `max(src_fetched_at)` des événements de l'export : fraîcheur réelle des données. |
| `gdelt_batches` | entier | Nombre de lots GDELT (15 min) couverts. |
| `events_total` | entier | Événements GDELT lus pour calculer ces scores (avant jointure). |
| `score_version` | texte | Ex. `v1`. En exemple : `exemple-0`. |
| `risk_levels` | liste | `[{id, label_fr, min, max, alert}]` (`alert` : booléen, ce niveau est listé comme alerte), bornes du score par niveau (*à valider*, voir plus bas). Le site ne code **aucun** seuil en dur : il lit celui-ci. |
| `portfolio_label` | texte | `FICTIF - Entreprise démo` (étiquette du portefeuille). |

## `supplier_scores.parquet`

| Colonne | Type | Note |
|---|---|---|
| `supplier_id` | VARCHAR | `S001`..`S100`, clé |
| `name`, `category`, `category_label` | VARCHAR | `portfolio/suppliers.csv`. `category` est l'identifiant, `category_label` le libellé français. |
| `city`, `country`, `iso2`, `region` | VARCHAR | idem |
| `lat`, `lon` | DOUBLE | Centre-ville Wikidata, **pas le port** |
| `sole_source` | BOOLEAN | Fournisseur unique |
| `purchase_share_pct` | DOUBLE | Part des achats, somme 100,00 |
| `label` | VARCHAR | `FICTIF - Entreprise démo`, sur toutes les lignes |
| `risk_score` | DOUBLE | 0 à 100. NULL interdit : un fournisseur sans événement a un score, pas un NULL. |
| `risk_level` | VARCHAR | Identifiant d'un niveau de `meta.risk_levels`. |
| `score_local` | DOUBLE | Composante « événements près de la ville » (0-100) |
| `score_route` | DOUBLE | Composante « événements près des détroits de la route » (0-100) |
| `events_local` | INTEGER | Événements récents à 150 km (fenêtre temporelle appliquée) |
| `events_route` | INTEGER | Événements récents dans le rayon d'un détroit **de la route** |
| `top_driver` | VARCHAR \| NULL | Phrase française produite par le backend qui dit pourquoi ce score (ex. « 3 événements près du détroit d'Ormuz »). NULL si aucun événement. Le site n'écrit jamais cette phrase lui-même. |

Les deux composantes sont celles de la formule `risk_score` ; la façon de les combiner (pondération, criticité, normalisation par la couverture médiatique de la ville) est la décision du backend, le site n'en a pas besoin pour afficher. *À valider* : si le score v1 n'a pas cette décomposition en deux, la page détail n'affichera que `risk_score`.

## `supplier_events.parquet`

Un événement explique le score d'un fournisseur par **un lien** : la ville du fournisseur ou un détroit de sa route. Le même événement peut apparaître pour plusieurs fournisseurs, et deux fois pour un fournisseur (ville et détroit).

| Colonne | Type | Note |
|---|---|---|
| `supplier_id` | VARCHAR | |
| `link_type` | VARCHAR | `city` ou `chokepoint` |
| `chokepoint_id` | VARCHAR \| NULL | Renseigné si et seulement si `link_type = 'chokepoint'`, et dans la route du fournisseur |
| `distance_km` | DOUBLE | Distance à la ville ou au point du détroit |
| `global_event_id` | BIGINT | Clé GDELT |
| `event_date` | DATE | `sql_date` : jour de l'événement |
| `fetched_at` | TIMESTAMP | `src_fetched_at` : quand le fichier GDELT a été téléchargé |
| `action_geo_name` | VARCHAR | `action_geo_full_name` |
| `action_geo_type` | INTEGER | 3 ou 4 seulement (règle de proximité de `docs/geo-join.md`) |
| `lat`, `lon` | DOUBLE | ActionGeo de l'événement |
| `event_code`, `event_root_code` | VARCHAR | Codes CAMEO en texte (zéros de tête) |
| `quad_class` | INTEGER | 1-4 |
| `goldstein_scale` | DOUBLE | -10..10 |
| `avg_tone` | DOUBLE | |
| `num_mentions`, `num_sources`, `num_articles` | INTEGER | |
| `source_url` | VARCHAR | Article d'origine (SOURCEURL) : le lien « voir la source » |
| `src_file_url` | VARCHAR | Fichier GDELT d'où vient la ligne (lignage) |
| `contribution` | DOUBLE | Points de score apportés. Optionnel (NULL accepté) : le site trie alors par `num_mentions`. |
| `llm_category`, `llm_summary` | VARCHAR \| NULL | **Phase 3.** NULL tant que la classification n'existe pas ; le site n'affiche rien à leur place (pas de « inconnu » inventé). |

Les événements d'une fenêtre unique : le backend n'exporte que les événements récents au sens de `silver/recency.py`.

## `chokepoint_scores.parquet`

| Colonne | Type | Note |
|---|---|---|
| `chokepoint_id` | VARCHAR | Identifiant de `ref/chokepoints.csv` |
| `name_fr`, `name_en` | VARCHAR | |
| `lat`, `lon` | DOUBLE | Point Wikidata (représentatif, pas une ligne) |
| `radius_km` | DOUBLE | **Choix de conception**, pas une mesure ; le site le dit |
| `risk_score`, `risk_level` | DOUBLE, VARCHAR | Même échelle que les fournisseurs. *À valider* : le backend définit-il un score de détroit ? Sinon `risk_level` est dérivé du seul nombre d'événements et la colonne `risk_score` est NULL. |
| `events_recent` | INTEGER | Événements récents dans le rayon (chiffre de `docs/geo-join.md`) |
| `suppliers_exposed` | INTEGER | Fournisseurs dont la route le traverse |
| `purchase_share_exposed_pct` | DOUBLE | Part des achats correspondante |
| `sole_source_exposed` | INTEGER | Fournisseurs uniques exposés |

## `supplier_routes.parquet`

`supplier_id`, `position` (1 = le plus proche de l'origine), `chokepoint_id`.

## Niveaux de risque (*à valider*)

Proposition pour le fichier d'exemple uniquement : `low` [0, 25[, `medium` [25, 50[, `high` [50, 75[, `critical` [75, 100]. Le vrai score peut être très concentré sur le bas (le nombre d'événements est très inégal d'une ville à l'autre) ; les bornes se fixent sur la distribution réelle, pas ici.

## Règles que le site applique

1. `is_sample = true` -> bandeau « EXEMPLE » permanent, et aucun chiffre n'est présenté sans lui.
2. Bandeau permanent : « Entreprise démo, données fictives pour le portefeuille, événements réels GDELT ».
3. Fraîcheur : `generated_at` (mise à jour) et `latest_fetched_at` (dernier téléchargement GDELT). Jamais une étiquette de lot.
4. Un champ NULL n'est jamais remplacé par une valeur plausible : il est masqué ou affiché « n/d ».
5. Un fichier absent, d'un `schema_version` inconnu, ou dont une colonne manque : le site affiche l'erreur nommée (fichier, colonne), pas une page vide.
6. Rayon des détroits et position des villes : mention « choix de conception » / « centre-ville » dans l'interface.
