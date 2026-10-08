# supply-chain-risk-radar

## Contexte
Radar de risque supply chain. Ingère GDELT 2.0 (toutes les 15 min) et IMF PortWatch, stocke dans un lakehouse Apache Iceberg (bronze/silver/gold), classe les événements avec un LLM, calcule un score de risque 0-100 pour 100 fournisseurs d'une « Entreprise démo ».

Deux sorties :
- (a) pipeline local complet : Kafka + Spark Structured Streaming + Iceberg + dbt + Dagster.
- (b) démo en ligne « live-lite » : GitHub Actions toutes les 30 min -> tables gold en Parquet -> site statique React lisant avec DuckDB-WASM.

Environnement : WSL2 Ubuntu 24.04, Python 3.11 (venv), Docker. pyspark 4.1.1 déjà utilisé sur un autre projet.
Rôle de Claude : mentor senior data engineer, réponses en français, expliquer le pourquoi de chaque choix.

## Règles non négociables
1. Aucune donnée inventée. Seul le portefeuille fournisseurs est fictif (graine fixe, étiqueté « Entreprise démo »). Tout événement ou chiffre de trafic vient d'un fichier réellement téléchargé, avec URL source et horodatage en bronze.
2. Avant d'ajouter une dépendance Spark/Iceberg : vérifier sa compatibilité exacte avec Spark 4.1.x sur Maven Central, dire ce qui a été vérifié. Si non confirmé, proposer une alternative.
3. Aucun service cloud payant ni clé d'API LLM utilisée sans confirmation de l'utilisateur.
4. Tests pytest pour chaque transformation.

Méthode : une étape à la fois, arrêt après chaque étape pour validation.

## Plan des phases
- Phase 0 — Fondations locales : scaffold, infra docker (Kafka KRaft, MinIO, catalogue Iceberg REST), producteur GDELT, streaming vers bronze.
- Phase 1 — Bronze/silver : mentions, parsing et typage, déduplication, ingestion IMF PortWatch.
- Phase 2 — Portefeuille et gold : générateur des 100 fournisseurs (graine fixe), dbt, modèles gold, score de risque 0-100.
- Phase 3 — Classification LLM : taxonomie d'événements, évaluation sur échantillon, coûts (accord préalable obligatoire).
- Phase 4 — Orchestration : assets Dagster, planification 15 min, sensors, reprise sur incident.
- Phase 5 — Live-lite : export gold en Parquet, GitHub Actions 30 min, site React + DuckDB-WASM, API éventuelle.
- Phase 6 — Durcissement : qualité des données, observabilité, CI, documentation, préparation entretien.

## Décisions
(à compléter au fil des étapes : date, décision, raison, alternatives écartées)

- 2026-10-08 — Dépendances Python minimales à l'étape 1 (pytest, ruff). Pyspark et Iceberg ajoutés à l'étape 2 après vérification Maven Central (règle 2).
