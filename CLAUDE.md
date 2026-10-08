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
- 2026-10-08 — Projet placé dans le système de fichiers WSL (~/supply-chain-risk-radar) et non sous /mnt/c. Raison : performances I/O de Docker et Spark (les montages Windows passent par 9P, très lents sur beaucoup de petits fichiers).
- 2026-10-08 — Python 3.11 installé via le PPA deadsnakes : Ubuntu 24.04 ne fournit que 3.12.
- 2026-10-08 — pyspark 4.1.1 + Iceberg 1.11.0 (`iceberg-spark-runtime-4.1_2.13` et `iceberg-aws-bundle`). Vérifié sur Maven Central : l'artefact 4.1_2.13 existe en 1.11.0 (2026-05-15) et 1.12.0 (2026-09-25) ; 1.11.0 est compilé contre Spark 4.1.1, 1.12.0 contre Spark 4.1.3. Écarté : 1.12.0, à adopter en même temps que pyspark 4.1.3. Validé de bout en bout par `make smoke`.
- 2026-10-08 — Catalogue Iceberg REST (`apache/iceberg-rest-fixture:1.10.1`, état dans SQLite sur volume). Raison : protocole standard et neutre, le client ne connaît qu'une URL, le même catalogue sert Spark, dbt, DuckDB et PyIceberg. Écartés : Hive Metastore (lourd), catalogue Hadoop (pas de commit atomique sur S3), JDBC direct (chaque client doit avoir le driver et les identifiants de la base), Nessie/Polaris (surdimensionnés ici). Limite : image de dev/test, pas de tag 1.11.x publié (client 1.11.0 vers serveur 1.10.1, validé par `make smoke`).
- 2026-10-08 — MinIO via `cgr.dev/chainguard/minio`, épinglé par digest (MinIO RELEASE.2026-09-22). Raison : les images officielles minio/minio et minio/mc ne sont plus disponibles sur Docker Hub ni Quay. Écartés : image locale en cache (non reproductible), SeaweedFS, RustFS (trop jeune). Limite : seul `latest` est gratuit chez Chainguard, pérennité du digest non garantie.
- 2026-10-08 — Kafka `apache/kafka:4.3.1` en KRaft mode combiné, un seul nœud, création automatique de topics désactivée. Spark 4.1.1 embarque kafka-clients 3.9.1, compatible avec un broker 4.x.
- 2026-10-08 — Ports hôte décalés (Kafka 19092, MinIO 19000/19001, REST 8181), liés à 127.0.0.1. Raison : 9092/9000/9001 déjà pris par une autre stack locale. Spark tourne sur l'hôte WSL, pas en conteneur.
