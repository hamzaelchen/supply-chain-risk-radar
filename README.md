# supply-chain-risk-radar

Radar de risque supply chain : ingestion GDELT 2.0 et IMF PortWatch dans un lakehouse Apache Iceberg (bronze/silver/gold), classification LLM, score de risque 0-100 pour 100 fournisseurs d'une « Entreprise démo » (portefeuille fictif, graine fixe).

Statut : Phase 0, étape 4 (streaming Kafka vers `bronze.gdelt_events`).

## Démarrage (WSL2 Ubuntu, Python 3.11)

    make setup   # venv + dépendances dev
    make test    # tests sans infra (les tests Spark démarrent une JVM locale)
    make up      # Kafka, MinIO, catalogue Iceberg REST (Docker)
    make smoke   # aller-retour Spark -> catalogue REST -> MinIO
    make produce # un cycle GDELT : zip brut -> MinIO, événements -> topic gdelt.events
    make bronze  # Kafka -> bronze.gdelt_events (traite ce qui est en attente, puis s'arrête)
    make test-integration  # tests de bout en bout, infra démarrée requise
    make down    # arrêt (les volumes sont conservés)

Prérequis : Docker, Java 17. Services : Kafka `localhost:19092`, MinIO `localhost:19000` (console `localhost:19001`), catalogue REST `localhost:8181`. Ports et identifiants surchargeables via `.env` (voir [.env.example](.env.example)).

Contexte, règles et plan des phases : voir [CLAUDE.md](CLAUDE.md).
