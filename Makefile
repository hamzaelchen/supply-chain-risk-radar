PY ?= python3.11
VENV := .venv
BIN := $(VENV)/bin

# .env optionnel : mêmes variables pour docker compose et pour Spark
-include .env
export

.PHONY: setup jars test test-integration up down ps smoke produce bronze silver ref portfolio proximity backfill gold export catchup-history site-data ci-local

setup:
	$(PY) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"

# Résout les jars Spark une fois dans jars/<empreinte des versions>/ (environ 800 Mo)
jars:
	$(BIN)/python -m lakehouse.jars

# Tests sans infra (les tests Spark démarrent une JVM locale)
test:
	$(BIN)/pytest -m "not integration"

# Tests de bout en bout : infra démarrée requise (make up)
test-integration:
	$(BIN)/pytest -m integration

# Infra locale : Kafka KRaft, MinIO, catalogue Iceberg REST
up:
	docker compose up -d --wait

down:
	docker compose down

ps:
	docker compose ps -a

# Aller-retour Spark -> catalogue REST -> MinIO (infra démarrée requise)
smoke:
	$(BIN)/python -m lakehouse.smoke_test

# Un cycle GDELT : lastupdate.txt -> zip dans MinIO -> topic gdelt.events
produce:
	$(BIN)/python -m ingestion.gdelt_producer

# Kafka -> bronze.gdelt_events : traite ce qui est en attente, puis s'arrête
bronze:
	$(BIN)/python -m streaming.bronze_gdelt_events

# bronze -> silver.gdelt_events (+ quarantaine) : traite ce qui est arrivé depuis le dernier passage
silver:
	$(BIN)/python -m silver.gdelt_events

# silver.ref_chokepoints et silver.ref_cities : rechargés depuis les CSV de ref/
ref:
	$(BIN)/python -m ref.chokepoints load
	$(BIN)/python -m ref.cities load

# silver.ref_suppliers et silver.ref_supplier_routes : portefeuille FICTIF depuis portfolio/*.csv
portfolio:
	$(BIN)/python -m portfolio.generate load

# silver.event_supplier_proximity et silver.event_chokepoint_proximity : événements proches (incrémental)
proximity:
	$(BIN)/python -m silver.proximity

# Chargement historique GDELT (en lot, sans Kafka). Exemple :
#   make backfill ARGS="--range 2026-01-04:2026-03-31 --range 2026-07-13:2026-10-08 --dry-run"
backfill:
	$(BIN)/python -m ingestion.gdelt_backfill $(ARGS)

# Gold (dbt + DuckDB) : score v1 sur la silver, tests dbt, puis export du contrat du site
gold:
	cd transform && DUCKDB_MEMORY_LIMIT=$${DUCKDB_MEMORY_LIMIT:-4GB} ../$(BIN)/dbt seed --profiles-dir . $(DBT_ARGS)
	cd transform && DUCKDB_MEMORY_LIMIT=$${DUCKDB_MEMORY_LIMIT:-4GB} ../$(BIN)/dbt run --profiles-dir . $(DBT_ARGS)
	cd transform && DUCKDB_MEMORY_LIMIT=$${DUCKDB_MEMORY_LIMIT:-4GB} ../$(BIN)/dbt test --profiles-dir . $(DBT_ARGS)

export:
	$(BIN)/python transform/export.py --db transform/target/gold.duckdb --out build/gold/data

# Fin du chargement historique : silver puis proximité par lots de jours (mémoire bornée), puis gold
# et export. À lancer APRÈS la fin du backfill (jamais en même temps : curseur sur ingested_at).
#   make catchup-history            # 10 jours de silver et 20 jours de proximité par passage
catchup-history:
	SPARK_DRIVER_MEMORY=$${SPARK_DRIVER_MEMORY:-6g} $(BIN)/python -m silver.gdelt_events --max-batches $${SILVER_BATCHES:-10} --until-caught-up
	SPARK_DRIVER_MEMORY=$${SPARK_DRIVER_MEMORY:-6g} $(BIN)/python -m silver.proximity --max-batches $${PROXIMITY_BATCHES:-20} --until-caught-up
	$(MAKE) gold export

# Copie l'export du backend dans les données publiées du site
# Instantané « Ormuz au 2 mars 2026 » : même modèles, base séparée, variable as_of_day
gold-ormuz:
	cd transform && GOLD_DB_PATH=target/ormuz.duckdb DUCKDB_MEMORY_LIMIT=$${DUCKDB_MEMORY_LIMIT:-4GB} ../$(BIN)/dbt build --profiles-dir . --vars '{as_of_day: 2026-03-02}'

# Données publiées du site : un dossier par instantané + index.json (web/public/data)
site-data:
	$(BIN)/python transform/site_data.py --out web/public/data

# Même enchaînement que la CI GitHub, en local (hors tests d'intégration)
ci-local:
	$(BIN)/ruff format --check .
	$(BIN)/ruff check .
	$(BIN)/pytest -m "not integration" -q
	$(BIN)/python transform/ci_fixture.py
	cd transform && ../$(BIN)/dbt build --target ci --profiles-dir . --vars '{min_batches_per_day: 1, baseline_days: 5, min_baseline_days: 3}'
	cd web && npm test && npm run build
