PY ?= python3.11
VENV := .venv
BIN := $(VENV)/bin

# .env optionnel : mêmes variables pour docker compose et pour Spark
-include .env
export

.PHONY: setup jars test test-integration up down ps smoke produce bronze silver ref portfolio proximity

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
