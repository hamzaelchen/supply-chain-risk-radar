PY ?= python3.11
VENV := .venv
BIN := $(VENV)/bin

# .env optionnel : mêmes variables pour docker compose et pour Spark
-include .env
export

.PHONY: setup test up down ps smoke

setup:
	$(PY) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"

test:
	$(BIN)/pytest

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
