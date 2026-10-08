PY ?= python3.11
VENV := .venv
BIN := $(VENV)/bin

.PHONY: setup test up down

setup:
	$(PY) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"

test:
	$(BIN)/pytest

# Infra locale : docker-compose.yml arrive à l'étape 2
up:
	docker compose up -d

down:
	docker compose down
