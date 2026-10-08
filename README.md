# supply-chain-risk-radar

Radar de risque supply chain : ingestion GDELT 2.0 et IMF PortWatch dans un lakehouse Apache Iceberg (bronze/silver/gold), classification LLM, score de risque 0-100 pour 100 fournisseurs d'une « Entreprise démo » (portefeuille fictif, graine fixe).

Statut : Phase 0, étape 1 (scaffold).

## Démarrage (WSL2 Ubuntu, Python 3.11)

    make setup   # venv + dépendances dev
    make test    # pytest

Contexte, règles et plan des phases : voir [CLAUDE.md](CLAUDE.md).
