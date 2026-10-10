# Site de démo (phase 5)

React + Vite + TypeScript. Lit des fichiers Parquet avec DuckDB-WASM (binaires et extension Parquet servis par le site, aucune requête vers un CDN ; vérifié en navigateur) ; la carte est un SVG d3-geo sur un fond Natural Earth embarqué (`world-atlas`), sans service de tuiles.

Contrat des données : [CONTRACT.md](CONTRACT.md).

    npm install
    npm run dev        # http://localhost:5173
    npm test           # vitest
    npm run build      # dist/ ; télécharge l'extension Parquet de DuckDB (scripts/fetch-duckdb-ext.mjs) (les .wasm pèsent ~34 Mo, ~8 Mo compressés)

`public/data/` contient l'**export réel** du backend (`meta.is_sample = false`) : fichiers produits par `make export` (voir [../transform/export.py](../transform/export.py)) puis copiés par `make site-data`. Un jeu d'**exemple** (bandeau permanent « EXEMPLE » quand `meta.is_sample` est vrai) se régénère avec :

    PYTHONPATH=. python web/scripts/make_sample.py   # nécessite pyarrow

Réel dans l'export : événements GDELT, villes et détroits (Wikidata), score v1 (dbt). Fictif : le portefeuille de fournisseurs, étiqueté partout.

DuckDB-WASM est chargé à la demande : la page, le bandeau et la date de mise à jour s'affichent depuis `meta.json` avant le téléchargement du moteur (~8 Mo compressés).

Déploiement : [.github/workflows/web-pages.yml](../.github/workflows/web-pages.yml), manuel et non activé (dépôt privé), voir le backlog.
