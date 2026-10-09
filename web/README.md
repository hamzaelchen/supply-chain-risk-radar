# Site de démo (phase 5)

React + Vite + TypeScript. Lit des fichiers Parquet avec DuckDB-WASM (binaires servis par le site, pas de CDN) ; la carte est un SVG d3-geo sur un fond Natural Earth embarqué (`world-atlas`), sans service de tuiles.

Contrat des données : [CONTRACT.md](CONTRACT.md).

    npm install
    npm run dev        # http://localhost:5173
    npm test           # vitest
    npm run build      # dist/ (les .wasm pèsent ~34 Mo, ~8 Mo compressés)

`public/data/` contient aujourd'hui un jeu d'**EXEMPLE** (bandeau permanent tant que `meta.is_sample` est vrai), régénérable depuis la racine du dépôt :

    PYTHONPATH=. python web/scripts/make_sample.py   # nécessite pyarrow

Réel : portefeuille (généré), détroits et villes (Wikidata), événements (un lot GDELT de `tests/fixtures`). Illustration : le score, en attendant celui du backend.
