# Tâches différées

Chaque ligne : ce qu'il reste à faire, à partir de quand, et pourquoi on ne l'a pas fait tout de suite. Une tâche terminée est supprimée de ce fichier ; la décision correspondante reste dans CLAUDE.md.

| Échéance | Tâche | Raison du report |
|---|---|---|
| à partir du 2026-10-16 | Retirer la compatibilité avec l'ancien en-tête Kafka `slot_ts` dans `streaming/bronze_gdelt_events.py` (le `coalesce` et son test `test_legacy_slot_ts_header_still_fills_the_batch_label`). | Le topic `gdelt.events` garde 7 jours de messages : jusqu'au 2026-10-15, un rejeu lirait encore des messages publiés avec l'ancien nom. Voir la décision du renommage dans CLAUDE.md. |
| quand `make jars` change de versions | Supprimer les anciens dossiers `jars/<empreinte>/`. Un seul existe aujourd'hui (`c628e747d51b`, 736 Mo) ; chaque changement de version en laisse un de plus (~770 Mo). | `make jars` ne purge pas les dossiers précédents : on garde la possibilité de revenir en arrière. À automatiser ou à faire à la main. |
| phase 4 | Expiration des snapshots et compaction des petits fichiers de `bronze.gdelt_events` et de `bronze.ingestion_manifest`. | Environ 96 lots GDELT par jour donnent ~96 petits fichiers et ~96 snapshots de manifeste par jour. Sans effet visible à notre volume actuel, mais cela se dégrade avec le temps. À orchestrer avec Dagster. |
| phase 4 | Remplacer `apache/iceberg-rest-fixture:1.10.1` par Lakekeeper ou Polaris. | Image de dev/test, sans tag 1.11.x ni 1.12.x publié. Le client 1.12.0 parle à un serveur 1.10.1 (validé par les tests), mais ce n'est pas une matrice officielle. |
