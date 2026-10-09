"""Schéma explicite de bronze.gdelt_events.

Les 61 colonnes suivent l'ordre du fichier export GDELT 2.0 (Event Database
Codebook v2.0), renommées en snake_case. Toutes en STRING : le bronze ne juge
pas le contenu, le typage et les valeurs vides sont traités en silver.
"""

_ACTOR_ATTRIBUTES = [
    "code", "name", "country_code", "known_group_code", "ethnic_code",
    "religion1_code", "religion2_code", "type1_code", "type2_code", "type3_code",
]  # fmt: skip
_GEO_ATTRIBUTES = [
    "type",
    "full_name",
    "country_code",
    "adm1_code",
    "adm2_code",
    "lat",
    "long",
    "feature_id",
]

GDELT_EVENT_COLUMNS = [
    "global_event_id",  # GLOBALEVENTID
    "sql_date",  # Day (YYYYMMDD) : date de l'événement, pas de sa publication
    "month_year",
    "year",
    "fraction_date",
    *[f"actor1_{a}" for a in _ACTOR_ATTRIBUTES],
    *[f"actor2_{a}" for a in _ACTOR_ATTRIBUTES],
    "is_root_event",
    "event_code",
    "event_base_code",
    "event_root_code",
    "quad_class",
    "goldstein_scale",
    "num_mentions",
    "num_sources",
    "num_articles",
    "avg_tone",
    *[f"actor1_geo_{a}" for a in _GEO_ATTRIBUTES],
    *[f"actor2_geo_{a}" for a in _GEO_ATTRIBUTES],
    *[f"action_geo_{a}" for a in _GEO_ATTRIBUTES],
    # DATEADDED (YYYYMMDDHHMMSS UTC) : étiquette du lot de 15 minutes, identique au nom
    # du fichier. Ce n'est pas l'heure de publication (voir ingestion/gdelt.py).
    "date_added",
    "source_url",  # SOURCEURL : article d'origine
]

# Lignage : d'où vient la ligne (en-têtes Kafka posés par le producteur), où elle
# était dans Kafka, quand elle est entrée en bronze. `src_gdelt_batch_label_ts` est l'étiquette
# GDELT du lot ; les heures réelles sont `src_fetched_at`, `kafka_timestamp` et
# `ingested_at`.
LINEAGE_COLUMNS = [
    ("src_file_url", "STRING"),
    ("src_gdelt_batch_label_ts", "TIMESTAMP"),
    ("src_fetched_at", "TIMESTAMP"),
    ("src_raw_object_uri", "STRING"),
    ("kafka_topic", "STRING"),
    ("kafka_partition", "INT"),
    ("kafka_offset", "BIGINT"),
    ("kafka_timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
]

# Mode d'ingestion et identité d'une ligne hors Kafka. Ajoutées EN FIN de table : sur une table
# existante, ALTER TABLE ADD COLUMN les place là, et l'ordre doit être celui d'une table neuve.
#   ingestion_mode : « stream » (Kafka) ou « backfill » (chargement historique par lot). NULL sur
#                    les lignes écrites avant l'ajout de la colonne : à lire comme « stream ».
#   src_line_no    : numéro de ligne (à partir de 1) dans le fichier GDELT, pour le backfill ;
#                    NULL en flux. Identité d'une ligne backfill = (src_file_url, src_line_no),
#                    là où une ligne du flux est identifiée par (topic, partition, offset) Kafka.
INGESTION_COLUMNS = [
    ("ingestion_mode", "STRING"),
    ("src_line_no", "INT"),
]

BRONZE_COLUMNS = [
    ("raw_line", "STRING"),
    # Nombre de champs trouvés dans la ligne : 61 attendu. Une ligne mal formée
    # est conservée et se repère ici, elle n'est pas rejetée.
    ("field_count", "INT"),
    *[(name, "STRING") for name in GDELT_EVENT_COLUMNS],
    *LINEAGE_COLUMNS,
    *INGESTION_COLUMNS,
]
