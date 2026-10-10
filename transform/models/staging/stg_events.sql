{{ config(materialized='table') }}
-- Événements GDELT proches d'un fournisseur ou d'un détroit, typés, classés par règles CAMEO.
--
-- Axe du temps : `batch_day` = jour de l'étiquette du lot GDELT, pour le flux comme pour
-- l'historique (décision « à la date du lot », 2026-10-09). La récence se mesure depuis
-- `observed_at` : heure de téléchargement pour le flux, étiquette du lot pour le backfill
-- (exception du 2026-10-09, voir silver/recency.py).
with near as (
    select global_event_id from {{ source('lh_pairs', 'supplier_pairs') }}
    union
    select global_event_id from {{ source('lh_pairs', 'chokepoint_pairs') }}
),
ev as (
    select
        e.global_event_id,
        e.sql_date,
        e.event_code,
        e.event_root_code,
        e.quad_class,
        e.goldstein_scale,
        e.avg_tone,
        e.num_mentions,
        e.num_sources,
        e.num_articles,
        e.source_url,
        e.src_file_url,
        e.action_geo_full_name,
        e.action_geo_type,
        e.action_geo_lat,
        e.action_geo_long,
        e.src_fetched_at,
        e.src_gdelt_batch_label_ts,
        coalesce(e.ingestion_mode, 'stream') as ingestion_mode
    from {{ source('lh_events', 'events') }} e
    join near using (global_event_id)
),
classified as (
    select
        ev.*,
        r.family,
        r.family_label_fr,
        r.family_weight,
        r.code_prefix as rule_code_prefix
    from ev
    left join {{ ref('cameo_rules') }} r on starts_with(ev.event_code, r.code_prefix)
    -- Règle la plus précise : le préfixe le plus long l'emporte (ex. 1723 sur 17).
    qualify row_number() over (
        partition by ev.global_event_id order by length(r.code_prefix) desc nulls last
    ) = 1
),
timed as (
    select
        *,
        cast(src_gdelt_batch_label_ts as date) as batch_day,
        case when ingestion_mode = 'backfill' then src_gdelt_batch_label_ts else src_fetched_at end
            as observed_at
    from classified
)
select
    *,
    date_diff('day', sql_date, cast(observed_at as date)) as lag_days,
    date_diff('day', sql_date, cast(observed_at as date))
        between {{ var('recency_min_lag_days') }} and {{ var('recency_max_lag_days') }} as is_recent,
    (quad_class in (3, 4) and family is not null) as is_relevant,
    -- Poids d'un événement : famille x gravité (Goldstein, 0,5 à 1) x sources (1 à 2).
    case
        when quad_class in (3, 4) and family is not null then
            family_weight
            * (0.5 + 0.5 * least(1.0, greatest(0.0, -goldstein_scale / 10.0)))
            * least(2.0, 1.0 + 0.5 * ln(greatest(num_sources, 1)))
        else 0.0
    end as event_weight
from timed
