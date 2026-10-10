{{ config(materialized='table') }}
-- Jours couverts par des lots GDELT (au moins `min_batches_per_day` sur 96). Un jour sans lots
-- (l'écart entre deux périodes chargées) n'est PAS un jour calme : il est exclu des normales.
select
    cast(src_gdelt_batch_label_ts as date) as day,
    count(distinct src_gdelt_batch_label_ts) as batches,
    count(*) as events,
    min(src_fetched_at) as first_fetched_at,
    max(src_fetched_at) as last_fetched_at,
    count(distinct src_gdelt_batch_label_ts) >= {{ var('min_batches_per_day') }} as covered
from {{ source('lh_events', 'events') }}
group by 1
