{{ config(materialized='table') }}
-- Score d'un détroit par jour : anomalie des événements pertinents du rayon par rapport à sa
-- normale. Sans au moins `min_articles_for_critical` articles distincts, jamais de niveau critique.
with s as (
    select
        entity_id as chokepoint_id,
        day,
        {{ component_from_z('x', 'mu', 'sd', 'n_base') }} as raw_score,
        n_base >= {{ var('min_baseline_days') }} as baseline_ok
    from ({{ anomaly_series(ref('int_chokepoint_day'), 'chokepoint_id', 'i_zone_norm') }})
)
select
    s.chokepoint_id,
    s.day,
    case
        when coalesce(a.n_articles, 0) < {{ var('min_articles_for_critical') }}
            then least(s.raw_score, {{ var('threshold_critical') }})
        else s.raw_score
    end as risk_score,
    coalesce(a.n_articles, 0) as n_articles,
    s.baseline_ok
from s
left join {{ ref('int_chokepoint_articles') }} a using (chokepoint_id, day)
