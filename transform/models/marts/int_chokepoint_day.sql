{{ config(materialized='table') }}
-- Une ligne par détroit et par jour couvert : intensité des événements pertinents dans son rayon
-- (tous, sans anti double comptage : c'est la mesure du détroit lui-même, pas d'un fournisseur).
with calendar as (
    select k.chokepoint_id, c.day
    from {{ ref('stg_chokepoints') }} k
    cross join (select day from {{ ref('stg_coverage') }} where covered) c
),
agg as (
    select
        cp.chokepoint_id,
        e.batch_day as day,
        sum(e.event_weight) filter (where e.is_relevant) as i_zone,
        count(*) filter (where e.is_relevant) as n_relevant,
        count(*) as n_all
    from {{ source('lh_pairs', 'chokepoint_pairs') }} cp
    join {{ ref('stg_events') }} e using (global_event_id)
    where e.is_recent
    group by 1, 2
)
select
    c.chokepoint_id,
    c.day,
    coalesce(a.i_zone, 0.0) as i_zone,
    coalesce(a.i_zone, 0.0) * {{ var('volume_reference') }} / cov.events as i_zone_norm,
    coalesce(a.n_relevant, 0) as n_relevant,
    coalesce(a.n_all, 0) as n_all
from calendar c
join {{ ref('stg_coverage') }} cov on cov.day = c.day
left join agg a on a.chokepoint_id = c.chokepoint_id and a.day = c.day
