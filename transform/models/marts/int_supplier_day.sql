{{ config(materialized='table') }}
-- Une ligne par fournisseur et par jour couvert : intensité locale (E), de route (R) et ton (T).
with calendar as (
    select s.supplier_id, c.day
    from {{ ref('stg_suppliers') }} s
    cross join (select day from {{ ref('stg_coverage') }} where covered) c
),
intensity as (
    select
        supplier_id, day,
        sum(event_weight) filter (where link_type = 'city') as i_local,
        count(*) filter (where link_type = 'city') as n_local,
        sum(event_weight) filter (where link_type = 'chokepoint') as i_route,
        count(*) filter (where link_type = 'chokepoint') as n_route
    from {{ ref('int_supplier_event_links') }}
    where is_relevant
    group by 1, 2
),
tone as (
    -- Ton de tous les événements précis récents près de la ville (pas seulement les négatifs).
    select
        sp.supplier_id,
        e.batch_day as day,
        sum(e.avg_tone * e.num_mentions) as tone_x_mentions,
        sum(e.num_mentions) as mentions
    from {{ source('lh_pairs', 'supplier_pairs') }} sp
    join {{ ref('stg_events') }} e using (global_event_id)
    where e.is_recent
    group by 1, 2
)
select
    c.supplier_id,
    c.day,
    coalesce(i.i_local, 0.0) as i_local,
    coalesce(i.n_local, 0) as n_local,
    coalesce(i.i_route, 0.0) as i_route,
    coalesce(i.n_route, 0) as n_route,
    -- Intensités ramenées à un volume GDELT de référence : le nombre d'événements du monde varie
    -- de 65 000 (dimanche) à 130 000 (jeudi) par jour ; sans cela, chaque lundi serait une anomalie.
    coalesce(i.i_local, 0.0) * {{ var('volume_reference') }} / cov.events as i_local_norm,
    coalesce(i.i_route, 0.0) * {{ var('volume_reference') }} / cov.events as i_route_norm,
    coalesce(t.tone_x_mentions, 0.0) as tone_x_mentions,
    coalesce(t.mentions, 0) as mentions,
    case when coalesce(t.mentions, 0) > 0 then t.tone_x_mentions / t.mentions end as tone_day
from calendar c
join {{ ref('stg_coverage') }} cov on cov.day = c.day
left join intensity i on i.supplier_id = c.supplier_id and i.day = c.day
left join tone t on t.supplier_id = c.supplier_id and t.day = c.day
