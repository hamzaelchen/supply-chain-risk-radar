{{ config(materialized='table') }}
-- Un événement pertinent compte pour un fournisseur par UN lien et un seul :
--  * la ville (à moins de 150 km) : composante E ;
--  * sinon le détroit de sa route le plus proche dont le rayon le contient : composante R.
-- Anti double comptage (décision du 2026-10-09) : un événement déjà compté près de la ville
-- n'est jamais recompté dans R pour ce fournisseur, et un événement dans le rayon de plusieurs
-- détroits de la route n'est compté qu'une fois.
with city as (
    select supplier_id, global_event_id, distance_km
    from {{ source('lh_pairs', 'supplier_pairs') }}
),
route as (
    select
        r.supplier_id,
        cp.global_event_id,
        cp.chokepoint_id,
        cp.distance_km,
        row_number() over (
            partition by r.supplier_id, cp.global_event_id order by cp.distance_km, r.position
        ) as rn
    from {{ ref('stg_supplier_routes') }} r
    join {{ source('lh_pairs', 'chokepoint_pairs') }} cp using (chokepoint_id)
    anti join city c on c.supplier_id = r.supplier_id and c.global_event_id = cp.global_event_id
),
links as (
    select supplier_id, global_event_id, 'city' as link_type, cast(null as varchar) as chokepoint_id,
           distance_km
    from city
    union all
    select supplier_id, global_event_id, 'chokepoint', chokepoint_id, distance_km
    from route where rn = 1
)
select
    l.supplier_id,
    l.global_event_id,
    l.link_type,
    l.chokepoint_id,
    l.distance_km,
    e.batch_day as day,
    e.event_weight,
    e.is_relevant,
    e.source_url
from links l
join {{ ref('stg_events') }} e using (global_event_id)
where e.is_recent
