{{ config(materialized='table') }}
-- Export « chokepoint_scores » du contrat : un détroit, une ligne, au dernier jour couvert.
with w as (select * from {{ ref('score_window') }}),
cur as (
    select s.* from {{ ref('chokepoint_scores_daily') }} s join w on s.day = w.day_end
),
zone as (
    -- Événements précis récents dans le rayon, sur la fenêtre courante (tous, pas seulement les
    -- négatifs : c'est le chiffre de docs/geo-join.md).
    select d.chokepoint_id, sum(d.n_all) as events_recent
    from {{ ref('int_chokepoint_day') }} d cross join w
    where d.day between w.day_start and w.day_end
    group by 1
),
exposed as (
    select
        r.chokepoint_id,
        count(distinct s.supplier_id) as suppliers_exposed,
        sum(s.purchase_share_pct) as purchase_share_exposed_pct,
        count(distinct s.supplier_id) filter (where s.sole_source) as sole_source_exposed
    from {{ ref('stg_supplier_routes') }} r
    join {{ ref('stg_suppliers') }} s using (supplier_id)
    group by 1
)
select
    k.chokepoint_id,
    k.name_fr,
    k.name_en,
    k.lat,
    k.lon,
    k.radius_km,
    round(coalesce(c.risk_score, 0.0), 2) as risk_score,
    {{ risk_level('round(coalesce(c.risk_score, 0.0), 2)') }} as risk_level,
    cast(coalesce(z.events_recent, 0) as integer) as events_recent,
    cast(coalesce(e.suppliers_exposed, 0) as integer) as suppliers_exposed,
    round(coalesce(e.purchase_share_exposed_pct, 0.0), 2) as purchase_share_exposed_pct,
    cast(coalesce(e.sole_source_exposed, 0) as integer) as sole_source_exposed
from {{ ref('stg_chokepoints') }} k
left join cur c using (chokepoint_id)
left join zone z using (chokepoint_id)
left join exposed e using (chokepoint_id)
order by k.chokepoint_id
