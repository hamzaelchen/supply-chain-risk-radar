{{ config(materialized='table') }}
-- Export « supplier_scores » du contrat : un fournisseur, une ligne, au dernier jour couvert.
with w as (select * from {{ ref('score_window') }}),
cur as (
    select s.* from {{ ref('supplier_scores_daily') }} s join w on s.day = w.day_end
),
window_counts as (
    select
        supplier_id,
        count(*) filter (where link_type = 'city') as events_local,
        count(*) filter (where link_type = 'chokepoint') as events_route
    from {{ ref('supplier_events_all') }}
    group by 1
),
top_choke as (
    select supplier_id, chokepoint_id, count(*) as n,
           row_number() over (partition by supplier_id order by count(*) desc, chokepoint_id) as rk
    from {{ ref('supplier_events_all') }}
    where link_type = 'chokepoint'
    group by 1, 2
)
select
    sp.supplier_id,
    sp.name,
    sp.category,
    sp.category_label,
    sp.city,
    sp.country,
    sp.iso2,
    sp.region,
    sp.lat,
    sp.lon,
    sp.sole_source,
    sp.purchase_share_pct,
    sp.label,
    round(coalesce(c.risk_score, 0.0), 2) as risk_score,
    {{ risk_level('round(coalesce(c.risk_score, 0.0), 2)') }} as risk_level,
    round(coalesce(c.component_e, 0.0), 2) as component_e,
    round(coalesce(c.component_t, 0.0), 2) as component_t,
    round(coalesce(c.component_r, 0.0), 2) as component_r,
    round(sp.criticality, 2) as criticality,
    -- Lecture complémentaire : l'aléa d'origine « ville » et d'origine « route », sur 0-100.
    round(coalesce(c.component_e, 0.0), 2) as score_local,
    round(coalesce(c.component_r, 0.0), 2) as score_route,
    cast(coalesce(wc.events_local, 0) as integer) as events_local,
    cast(coalesce(wc.events_route, 0) as integer) as events_route,
    case
        when coalesce(wc.events_local, 0) + coalesce(wc.events_route, 0) = 0 then null
        when {{ var('weight_r') }} * coalesce(c.component_r, 0) > {{ var('weight_e') }} * coalesce(c.component_e, 0)
             and {{ var('weight_r') }} * coalesce(c.component_r, 0) >= {{ var('weight_t') }} * coalesce(c.component_t, 0)
            then format('{} {} près du détroit {} sur les 3 derniers jours',
                        wc.events_route,
                        case when wc.events_route = 1 then 'événement négatif' else 'événements négatifs' end,
                        k.name_fr)
        when {{ var('weight_t') }} * coalesce(c.component_t, 0) > {{ var('weight_e') }} * coalesce(c.component_e, 0)
            then format('Ton des médias plus négatif que d''habitude près de {}', sp.city)
        else format('{} {} près de {} sur les 3 derniers jours',
                    wc.events_local,
                    case when wc.events_local = 1 then 'événement négatif' else 'événements négatifs' end,
                    sp.city)
    end as top_driver
from {{ ref('stg_suppliers') }} sp
left join cur c using (supplier_id)
left join window_counts wc using (supplier_id)
left join top_choke tc on tc.supplier_id = sp.supplier_id and tc.rk = 1
left join {{ ref('stg_chokepoints') }} k on k.chokepoint_id = tc.chokepoint_id
order by sp.supplier_id
