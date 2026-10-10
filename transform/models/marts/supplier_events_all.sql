{{ config(materialized='table') }}
-- TOUS les événements explicatifs (avant limitation par fournisseur). Événements : ceux de la fenêtre courante qui comptent dans E (lien « city ») ou R
-- (lien « chokepoint »), avec leur contribution en points de risque. Les N plus contributifs par
-- fournisseur. Le lien « ton » (T) n'est pas attribuable à des événements précis.
with w as (select * from {{ ref('score_window') }}),
scores as (
    select s.supplier_id, s.component_e, s.component_r, s.criticality, s.risk_score, s.hazard
    from {{ ref('supplier_scores_daily') }} s join w on s.day = w.day_end
),
cand as (
    select l.*, e.*  exclude (global_event_id, source_url, event_weight, is_relevant, is_recent)
    from {{ ref('int_supplier_event_links') }} l
    join {{ ref('stg_events') }} e using (global_event_id)
    cross join w
    where l.is_relevant and l.day between w.day_start and w.day_end
),
share as (
    select
        c.*,
        sc.risk_score, sc.hazard, sc.component_e, sc.component_r, sc.criticality,
        sum(c.event_weight) over (partition by c.supplier_id, c.link_type) as weight_of_link
    from cand c join scores sc using (supplier_id)
)
select
    supplier_id,
    link_type,
    chokepoint_id,
    distance_km,
    global_event_id,
    sql_date as event_date,
    src_fetched_at as fetched_at,
    action_geo_full_name as action_geo_name,
    action_geo_type,
    action_geo_lat as lat,
    action_geo_long as lon,
    event_code,
    event_root_code,
    quad_class,
    goldstein_scale,
    avg_tone,
    num_mentions,
    num_sources,
    num_articles,
    source_url,
    src_file_url,
    -- Points de risque_score : part de l'aléa du lien (E ou R, pondéré) répartie au prorata du
    -- poids de l'événement, puis ramenée à l'échelle du score final (criticité, plafond).
    event_weight / nullif(weight_of_link, 0)
        * (case link_type when 'city' then {{ var('weight_e') }} * component_e
                          else {{ var('weight_r') }} * component_r end)
        * (risk_score / nullif(hazard, 0)) as contribution,
    family as event_family,
    family_label_fr as event_family_label_fr,
    cast(null as varchar) as llm_category,
    cast(null as varchar) as llm_summary,
    event_weight
from share
