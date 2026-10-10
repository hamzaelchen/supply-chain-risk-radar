{{ config(materialized='table') }}
-- Articles distincts (SOURCEURL) qui soutiennent un détroit sur la fenêtre de 3 jours se terminant
-- à `day` : même garde-fou « pas de critique sur un seul article » que pour les fournisseurs.
select
    cp.chokepoint_id,
    e.batch_day + k.k as day,
    count(distinct e.source_url) as n_articles
from {{ source('lh_pairs', 'chokepoint_pairs') }} cp
join {{ ref('stg_events') }} e using (global_event_id)
cross join (select unnest([0, 1, 2]) as k) k
where e.is_recent and e.is_relevant
group by 1, 2
