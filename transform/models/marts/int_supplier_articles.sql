{{ config(materialized='table') }}
-- Articles distincts (SOURCEURL) qui soutiennent un fournisseur sur la fenêtre de 3 jours se
-- terminant à `day`. Sert à interdire une alerte critique appuyée sur un seul article.
select
    l.supplier_id,
    l.day + k.k as day,
    count(distinct l.source_url) as n_articles
from {{ ref('int_supplier_event_links') }} l
cross join (select unnest([0, 1, 2]) as k) k
where l.is_relevant
group by 1, 2
