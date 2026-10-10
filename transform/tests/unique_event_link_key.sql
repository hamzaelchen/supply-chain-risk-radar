-- Clé unique du contrat : (supplier_id, global_event_id, link_type, chokepoint_id).
select supplier_id, global_event_id, link_type, coalesce(chokepoint_id, '') as chokepoint_id, count(*) as n
from {{ ref('supplier_events') }}
group by 1, 2, 3, 4
having count(*) > 1
