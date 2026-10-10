-- Anti double comptage : pour un fournisseur, un événement lié à la ville n'a aucune ligne détroit.
select a.supplier_id, a.global_event_id
from {{ ref('supplier_events_all') }} a
join {{ ref('supplier_events_all') }} b
  on a.supplier_id = b.supplier_id and a.global_event_id = b.global_event_id
 and a.link_type = 'city' and b.link_type = 'chokepoint'
