-- Un lien « chokepoint » désigne un détroit de la route du fournisseur, et seulement dans ce cas.
select e.supplier_id, e.global_event_id, e.chokepoint_id
from {{ ref('supplier_events') }} e
where (e.link_type = 'chokepoint' and not exists (
          select 1 from {{ ref('supplier_routes') }} r
          where r.supplier_id = e.supplier_id and r.chokepoint_id = e.chokepoint_id))
   or (e.link_type = 'city' and e.chokepoint_id is not null)
