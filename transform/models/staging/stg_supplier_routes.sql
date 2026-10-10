select supplier_id, position, chokepoint_id
from {{ source('lh_refs', 'supplier_routes') }}
