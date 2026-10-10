select chokepoint_id, name_fr, name_en, kind, lat, lon, radius_km
from {{ source('lh_refs', 'chokepoints') }}
