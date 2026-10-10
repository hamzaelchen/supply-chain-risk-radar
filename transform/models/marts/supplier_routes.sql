{{ config(materialized='table') }}
select supplier_id, cast(position as integer) as position, chokepoint_id
from {{ ref('stg_supplier_routes') }}
order by supplier_id, position
