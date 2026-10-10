{{ config(materialized='table') }}
select
    entity_id as chokepoint_id,
    day,
    {{ component_from_z('x', 'mu', 'sd', 'n_base') }} as risk_score,
    n_base >= {{ var('min_baseline_days') }} as baseline_ok
from ({{ anomaly_series(ref('int_chokepoint_day'), 'chokepoint_id', 'i_zone_norm') }})
