select supplier_id, day
from {{ ref('supplier_scores_daily') }}
where component_e not between 0 and 100 or component_t not between 0 and 100
   or component_r not between 0 and 100 or criticality not between 0 and 100
   or risk_score is null or risk_score not between 0 and 100
