-- Seuils : < 40 surveillance, 40 à 70 inclus alerte, > 70 critique.
select supplier_id, risk_score, risk_level
from {{ ref('supplier_scores') }}
where risk_score < 0 or risk_score > 100
   or not (
        (risk_score > {{ var('threshold_critical') }} and risk_level = 'critique')
     or (risk_score >= {{ var('threshold_alert') }} and risk_score <= {{ var('threshold_critical') }} and risk_level = 'alerte')
     or (risk_score < {{ var('threshold_alert') }} and risk_level = 'surveillance'))
