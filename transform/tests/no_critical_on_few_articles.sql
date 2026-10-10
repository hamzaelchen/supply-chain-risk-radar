-- Jamais de niveau critique appuyé sur moins de `min_articles_for_critical` articles distincts.
select d.supplier_id, d.day, d.risk_score, d.n_articles
from {{ ref('supplier_scores_daily') }} d
where d.risk_score > {{ var('threshold_critical') }}
  and d.n_articles < {{ var('min_articles_for_critical') }}
