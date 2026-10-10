-- Les événements exportés sont récents (règle -1 à 2 jours) et dans la fenêtre courante.
select e.supplier_id, e.global_event_id
from {{ ref('supplier_events') }} e
join {{ ref('stg_events') }} s using (global_event_id)
cross join {{ ref('score_window') }} w
where not s.is_recent or s.batch_day not between w.day_start and w.day_end
