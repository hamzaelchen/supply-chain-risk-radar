{# Série « à la normale » : valeur décroissante sur 3 jours (J, J-1, J-2 pondérés 1, d, d²), puis
   moyenne et écart-type de cette même série sur les `baseline_days` jours PRÉCÉDENTS.
   Les jours sans lots GDELT ne sont pas dans la relation (voir stg_coverage), ils ne comptent donc
   ni comme jours calmes ni dans la normale : la fenêtre se mesure en jours calendaires. #}
{% macro anomaly_series(relation, entity, value) %}
with d0 as (
    select {{ entity }} as entity_id, day, {{ value }} as v from {{ relation }}
),
x as (
    select
        a.entity_id,
        a.day,
        a.v + {{ var('decay') }} * coalesce(b.v, 0)
            + {{ var('decay') }} * {{ var('decay') }} * coalesce(c.v, 0) as x
    from d0 a
    left join d0 b on b.entity_id = a.entity_id and b.day = a.day - 1
    left join d0 c on c.entity_id = a.entity_id and c.day = a.day - 2
)
select
    entity_id,
    day,
    x,
    avg(x) over w as mu,
    stddev_samp(x) over w as sd,
    count(x) over w as n_base
from x
window w as (
    partition by entity_id order by day
    range between interval '{{ var('baseline_days') }} days' preceding and interval '1 day' preceding
)
{% endmacro %}

{# Composante 0-100 à partir de l'anomalie : z = (x - normale) / écart-type (avec plancher),
   nul quand la normale n'est pas fiable, saturé à `z_saturation`. #}
{% macro component_from_z(x, mu, sd, n_base) %}
case
    when {{ n_base }} < {{ var('min_baseline_days') }} then 0.0
    else 100.0 * least(1.0, greatest(0.0,
        ({{ x }} - {{ mu }}) / greatest(coalesce({{ sd }}, 0.0), {{ var('sigma_floor') }})
    ) / {{ var('z_saturation') }})
end
{% endmacro %}

{% macro risk_level(score) %}
case
    when {{ score }} > {{ var('threshold_critical') }} then 'critique'
    when {{ score }} >= {{ var('threshold_alert') }} then 'alerte'
    else 'surveillance'
end
{% endmacro %}
