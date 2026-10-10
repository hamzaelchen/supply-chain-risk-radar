{{ config(materialized='table') }}
-- Score v1 par fournisseur et par jour couvert, « à la date du lot ». Historique complet : sert
-- aux courbes et à valider que le score ne s'allume pas toute l'année.
with e as ({{ anomaly_series(ref('int_supplier_day'), 'supplier_id', 'i_local_norm') }}),
r as ({{ anomaly_series(ref('int_supplier_day'), 'supplier_id', 'i_route_norm') }}),
-- Ton : numérateur et dénominateur décroissants, normale = ton moyen journalier passé.
tone_series as (
    select
        a.supplier_id as entity_id,
        a.day,
        a.tone_day,
        (a.tone_x_mentions
            + {{ var('decay') }} * coalesce(b.tone_x_mentions, 0)
            + {{ var('decay') }} * {{ var('decay') }} * coalesce(c.tone_x_mentions, 0)) as num,
        (a.mentions
            + {{ var('decay') }} * coalesce(b.mentions, 0)
            + {{ var('decay') }} * {{ var('decay') }} * coalesce(c.mentions, 0)) as den
    from {{ ref('int_supplier_day') }} a
    left join {{ ref('int_supplier_day') }} b on b.supplier_id = a.supplier_id and b.day = a.day - 1
    left join {{ ref('int_supplier_day') }} c on c.supplier_id = a.supplier_id and c.day = a.day - 2
),
tone as (
    select
        entity_id, day, num / nullif(den, 0) as tone_window, den,
        avg(tone_day) over w as tone_mu,
        stddev_samp(tone_day) over w as tone_sd,
        count(tone_day) over w as n_base
    from tone_series
    window w as (
        partition by entity_id order by day
        range between interval '{{ var('baseline_days') }} days' preceding and interval '1 day' preceding
    )
),
parts as (
    select
        e.entity_id as supplier_id,
        e.day,
        e.x as x_local,
        e.mu as mu_local,
        e.n_base as baseline_days_local,
        r.x as x_route,
        {{ component_from_z('e.x', 'e.mu', 'e.sd', 'e.n_base') }} as component_e,
        {{ component_from_z('r.x', 'r.mu', 'r.sd', 'r.n_base') }} as component_r,
        case
            when t.n_base < {{ var('min_baseline_days') }} or t.den < {{ var('min_tone_mentions') }} then 0.0
            else 100.0 * least(1.0, greatest(0.0,
                (t.tone_mu - t.tone_window) / greatest(coalesce(t.tone_sd, 0.0), {{ var('tone_sigma_floor') }})
            ) / {{ var('z_saturation') }})
        end as component_t,
        e.n_base >= {{ var('min_baseline_days') }} as baseline_ok
    from e
    join r using (entity_id, day)
    join tone t using (entity_id, day)
),
combined as (
    select
        p.*,
        s.criticality,
        {{ var('weight_e') }} * p.component_e
            + {{ var('weight_t') }} * p.component_t
            + {{ var('weight_r') }} * p.component_r as hazard
    from parts p
    join {{ ref('stg_suppliers') }} s using (supplier_id)
)
select
    c.supplier_id,
    c.day,
    c.component_e,
    c.component_t,
    c.component_r,
    c.criticality,
    c.hazard,
    c.baseline_ok,
    coalesce(a.n_articles, 0) as n_articles,
    -- La criticité module l'aléa (de 60 % à 100 %) ; sans au moins N articles distincts,
    -- jamais de niveau critique (plafond au seuil).
    case
        when coalesce(a.n_articles, 0) < {{ var('min_articles_for_critical') }}
            then least(c.hazard * ({{ var('criticality_floor') }} + (1 - {{ var('criticality_floor') }}) * c.criticality / 100.0),
                       {{ var('threshold_critical') }})
        else c.hazard * ({{ var('criticality_floor') }} + (1 - {{ var('criticality_floor') }}) * c.criticality / 100.0)
    end as risk_score
from combined c
left join {{ ref('int_supplier_articles') }} a using (supplier_id, day)
