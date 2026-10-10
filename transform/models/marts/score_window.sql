{{ config(materialized='table') }}
-- Fenêtre courante : le dernier jour couvert (au moins `min_batches_per_day` lots) et les 3 jours
-- qui se terminent à lui. Un jour partiel (aujourd'hui) n'est pas retenu : il fausserait la
-- comparaison à une normale faite de jours complets.
-- `as_of_day` (variable dbt) produit un instantané « tel qu'il aurait été ce jour-là », par ex.
-- pour le cas de démo Ormuz : dbt build --vars '{as_of_day: 2026-03-02}'.
with last_day as (
    select max(day) as day_end
    from {{ ref('stg_coverage') }}
    where covered
    {% if var('as_of_day', none) %}and day = date '{{ var("as_of_day") }}'{% endif %}
)
select
    day_end,
    day_end - 2 as day_start,
    cast(day_end - 2 as timestamp) as window_start,
    cast(day_end + 1 as timestamp) as window_end
from last_day
where day_end is not null
