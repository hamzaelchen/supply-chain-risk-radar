{{ config(materialized='table') }}
-- Export « supplier_events » du contrat : les `explaining_events_per_supplier` événements les
-- plus contributifs de chaque fournisseur (le détail complet reste dans supplier_events_all).
select * exclude (rk)
from (
    select
        *,
        row_number() over (
            partition by supplier_id order by contribution desc nulls last, num_mentions desc,
                                              global_event_id
        ) as rk
    from {{ ref('supplier_events_all') }}
)
where rk <= {{ var('explaining_events_per_supplier') }}
