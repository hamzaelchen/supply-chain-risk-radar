-- 100 fournisseurs, parts des achats = 100,00 %, 16 détroits.
select 'fournisseurs' as controle, count(*) as valeur from {{ ref('supplier_scores') }} having count(*) != 100
union all
select 'parts des achats', round(sum(purchase_share_pct), 2) from {{ ref('supplier_scores') }} having round(sum(purchase_share_pct), 2) != 100.00
union all
select 'détroits', count(*) from {{ ref('chokepoint_scores') }} having count(*) != 16
