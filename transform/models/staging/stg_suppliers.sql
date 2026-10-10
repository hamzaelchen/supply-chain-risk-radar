select
    s.supplier_id, s.name, s.category, s.category_label, s.city_id, s.city, s.country, s.iso2,
    s.region, s.lat, s.lon, s.sole_source, cast(s.purchase_share_pct as double) as purchase_share_pct,
    s.label,
    -- Criticité 0-100 : fournisseur unique (55) + part des achats (jusqu'à 45, plafonnée à 5 %).
    least(100.0,
          55.0 * (case when s.sole_source then 1 else 0 end)
          + 45.0 * least(1.0, cast(s.purchase_share_pct as double) / 5.0)) as criticality
from {{ source('lh_refs', 'suppliers') }} s
