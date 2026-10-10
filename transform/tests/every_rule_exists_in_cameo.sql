-- Chaque préfixe de règle correspond à au moins un code de la table officielle CAMEO (la table
-- est ref/sources/cameo/CAMEO.eventcodes.txt, rapprochée par tests/test_cameo_rules.py).
select code_prefix from {{ ref('cameo_rules') }} where length(code_prefix) not between 2 and 4
