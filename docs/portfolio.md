# Portefeuille démo (FICTIF)

Les 100 fournisseurs de l'« Entreprise démo » (basée à Casablanca) sont **inventés**. Seules les villes et les détroits sont réels. Chaque ligne porte l'étiquette `FICTIF - Entreprise démo`.

| Élément | Origine |
|---|---|
| Nom du fournisseur, catégorie, criticité (fournisseur unique, part des achats) | Fictif, généré avec la graine `20261009` (`portfolio/generate.py`) |
| Ville et coordonnées | Réel : Wikidata, CC0, révision citée (`ref/cities.csv`) |
| Détroits de la route | Règle de routage ci-dessous (`portfolio/routing.py`), sur le référentiel réel `ref/chokepoints.csv` |

Fichiers : `portfolio/suppliers.csv` (100 lignes), `portfolio/supplier_routes.csv` (une ligne par détroit traversé). Tables : `silver.ref_suppliers`, `silver.ref_supplier_routes` (`make portfolio`). Régénération : `python -m portfolio.generate generate` ; un test vérifie que les CSV versionnés sont exactement ce que produit le générateur.

## Règle de routage

Région d'origine de la ville -> détroits traversés, de l'origine vers Casablanca. Chemin maritime habituel le plus court en nombre de détroits ; Suez plutôt que le cap de Bonne-Espérance ; Casablanca étant sur l'Atlantique, tout ce qui vient de Méditerranée traverse Gibraltar.

| Région | Route |
|---|---|
| `MA` | (aucun détroit) |
| `EU_ATLANTIC` | (aucun détroit) |
| `EU_NORTH_SEA` | dover |
| `EU_BALTIC` | oresund > dover |
| `EU_MEDITERRANEAN` | gibraltar |
| `TR_MARMARA` | dardanelles > gibraltar |
| `TR_AEGEAN` | gibraltar |
| `BLACK_SEA` | bosphorus > dardanelles > gibraltar |
| `EGYPT_MEDITERRANEAN` | gibraltar |
| `RED_SEA` | suez > gibraltar |
| `PERSIAN_GULF` | hormuz > bab_el_mandeb > suez > gibraltar |
| `SOUTH_ASIA` | bab_el_mandeb > suez > gibraltar |
| `SOUTHEAST_ASIA` | malacca > bab_el_mandeb > suez > gibraltar |
| `CHINA_NORTH` | taiwan > malacca > bab_el_mandeb > suez > gibraltar |
| `CHINA_SOUTH` | malacca > bab_el_mandeb > suez > gibraltar |
| `KOREA` | korea > taiwan > malacca > bab_el_mandeb > suez > gibraltar |
| `SOUTHERN_AFRICA` | cape_of_good_hope |
| `AMERICAS_ATLANTIC` | (aucun détroit) |
| `AMERICAS_PACIFIC` | panama |

Limites : route fictive et simplifiée (pas de trajet réel) ; villes = centres-villes, pas les ports ; la marchandise peut arriver par Tanger Med, dans la zone du détroit de Gibraltar, ce qui revient au même pour la surveillance ; le détroit de Taïwan est compté pour tout le nord de la Chine et la Corée ; les déroutages (cap de Bonne-Espérance en cas de crise en mer Rouge) ne sont pas modélisés.

## Répartition (graine 20261009)

```
100 fournisseurs (FICTIF - Entreprise démo), acheteur : Entreprise démo, Casablanca

-- par pays
   14    9.73 % des achats  China (CN)
   12    9.72 % des achats  Morocco (MA)
   11   10.96 % des achats  Spain (ES)
    8   13.82 % des achats  India (IN)
    7    7.01 % des achats  France (FR)
    7    7.21 % des achats  Turkey (TR)
    6    7.58 % des achats  Germany (DE)
    4    2.17 % des achats  Italy (IT)
    4    4.70 % des achats  United Arab Emirates (AE)
    3    1.71 % des achats  Saudi Arabia (SA)
    2    2.51 % des achats  Belgium (BE)
    2    4.33 % des achats  Brazil (BR)
    2    1.37 % des achats  Egypt (EG)
    2    3.25 % des achats  Netherlands (NL)
    2    0.91 % des achats  Poland (PL)
    2    1.09 % des achats  Romania (RO)
    2    0.82 % des achats  South Korea (KR)
    2    2.33 % des achats  Thailand (TH)
    2    1.09 % des achats  United States (US)
    2    5.61 % des achats  Vietnam (VN)
    1    0.43 % des achats  Chile (CL)
    1    1.10 % des achats  Pakistan (PK)
    1    0.26 % des achats  Singapore (SG)
    1    0.29 % des achats  South Africa (ZA)

-- par catégorie
   18   21.82 % des achats  Pièces automobiles
   15   11.38 % des achats  Acier et métaux
   14   14.02 % des achats  Emballage
   13   14.57 % des achats  Textile et matières premières
   11   10.89 % des achats  Produits chimiques
   10   10.56 % des achats  Agroalimentaire (intrants)
   10    7.77 % des achats  Machines et équipements
    9    8.99 % des achats  Composants électroniques

-- par région d'origine
   15   12.76 % des achats  EU_MEDITERRANEAN
   12    9.72 % des achats  MA
   10   13.34 % des achats  EU_NORTH_SEA
    9   14.92 % des achats  SOUTH_ASIA
    7    3.73 % des achats  CHINA_NORTH
    7    6.00 % des achats  CHINA_SOUTH
    7    7.38 % des achats  EU_ATLANTIC
    6    6.25 % des achats  PERSIAN_GULF
    5    8.20 % des achats  SOUTHEAST_ASIA
    5    4.75 % des achats  TR_MARMARA
    3    4.83 % des achats  AMERICAS_ATLANTIC
    2    1.02 % des achats  AMERICAS_PACIFIC
    2    1.09 % des achats  BLACK_SEA
    2    1.37 % des achats  EGYPT_MEDITERRANEAN
    2    0.91 % des achats  EU_BALTIC
    2    0.82 % des achats  KOREA
    2    2.46 % des achats  TR_AEGEAN
    1    0.16 % des achats  RED_SEA
    1    0.29 % des achats  SOUTHERN_AFRICA

-- par route (détroits, de l'origine vers Casablanca)
   22   21.93 % des achats  (aucun détroit)
   19   16.59 % des achats  gibraltar
   12   14.20 % des achats  malacca > bab_el_mandeb > suez > gibraltar
   10   13.34 % des achats  dover
    9   14.92 % des achats  bab_el_mandeb > suez > gibraltar
    7    3.73 % des achats  taiwan > malacca > bab_el_mandeb > suez > gibraltar
    6    6.25 % des achats  hormuz > bab_el_mandeb > suez > gibraltar
    5    4.75 % des achats  dardanelles > gibraltar
    2    1.09 % des achats  bosphorus > dardanelles > gibraltar
    2    0.82 % des achats  korea > taiwan > malacca > bab_el_mandeb > suez > gibraltar
    2    0.91 % des achats  oresund > dover
    2    1.02 % des achats  panama
    1    0.29 % des achats  cape_of_good_hope
    1    0.16 % des achats  suez > gibraltar

-- exposition par détroit (fournisseurs dont la route le traverse)
   63   62.51 % des achats  dont 7 fournisseur(s) unique(s)  gibraltar
   37   40.08 % des achats  dont 6 fournisseur(s) unique(s)  suez
   36   39.92 % des achats  dont 6 fournisseur(s) unique(s)  bab_el_mandeb
   21   18.75 % des achats  dont 4 fournisseur(s) unique(s)  malacca
   12   14.25 % des achats  dont 0 fournisseur(s) unique(s)  dover
    9    4.55 % des achats  dont 2 fournisseur(s) unique(s)  taiwan
    7    5.84 % des achats  dont 0 fournisseur(s) unique(s)  dardanelles
    6    6.25 % des achats  dont 2 fournisseur(s) unique(s)  hormuz
    2    0.91 % des achats  dont 0 fournisseur(s) unique(s)  oresund
    2    0.82 % des achats  dont 0 fournisseur(s) unique(s)  korea
    2    1.02 % des achats  dont 0 fournisseur(s) unique(s)  panama
    2    1.09 % des achats  dont 0 fournisseur(s) unique(s)  bosphorus
    1    0.29 % des achats  dont 0 fournisseur(s) unique(s)  cape_of_good_hope

fournisseurs uniques : 10 (10.33 % des achats)
plus gros : S084 6.71 %, S020 4.42 %, S049 3.73 %, S081 3.16 %, S094 3.05 %

```
