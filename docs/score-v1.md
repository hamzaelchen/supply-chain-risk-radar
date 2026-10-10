# Score de risque v1

Un score de 0 à 100 par fournisseur de l'« Entreprise démo » (fictive), calculé par dbt dans [transform/](../transform/) à partir d'événements GDELT réels. Aucun LLM : la classification est faite par règles sur les codes CAMEO.

## En une phrase

> Pour chaque fournisseur, combien d'événements négatifs et pertinents ont eu lieu près de sa ville et près des détroits de sa route **par rapport à la normale de cette ville et de ces détroits**, pondérés par la criticité du fournisseur.

## Pourquoi une anomalie et pas un compte

Le nombre d'événements GDELT mesure d'abord la couverture médiatique : Los Angeles a plus de 1 000 événements récents dans les 150 km, une petite ville industrielle en a un. Comparer des comptes bruts compare des villes, pas du risque. Chaque ville et chaque détroit est donc comparé à **sa propre normale** (les 28 jours précédents).

## Étapes

1. **Événements candidats** : QuadClass 3 ou 4 (conflit verbal ou matériel), lieu précis (ville, types 3 et 4 de GDELT, pas un centroïde de pays), récents (écart entre la date de l'événement et son observation de -1 à 2 jours).
2. **Classification par règles** ([transform/seeds/cameo_rules.csv](../transform/seeds/cameo_rules.csv)) : seuls les codes de 5 familles comptent.

   | Famille | Poids | Codes CAMEO |
   |---|---|---|
   | Violence ou attaque | 1,0 | 180, 181, 183, 190, 192 à 196, 20 |
   | Embargo, saisie ou restriction | 1,0 | 163, 171, 1723, 1724, 191 |
   | Grève ou blocage | 0,8 | 143, 144, 145 |
   | Menace d'embargo ou de blocus | 0,5 | 1312, 1381 |
   | Posture militaire | 0,5 | 152, 154 |

3. **Poids d'un événement** = poids de famille x gravité (échelle de Goldstein, de 0,5 à 1) x sources (1 + 0,5 ln n, plafonné à 2). **Un article, une voix** : un article qui produit plusieurs événements (un par lieu cité) pèse comme un seul, au poids moyen, par fournisseur et par détroit.
4. **Intensité du jour** = somme des poids, ramenée à 100 000 événements GDELT par jour (le volume du monde varie de 65 000 à 130 000 selon le jour de la semaine).
5. **Fenêtre** : les 3 derniers jours, pondérés 1, 0,6 et 0,36. **Normale** : moyenne et écart-type de cette même quantité sur les 28 jours précédents (au moins 14 jours, sinon la composante vaut 0). Les jours sans lots GDELT ne comptent ni comme jours calmes ni dans la normale.
6. **Composante** = 100 x z / 4, bornée à [0, 100], où z = (valeur - normale) / max(écart-type, 1,5).

## Les quatre composantes

| | Mesure | Poids |
|---|---|---|
| **E** | Événements pertinents à moins de 150 km de la ville | 0,55 |
| **T** | Ton des articles près de la ville plus négatif que la normale (au moins 10 mentions) | 0,20 |
| **R** | Événements pertinents dans le rayon d'un détroit de la route, **hors** ceux déjà comptés près de la ville | 0,25 |
| **C** | Criticité : 55 si fournisseur unique + jusqu'à 45 selon la part des achats (plafonnée à 5 %) | module |

`risque = (0,55 E + 0,20 T + 0,25 R) x (0,6 + 0,4 C / 100)`

La criticité amplifie le score (de 60 % à 100 % de l'aléa), elle ne le déclenche pas. R pèse moins que E parce que les événements « près d'un détroit » sont souvent ceux d'une grande ville voisine.

## Niveaux et garde-fous

- **< 40 surveillance**, **40 à 70 (inclus) alerte**, **> 70 critique**.
- **Jamais de critique sur un seul article** : sans au moins 3 articles distincts (`SOURCEURL`) dans la fenêtre, le score est plafonné à 70 (fournisseurs et détroits).
- **Pas de double comptage** : un événement compté près de la ville n'est pas recompté dans R pour ce fournisseur ; un événement dans le rayon de plusieurs détroits de la route n'est compté qu'une fois.
- **Traçabilité** : chaque fournisseur garde ses 40 événements les plus contributifs, avec l'article source, le fichier GDELT d'origine et la contribution en points.

## Limites connues

- Les règles sur les codes CAMEO ne voient pas le contexte : des **faux positifs sont possibles** (un LLM local est prévu en v2, limité aux alertes affichées).
- Les noms de lieux sont parfois abîmés par GDELT lui-même.
- Le score s'arrête au dernier jour **complet** : jusqu'à 24 h de retard.
- Portefeuille et routes sont fictifs ou simplifiés ; villes à leur centre ; rayons des détroits choisis par conception.
- Les paramètres ont été fixés avant d'observer l'historique complet : à recalibrer.
- Instantané historique : `dbt build --vars '{as_of_day: 2026-03-02}'` (base séparée via `GOLD_DB_PATH`) produit le score tel qu'il aurait été ce jour-là.

Paramètres : [transform/dbt_project.yml](../transform/dbt_project.yml). Décisions et mesures : [CLAUDE.md](../CLAUDE.md).
