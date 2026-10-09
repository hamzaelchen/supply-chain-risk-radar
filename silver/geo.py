"""Briques géographiques : distance haversine et deux jointures « à moins de X km ».

- `within_radius_cross` : produit croisé avec les points en diffusion (broadcast), préfiltre sur la
  latitude, distance haversine exacte. Simple et sans perte : c'est la méthode retenue (mesures
  dans docs/geo-join.md) et la référence de correction des autres.
- `within_radius_grid` : index en grille (les points s'étalent sur les cellules qui couvrent leur
  cercle, les événements tombent dans une cellule, jointure d'égalité sur la cellule, puis
  distance exacte). Même principe qu'un index H3, sans bibliothèque ni UDF Python.

Les deux rendent exactement les mêmes paires (tests et mesures le vérifient). Les points sont peu
nombreux (villes des fournisseurs + détroits), les événements beaucoup : seuls les événements
sont distribués.

Schéma attendu : `events` porte `event_lat`, `event_lon` (et ce qu'on veut en plus) ; `points`
porte `point_id`, `point_lat`, `point_lon`, `radius_km`. La sortie ajoute `distance_km`.
"""

import math

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

EARTH_RADIUS_KM = 6371.0088  # rayon moyen (IUGG)
KM_PER_DEGREE = math.pi * EARTH_RADIUS_KM / 180  # ~ 111,195 km
DEFAULT_CELL_DEG = 1.0
_MARGIN_DEG = 1e-6


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Distance orthodromique en km sur une sphère de rayon moyen terrestre."""
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(d_lat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(
        F.sin(d_lon / 2), 2
    )
    # least(1, ...) : l'arrondi peut donner 1,0000000000000002 pour deux points antipodaux.
    return 2 * EARTH_RADIUS_KM * F.asin(F.least(F.lit(1.0), F.sqrt(a)))


def grid_shape(cell_deg: float) -> tuple[int, int]:
    """(nombre de cellules en latitude, en longitude)."""
    return math.ceil(180 / cell_deg), math.ceil(360 / cell_deg)


def covering_cells(
    lat: float, lon: float, radius_km: float, cell_deg: float = DEFAULT_CELL_DEG
) -> list[tuple[int, int]]:
    """Cellules (ligne de latitude, colonne de longitude) qui couvrent le cercle de ce rayon.

    Le cercle est contenu dans la boîte [lat - d, lat + d] x [lon - dl, lon + dl], avec
    d = rayon / rayon terrestre et dl = asin(sin d / cos lat), l'étendue exacte en longitude d'un
    cercle sur la sphère. Si le cercle contient un pôle, toutes les longitudes sont prises. La
    longitude est circulaire : un cercle qui franchit l'antiméridien (180°) reprend les cellules
    de l'autre côté.
    """
    n_lat, n_lon = grid_shape(cell_deg)
    angular = math.degrees(radius_km / EARTH_RADIUS_KM)
    lat_lo, lat_hi = lat - angular - _MARGIN_DEG, lat + angular + _MARGIN_DEG
    rows = range(
        max(0, math.floor((lat_lo + 90) / cell_deg)),
        min(n_lat - 1, math.floor((lat_hi + 90) / cell_deg)) + 1,
    )
    if lat_hi >= 90 or lat_lo <= -90:
        cols: list[int] = list(range(n_lon))  # le cercle englobe un pôle
    else:
        half = math.degrees(
            math.asin(math.sin(math.radians(angular)) / math.cos(math.radians(lat)))
        )
        half += _MARGIN_DEG
        if half >= 180:
            cols = list(range(n_lon))
        else:
            first = math.floor((lon - half + 180) / cell_deg)
            last = math.floor((lon + half + 180) / cell_deg)
            cols = sorted({i % n_lon for i in range(first, last + 1)})
    return [(r, c) for r in rows for c in cols]


def cell_columns(lat: Column, lon: Column, cell_deg: float) -> tuple[Column, Column]:
    """Cellule d'un événement : mêmes conventions que `covering_cells`."""
    n_lat, n_lon = grid_shape(cell_deg)
    row = F.least(F.floor((lat + 90) / cell_deg), F.lit(n_lat - 1))
    col = F.pmod(F.floor((lon + 180) / cell_deg), F.lit(n_lon))
    return row.cast("int"), col.cast("int")


def within_radius_cross(events: DataFrame, points: DataFrame, prefilter: bool = True) -> DataFrame:
    """Produit croisé (points diffusés) puis distance exacte.

    `prefilter` : écarte d'abord, sans trigonométrie, les paires dont l'écart de latitude dépasse
    déjà le rayon. Sans perte : l'écart de latitude, en degrés, ne peut pas dépasser la distance
    angulaire entre deux points, donc aucune paire à portée n'est écartée. Ne dépend pas de la
    longitude, donc aucun cas particulier à l'antiméridien ni aux pôles.
    """
    joined = events.crossJoin(F.broadcast(points))
    if prefilter:
        lat_gap = F.abs(F.col("event_lat") - F.col("point_lat"))
        joined = joined.filter(lat_gap <= F.col("radius_km") / KM_PER_DEGREE + _MARGIN_DEG)
    distance = haversine_km(
        F.col("event_lat"), F.col("event_lon"), F.col("point_lat"), F.col("point_lon")
    )
    return joined.withColumn("distance_km", distance).filter(
        F.col("distance_km") <= F.col("radius_km")
    )


def within_radius_grid(
    events: DataFrame, points: DataFrame, cell_deg: float = DEFAULT_CELL_DEG
) -> DataFrame:
    spark = events.sparkSession
    cells = [
        (p.point_id, row, col)
        for p in points.select("point_id", "point_lat", "point_lon", "radius_km").collect()
        for row, col in covering_cells(p.point_lat, p.point_lon, p.radius_km, cell_deg)
    ]
    cell_df = spark.createDataFrame(cells, "point_id STRING, cell_row INT, cell_col INT")
    row, col = cell_columns(F.col("event_lat"), F.col("event_lon"), cell_deg)
    keyed = events.withColumn("cell_row", row).withColumn("cell_col", col)
    candidates = keyed.join(F.broadcast(cell_df), ["cell_row", "cell_col"]).join(
        F.broadcast(points), "point_id"
    )
    distance = haversine_km(
        F.col("event_lat"), F.col("event_lon"), F.col("point_lat"), F.col("point_lon")
    )
    return (
        candidates.withColumn("distance_km", distance)
        .filter(F.col("distance_km") <= F.col("radius_km"))
        .drop("cell_row", "cell_col")
    )
