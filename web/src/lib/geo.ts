import { geoCircle } from "d3-geo";
import type { ChokepointScore, MapSupplier, SupplierRoute } from "../data/types";

export const EARTH_RADIUS_KM = 6371.0088;

/** Cercle géodésique de rayon `km` autour d'un point (GeoJSON), pour dessiner le rayon de surveillance. */
export function radiusCircle(lon: number, lat: number, km: number) {
  return geoCircle().center([lon, lat]).radius((km / EARTH_RADIUS_KM) * (180 / Math.PI)).precision(2)();
}

export interface CityGroup<T extends MapSupplier = MapSupplier> {
  key: string;
  city: string;
  country: string;
  lat: number;
  lon: number;
  suppliers: T[];
  worst: T;
}

/** Une pastille par ville (plusieurs fournisseurs partagent la même ville), colorée par le pire score. */
export function groupByCity<T extends MapSupplier>(suppliers: T[]): CityGroup<T>[] {
  const groups = new Map<string, CityGroup<T>>();
  for (const s of suppliers) {
    const key = `${s.lat.toFixed(3)},${s.lon.toFixed(3)}`;
    const g = groups.get(key);
    if (g) {
      g.suppliers.push(s);
      if (s.risk_score > g.worst.risk_score) g.worst = s;
    } else {
      groups.set(key, { key, city: s.city, country: s.country, lat: s.lat, lon: s.lon, suppliers: [s], worst: s });
    }
  }
  return [...groups.values()].sort((a, b) => a.worst.risk_score - b.worst.risk_score);
}

export type { ChokepointScore };

export interface MapRoute {
  /** Ville d'origine (clé de groupByCity) et détroits traversés, dans l'ordre. */
  cityKey: string;
  chokepointIds: string[];
  /** Points [lon, lat] : ville, détroits, port d'arrivée. */
  coords: [number, number][];
  /** Niveau du pire fournisseur de la ville (même règle que la pastille). */
  level: string;
  alert: boolean;
  suppliers: number;
}

/**
 * Une route par (ville, suite de détroits) : tous les fournisseurs d'une ville partagent la même route
 * (règle de routage par région d'origine). Tracé schématique : des arcs entre points, pas un itinéraire réel.
 * Les routes en alerte sont rendues en dernier pour passer au-dessus des autres.
 */
export function buildRoutes(
  suppliers: MapSupplier[],
  routes: SupplierRoute[],
  chokepoints: ChokepointScore[],
  hub: { lon: number; lat: number },
  alertLevels: ReadonlySet<string>,
): MapRoute[] {
  const cp = new Map(chokepoints.map((c) => [c.chokepoint_id, c]));
  const stepsBySupplier = new Map<string, SupplierRoute[]>();
  for (const r of routes) {
    const list = stepsBySupplier.get(r.supplier_id) ?? [];
    list.push(r);
    stepsBySupplier.set(r.supplier_id, list);
  }
  const out = new Map<string, MapRoute>();
  for (const g of groupByCity(suppliers)) {
    for (const s of g.suppliers) {
      const ids = (stepsBySupplier.get(s.supplier_id) ?? [])
        .sort((a, b) => a.position - b.position)
        .map((r) => r.chokepoint_id)
        .filter((id) => cp.has(id));
      const key = `${g.key}|${ids.join(",")}`;
      const have = out.get(key);
      if (have) {
        have.suppliers += 1;
        continue;
      }
      const coords: [number, number][] = [
        [g.lon, g.lat],
        ...ids.map((id) => [cp.get(id)!.lon, cp.get(id)!.lat] as [number, number]),
        [hub.lon, hub.lat],
      ];
      out.set(key, {
        cityKey: g.key, chokepointIds: ids, coords, level: g.worst.risk_level,
        alert: alertLevels.has(g.worst.risk_level), suppliers: 1,
      }); // fmt: skip
    }
  }
  // Un fournisseur dont la ville est le port d'arrivée n'a pas de route à tracer.
  return [...out.values()]
    .filter((r) => r.coords.length > 2 || Math.abs(r.coords[0][0] - hub.lon) > 0.01 || Math.abs(r.coords[0][1] - hub.lat) > 0.01)
    .sort((a, b) => Number(a.alert) - Number(b.alert));
}
