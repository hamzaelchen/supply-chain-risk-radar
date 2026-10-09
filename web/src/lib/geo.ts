import { geoCircle } from "d3-geo";
import type { ChokepointScore, SupplierScore } from "../data/types";

export const EARTH_RADIUS_KM = 6371.0088;

/** Cercle géodésique de rayon `km` autour d'un point (GeoJSON), pour dessiner le rayon de surveillance. */
export function radiusCircle(lon: number, lat: number, km: number) {
  return geoCircle().center([lon, lat]).radius((km / EARTH_RADIUS_KM) * (180 / Math.PI)).precision(2)();
}

export interface CityGroup {
  key: string;
  city: string;
  country: string;
  lat: number;
  lon: number;
  suppliers: SupplierScore[];
  worst: SupplierScore;
}

/** Une pastille par ville (plusieurs fournisseurs partagent la même ville), colorée par le pire score. */
export function groupByCity(suppliers: SupplierScore[]): CityGroup[] {
  const groups = new Map<string, CityGroup>();
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

export function levelOf(score: number | null, levels: { id: string; max: number }[]): string | null {
  if (score === null) return null;
  return (levels.find((l) => score < l.max) ?? levels[levels.length - 1]).id;
}

export type { ChokepointScore };
