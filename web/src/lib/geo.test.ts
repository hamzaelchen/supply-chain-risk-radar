import { describe, expect, it } from "vitest";
import type { SupplierScore } from "../data/types";
import { groupByCity, radiusCircle } from "./geo";

const mk = (id: string, lat: number, lon: number, score: number) =>
  ({ supplier_id: id, city: "X", country: "Y", lat, lon, risk_score: score }) as SupplierScore;

describe("groupByCity", () => {
  it("regroupe les fournisseurs d'une même ville et garde le pire score", () => {
    const g = groupByCity([mk("S1", 1, 2, 10), mk("S2", 1, 2, 80), mk("S3", 5, 6, 30)]);
    expect(g).toHaveLength(2);
    const city = g.find((x) => x.suppliers.length === 2)!;
    expect(city.worst.supplier_id).toBe("S2");
  });
});

describe("radiusCircle", () => {
  const hav = (lat1: number, lon1: number, lat2: number, lon2: number) => {
    const r = (d: number) => (d * Math.PI) / 180;
    const a = Math.sin(r(lat2 - lat1) / 2) ** 2 + Math.cos(r(lat1)) * Math.cos(r(lat2)) * Math.sin(r(lon2 - lon1) / 2) ** 2;
    return 2 * 6371.0088 * Math.asin(Math.sqrt(a));
  };
  it("a des sommets à la distance demandée du centre", () => {
    const c = radiusCircle(56.5, 26.6, 200);
    expect(c.type).toBe("Polygon");
    for (const [lon, lat] of c.coordinates[0]) {
      expect(hav(26.6, 56.5, lat, lon)).toBeCloseTo(200, 0);
    }
  });
});
