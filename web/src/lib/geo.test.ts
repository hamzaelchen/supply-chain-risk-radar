import { describe, expect, it } from "vitest";
import type { ChokepointScore, SupplierRoute, SupplierScore } from "../data/types";
import { buildRoutes, groupByCity, radiusCircle } from "./geo";

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

describe("buildRoutes", () => {
  const sup = (id: string, lat: number, lon: number, level: string, score: number) =>
    ({ supplier_id: id, city: id, country: "Y", lat, lon, risk_level: level, risk_score: score }) as SupplierScore;
  const cps = [
    { chokepoint_id: "suez", lon: 32.5, lat: 30 },
    { chokepoint_id: "gib", lon: -5.6, lat: 36 },
  ] as ChokepointScore[];
  const step = (s: string, position: number, c: string) => ({ supplier_id: s, position, chokepoint_id: c }) as SupplierRoute;
  const hub = { lon: -7.62, lat: 33.6 };
  const alerts = new Set(["alerte", "critique"]);

  it("passe par les détroits dans l'ordre de la route, puis arrive au port", () => {
    const r = buildRoutes([sup("A", 10, 100, "surveillance", 5)], [step("A", 2, "gib"), step("A", 1, "suez")], cps, hub, alerts);
    expect(r).toHaveLength(1);
    expect(r[0].chokepointIds).toEqual(["suez", "gib"]);
    expect(r[0].coords).toEqual([[100, 10], [32.5, 30], [-5.6, 36], [-7.62, 33.6]]);
  });
  it("fusionne les fournisseurs d'une même ville et prend le pire niveau", () => {
    const r = buildRoutes(
      [sup("A", 10, 100, "surveillance", 5), sup("B", 10, 100, "critique", 80)],
      [step("A", 1, "suez"), step("B", 1, "suez")], cps, hub, alerts);
    expect(r).toHaveLength(1);
    expect(r[0]).toMatchObject({ suppliers: 2, level: "critique", alert: true });
  });
  it("met les routes en alerte en dernier, donc au-dessus", () => {
    const r = buildRoutes(
      [sup("A", 1, 1, "critique", 90), sup("B", 2, 2, "surveillance", 5)],
      [step("A", 1, "suez"), step("B", 1, "gib")], cps, hub, alerts);
    expect(r.map((x) => x.alert)).toEqual([false, true]);
  });
  it("ne trace rien pour un fournisseur situé au port d'arrivée", () => {
    expect(buildRoutes([sup("C", 33.6, -7.62, "surveillance", 1)], [], cps, hub, alerts)).toEqual([]);
  });
  it("relie directement à l'arrivée un fournisseur sans détroit", () => {
    const r = buildRoutes([sup("D", 40, -3, "surveillance", 1)], [], cps, hub, alerts);
    expect(r[0].coords).toEqual([[-3, 40], [-7.62, 33.6]]);
  });
});
