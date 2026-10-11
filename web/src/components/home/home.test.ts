import { describe, expect, it } from "vitest";
import { validatePipelineStats, validatePreview } from "../../data/home";
import { BRANDS, luminance, tileFor } from "./brands";
import { TALL, WIDE, pathLength } from "./architectureLayout";
import { formatMetric } from "./Pipeline";

const ratio = (a: string, b: string) => {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
};

describe("schéma d'architecture", () => {
  it("mesure un tracé en M, H, V", () => {
    expect(pathLength("M0 0H100V50H20")).toBe(230);
  });
  it.each([
    ["large", WIDE],
    ["verticale", TALL],
  ])("disposition %s : arêtes reliées à des blocs existants, blocs dans le cadre", (_n, layout) => {
    const ids = new Set(layout.nodes.map((n) => n.id));
    for (const e of layout.edges) {
      expect(ids.has(e.from), e.id).toBe(true);
      expect(ids.has(e.to), e.id).toBe(true);
    }
    for (const n of layout.nodes) {
      expect(n.x + n.w, n.id).toBeLessThanOrEqual(layout.vw);
      expect(n.y + n.h, n.id).toBeLessThanOrEqual(layout.vh);
    }
    expect(new Set(layout.nodes.map((n) => n.id)).size).toBe(layout.nodes.length);
  });
  it("les deux dispositions montrent les mêmes blocs", () => {
    expect(TALL.nodes.map((n) => n.id).sort()).toEqual(WIDE.nodes.map((n) => n.id).sort());
  });
  it("chaque bloc a une phrase de rôle", () => {
    for (const n of WIDE.nodes) expect(n.tip.length, n.id).toBeGreaterThan(20);
  });
});

describe("logos", () => {
  it("la tuile contraste avec la couleur du logo (>= 3, graphique WCAG)", () => {
    for (const [key, b] of Object.entries(BRANDS)) {
      expect(ratio(b.hex, tileFor(b.hex).slice(1)), key).toBeGreaterThanOrEqual(3);
    }
  });
});

describe("fichiers de la page d'accueil", () => {
  it("refuse un pipeline_stats.json d'une autre version", () => {
    expect(() => validatePipelineStats({ schema_version: 2, stages: [] })).toThrow(/schema_version/);
  });
  it("refuse une mesure mal formée", () => {
    expect(() =>
      validatePipelineStats({ schema_version: 1, stages: [{ id: "bronze", metrics: [{ id: "x" }] }] }),
    ).toThrow();
  });
  it("accepte un fichier valide", () => {
    expect(() =>
      validatePipelineStats({
        schema_version: 1,
        stages: [{ id: "bronze", metrics: [{ id: "rows", label_fr: "Lignes", value: 3, kind: "count" }] }],
      }),
    ).not.toThrow();
  });
  it("refuse un aperçu sans routes", () => {
    expect(() => validatePreview({ schema_version: 1, suppliers: [], chokepoints: [] })).toThrow(/preview/);
  });
});

describe("formatage des mesures", () => {
  it("durées, poids et dates lisibles", () => {
    expect(formatMetric({ id: "a", label_fr: "", kind: "seconds", value: 42.3 })).toMatch(/42,3 s/);
    expect(formatMetric({ id: "a", label_fr: "", kind: "seconds", value: 3776.6 })).toMatch(/1 h 03 min/);
    expect(formatMetric({ id: "a", label_fr: "", kind: "bytes", value: 188472 })).toMatch(/184 Ko/);
    expect(formatMetric({ id: "a", label_fr: "", kind: "date", value: "2026-03-02" })).toMatch(/2026/);
  });
});
