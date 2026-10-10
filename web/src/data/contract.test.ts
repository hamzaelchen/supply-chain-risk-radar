import { describe, expect, it } from "vitest";
import { ContractError, missingColumns, validateMeta } from "./contract";

const base = {
  schema_version: 1, is_sample: false, sample_notice: null, generated_at: "2026-10-09T10:00:00Z",
  window_start: "a", window_end: "b", latest_fetched_at: "c", gdelt_batches: 1, events_total: 1,
  score_version: "v1", risk_levels: [{ id: "low", label_fr: "Faible", min: 0, max: 100, alert: false }],
  components: ["component_e", "component_t", "component_r", "criticality"].map((id) => ({ id, label_fr: id, description_fr: "d" })),
  portfolio_label: "FICTIF - Entreprise démo",
};

describe("validateMeta", () => {
  it("accepte un meta conforme", () => {
    expect(() => validateMeta(base)).not.toThrow();
  });
  it("nomme la clé absente", () => {
    const { score_version: _omit, ...rest } = base;
    expect(() => validateMeta(rest)).toThrow(/score_version/);
  });
  it("refuse une autre version de schéma", () => {
    expect(() => validateMeta({ ...base, schema_version: 2 })).toThrow(ContractError);
  });
  it("refuse un niveau de risque sans alert", () => {
    const bad = { ...base, risk_levels: [{ id: "low", label_fr: "Faible", min: 0, max: 100 }] };
    expect(() => validateMeta(bad)).toThrow(/risk_levels\[0\]/);
  });
  it("exige les quatre composantes", () => {
    expect(() => validateMeta({ ...base, components: base.components.slice(0, 3) })).toThrow(/components/);
  });
  it("exige une notice quand is_sample", () => {
    expect(() => validateMeta({ ...base, is_sample: true })).toThrow(/sample_notice/);
  });
});

describe("missingColumns", () => {
  it("liste les colonnes manquantes", () => {
    expect(missingColumns("supplier_routes", ["supplier_id", "position"])).toEqual(["chokepoint_id"]);
  });
});

import { pickSnapshot, validateIndex } from "./snapshots";

const snap = (id: string, kind: "live" | "episode" = "live") => ({
  id, label_fr: id, kind, as_of: "2026-03-02", description_fr: "d",
});
const index = { schema_version: 1, default: "ormuz-2026-03-02", snapshots: [snap("today"), snap("ormuz-2026-03-02", "episode")] };

describe("index des instantanés", () => {
  it("accepte un index conforme", () => {
    expect(() => validateIndex(index)).not.toThrow();
  });
  it("refuse un défaut qui n'est pas déclaré", () => {
    expect(() => validateIndex({ ...index, default: "inconnu" })).toThrow(/default/);
  });
  it("refuse un identifiant en double", () => {
    expect(() => validateIndex({ ...index, snapshots: [snap("a"), snap("a")], default: "a" })).toThrow(/double/);
  });
  it("refuse un identifiant qui sortirait du dossier", () => {
    expect(() => validateIndex({ ...index, snapshots: [snap("../x")], default: "../x" })).toThrow(/snapshots\[0\]/);
  });
  it("nomme la version non gérée", () => {
    expect(() => validateIndex({ ...index, schema_version: 2 })).toThrow(/schema_version/);
  });
  it("prend le défaut sans demande, ou avec une demande inconnue", () => {
    validateIndex(index);
    expect(pickSnapshot(index, null).id).toBe("ormuz-2026-03-02");
    expect(pickSnapshot(index, "nimporte").id).toBe("ormuz-2026-03-02");
  });
  it("prend l'instantané demandé s'il existe", () => {
    validateIndex(index);
    expect(pickSnapshot(index, "today").id).toBe("today");
  });
  it("accepte les chiffres et le port d'arrivée, ou leur absence", () => {
    const stats = { events_total: 1, gdelt_batches: 2, days_covered: 3, first_day: "2026-01-04", last_day: "2026-01-06", suppliers: 100, chokepoints: 16 };
    const hub = { name: "Casablanca", country: "Morocco", lat: 33.6, lon: -7.6, source: "Wikidata", source_url: "https://x" };
    expect(() => validateIndex({ ...index, stats, hub })).not.toThrow();
    expect(() => validateIndex(index)).not.toThrow();
  });
  it("refuse des chiffres douteux plutôt que de les afficher", () => {
    expect(() => validateIndex({ ...index, stats: { events_total: -1 } })).toThrow(/stats/);
    expect(() => validateIndex({ ...index, hub: { name: "x", country: "y", lat: 120, lon: 0, source: "s", source_url: "u" } })).toThrow(/hub/);
  });
});
