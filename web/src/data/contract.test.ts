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
