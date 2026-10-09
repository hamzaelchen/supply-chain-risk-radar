// Colonnes exigées par web/CONTRACT.md. Une colonne absente est une erreur nommée.
export const SUPPORTED_SCHEMA_VERSION = 1;

export const REQUIRED_COLUMNS: Record<string, string[]> = {
  supplier_scores: [
    "supplier_id", "name", "category", "category_label", "city", "country", "iso2", "region",
    "lat", "lon", "sole_source", "purchase_share_pct", "label", "risk_score", "risk_level",
    "component_e", "component_t", "component_r", "criticality", "score_local", "score_route", "events_local", "events_route", "top_driver",
  ],
  supplier_events: [
    "supplier_id", "link_type", "chokepoint_id", "distance_km", "global_event_id", "event_date",
    "fetched_at", "action_geo_name", "action_geo_type", "lat", "lon", "event_code",
    "event_root_code", "quad_class", "goldstein_scale", "avg_tone", "num_mentions", "num_sources",
    "num_articles", "source_url", "src_file_url", "contribution", "llm_category", "llm_summary",
  ],
  chokepoint_scores: [
    "chokepoint_id", "name_fr", "name_en", "lat", "lon", "radius_km", "risk_score", "risk_level",
    "events_recent", "suppliers_exposed", "purchase_share_exposed_pct", "sole_source_exposed",
  ],
  supplier_routes: ["supplier_id", "position", "chokepoint_id"],
};

export const REQUIRED_META_KEYS = [
  "schema_version", "is_sample", "sample_notice", "generated_at", "window_start", "window_end",
  "latest_fetched_at", "gdelt_batches", "events_total", "score_version", "risk_levels", "components",
  "portfolio_label",
] as const;

export class ContractError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ContractError";
  }
}

export function missingColumns(table: string, present: string[]): string[] {
  const have = new Set(present);
  return (REQUIRED_COLUMNS[table] ?? []).filter((c) => !have.has(c));
}

/** Valide meta.json ; lève une ContractError qui nomme la clé ou la règle en cause. */
export function validateMeta(raw: unknown): asserts raw is import("./types").Meta {
  if (typeof raw !== "object" || raw === null) throw new ContractError("meta.json : objet attendu");
  const m = raw as Record<string, unknown>;
  for (const k of REQUIRED_META_KEYS) {
    if (!(k in m)) throw new ContractError(`meta.json : clé « ${k} » absente`);
  }
  if (m.schema_version !== SUPPORTED_SCHEMA_VERSION) {
    throw new ContractError(
      `meta.json : schema_version ${String(m.schema_version)} non géré (le site attend ${SUPPORTED_SCHEMA_VERSION})`,
    );
  }
  if (m.is_sample === true && !m.sample_notice) {
    throw new ContractError("meta.json : is_sample est vrai mais sample_notice est vide");
  }
  if (!Array.isArray(m.risk_levels) || m.risk_levels.length === 0) {
    throw new ContractError("meta.json : risk_levels doit être une liste non vide");
  }
  for (const [i, l] of (m.risk_levels as Record<string, unknown>[]).entries()) {
    const ok =
      typeof l?.id === "string" && typeof l.label_fr === "string" && typeof l.min === "number" &&
      typeof l.max === "number" && typeof l.alert === "boolean";
    if (!ok) {
      throw new ContractError(`meta.json : risk_levels[${i}] doit avoir id, label_fr, min, max (nombres) et alert (booléen)`);
    }
  }
  const ids = Array.isArray(m.components) ? (m.components as Record<string, unknown>[]).map((c) => c?.id) : [];
  const expected = ["component_e", "component_t", "component_r", "criticality"];
  if (ids.length !== 4 || !expected.every((id) => ids.includes(id))) {
    throw new ContractError(`meta.json : components doit contenir exactement ${expected.join(", ")}`);
  }
  for (const [i, c] of (m.components as Record<string, unknown>[]).entries()) {
    if (typeof c.label_fr !== "string" || typeof c.description_fr !== "string") {
      throw new ContractError(`meta.json : components[${i}] doit avoir label_fr et description_fr`);
    }
  }
}
