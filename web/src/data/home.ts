import { useEffect, useState } from "react";
import { ContractError } from "./contract";
import { dataUrl } from "./meta";
import type { ChokepointScore, MapSupplier, RiskLevel, SupplierRoute } from "./types";

/**
 * Deux petits fichiers JSON pour la page d'accueil, produits par transform/site_data.py :
 * pipeline_stats.json (chiffres du pipeline) et preview.json (aperçu de la carte).
 * Aucun des deux n'a besoin de DuckDB-WASM. Absents ou invalides, la section l'indique (« n/d »)
 * au lieu d'afficher un chiffre.
 */

export type MetricKind = "count" | "seconds" | "bytes" | "date";

export interface Metric {
  id: string;
  label_fr: string;
  value: number | string;
  kind: MetricKind;
}

export interface StageStats {
  id: string;
  metrics: Metric[];
}

export interface PipelineStats {
  schema_version: number;
  generated_at: string;
  stages: StageStats[];
}

export interface PreviewKpis {
  suppliers: number;
  alerts: number;
  alert_purchase_share_pct: number;
  sole_source_alerts: number;
  events_total: number;
  focus_chokepoint: ChokepointScore | null;
}

export interface Preview {
  schema_version: number;
  snapshot_id: string;
  window_end: string;
  risk_levels: RiskLevel[];
  suppliers: MapSupplier[];
  chokepoints: ChokepointScore[];
  routes: SupplierRoute[];
  kpis: PreviewKpis;
}

const KINDS = new Set<string>(["count", "seconds", "bytes", "date"]);

export function validatePipelineStats(raw: unknown): asserts raw is PipelineStats {
  const r = raw as Record<string, unknown> | null;
  if (typeof r !== "object" || r === null || r.schema_version !== 1 || !Array.isArray(r.stages)) {
    throw new ContractError("pipeline_stats.json : schema_version 1 et liste « stages » attendus");
  }
  for (const s of r.stages as Record<string, unknown>[]) {
    const ok =
      typeof s?.id === "string" &&
      Array.isArray(s.metrics) &&
      (s.metrics as Record<string, unknown>[]).every(
        (m) =>
          typeof m.id === "string" &&
          typeof m.label_fr === "string" &&
          KINDS.has(String(m.kind)) &&
          (typeof m.value === "number" || typeof m.value === "string"),
      );
    if (!ok) throw new ContractError("pipeline_stats.json : une étape ou une mesure est mal formée");
  }
}

export function validatePreview(raw: unknown): asserts raw is Preview {
  const r = raw as Record<string, unknown> | null;
  const ok =
    typeof r === "object" && r !== null && r.schema_version === 1 &&
    Array.isArray(r.suppliers) && Array.isArray(r.chokepoints) && Array.isArray(r.routes) &&
    Array.isArray(r.risk_levels) && typeof r.kpis === "object" && r.kpis !== null;
  if (!ok) throw new ContractError("preview.json : schema_version 1, suppliers, chokepoints, routes, risk_levels, kpis attendus");
}

export type Loaded<T> = { status: "loading" } | { status: "ready"; value: T } | { status: "error"; message: string };

function useJson<T>(file: string, validate: (raw: unknown) => asserts raw is T, enabled = true): Loaded<T> {
  const [state, setState] = useState<Loaded<T>>({ status: "loading" });
  useEffect(() => {
    if (!enabled) return;
    let alive = true;
    fetch(dataUrl(file))
      .then((res) => {
        if (!res.ok) throw new ContractError(`${file} introuvable (HTTP ${res.status})`);
        return res.json() as Promise<unknown>;
      })
      .then((raw) => {
        validate(raw);
        if (alive) setState({ status: "ready", value: raw });
      })
      .catch((e: unknown) => alive && setState({ status: "error", message: e instanceof Error ? e.message : String(e) }));
    return () => {
      alive = false;
    };
  }, [file, validate, enabled]);
  return state;
}

export const usePipelineStats = () => useJson("pipeline_stats.json", validatePipelineStats);
/** `enabled` : tant que la section n'approche pas de l'écran, l'aperçu n'est pas téléchargé. */
export const usePreview = (enabled: boolean) => useJson("preview.json", validatePreview, enabled);

export function metricOf(stats: PipelineStats | null, stage: string, id: string): Metric | undefined {
  return stats?.stages.find((s) => s.id === stage)?.metrics.find((m) => m.id === id);
}
