import { ContractError } from "./contract";

/** Un jeu d'export publié sous data/<id>/ (voir CONTRACT.md, « Instantanés »). */
export interface Snapshot {
  id: string;
  label_fr: string;
  /** « live » : le dernier jour couvert ; « episode » : instantané historique. */
  kind: "live" | "episode";
  /** Jour de calcul (AAAA-MM-JJ). */
  as_of: string;
  /** Texte factuel affiché sous le sélecteur : ce qu'on voit, jamais pourquoi. */
  description_fr: string;
}

export interface SnapshotIndex {
  schema_version: number;
  default: string;
  snapshots: Snapshot[];
}

export const SUPPORTED_INDEX_VERSION = 1;

export function validateIndex(raw: unknown): asserts raw is SnapshotIndex {
  if (typeof raw !== "object" || raw === null) throw new ContractError("index.json : objet attendu");
  const r = raw as Record<string, unknown>;
  if (r.schema_version !== SUPPORTED_INDEX_VERSION) {
    throw new ContractError(`index.json : schema_version ${String(r.schema_version)} non géré (attendu ${SUPPORTED_INDEX_VERSION})`);
  }
  if (!Array.isArray(r.snapshots) || r.snapshots.length === 0) {
    throw new ContractError("index.json : snapshots doit être une liste non vide");
  }
  const ids = new Set<string>();
  for (const [i, s] of (r.snapshots as Record<string, unknown>[]).entries()) {
    const ok =
      typeof s?.id === "string" && /^[a-z0-9][a-z0-9-]*$/.test(s.id) && typeof s.label_fr === "string" &&
      (s.kind === "live" || s.kind === "episode") && typeof s.as_of === "string" && typeof s.description_fr === "string";
    if (!ok) throw new ContractError(`index.json : snapshots[${i}] doit avoir id (a-z, 0-9, tirets), label_fr, kind (live|episode), as_of, description_fr`);
    if (ids.has(s.id as string)) throw new ContractError(`index.json : identifiant « ${String(s.id)} » en double`);
    ids.add(s.id as string);
  }
  if (typeof r.default !== "string" || !ids.has(r.default)) {
    throw new ContractError(`index.json : default « ${String(r.default)} » n'est pas un instantané déclaré`);
  }
}

/** Instantané à afficher : celui demandé dans l'URL s'il existe, sinon le défaut de l'index. */
export function pickSnapshot(index: SnapshotIndex, requested: string | null): Snapshot {
  const found = index.snapshots.find((s) => s.id === requested);
  return found ?? index.snapshots.find((s) => s.id === index.default)!;
}
