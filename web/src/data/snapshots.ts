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

/** Chiffres de la page d'accueil : lus par le backend dans la base, jamais écrits à la main. */
export interface LakeStats {
  events_total: number;
  gdelt_batches: number;
  /** Jours entièrement couverts par des lots GDELT. */
  days_covered: number;
  first_day: string;
  last_day: string;
  suppliers: number;
  chokepoints: number;
}

/** Ville d'arrivée de toutes les routes (siège de l'Entreprise démo), avec la source de ses coordonnées. */
export interface Hub {
  name: string;
  country: string;
  lat: number;
  lon: number;
  source: string;
  source_url: string;
}

export interface SnapshotIndex {
  schema_version: number;
  default: string;
  /** Facultatif : sans lui, la page d'accueil affiche « n/d » plutôt qu'un chiffre. */
  stats?: LakeStats;
  /** Facultatif : sans lui, les routes de la carte ne sont pas tracées. */
  hub?: Hub;
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
  const num = (v: unknown) => typeof v === "number" && Number.isFinite(v);
  if (r.stats !== undefined) {
    const t = r.stats as Record<string, unknown> | null;
    const ok =
      typeof t === "object" && t !== null &&
      ["events_total", "gdelt_batches", "days_covered", "suppliers", "chokepoints"].every((k) => num(t[k]) && (t[k] as number) >= 0) &&
      typeof t.first_day === "string" && typeof t.last_day === "string";
    if (!ok) throw new ContractError("index.json : stats doit avoir events_total, gdelt_batches, days_covered, suppliers, chokepoints (nombres >= 0), first_day, last_day");
  }
  if (r.hub !== undefined) {
    const h = r.hub as Record<string, unknown> | null;
    const ok =
      typeof h === "object" && h !== null && typeof h.name === "string" && typeof h.country === "string" &&
      num(h.lat) && num(h.lon) && Math.abs(h.lat as number) <= 90 && Math.abs(h.lon as number) <= 180 &&
      typeof h.source === "string" && typeof h.source_url === "string";
    if (!ok) throw new ContractError("index.json : hub doit avoir name, country, lat, lon (plages valides), source, source_url");
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
