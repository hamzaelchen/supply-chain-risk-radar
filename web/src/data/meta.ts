import { ContractError, validateMeta } from "./contract";
import { validateIndex, type SnapshotIndex } from "./snapshots";
import type { Meta } from "./types";

/** URL d'un fichier publié. Sans instantané : data/<fichier> ; avec : data/<instantané>/<fichier>. */
export const dataUrl = (file: string, snapshot?: string) =>
  new URL(`${import.meta.env.BASE_URL}data/${snapshot ? `${snapshot}/` : ""}${file}`, window.location.href).href;

export async function fetchIndex(): Promise<SnapshotIndex> {
  const res = await fetch(dataUrl("index.json"));
  if (!res.ok) throw new ContractError(`index.json introuvable (HTTP ${res.status})`);
  const index: unknown = await res.json();
  validateIndex(index);
  return index;
}

/** Lit meta.json d'un instantané : léger, affichable avant tout téléchargement de DuckDB-WASM. */
export async function fetchMeta(snapshot: string): Promise<Meta> {
  const res = await fetch(dataUrl("meta.json", snapshot));
  if (!res.ok) throw new ContractError(`${snapshot}/meta.json introuvable (HTTP ${res.status})`);
  const meta: unknown = await res.json();
  validateMeta(meta);
  return meta;
}
