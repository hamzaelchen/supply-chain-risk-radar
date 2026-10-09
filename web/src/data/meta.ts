import { ContractError, validateMeta } from "./contract";
import type { Meta } from "./types";

export const dataUrl = (file: string) =>
  new URL(`${import.meta.env.BASE_URL}data/${file}`, window.location.href).href;

/** Lit meta.json seul : léger, affichable avant tout téléchargement de DuckDB-WASM. */
export async function fetchMeta(): Promise<Meta> {
  const res = await fetch(dataUrl("meta.json"));
  if (!res.ok) throw new ContractError(`meta.json introuvable (HTTP ${res.status})`);
  const meta: unknown = await res.json();
  validateMeta(meta);
  return meta;
}
