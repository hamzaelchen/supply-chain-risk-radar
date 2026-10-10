import { createContext, useContext } from "react";
import type { Hub, Snapshot } from "../data/snapshots";
import type { Dataset } from "../data/types";

export interface Radar {
  data: Dataset;
  snapshot: Snapshot;
  /** Port d'arrivée des routes ; null si l'index ne le fournit pas (routes non tracées). */
  hub: Hub | null;
}

export const RadarContext = createContext<Radar | null>(null);

export function useRadar(): Radar {
  const r = useContext(RadarContext);
  if (!r) throw new Error("useRadar doit être appelé sous RadarLayout");
  return r;
}

/** Lien interne qui conserve l'instantané affiché (#/fournisseur/S001?snapshot=...). */
export function useRadarLink() {
  const { snapshot } = useRadar();
  return (pathname: string) => ({ pathname, search: `?snapshot=${snapshot.id}` });
}
