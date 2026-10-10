import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { fetchIndex } from "./meta";
import type { SnapshotIndex } from "./snapshots";

/**
 * index.json seul (quelques Ko) : c'est tout ce dont la page d'accueil a besoin.
 * DuckDB-WASM et les Parquet ne se chargent qu'en entrant dans le radar (voir pages/RadarLayout.tsx).
 */
type IndexState =
  | { status: "loading" }
  | { status: "ready"; index: SnapshotIndex }
  | { status: "error"; message: string };

const Ctx = createContext<IndexState>({ status: "loading" });

export function IndexProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<IndexState>({ status: "loading" });
  useEffect(() => {
    let alive = true;
    fetchIndex()
      .then((index) => alive && setState({ status: "ready", index }))
      .catch((e: unknown) => alive && setState({ status: "error", message: e instanceof Error ? e.message : String(e) }));
    return () => {
      alive = false;
    };
  }, []);
  return <Ctx.Provider value={state}>{children}</Ctx.Provider>;
}

export const useIndexState = () => useContext(Ctx);
