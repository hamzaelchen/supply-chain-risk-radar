import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { fetchMeta } from "./meta";
import type { Dataset, Meta } from "./types";

/**
 * Trois temps : meta.json (léger) -> la page s'affiche ; puis DuckDB-WASM est importé à la
 * demande (code + binaire, plusieurs Mo) et lit les Parquet -> les données arrivent.
 */
type State =
  | { status: "loading" }
  | { status: "engine"; meta: Meta }
  | { status: "ready"; meta: Meta; data: Dataset }
  | { status: "error"; message: string; meta: Meta | null };

const Ctx = createContext<State>({ status: "loading" });

export function DataProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<State>({ status: "loading" });
  useEffect(() => {
    let alive = true;
    const fail = (meta: Meta | null) => (e: unknown) =>
      alive && setState({ status: "error", message: e instanceof Error ? e.message : String(e), meta });
    fetchMeta()
      .then((meta) => {
        if (!alive) return;
        setState({ status: "engine", meta });
        return import("./db")
          .then((m) => m.loadDataset(meta))
          .then((data) => alive && setState({ status: "ready", meta, data }))
          .catch(fail(meta));
      })
      .catch(fail(null));
    return () => {
      alive = false;
    };
  }, []);
  return <Ctx.Provider value={state}>{children}</Ctx.Provider>;
}

export const useDataState = () => useContext(Ctx);
