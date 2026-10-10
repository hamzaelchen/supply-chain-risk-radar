import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";
import { fetchIndex, fetchMeta } from "./meta";
import { pickSnapshot, type Snapshot, type SnapshotIndex } from "./snapshots";
import type { Dataset, Meta } from "./types";

/**
 * Quatre temps : index.json -> meta.json de l'instantané (léger, la page s'affiche) -> DuckDB-WASM
 * importé à la demande (une seule fois) -> Parquet de l'instantané.
 * L'instantané choisi se lit dans l'URL (?snapshot=...) et s'y écrit : le lien se partage.
 */
type Loaded = { index: SnapshotIndex; snapshot: Snapshot; select: (id: string) => void };
type State =
  | { status: "loading" }
  | ({ status: "engine"; meta: Meta } & Loaded)
  | ({ status: "ready"; meta: Meta; data: Dataset } & Loaded)
  | { status: "error"; message: string; meta: Meta | null; index: SnapshotIndex | null; select: ((id: string) => void) | null };

const Ctx = createContext<State>({ status: "loading" });

const requestedFromUrl = () => new URLSearchParams(window.location.search).get("snapshot");

export function DataProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<State>({ status: "loading" });
  const [requested, setRequested] = useState<string | null>(requestedFromUrl);
  const [index, setIndex] = useState<SnapshotIndex | null>(null);

  const select = useCallback((id: string) => {
    const url = new URL(window.location.href);
    url.searchParams.set("snapshot", id);
    window.history.replaceState(null, "", url);
    setRequested(id);
  }, []);

  useEffect(() => {
    fetchIndex().then(setIndex).catch((e: unknown) =>
      setState({ status: "error", message: e instanceof Error ? e.message : String(e), meta: null, index: null, select: null }),
    );
  }, []);

  useEffect(() => {
    if (!index) return;
    let alive = true;
    const snapshot = pickSnapshot(index, requested);
    const fail = (meta: Meta | null) => (e: unknown) =>
      alive && setState({ status: "error", message: e instanceof Error ? e.message : String(e), meta, index, select });
    fetchMeta(snapshot.id)
      .then((meta) => {
        if (!alive) return;
        setState({ status: "engine", meta, index, snapshot, select });
        return import("./db")
          .then((m) => m.loadDataset(meta, snapshot.id))
          .then((data) => alive && setState({ status: "ready", meta, data, index, snapshot, select }))
          .catch(fail(meta));
      })
      .catch(fail(null));
    return () => {
      alive = false;
    };
  }, [index, requested, select]);

  return <Ctx.Provider value={state}>{children}</Ctx.Provider>;
}

export const useDataState = () => useContext(Ctx);
