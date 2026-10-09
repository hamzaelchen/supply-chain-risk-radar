import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { loadDataset } from "./db";
import type { Dataset } from "./types";

type State = { status: "loading" } | { status: "error"; message: string } | { status: "ready"; data: Dataset };

const Ctx = createContext<State>({ status: "loading" });

export function DataProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<State>({ status: "loading" });
  useEffect(() => {
    let alive = true;
    loadDataset()
      .then((data) => alive && setState({ status: "ready", data }))
      .catch((e: unknown) => alive && setState({ status: "error", message: e instanceof Error ? e.message : String(e) }));
    return () => {
      alive = false;
    };
  }, []);
  return <Ctx.Provider value={state}>{children}</Ctx.Provider>;
}

export const useDataState = () => useContext(Ctx);
