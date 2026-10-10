import { useEffect, useState } from "react";
import { Outlet, useSearchParams } from "react-router-dom";
import { Banner } from "../components/Banner";
import { SiteFooter, SiteHeader } from "../components/Site";
import { SnapshotPicker } from "../components/SnapshotPicker";
import { useIndexState } from "../data/context";
import { fetchMeta } from "../data/meta";
import { pickSnapshot, type Hub, type Snapshot } from "../data/snapshots";
import type { Dataset, Meta } from "../data/types";
import { RadarContext } from "./radarContext";

type Load =
  | { status: "meta" }
  | { status: "engine"; meta: Meta }
  | { status: "ready"; meta: Meta; data: Dataset }
  | { status: "error"; message: string; meta: Meta | null };

/**
 * Entrer ici déclenche le chargement : meta.json (léger, la bannière s'affiche), puis DuckDB-WASM
 * importé à la demande, puis les Parquet de l'instantané. L'instantané vient de l'URL
 * (#/radar?snapshot=...) : le lien se partage.
 */
export default function RadarLayout() {
  const idx = useIndexState();
  const [params, setParams] = useSearchParams();
  const requested = params.get("snapshot");
  const [load, setLoad] = useState<Load>({ status: "meta" });

  const index = idx.status === "ready" ? idx.index : null;
  const snapshot: Snapshot | null = index ? pickSnapshot(index, requested) : null;
  const snapshotId = snapshot?.id ?? null;

  useEffect(() => {
    if (!snapshotId) return;
    let alive = true;
    setLoad({ status: "meta" });
    const fail = (meta: Meta | null) => (e: unknown) =>
      alive && setLoad({ status: "error", message: e instanceof Error ? e.message : String(e), meta });
    fetchMeta(snapshotId)
      .then((meta) => {
        if (!alive) return;
        setLoad({ status: "engine", meta });
        return import("../data/db")
          .then((m) => m.loadDataset(meta, snapshotId))
          .then((data) => alive && setLoad({ status: "ready", meta, data }))
          .catch(fail(meta));
      })
      .catch(fail(null));
    return () => {
      alive = false;
    };
  }, [snapshotId]);

  const select = (id: string) => setParams({ snapshot: id }, { replace: true });
  const meta = load.status === "meta" ? null : load.meta;
  const historical = snapshot?.kind === "episode";

  return (
    <>
      <SiteHeader radar />
      <Banner meta={meta} historical={historical} />
      {index && snapshot && <SnapshotPicker index={index} current={snapshot} onSelect={select} />}
      {idx.status === "error" && (
        <main className="page" role="alert">
          <h1>Données inutilisables</h1>
          <p>{idx.message}</p>
        </main>
      )}
      {(idx.status === "loading" || load.status === "meta") && idx.status !== "error" && (
        <p className="page muted" role="status">Chargement…</p>
      )}
      {load.status === "engine" && (
        <main className="page" role="status">
          <h1>Chargement du radar</h1>
          <p className="muted">Moteur de requêtes DuckDB-WASM (environ 8 Mo la première fois), puis les données de l'instantané…</p>
          <div className="loader" aria-hidden="true" />
        </main>
      )}
      {load.status === "error" && (
        <main className="page" role="alert">
          <h1>Données inutilisables</h1>
          <p>{load.message}</p>
          <p className="muted">Le site n'affiche rien plutôt qu'un chiffre douteux. Voir web/CONTRACT.md.</p>
        </main>
      )}
      {load.status === "ready" && snapshot && (
        <RadarContext.Provider value={{ data: load.data, snapshot, hub: (index?.hub ?? null) as Hub | null }}>
          <Outlet />
        </RadarContext.Provider>
      )}
      <SiteFooter />
    </>
  );
}
