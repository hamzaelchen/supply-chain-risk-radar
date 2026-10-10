import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import ehWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";

import { ContractError, REQUIRED_COLUMNS, missingColumns } from "./contract";
import { dataUrl } from "./meta";
import type { Dataset, Meta, SupplierEvent } from "./types";

let engine: Promise<duckdb.AsyncDuckDB> | null = null;

/** Un seul moteur par page : changer d'instantané ne recharge pas les ~8 Mo de WASM. */
function startDuckDB(): Promise<duckdb.AsyncDuckDB> {
  engine ??= instantiate();
  return engine;
}

async function instantiate(): Promise<duckdb.AsyncDuckDB> {
  // Les binaires WASM sont servis par le site lui-même (aucun CDN).
  const bundle = await duckdb.selectBundle({
    mvp: { mainModule: duckdbMvpWasm, mainWorker: mvpWorker },
    eh: { mainModule: duckdbEhWasm, mainWorker: ehWorker },
  });
  const worker = new Worker(bundle.mainWorker!);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await db.instantiate(bundle.mainModule);
  return db;
}

// Les entiers 64 bits arrivent en BigInt et les dates en nombres : on normalise ici.
function plain(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    out[k] = typeof v === "bigint" ? Number(v) : v;
  }
  return out;
}

const isoDate = (ms: unknown) => (typeof ms === "number" ? new Date(ms).toISOString().slice(0, 10) : String(ms));
const isoTs = (ms: unknown) => (typeof ms === "number" ? new Date(ms).toISOString() : String(ms));

/** Charge DuckDB-WASM (module et binaire téléchargés ici seulement) puis les tables. */
export async function loadDataset(meta: Meta, snapshot: string): Promise<Dataset> {
  const db = await startDuckDB();
  const conn = await db.connect();
  // Extension Parquet servie par le site (voir scripts/fetch-duckdb-ext.mjs), pas par extensions.duckdb.org.
  await conn.query(`SET custom_extension_repository='${new URL(`${import.meta.env.BASE_URL}duckdb-ext`, window.location.href).href}'`);

  // Fichiers et vues préfixés par l'instantané : plusieurs jeux peuvent coexister dans le moteur.
  const prefix = snapshot.replaceAll("-", "_");
  const view = (table: string) => `${prefix}__${table}`;
  for (const table of Object.keys(REQUIRED_COLUMNS)) {
    const shown = `${snapshot}/${table}.parquet`;
    const registered = `${prefix}__${table}.parquet`;
    const res = await fetch(dataUrl(`${table}.parquet`, snapshot), { method: "HEAD" });
    if (!res.ok) throw new ContractError(`${shown} introuvable (HTTP ${res.status})`);
    await db.registerFileURL(registered, dataUrl(`${table}.parquet`, snapshot), duckdb.DuckDBDataProtocol.HTTP, false);
    const cols = (await conn.query(`DESCRIBE SELECT * FROM read_parquet('${registered}')`))
      .toArray()
      .map((r) => String(r.toJSON().column_name));
    const missing = missingColumns(table, cols);
    if (missing.length) throw new ContractError(`${shown} : colonne(s) absente(s) : ${missing.join(", ")}`);
    await conn.query(`CREATE OR REPLACE VIEW ${view(table)} AS SELECT * FROM read_parquet('${registered}')`);
  }

  const rows = async <T,>(sql: string): Promise<T[]> =>
    (await conn.query(sql)).toArray().map((r) => plain(r.toJSON()) as T);

  const suppliers = await rows<Dataset["suppliers"][number]>(
    `SELECT * FROM ${view("supplier_scores")} ORDER BY risk_score DESC, supplier_id`,
  );
  const chokepoints = await rows<Dataset["chokepoints"][number]>(
    `SELECT * FROM ${view("chokepoint_scores")} ORDER BY events_recent DESC, chokepoint_id`,
  );
  const routes = await rows<Dataset["routes"][number]>(
    `SELECT * FROM ${view("supplier_routes")} ORDER BY supplier_id, position`,
  );

  async function eventsFor(ids: string[]): Promise<Map<string, SupplierEvent[]>> {
    const out = new Map<string, SupplierEvent[]>();
    if (ids.length === 0) return out;
    // Identifiants issus de nos propres données ; on les échappe quand même.
    const list = ids.map((i) => `'${i.replaceAll("'", "''")}'`).join(",");
    const events = await rows<Record<string, unknown>>(
      `SELECT * FROM ${view("supplier_events")} WHERE supplier_id IN (${list})
       ORDER BY supplier_id, coalesce(contribution, 0) DESC, coalesce(num_mentions, 0) DESC, global_event_id`,
    );
    for (const e of events) {
      const ev = { ...e, event_date: isoDate(e.event_date), fetched_at: isoTs(e.fetched_at) } as SupplierEvent;
      const arr = out.get(ev.supplier_id) ?? [];
      arr.push(ev);
      out.set(ev.supplier_id, arr);
    }
    return out;
  }

  return { meta, suppliers, chokepoints, routes, eventsFor };
}
