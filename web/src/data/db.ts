import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import ehWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";

import { ContractError, REQUIRED_COLUMNS, missingColumns, validateMeta } from "./contract";
import type { Dataset, Meta, SupplierEvent } from "./types";

const dataUrl = (file: string) => new URL(`${import.meta.env.BASE_URL}data/${file}`, window.location.href).href;

async function startDuckDB(): Promise<duckdb.AsyncDuckDB> {
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

export async function loadDataset(): Promise<Dataset> {
  const metaRes = await fetch(dataUrl("meta.json"));
  if (!metaRes.ok) throw new ContractError(`meta.json introuvable (HTTP ${metaRes.status})`);
  const meta: unknown = await metaRes.json();
  validateMeta(meta);

  const db = await startDuckDB();
  const conn = await db.connect();

  for (const table of Object.keys(REQUIRED_COLUMNS)) {
    const file = `${table}.parquet`;
    const res = await fetch(dataUrl(file), { method: "HEAD" });
    if (!res.ok) throw new ContractError(`${file} introuvable (HTTP ${res.status})`);
    await db.registerFileURL(file, dataUrl(file), duckdb.DuckDBDataProtocol.HTTP, false);
    const cols = (await conn.query(`DESCRIBE SELECT * FROM read_parquet('${file}')`))
      .toArray()
      .map((r) => String(r.toJSON().column_name));
    const missing = missingColumns(table, cols);
    if (missing.length) throw new ContractError(`${file} : colonne(s) absente(s) : ${missing.join(", ")}`);
    await conn.query(`CREATE VIEW ${table} AS SELECT * FROM read_parquet('${file}')`);
  }

  const rows = async <T,>(sql: string): Promise<T[]> =>
    (await conn.query(sql)).toArray().map((r) => plain(r.toJSON()) as T);

  const suppliers = await rows<Dataset["suppliers"][number]>(
    "SELECT * FROM supplier_scores ORDER BY risk_score DESC, supplier_id",
  );
  const chokepoints = await rows<Dataset["chokepoints"][number]>(
    "SELECT * FROM chokepoint_scores ORDER BY events_recent DESC, chokepoint_id",
  );
  const routes = await rows<Dataset["routes"][number]>(
    "SELECT * FROM supplier_routes ORDER BY supplier_id, position",
  );

  async function eventsFor(ids: string[]): Promise<Map<string, SupplierEvent[]>> {
    const out = new Map<string, SupplierEvent[]>();
    if (ids.length === 0) return out;
    // Identifiants issus de nos propres données ; on les échappe quand même.
    const list = ids.map((i) => `'${i.replaceAll("'", "''")}'`).join(",");
    const events = await rows<Record<string, unknown>>(
      `SELECT * FROM supplier_events WHERE supplier_id IN (${list})
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

  return { meta: meta as Meta, suppliers, chokepoints, routes, eventsFor };
}
