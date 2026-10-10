// Télécharge les extensions DuckDB-WASM (parquet) dans public/duckdb-ext/ pour que le site les
// serve lui-même au lieu d'aller sur extensions.duckdb.org à chaque visite.
// Appelé par `npm run dev` et `npm run build` ; sans effet si les fichiers existent déjà.
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const cfg = JSON.parse(await readFile(join(root, "duckdb-ext.json"), "utf8"));

for (const platform of cfg.platforms) {
  for (const ext of cfg.extensions) {
    const rel = `${cfg.engine_version}/${platform}/${ext}.duckdb_extension.wasm`;
    const dest = join(root, "public/duckdb-ext", rel);
    if (await stat(dest).then((s) => s.size > 0, () => false)) continue;
    const url = `https://extensions.duckdb.org/${rel}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${url} : HTTP ${res.status}`);
    await mkdir(dirname(dest), { recursive: true });
    await writeFile(dest, Buffer.from(await res.arrayBuffer()));
    console.log(`téléchargé ${url} (${(await stat(dest)).size} octets)`);
  }
}
