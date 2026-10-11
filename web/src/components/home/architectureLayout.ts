import type { BrandKey } from "./brands";

/**
 * Schéma d'architecture : n'y figure que ce qui est dans le dépôt. Chaque bloc a été relevé dans
 * docker-compose.yml (Kafka, MinIO, catalogue REST), pyproject.toml (pyspark, pyiceberg, confluent-kafka,
 * dbt-duckdb, dagster, ruff, pytest), le Makefile, orchestration/definitions.py et .github/workflows/.
 * Le gold n'est PAS une table Iceberg : c'est une base DuckDB exportée en Parquet (CLAUDE.md, 2026-10-10).
 */
export type Badge = { icon: BrandKey } | { text: string };

export interface NodeDef {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  name: string;
  sub?: string;
  badges: Badge[];
  /** Une phrase : le rôle du bloc, montrée au survol, au focus ou au clic. */
  tip: string;
  compact?: boolean;
}

export interface EdgeDef {
  id: string;
  d: string;
  from: string;
  to: string;
}

export interface FrameDef {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
  dashed?: boolean;
}

export interface Layout {
  vw: number;
  vh: number;
  frames: FrameDef[];
  nodes: NodeDef[];
  edges: EdgeDef[];
}

type Spec = Omit<NodeDef, "x" | "y" | "w" | "h" | "compact">;

const spec = {
  gdelt: { id: "gdelt", name: "GDELT 2.0", sub: "événements, 15 min", badges: [{ text: "GDELT" }],
    tip: "Base ouverte d'événements du monde entier, publiée en un fichier toutes les 15 minutes ; le projet télécharge les fichiers tels quels et en garde l'URL." },
  wikidata: { id: "wikidata", name: "Wikidata (CC0)", sub: "villes et détroits", badges: [{ icon: "wikidata" }],
    tip: "Coordonnées des villes et des détroits, sous licence CC0, avec la révision citée pour chaque point." },
  portfolio: { id: "portfolio", name: "Portefeuille fictif", sub: "100 fournisseurs, CSV", badges: [{ text: "CSV" }],
    tip: "Les 100 fournisseurs de l'« Entreprise démo » sont inventés : graine fixe, versionnés en CSV, étiquetés fictifs partout." },
  producer: { id: "producer", name: "Producteur", sub: "Python", badges: [{ icon: "python" }],
    tip: "Script Python qui télécharge chaque nouveau fichier GDELT, archive le zip dans MinIO et publie les lignes brutes dans Kafka." },
  kafka: { id: "kafka", name: "Kafka", sub: "mode KRaft, un nœud", badges: [{ icon: "kafka" }],
    tip: "Journal d'événements en mode KRaft : il sépare le téléchargement de l'écriture et garde 7 jours de messages à rejouer." },
  spark: { id: "spark", name: "Spark", sub: "Structured Streaming", badges: [{ icon: "spark" }],
    tip: "Spark Structured Streaming lit le topic Kafka et écrit la table bronze ; un checkpoint garantit qu'aucune ligne n'est écrite deux fois." },
  backfill: { id: "backfill", name: "Historique", sub: "chargement en lot", badges: [{ icon: "python" }, { icon: "spark" }],
    tip: "Chemin séparé pour l'historique GDELT : chargement en lot, sans Kafka (ce n'est pas un flux), dans la même table bronze et le même manifeste." },
  iceberg: { id: "iceberg", name: "Lakehouse : bronze, silver, proximité", sub: "catalogue REST · stockage S3 sur MinIO",
    badges: [{ text: "Apache Iceberg" }, { icon: "minio" }],
    tip: "Tables Apache Iceberg sur MinIO : bronze brut, silver typée et dédoublonnée (Spark en lot), proximité événement-lieu. Le catalogue REST est partagé par Spark, DuckDB et PyIceberg." },
  dbt: { id: "dbt", name: "dbt-duckdb", sub: "score 0-100 · tests qualité · gold", badges: [{ text: "dbt" }, { icon: "duckdb" }],
    tip: "dbt lit la silver avec DuckDB et calcule le gold : le score de risque, avec des tests de qualité. Le gold est une base DuckDB, pas une table Iceberg." },
  export: { id: "export", name: "Export Parquet", sub: "un dossier par instantané", badges: [{ icon: "parquet" }],
    tip: "Le gold est exporté en fichiers Parquet, validés contre le contrat du site avant d'être publiés." },
  react: { id: "react", name: "React + DuckDB-WASM", sub: "lit les Parquet", badges: [{ icon: "react" }, { icon: "duckdb" }],
    tip: "Le navigateur interroge les Parquet avec DuckDB-WASM : ni API ni serveur, et le site n'a jamais à recalculer un score." },
  pages: { id: "pages", name: "GitHub Pages", sub: "site statique", badges: [{ icon: "githubpages" }],
    tip: "Hébergement statique du site. Le workflow de déploiement existe et se lance à la main." },
  dagster: { id: "dagster", name: "Dagster", badges: [{ text: "Dagster" }],
    tip: "Un job Dagster enchaîne producteur, bronze, silver, proximité, gold et export ; le planning toutes les 15 minutes est écrit mais pas activé." },
  compose: { id: "compose", name: "Docker Compose", badges: [{ icon: "docker" }],
    tip: "Docker Compose démarre Kafka, MinIO et le catalogue REST en local ; Spark tourne sur la machine hôte." },
  gha: { id: "gha", name: "GitHub Actions", badges: [{ icon: "githubactions" }],
    tip: "À chaque push, GitHub Actions rejoue le format, le lint, les tests Python, le projet dbt sur un lac de test, puis les tests et le build du site." },
  ruff: { id: "ruff", name: "ruff", badges: [{ icon: "ruff" }],
    tip: "Formatage et lint du code Python." },
  pytest: { id: "pytest", name: "pytest", badges: [{ icon: "pytest" }],
    tip: "Tests Python de chaque transformation, sans infrastructure en CI ; les tests d'intégration tournent en local." },
  dbtci: { id: "dbtci", name: "dbt build", badges: [{ text: "dbt" }],
    tip: "Les modèles et les tests dbt tournent sur un petit lac fabriqué à partir d'un vrai cycle GDELT." },
  site: { id: "site", name: "Build du site", badges: [{ icon: "vite" }],
    tip: "Tests puis build du site (Vite) : la CI vérifie que ce que vous lisez se construit." },
} satisfies Record<string, Spec>;

const node = (s: Spec, x: number, y: number, w: number, h = 76, compact = false): NodeDef => ({
  ...s, x, y, w, h, compact,
}); // fmt: skip

export const WIDE: Layout = {
  vw: 1100,
  vh: 668,
  frames: [
    { id: "f-orch", x: 20, y: 8, w: 1060, h: 56, label: "ORCHESTRATION" },
    { id: "f-src", x: 10, y: 84, w: 240, h: 440, label: "SOURCES" },
    { id: "f-lake", x: 270, y: 84, w: 570, h: 510, label: "LAKEHOUSE · local, Docker Compose", dashed: true },
    { id: "f-demo", x: 870, y: 84, w: 220, h: 510, label: "DÉMO" },
    { id: "f-ci", x: 20, y: 604, w: 1060, h: 56, label: "CI/CD" },
  ],
  nodes: [
    node(spec.dagster, 200, 18, 150, 36, true),
    node(spec.compose, 370, 18, 210, 36, true),
    node(spec.gha, 200, 614, 200, 36, true),
    node(spec.ruff, 420, 614, 110, 36, true),
    node(spec.pytest, 550, 614, 130, 36, true),
    node(spec.dbtci, 700, 614, 160, 36, true),
    node(spec.site, 880, 614, 190, 36, true),
    node(spec.gdelt, 20, 112, 220),
    node(spec.wikidata, 20, 318, 220),
    node(spec.portfolio, 20, 440, 220),
    node(spec.producer, 290, 118, 140),
    node(spec.kafka, 455, 118, 140),
    node(spec.spark, 620, 118, 140),
    node(spec.backfill, 290, 250, 140),
    node(spec.iceberg, 290, 372, 520, 94),
    node(spec.dbt, 290, 512, 270),
    node(spec.export, 890, 130, 180),
    node(spec.react, 890, 262, 180),
    node(spec.pages, 890, 394, 180),
  ],
  edges: [
    { id: "e1", d: "M240 150H290", from: "gdelt", to: "producer" },
    { id: "e2", d: "M240 166H258V288H290", from: "gdelt", to: "backfill" },
    { id: "e3", d: "M430 156H455", from: "producer", to: "kafka" },
    { id: "e4", d: "M595 156H620", from: "kafka", to: "spark" },
    { id: "e5", d: "M690 194V372", from: "spark", to: "iceberg" },
    { id: "e6", d: "M360 326V372", from: "backfill", to: "iceberg" },
    { id: "e7", d: "M240 356H262V404H290", from: "wikidata", to: "iceberg" },
    { id: "e8", d: "M240 478H262V438H290", from: "portfolio", to: "iceberg" },
    { id: "e9", d: "M425 466V512", from: "iceberg", to: "dbt" },
    { id: "e10", d: "M560 550H850V168H890", from: "dbt", to: "export" },
    { id: "e11", d: "M980 206V262", from: "export", to: "react" },
    { id: "e12", d: "M980 338V394", from: "react", to: "pages" },
  ],
};

/** Disposition verticale (mobile et tablette) : sources en haut, démo en bas. */
export const TALL: Layout = {
  vw: 360,
  vh: 1500,
  frames: [
    { id: "f-orch", x: 8, y: 8, w: 344, h: 84, label: "ORCHESTRATION" },
    { id: "f-src", x: 8, y: 108, w: 344, h: 206, label: "SOURCES" },
    { id: "f-lake", x: 8, y: 320, w: 344, h: 600, label: "LAKEHOUSE", dashed: true },
    { id: "f-demo", x: 8, y: 940, w: 344, h: 340, label: "DÉMO" },
    { id: "f-ci", x: 8, y: 1300, w: 344, h: 190, label: "CI/CD" },
  ],
  nodes: [
    node(spec.dagster, 20, 40, 120, 36, true),
    node(spec.compose, 150, 40, 190, 36, true),
    node(spec.gdelt, 20, 144, 150),
    node(spec.wikidata, 190, 144, 150),
    node(spec.portfolio, 190, 232, 150),
    node(spec.producer, 20, 362, 150),
    node(spec.kafka, 20, 470, 150),
    node(spec.spark, 20, 578, 150),
    node(spec.backfill, 190, 362, 150),
    node(spec.iceberg, 20, 700, 320, 94),
    node(spec.dbt, 20, 830, 320),
    node(spec.export, 20, 982, 320),
    node(spec.react, 20, 1086, 320),
    node(spec.pages, 20, 1190, 320),
    node(spec.gha, 20, 1336, 320, 36, true),
    node(spec.ruff, 20, 1384, 150, 36, true),
    node(spec.pytest, 190, 1384, 150, 36, true),
    node(spec.dbtci, 20, 1432, 150, 36, true),
    node(spec.site, 190, 1432, 150, 36, true),
  ],
  edges: [
    { id: "e1", d: "M150 220V362", from: "gdelt", to: "producer" },
    { id: "e2", d: "M170 182H180V400H190", from: "gdelt", to: "backfill" },
    { id: "e3", d: "M95 438V470", from: "producer", to: "kafka" },
    { id: "e4", d: "M95 546V578", from: "kafka", to: "spark" },
    { id: "e5", d: "M95 654V700", from: "spark", to: "iceberg" },
    { id: "e6", d: "M265 438V700", from: "backfill", to: "iceberg" },
    { id: "e7", d: "M340 182H346V747H340", from: "wikidata", to: "iceberg" },
    { id: "e8", d: "M340 270H346V747", from: "portfolio", to: "iceberg" },
    { id: "e9", d: "M180 794V830", from: "iceberg", to: "dbt" },
    { id: "e10", d: "M180 906V982", from: "dbt", to: "export" },
    { id: "e11", d: "M180 1058V1086", from: "export", to: "react" },
    { id: "e12", d: "M180 1162V1190", from: "react", to: "pages" },
  ],
};

/** Longueur d'un tracé M/H/V (les seules commandes utilisées ici) : sert à régler la durée des particules. */
export function pathLength(d: string): number {
  const re = /([MHV])\s*(-?\d+(?:\.\d+)?)(?:\s+(-?\d+(?:\.\d+)?))?/g;
  let x = 0, y = 0, len = 0, m: RegExpExecArray | null;
  while ((m = re.exec(d))) {
    const [, c, a, b] = m;
    if (c === "M") { x = +a; y = +(b ?? 0); }
    else if (c === "H") { len += Math.abs(+a - x); x = +a; }
    else { len += Math.abs(+a - y); y = +a; }
  }
  return len;
}
