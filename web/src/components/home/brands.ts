import {
  siApacheparquet,
  siApachekafka,
  siApachespark,
  siDocker,
  siDuckdb,
  siGithubactions,
  siGithubpages,
  siMinio,
  siPytest,
  siPython,
  siReact,
  siRuff,
  siVite,
  siWikidata,
} from "simple-icons";

/**
 * Logos : chemins SVG du paquet npm « simple-icons » (CC0), affichés tels quels, dans leur couleur
 * officielle, sur une tuile claire ou sombre selon la luminance de cette couleur (le logo n'est pas
 * recoloré). Pas de logo dans simple-icons pour GDELT, Apache Iceberg, dbt et Dagster : ces
 * quatre-là sont écrits en texte, dans un badge.
 */
export interface Brand {
  title: string;
  hex: string;
  path: string;
}

export const BRANDS = {
  parquet: siApacheparquet,
  kafka: siApachekafka,
  spark: siApachespark,
  docker: siDocker,
  duckdb: siDuckdb,
  githubactions: siGithubactions,
  githubpages: siGithubpages,
  minio: siMinio,
  pytest: siPytest,
  python: siPython,
  react: siReact,
  ruff: siRuff,
  vite: siVite,
  wikidata: siWikidata,
} satisfies Record<string, Brand>;

export type BrandKey = keyof typeof BRANDS;

/** Luminance relative WCAG d'une couleur #rrggbb (sans le #). */
export function luminance(hex: string): number {
  const c = [0, 2, 4].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255);
  const [r, g, b] = c.map((v) => (v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4));
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/** Tuile sous le logo : celle des deux (sombre, claire) qui contraste le mieux avec la couleur du logo. */
export function tileFor(hex: string): string {
  const l = luminance(hex);
  const contrast = (tile: string) => {
    const t = luminance(tile.slice(1));
    return (Math.max(l, t) + 0.05) / (Math.min(l, t) + 0.05);
  };
  return contrast("#0b1220") >= contrast("#f1f6fc") ? "#0b1220" : "#f1f6fc";
}
