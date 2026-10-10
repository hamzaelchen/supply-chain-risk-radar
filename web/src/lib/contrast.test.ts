import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

/** Contraste WCAG 2.x entre deux couleurs #rrggbb. */
const lum = (hex: string) => {
  const c = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255);
  const [r, g, b] = c.map((v) => (v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4));
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
export const ratio = (a: string, b: string) => {
  const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
};

const css = readFileSync(new URL("../styles.css", import.meta.url), "utf8");
const root = css.slice(css.indexOf(":root {"), css.indexOf("}", css.indexOf(":root {")));
const tok = (name: string) => {
  const m = new RegExp(`--${name}:\\s*(#[0-9a-fA-F]{6})`).exec(root);
  if (!m) throw new Error(`jeton --${name} introuvable dans styles.css`);
  return m[1];
};

describe("palette : contraste WCAG AA (4,5 pour le texte)", () => {
  const surfaces = ["bg", "bg-2", "surface", "surface-2"];
  it.each(surfaces)("texte et texte atténué lisibles sur %s", (s) => {
    expect(ratio(tok("text"), tok(s))).toBeGreaterThanOrEqual(4.5);
    expect(ratio(tok("muted"), tok(s))).toBeGreaterThanOrEqual(4.5);
    expect(ratio(tok("link"), tok(s))).toBeGreaterThanOrEqual(4.5);
  });
  it.each(["surveillance", "alerte", "critique"])("niveau %s lisible sur les surfaces", (lv) => {
    for (const s of surfaces) expect(ratio(tok(`lv-${lv}`), tok(s))).toBeGreaterThanOrEqual(4.5);
  });
  it.each(["surveillance", "alerte", "critique"])("chiffre sur pastille %s lisible", (lv) => {
    expect(ratio(tok("on-level"), tok(`lv-${lv}`))).toBeGreaterThanOrEqual(4.5);
  });
  it("texte du bouton principal lisible sur le cyan", () => {
    expect(ratio(tok("accent-ink"), tok("accent"))).toBeGreaterThanOrEqual(4.5);
  });
  it("les trois niveaux restent distincts entre eux", () => {
    const l = ["surveillance", "alerte", "critique"].map((x) => tok(`lv-${x}`));
    expect(new Set(l).size).toBe(3);
  });
});
