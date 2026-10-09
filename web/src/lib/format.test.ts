import { describe, expect, it } from "vitest";
import { ageBetween, formatNumber, sourceDomain } from "./format";

describe("format", () => {
  it("affiche n/d pour une valeur absente, jamais 0", () => {
    expect(formatNumber(null)).toBe("n/d");
    expect(formatNumber(undefined)).toBe("n/d");
  });
  it("extrait le domaine source", () => {
    expect(sourceDomain("https://www.example.com/a/b")).toBe("example.com");
    expect(sourceDomain("pas une url")).toBeNull();
    expect(sourceDomain(null)).toBeNull();
  });
  it("calcule l'âge", () => {
    expect(ageBetween("2026-10-08T17:11:00Z", new Date("2026-10-08T20:23:00Z"))).toBe("3 h 12 min");
    expect(ageBetween("2026-10-08T17:11:00Z", new Date("2026-10-08T17:40:00Z"))).toBe("29 min");
    expect(ageBetween("2026-10-08T17:11:00Z", new Date("2026-10-08T16:00:00Z"))).toBe("n/d");
  });
});
