import { describe, expect, it } from "vitest";
import { cameoRootLabel, quadLabel } from "./cameo";

describe("cameo", () => {
  it("garde les zéros de tête", () => {
    expect(cameoRootLabel("14")).toBe("Protestation");
    expect(cameoRootLabel("1")).toBe("Déclaration publique");
  });
  it("renvoie null plutôt qu'un libellé inventé", () => {
    expect(cameoRootLabel("99")).toBeNull();
    expect(cameoRootLabel(null)).toBeNull();
    expect(quadLabel(7)).toBeNull();
  });
});
