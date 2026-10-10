// Racines CAMEO (codebook GDELT/CAMEO) et classes Quad. Libellés français.
// Les codes sont ceux des données ; seul le libellé est une traduction.
const ROOT: Record<string, string> = {
  "01": "Déclaration publique", "02": "Appel", "03": "Intention de coopérer",
  "04": "Consultation", "05": "Coopération diplomatique", "06": "Coopération matérielle",
  "07": "Aide", "08": "Concession", "09": "Enquête", "10": "Exigence",
  "11": "Désapprobation", "12": "Rejet", "13": "Menace", "14": "Protestation",
  "15": "Démonstration de force", "16": "Réduction des relations", "17": "Coercition",
  "18": "Agression", "19": "Combat", "20": "Violence de masse",
};
const QUAD: Record<number, string> = {
  1: "Coopération verbale", 2: "Coopération matérielle", 3: "Conflit verbal", 4: "Conflit matériel",
};

export function cameoRootLabel(root: string | null): string | null {
  if (!root) return null;
  return ROOT[root.padStart(2, "0")] ?? null;
}

export function quadLabel(q: number | null): string | null {
  return q === null ? null : (QUAD[q] ?? null);
}
