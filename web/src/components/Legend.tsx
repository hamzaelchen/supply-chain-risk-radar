import type { RiskLevel } from "../data/types";
import { levelStyleVar } from "./LevelBadge";

export function Legend({ levels }: { levels: RiskLevel[] }) {
  return (
    <ul className="legend" aria-label="Légende des niveaux de risque">
      {levels.map((l) => (
        <li key={l.id}>
          <span className="dot" style={{ background: levelStyleVar(l.id) }} />
          {l.label_fr} <span className="muted">({l.min}–{l.max})</span>
        </li>
      ))}
      <li className="muted">● ville (taille = nb de fournisseurs) · ◆ détroit (cercle = rayon de surveillance, choix de conception)</li>
    </ul>
  );
}
