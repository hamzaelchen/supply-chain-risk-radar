import type { RiskLevel } from "../data/types";

export function levelStyleVar(id: string | null): string {
  return id ? `var(--lv-${id}, var(--lv-unknown))` : "var(--lv-unknown)";
}

export function LevelBadge({ id, levels, score }: { id: string | null; levels: RiskLevel[]; score?: number | null }) {
  const label = levels.find((l) => l.id === id)?.label_fr ?? "n/d";
  return (
    <span className="badge" style={{ ["--c" as string]: levelStyleVar(id) }}>
      {score !== undefined && score !== null && <b>{Math.round(score)}</b>}
      {label}
    </span>
  );
}
