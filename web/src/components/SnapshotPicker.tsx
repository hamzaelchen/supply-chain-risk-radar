import type { Snapshot, SnapshotIndex } from "../data/snapshots";
import { formatDay } from "../lib/format";

/** Choix de l'instantané + texte factuel : ce qu'on voit, jamais pourquoi. */
export function SnapshotPicker({
  index,
  current,
  onSelect,
}: {
  index: SnapshotIndex;
  current: Snapshot;
  onSelect: (id: string) => void;
}) {
  return (
    <section className="snapshot" aria-label="Instantané affiché">
      <div className="snapshot__choices" role="radiogroup" aria-label="Instantané">
        {index.snapshots.map((s) => (
          <button
            key={s.id}
            type="button"
            role="radio"
            aria-checked={s.id === current.id}
            className={s.id === current.id ? "is-on" : ""}
            onClick={() => onSelect(s.id)}
          >
            {s.label_fr}
          </button>
        ))}
      </div>
      <p className="snapshot__text">
        {current.kind === "episode" && (
          <span className="tag tag--episode">Instantané historique, au {formatDay(current.as_of)}</span>
        )}{" "}
        {current.description_fr}
      </p>
    </section>
  );
}
