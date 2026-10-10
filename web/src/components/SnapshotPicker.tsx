import type { Snapshot, SnapshotIndex } from "../data/snapshots";
import { formatDay } from "../lib/format";

/** Choix de l'instantané + texte factuel : ce qu'on voit, jamais pourquoi. Onglets en grand écran, sélecteur compact en mobile. */
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
      <label className="snapshot__select">
        <span className="sr-only">Instantané</span>
        <select value={current.id} onChange={(e) => onSelect(e.target.value)}>
          {index.snapshots.map((s) => (
            <option key={s.id} value={s.id}>{s.label_fr}</option>
          ))}
        </select>
      </label>
      <details className="snapshot__more">
        <summary>
          {current.kind === "episode" ? `Instantané historique, au ${formatDay(current.as_of)}` : "Ce que montre cet instantané"}
        </summary>
        <p>{current.description_fr}</p>
      </details>
      <p className="snapshot__text">
        {current.kind === "episode" && (
          <span className="tag tag--episode">Instantané historique, au {formatDay(current.as_of)}</span>
        )}{" "}
        {current.description_fr}
      </p>
    </section>
  );
}
