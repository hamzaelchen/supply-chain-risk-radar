/** Icônes SVG originales de la section « Le problème » : décoratives, dessinées pour ce projet. */

const stroke = { fill: "none", stroke: "var(--accent)", strokeWidth: 2.2, strokeLinecap: "round", strokeLinejoin: "round" } as const;

/** Trop d'information : une pile de feuilles. */
export function IconStack() {
  return (
    <svg viewBox="0 0 64 64" className="pitem__icon" aria-hidden="true" focusable="false">
      <g {...stroke}>
        <rect x="14" y="8" width="34" height="42" rx="4" />
        <path d="M20 58h32a4 4 0 0 0 4-4V18" strokeOpacity=".6" />
        <path d="M21 18h20M21 26h20M21 34h12" />
      </g>
      <circle cx="46" cy="42" r="7" fill="#17497f" stroke="var(--accent)" strokeWidth="2.2" />
      <path d="M51 47l6 6" {...stroke} />
    </svg>
  );
}

/** Trop tard : un cadran dont l'aiguille a dépassé le seuil. */
export function IconLate() {
  return (
    <svg viewBox="0 0 64 64" className="pitem__icon" aria-hidden="true" focusable="false">
      <circle cx="32" cy="34" r="22" fill="#0d2a50" stroke="var(--accent)" strokeWidth="2.2" />
      <path d="M32 34V19M32 34l11 7" {...stroke} />
      <path d="M26 7h12" {...stroke} />
      <path d="M32 7v5" {...stroke} />
      <path d="M52 16l5-5" {...stroke} strokeOpacity=".7" />
    </svg>
  );
}

/** Pas le bon lieu : un repère sur une route maritime. */
export function IconWhere() {
  return (
    <svg viewBox="0 0 64 64" className="pitem__icon" aria-hidden="true" focusable="false">
      <path d="M6 50c10-8 18 2 26-4s14-10 26-4" {...stroke} strokeOpacity=".6" strokeDasharray="4 5" />
      <path d="M32 8c-8 0-14 6-14 13 0 10 14 23 14 23s14-13 14-23c0-7-6-13-14-13z" fill="#17497f" stroke="var(--accent)" strokeWidth="2.2" />
      <circle cx="32" cy="21" r="5" fill="var(--accent)" />
    </svg>
  );
}
