/**
 * Illustrations SVG originales, dessinées pour ce projet : formes géométriques simples,
 * aucune image, aucun logo, aucune police ni source externe. Décoratives (aria-hidden).
 */

export function RadarMark({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 32 32" aria-hidden="true" focusable="false">
      <circle cx="16" cy="16" r="13" fill="none" stroke="currentColor" strokeWidth="1.6" />
      <circle cx="16" cy="16" r="7" fill="none" stroke="currentColor" strokeWidth="1.2" opacity=".6" />
      <path d="M16 16 L26 8" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
      <circle cx="22" cy="21" r="2" fill="currentColor" />
    </svg>
  );
}

/**
 * Balayage de radar de la page d'accueil. Les points sont décoratifs : ils ne représentent aucune donnée.
 * Le faisceau est un dégradé conique en CSS (rotation fiable, coupée par prefers-reduced-motion).
 */
export function RadarSweep() {
  return (
    <div className="radar" aria-hidden="true">
      <svg viewBox="-200 -200 400 400" focusable="false">
        <defs>
          <radialGradient id="radarBg" r="1">
            <stop offset="0" stopColor="#134676" />
            <stop offset="1" stopColor="#07192f" />
          </radialGradient>
        </defs>
        <circle r="196" fill="url(#radarBg)" stroke="var(--accent)" strokeOpacity=".55" strokeWidth="2" />
        <g fill="none" stroke="var(--accent)" strokeOpacity=".3" strokeWidth="1">
          <circle r="65" /><circle r="130" />
          <path d="M-196 0H196M0-196V196" />
          <path d="M-138-138L138 138M138-138L-138 138" strokeOpacity=".15" />
        </g>
      </svg>
      <div className="radar__beam" />
      <svg viewBox="-200 -200 400 400" focusable="false" className="radar__blips">
        <g fill="var(--accent)">
          <circle className="radar__blip" cx="80" cy="-70" r="5" />
          <circle className="radar__blip radar__blip--2" cx="-100" cy="50" r="5" />
          <circle className="radar__blip radar__blip--3" cx="35" cy="115" r="5" />
          <circle className="radar__blip radar__blip--4" cx="-45" cy="-125" r="5" />
        </g>
      </svg>
    </div>
  );
}

export function Waves({ className = "" }: { className?: string }) {
  const wave = "M0 40 Q 60 10 120 40 T 240 40 T 360 40 T 480 40 T 600 40 T 720 40 T 840 40 T 960 40 T 1080 40 T 1200 40 T 1320 40 V80 H0 Z";
  return (
    <svg className={`waves ${className}`} viewBox="0 0 960 80" preserveAspectRatio="none" aria-hidden="true" focusable="false">
      <path className="waves__a" d={wave} />
      <path className="waves__b" d={wave} />
    </svg>
  );
}

const BOX = ["#2a77c9", "#1b5ea8", "#3a8fe0", "#164b87", "#4aa3ec", "#1f6cb8"];

export function ContainerShip({ className = "" }: { className?: string }) {
  const stacks = [0, 1, 2, 3, 4, 5, 6, 7];
  return (
    <svg className={`ship ${className}`} viewBox="0 0 360 120" aria-hidden="true" focusable="false">
      {stacks.map((i) =>
        [0, 1, 2].slice(0, i % 3 === 0 ? 3 : i % 3 === 1 ? 2 : 3).map((j) => (
          <rect key={`${i}-${j}`} x={30 + i * 26} y={62 - j * 15} width="24" height="14" rx="1.5" fill={BOX[(i + j * 2) % BOX.length]} />
        )),
      )}
      <rect x="262" y="26" width="42" height="50" rx="2" fill="#dbe9f7" />
      <rect x="268" y="32" width="30" height="8" fill="#0b2a4d" />
      <rect x="272" y="16" width="22" height="10" rx="1.5" fill="#b9d3ee" />
      <path d="M8 76 H348 L322 108 H36 Z" fill="#0b1f3a" stroke="var(--accent)" strokeWidth="1.5" />
      <path d="M20 90 H336" stroke="var(--accent)" strokeOpacity=".6" strokeWidth="1.5" />
    </svg>
  );
}

export function PortCranes({ className = "" }: { className?: string }) {
  const crane = (x: number, k: number) => (
    <g transform={`translate(${x} 0) scale(${k})`} key={x}>
      <path d="M0 140 L14 40 M44 140 L30 40" stroke="#6fb5f0" strokeWidth="4" strokeLinecap="round" />
      <path d="M6 100 H38 M10 70 H34" stroke="#6fb5f0" strokeWidth="2.5" />
      <rect x="-6" y="30" width="56" height="10" rx="2" fill="#2a77c9" />
      <path d="M-70 36 L110 36" stroke="#6fb5f0" strokeWidth="5" strokeLinecap="round" />
      <path d="M-70 36 L22 8 L110 36" stroke="#6fb5f0" strokeWidth="2.5" fill="none" />
      <path d="M-50 38 V70" stroke="#9cc9f2" strokeWidth="1.5" />
      <rect x="-60" y="70" width="20" height="12" fill="#4aa3ec" />
    </g>
  );
  return (
    <svg className={`port ${className}`} viewBox="0 0 420 160" aria-hidden="true" focusable="false">
      {crane(70, 1)}
      {crane(220, 0.85)}
      {crane(340, 0.7)}
      <rect x="0" y="140" width="420" height="20" fill="#0b2a4d" />
      {[0, 1, 2, 3, 4, 5, 6].map((i) => (
        <rect key={i} x={10 + i * 58} y="126" width="40" height="14" fill={BOX[i % BOX.length]} />
      ))}
    </svg>
  );
}

/** Étape 1 : actualité mondiale (globe + lignes d'articles). */
export function IconNews() {
  return (
    <svg viewBox="0 0 120 90" className="icon" aria-hidden="true" focusable="false">
      <g fill="none" stroke="var(--accent)" strokeWidth="2">
        <circle cx="44" cy="45" r="30" />
        <ellipse cx="44" cy="45" rx="13" ry="30" />
        <path d="M14 45H74M19 28H69M19 62H69" strokeOpacity=".7" />
      </g>
      <g fill="#7fb6ea">
        <rect x="82" y="20" width="30" height="5" rx="2.5" />
        <rect x="82" y="32" width="24" height="5" rx="2.5" opacity=".7" />
        <rect x="82" y="44" width="30" height="5" rx="2.5" />
        <rect x="82" y="56" width="20" height="5" rx="2.5" opacity=".7" />
      </g>
      <circle className="icon__ping" cx="56" cy="33" r="4" fill="var(--accent)" />
    </svg>
  );
}

/** Étape 2 : lakehouse (couches empilées). */
export function IconLake() {
  return (
    <svg viewBox="0 0 120 90" className="icon" aria-hidden="true" focusable="false">
      <g stroke="var(--accent)" strokeWidth="2" strokeLinejoin="round">
        <path d="M60 14 L100 30 L60 46 L20 30 Z" fill="#17497f" />
        <path d="M20 42 L60 58 L100 42" fill="none" strokeOpacity=".85" />
        <path d="M20 54 L60 70 L100 54" fill="none" strokeOpacity=".55" />
        <path d="M20 66 L60 82 L100 66" fill="none" strokeOpacity=".35" />
      </g>
      <path d="M60 22 V40" stroke="#dbe9f7" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

/** Étape 3 : score comparé à la normale (courbe, bande normale et pic). */
export function IconScore() {
  return (
    <svg viewBox="0 0 120 90" className="icon" aria-hidden="true" focusable="false">
      <rect x="10" y="46" width="100" height="16" rx="3" fill="#17497f" />
      <path d="M10 70 H110" stroke="#7fb6ea" strokeOpacity=".5" />
      <path d="M10 56 L28 54 L46 58 L64 52 L78 56 L92 20 L110 36" fill="none" stroke="var(--accent)" strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
      <circle cx="92" cy="20" r="5" fill="var(--accent)" />
    </svg>
  );
}
