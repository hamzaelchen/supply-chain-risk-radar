import { useId, useMemo, useState } from "react";
import { useNarrow, useReducedMotion } from "../../lib/hooks";
import { BRANDS, tileFor } from "./brands";
import { TALL, WIDE, pathLength, type Badge, type NodeDef } from "./architectureLayout";

const TILE = 24;

function textWidth(label: string) {
  return Math.round(label.length * 6.8 + 16);
}

/** Logo (tuile + chemin simple-icons inchangé) ou nom en texte dans un badge. */
function BadgeShape({ badge, x, y }: { badge: Badge; x: number; y: number }) {
  if ("icon" in badge) {
    const b = BRANDS[badge.icon];
    return (
      <g transform={`translate(${x} ${y})`}>
        <title>{b.title}</title>
        <rect width={TILE} height={TILE} rx={6} fill={tileFor(b.hex)} />
        <svg x={4} y={4} width={TILE - 8} height={TILE - 8} viewBox="0 0 24 24" aria-hidden="true">
          <path d={b.path} fill={`#${b.hex}`} />
        </svg>
      </g>
    );
  }
  const w = textWidth(badge.text);
  return (
    <g transform={`translate(${x} ${y})`}>
      <rect width={w} height={TILE} rx={6} className="arch__pill" />
      <text x={w / 2} y={16} textAnchor="middle" className="arch__pill-text">{badge.text}</text>
    </g>
  );
}

function badgesWidth(badges: Badge[]) {
  return badges.reduce((acc, b) => acc + ("icon" in b ? TILE : textWidth(b.text)) + 6, 0) - 6;
}

function NodeShape({ n, state, onEnter, onLeave, onToggle }: {
  n: NodeDef;
  state: "idle" | "hover" | "pinned";
  onEnter: () => void;
  onLeave: () => void;
  onToggle: () => void;
}) {
  // Un seul nom en texte : un badge « texte » qui répète le nom d'un bloc compact est omis.
  const showBadges = !(n.compact && n.badges.every((b) => "text" in b));
  let bx = n.compact ? 8 : 12;
  return (
    <g
      className={`arch__node${n.compact ? " is-compact" : ""} is-${state}`}
      tabIndex={0}
      role="button"
      aria-pressed={state === "pinned"}
      aria-label={`${n.name}${n.sub ? `, ${n.sub}` : ""}. ${n.tip}`}
      onPointerEnter={onEnter}
      onPointerLeave={onLeave}
      onFocus={onEnter}
      onBlur={onLeave}
      onClick={onToggle}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onToggle();
        }
      }}
    >
      <rect x={n.x} y={n.y} width={n.w} height={n.h} rx={10} className="arch__box" />
      {showBadges &&
        n.badges.map((b, i) => {
          const x = n.x + bx;
          bx += ("icon" in b ? TILE : textWidth(b.text)) + 6;
          return <BadgeShape key={i} badge={b} x={x} y={n.compact ? n.y + (n.h - TILE) / 2 : n.y + 12} />;
        })}
      {n.compact ? (
        <text x={n.x + 8 + (showBadges ? badgesWidth(n.badges) + 8 : 6)} y={n.y + n.h / 2 + 5} className="arch__name">
          {n.name}
        </text>
      ) : (
        <>
          <text x={n.x + 12} y={n.y + n.h - 26} className="arch__name">{n.name}</text>
          {n.sub && <text x={n.x + 12} y={n.y + n.h - 9} className="arch__sub">{n.sub}</text>}
        </>
      )}
    </g>
  );
}

/**
 * Schéma de l'architecture. Données dans architectureLayout.ts (une disposition large, une verticale).
 * Particules : un point par flèche ou deux, animés en SMIL, absents si prefers-reduced-motion.
 */
export function Architecture() {
  const uid = useId().replace(/:/g, "");
  const tall = useNarrow(900);
  const reduced = useReducedMotion();
  const layout = tall ? TALL : WIDE;
  const [hover, setHover] = useState<string | null>(null);
  const [pinned, setPinned] = useState<string | null>(null);
  const active = pinned ?? hover;
  const activeNode = layout.nodes.find((n) => n.id === active) ?? null;
  const lengths = useMemo(() => new Map(layout.edges.map((e) => [e.id, pathLength(e.d)])), [layout]);

  const bubbleBelow = activeNode ? activeNode.y < layout.vh * 0.55 : true;
  const bubbleStyle = activeNode
    ? ({
        ["--cx" as string]: `${((activeNode.x + activeNode.w / 2) / layout.vw) * 100}%`,
        ["--ty" as string]: `${(((bubbleBelow ? activeNode.y + activeNode.h : activeNode.y) / layout.vh) * 100).toFixed(2)}%`,
      } as React.CSSProperties)
    : undefined;

  return (
    <figure className={`arch${tall ? " arch--tall" : ""}`}>
      <div className="arch__stage" onKeyDown={(e) => e.key === "Escape" && setPinned(null)}>
        <svg
          viewBox={`0 0 ${layout.vw} ${layout.vh}`}
          role="group"
          aria-label="Architecture du projet : sources, lakehouse, démo, orchestration et CI/CD"
        >
          <defs>
            <marker id={`${uid}-a`} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0 0L10 5L0 10z" className="arch__arrow" />
            </marker>
            <marker id={`${uid}-b`} viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0 0L10 5L0 10z" className="arch__arrow is-on" />
            </marker>
          </defs>

          {layout.frames.map((f) => (
            <g key={f.id}>
              <rect x={f.x} y={f.y} width={f.w} height={f.h} rx={16} className={`arch__frame${f.dashed ? " is-dashed" : ""}`} />
              <text x={f.x + 16} y={f.y + 22} className="arch__frame-label">{f.label}</text>
            </g>
          ))}

          {layout.edges.map((e) => {
            const on = active === e.from || active === e.to;
            const len = lengths.get(e.id) ?? 100;
            const dur = Math.max(1.6, len / 70);
            return (
              <g key={e.id}>
                <path
                  d={e.d}
                  className={`arch__edge${on ? " is-on" : ""}`}
                  markerEnd={`url(#${uid}-${on ? "b" : "a"})`}
                />
                {!reduced &&
                  [0, 1].map((i) => (
                    <circle key={i} r={3.2} className="arch__particle">
                      <animateMotion dur={`${dur}s`} begin={`-${(dur / 2) * i}s`} repeatCount="indefinite" path={e.d} />
                    </circle>
                  ))}
              </g>
            );
          })}

          {layout.nodes.map((n) => (
            <NodeShape
              key={n.id}
              n={n}
              state={pinned === n.id ? "pinned" : hover === n.id ? "hover" : "idle"}
              onEnter={() => setHover(n.id)}
              onLeave={() => setHover((h) => (h === n.id ? null : h))}
              onToggle={() => setPinned((p) => (p === n.id ? null : n.id))}
            />
          ))}
        </svg>

        {activeNode && (
          <div
            className={`arch__bubble ${bubbleBelow ? "is-below" : "is-above"}`}
            style={bubbleStyle}
            role="tooltip"
          >
            <strong>{activeNode.name}</strong>
            <span>{activeNode.tip}</span>
          </div>
        )}
      </div>
      <figcaption className="arch__caption">
        Survolez ou touchez un bloc pour lire son rôle. Les points montrent le sens du flux ; le gold est une base DuckDB
        exportée en Parquet, pas une table Iceberg.
      </figcaption>
    </figure>
  );
}
