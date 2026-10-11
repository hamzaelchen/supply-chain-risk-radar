import { geoGraticule10, geoNaturalEarth1, geoPath } from "d3-geo";
import { useMemo, useRef, useState } from "react";
import { feature, mesh } from "topojson-client";
import type { GeometryCollection, Topology } from "topojson-specification";
import world from "world-atlas/countries-110m.json";
import type { Hub } from "../data/snapshots";
import type { ChokepointScore, MapSupplier, RiskLevel, SupplierRoute } from "../data/types";
import { buildRoutes, groupByCity, radiusCircle, type CityGroup } from "../lib/geo";
import { levelStyleVar } from "./LevelBadge";

const W = 960;
const H = 500;

export type MapSelection = { kind: "city"; key: string } | { kind: "chokepoint"; id: string } | null;

interface Props {
  suppliers: MapSupplier[];
  chokepoints: ChokepointScore[];
  levels: RiskLevel[];
  selection: MapSelection;
  onSelect: (s: MapSelection) => void;
  /** Aperçu de l'accueil : pas de zoom ni de glisser, pas de contrôles, cadrage sur le monde entier. */
  static?: boolean;
  /** Détroits à mettre en avant (route du fournisseur affiché). */
  routeIds?: string[];
  showRadius?: boolean;
  /** Étapes des routes et port d'arrivée : sans eux, les routes ne sont pas tracées. */
  routes?: SupplierRoute[];
  hub?: Hub | null;
}

const topo = world as unknown as Topology;
const countries = topo.objects.countries as GeometryCollection;

export function WorldMap({ suppliers, chokepoints, levels, selection, onSelect, routeIds, showRadius = true, routes, hub, static: isStatic = false }: Props) {
  const projection = useMemo(() => geoNaturalEarth1().fitSize([W, H], { type: "Sphere" }), []);
  const path = useMemo(() => geoPath(projection), [projection]);
  const land = useMemo(() => path(feature(topo, countries) as never) ?? "", [path]);
  const borders = useMemo(() => path(mesh(topo, countries, (a, b) => a !== b)) ?? "", [path]);
  const sphere = useMemo(() => path({ type: "Sphere" }) ?? "", [path]);
  const graticule = useMemo(() => path(geoGraticule10()) ?? "", [path]);
  const cities = useMemo(() => groupByCity(suppliers), [suppliers]);
  const alertLevels = useMemo(() => new Set(levels.filter((l) => l.alert).map((l) => l.id)), [levels]);
  const mapRoutes = useMemo(
    () => (routes && hub ? buildRoutes(suppliers, routes, chokepoints, hub, alertLevels) : []),
    [suppliers, routes, chokepoints, hub, alertLevels],
  );
  const routePaths = useMemo(
    () => mapRoutes.map((r) => ({ r, d: path({ type: "LineString", coordinates: r.coords }) ?? "" })),
    [mapRoutes, path],
  );
  const hubXY = hub ? projection([hub.lon, hub.lat]) : null;

  // En mobile le monde entier est minuscule : on démarre zoomé, centré sur le port d'arrivée (le bouton ⌂ y revient).
  const initialView = useMemo(() => {
    const narrow = typeof window !== "undefined" && window.matchMedia?.("(max-width: 640px)").matches;
    const hubPt = hub ? projection([hub.lon, hub.lat]) : null;
    if (isStatic || !narrow || !hubPt) return { k: 1, x: 0, y: 0 };
    const k = 1.9;
    return { k, x: Math.min(0, Math.max(W - W * k, W / 2 - hubPt[0] * k)), y: Math.min(0, Math.max(H - H * k, H / 2 - hubPt[1] * k)) };
  }, [hub, projection, isStatic]);
  const [view, setView] = useState(initialView);
  const drag = useRef<{ px: number; py: number; x: number; y: number } | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  const zoomBy = (f: number) =>
    setView((v) => {
      const k = Math.min(12, Math.max(1, v.k * f));
      // On garde le centre de la carte fixe.
      const cx = W / 2, cy = H / 2;
      return { k, x: cx - ((cx - v.x) / v.k) * k, y: cy - ((cy - v.y) / v.k) * k };
    });

  const clamp = (v: { k: number; x: number; y: number }) => ({
    k: v.k,
    x: Math.min(0, Math.max(W - W * v.k, v.x)),
    y: Math.min(0, Math.max(H - H * v.k, v.y)),
  });

  const onPointerDown = (e: React.PointerEvent) => {
    if (isStatic || (e.target as Element).closest("[data-hit]")) return;
    drag.current = { px: e.clientX, py: e.clientY, x: view.x, y: view.y };
    (e.currentTarget as Element).setPointerCapture(e.pointerId);
  };
  const onPointerMove = (e: React.PointerEvent) => {
    const d = drag.current;
    const el = svgRef.current;
    if (!d || !el) return;
    const scale = W / el.getBoundingClientRect().width;
    setView((v) => clamp({ k: v.k, x: d.x + (e.clientX - d.px) * scale, y: d.y + (e.clientY - d.py) * scale }));
  };
  const onPointerUp = () => {
    drag.current = null;
  };

  const r = (px: number) => px / Math.sqrt(view.k); // les marqueurs grossissent moins vite que la carte
  const routeSet = new Set(routeIds ?? []);
  const select = (s: MapSelection) => (e: React.SyntheticEvent) => {
    e.stopPropagation();
    onSelect(s);
  };

  return (
    <div className="map">
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        role="img"
        aria-label="Carte du monde : fournisseurs par ville et détroits, colorés par niveau de risque"
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerUp}
        onClick={() => onSelect(null)}
      >
        <g transform={`translate(${view.x} ${view.y}) scale(${view.k})`}>
          <path d={sphere} className="map__sea" />
          <path d={graticule} className="map__graticule" style={{ strokeWidth: 0.5 / view.k }} />
          <path d={land} className="map__land" />
          <path d={borders} className="map__borders" style={{ strokeWidth: 0.5 / view.k }} />

          {routePaths.map(({ r, d }) => {
            const related =
              !selection ||
              (selection.kind === "city" ? selection.key === r.cityKey : r.chokepointIds.includes(selection.id));
            return (
              <path
                key={`${r.cityKey}|${r.chokepointIds.join(",")}`}
                d={d}
                vectorEffect="non-scaling-stroke"
                className={`map__route${r.alert ? " is-alert" : ""}${related ? "" : " is-dim"}`}
                style={r.alert ? { stroke: levelStyleVar(r.level) } : undefined}
              />
            );
          })}

          {chokepoints.map((c) => {
            const [x, y] = projection([c.lon, c.lat]) ?? [0, 0];
            const circle = showRadius ? path(radiusCircle(c.lon, c.lat, c.radius_km)) : null;
            const sel = selection?.kind === "chokepoint" && selection.id === c.chokepoint_id;
            const inRoute = routeSet.has(c.chokepoint_id);
            const col = levelStyleVar(c.risk_level);
            return (
              <g key={c.chokepoint_id} data-hit>
                {circle && (
                  <path d={circle} className="map__radius" style={{ stroke: col, fill: col, strokeWidth: 1 / view.k }} />
                )}
                <rect
                  x={x - r(6)} y={y - r(6)} width={r(12)} height={r(12)}
                  transform={`rotate(45 ${x} ${y})`}
                  className={`map__cp${sel || inRoute ? " is-on" : ""}`}
                  style={{ fill: col, strokeWidth: (sel || inRoute ? 2.5 : 1.2) / view.k }}
                  tabIndex={0}
                  role="button"
                  aria-label={`Détroit : ${c.name_fr}, ${levels.find((l) => l.id === c.risk_level)?.label_fr ?? "niveau n/d"}`}
                  onClick={select({ kind: "chokepoint", id: c.chokepoint_id })}
                  onKeyDown={(e) => e.key === "Enter" && onSelect({ kind: "chokepoint", id: c.chokepoint_id })}
                >
                  <title>{c.name_fr}</title>
                </rect>
              </g>
            );
          })}

          {cities.map((g) => (
            <CityDot key={g.key} g={g} projection={projection} r={r} k={view.k}
              selected={selection?.kind === "city" && selection.key === g.key}
              levels={levels} onSelect={onSelect} />
          ))}
          {hubXY && hub && (
            <g className="map__hub" data-hit>
              <circle cx={hubXY[0]} cy={hubXY[1]} r={r(9)} className="map__hub-ring" />
              <rect x={hubXY[0] - r(4.5)} y={hubXY[1] - r(4.5)} width={r(9)} height={r(9)} className="map__hub-core" />
              <text x={hubXY[0] + r(13)} y={hubXY[1] + r(5)} fontSize={r(16)} className="map__hub-label">
                {hub.name} (siège)
              </text>
            </g>
          )}
        </g>
      </svg>
      {!isStatic && (
        <div className="map__controls">
          <button type="button" onClick={() => zoomBy(1.6)} aria-label="Zoom avant">+</button>
          <button type="button" onClick={() => zoomBy(1 / 1.6)} aria-label="Zoom arrière">−</button>
          <button type="button" onClick={() => setView(initialView)} aria-label="Réinitialiser la vue">⌂</button>
        </div>
      )}
    </div>
  );
}

function CityDot({ g, projection, r, k, selected, levels, onSelect }: {
  g: CityGroup;
  projection: ReturnType<typeof geoNaturalEarth1>;
  r: (px: number) => number;
  k: number;
  selected: boolean;
  levels: RiskLevel[];
  onSelect: (s: MapSelection) => void;
}) {
  const [x, y] = projection([g.lon, g.lat]) ?? [0, 0];
  const radius = r(3.5 + Math.min(4, g.suppliers.length));
  const label = levels.find((l) => l.id === g.worst.risk_level)?.label_fr ?? "n/d";
  const alert = levels.find((l) => l.id === g.worst.risk_level)?.alert ?? false;
  return (
    <g data-hit>
      {alert && <circle cx={x} cy={y} r={radius} className="map__pulse" style={{ stroke: levelStyleVar(g.worst.risk_level) }} />}
      <circle
      cx={x} cy={y} r={radius}
      className={`map__city${selected ? " is-on" : ""}`}
      style={{ fill: levelStyleVar(g.worst.risk_level), strokeWidth: (selected ? 2.5 : 1) / k }}
      tabIndex={0}
      role="button"
      aria-label={`${g.city} : ${g.suppliers.length} fournisseur(s), pire niveau ${label}`}
      onClick={(e) => {
        e.stopPropagation();
        onSelect({ kind: "city", key: g.key });
      }}
      onKeyDown={(e) => e.key === "Enter" && onSelect({ kind: "city", key: g.key })}
    >
      <title>{`${g.city} : ${g.suppliers.length} fournisseur(s)`}</title>
      </circle>
    </g>
  );
}
