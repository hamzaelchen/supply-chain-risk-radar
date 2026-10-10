import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { EventItem } from "../components/EventItem";
import { Legend } from "../components/Legend";
import { LevelBadge } from "../components/LevelBadge";
import { WorldMap, type MapSelection } from "../components/WorldMap";
import type { Dataset, SupplierEvent } from "../data/types";
import { formatNumber } from "../lib/format";
import { groupByCity } from "../lib/geo";

const EVENTS_PER_ALERT = 3;

export function Home({ data }: { data: Dataset }) {
  const { meta, suppliers, chokepoints, routes } = data;
  const [selection, setSelection] = useState<MapSelection>(null);
  const alertIds = useMemo(() => new Set(meta.risk_levels.filter((l) => l.alert).map((l) => l.id)), [meta]);
  const alerts = useMemo(() => suppliers.filter((s) => alertIds.has(s.risk_level)), [suppliers, alertIds]);

  const [events, setEvents] = useState<Map<string, SupplierEvent[]>>(new Map());
  useEffect(() => {
    let alive = true;
    data.eventsFor(alerts.map((a) => a.supplier_id)).then((m) => alive && setEvents(m));
    return () => {
      alive = false;
    };
  }, [data, alerts]);

  const alertShare = alerts.reduce((acc, s) => acc + s.purchase_share_pct, 0);
  const cities = useMemo(() => groupByCity(suppliers), [suppliers]);

  return (
    <main className="home">
      <section className="kpis" aria-label="Chiffres clés">
        <div><b>{alerts.length}</b><span>fournisseurs en alerte sur {suppliers.length}</span></div>
        <div><b>{formatNumber(alertShare, 1)} %</b><span>des achats concernés</span></div>
        <div><b>{alerts.filter((s) => s.sole_source).length}</b><span>fournisseurs uniques en alerte</span></div>
        <div><b>{formatNumber(meta.events_total)}</b><span>événements GDELT lus ({meta.gdelt_batches} lot(s) de 15 min)</span></div>
      </section>

      <div className="home__grid">
        <section className="card" aria-label="Carte">
          <WorldMap suppliers={suppliers} chokepoints={chokepoints} levels={meta.risk_levels}
            selection={selection} onSelect={setSelection} />
          <Legend levels={meta.risk_levels} />
          <SelectionPanel data={data} selection={selection} cityGroups={cities} routes={routes} />
        </section>

        <section className="card" aria-label="Alertes">
          <h2>Alertes <span className="muted">({alerts.length})</span></h2>
          {alerts.length === 0 && <p className="muted">Aucun fournisseur au niveau d'alerte.</p>}
          <ol className="alerts">
            {alerts.map((s) => {
              const evs = events.get(s.supplier_id) ?? [];
              return (
                <li key={s.supplier_id} className="alert">
                  <div className="alert__head">
                    <Link to={`/fournisseur/${s.supplier_id}`}>{s.name}</Link>
                    <LevelBadge id={s.risk_level} levels={meta.risk_levels} score={s.risk_score} />
                  </div>
                  <p className="muted">
                    {s.category_label} · {s.city}, {s.country}
                    {s.sole_source && " · fournisseur unique"} · {formatNumber(s.purchase_share_pct, 2)} % des achats
                  </p>
                  {s.top_driver && <p>{s.top_driver}</p>}
                  <details>
                    <summary>{evs.length} événement(s) explicatif(s)</summary>
                    <ul className="events">
                      {evs.slice(0, EVENTS_PER_ALERT).map((e) => (
                        <EventItem key={`${e.global_event_id}-${e.link_type}-${e.chokepoint_id}`} e={e} chokepoints={chokepoints} />
                      ))}
                    </ul>
                    {evs.length > EVENTS_PER_ALERT && (
                      <Link to={`/fournisseur/${s.supplier_id}`}>Voir les {evs.length} événements</Link>
                    )}
                  </details>
                </li>
              );
            })}
          </ol>
        </section>
      </div>
    </main>
  );
}

function SelectionPanel({ data, selection, cityGroups, routes }: {
  data: Dataset;
  selection: MapSelection;
  cityGroups: ReturnType<typeof groupByCity>;
  routes: Dataset["routes"];
}) {
  const { meta, chokepoints, suppliers } = data;
  if (!selection) return <p className="muted panel">Cliquez sur une ville ou un détroit.</p>;

  if (selection.kind === "city") {
    const g = cityGroups.find((c) => c.key === selection.key);
    if (!g) return null;
    return (
      <div className="panel">
        <h3>{g.city}, {g.country}</h3>
        <p className="muted">Position : centre-ville, pas le port.</p>
        <ul className="plain">
          {[...g.suppliers].sort((a, b) => b.risk_score - a.risk_score).map((s) => (
            <li key={s.supplier_id}>
              <Link to={`/fournisseur/${s.supplier_id}`}>{s.name}</Link>{" "}
              <LevelBadge id={s.risk_level} levels={meta.risk_levels} score={s.risk_score} />
            </li>
          ))}
        </ul>
      </div>
    );
  }

  const c = chokepoints.find((x) => x.chokepoint_id === selection.id);
  if (!c) return null;
  const exposed = suppliers.filter((s) => routes.some((r) => r.supplier_id === s.supplier_id && r.chokepoint_id === c.chokepoint_id));
  return (
    <div className="panel">
      <h3>{c.name_fr} <LevelBadge id={c.risk_level} levels={meta.risk_levels} score={c.risk_score} /></h3>
      <p>
        {formatNumber(c.events_recent)} événement(s) récent(s) dans un rayon de {formatNumber(c.radius_km)} km
        <span className="muted"> (rayon = choix de conception, point Wikidata)</span>
      </p>
      <p>
        {c.suppliers_exposed} fournisseur(s) exposé(s) par leur route, soit {formatNumber(c.purchase_share_exposed_pct, 1)} % des achats
        {c.sole_source_exposed > 0 && ` dont ${c.sole_source_exposed} fournisseur(s) unique(s)`}.
      </p>
      <details>
        <summary>Fournisseurs concernés ({exposed.length})</summary>
        <ul className="plain">
          {exposed.map((s) => (
            <li key={s.supplier_id}><Link to={`/fournisseur/${s.supplier_id}`}>{s.name}</Link>{" "}
              <LevelBadge id={s.risk_level} levels={meta.risk_levels} score={s.risk_score} /></li>
          ))}
        </ul>
      </details>
    </div>
  );
}
