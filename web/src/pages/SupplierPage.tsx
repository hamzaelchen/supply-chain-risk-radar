import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { EventItem } from "../components/EventItem";
import { Legend } from "../components/Legend";
import { LevelBadge, levelStyleVar } from "../components/LevelBadge";
import { WorldMap } from "../components/WorldMap";
import type { Dataset, SupplierEvent } from "../data/types";
import { formatNumber } from "../lib/format";

export function SupplierPage({ data }: { data: Dataset }) {
  const { id = "" } = useParams();
  const { meta, suppliers, chokepoints, routes } = data;
  const s = suppliers.find((x) => x.supplier_id === id);
  const [events, setEvents] = useState<SupplierEvent[] | null>(null);

  useEffect(() => {
    let alive = true;
    setEvents(null);
    data.eventsFor([id]).then((m) => alive && setEvents(m.get(id) ?? []));
    return () => {
      alive = false;
    };
  }, [data, id]);

  if (!s) {
    return (
      <main className="page">
        <p><Link to="/">← Retour</Link></p>
        <h1>Fournisseur introuvable</h1>
        <p>Aucun fournisseur « {id} » dans cet export.</p>
      </main>
    );
  }

  const route = routes.filter((r) => r.supplier_id === id).sort((a, b) => a.position - b.position);
  const routeNames = route.map((r) => chokepoints.find((c) => c.chokepoint_id === r.chokepoint_id));
  const cityEvents = (events ?? []).filter((e) => e.link_type === "city");
  const cpEvents = (events ?? []).filter((e) => e.link_type === "chokepoint");

  return (
    <main className="page">
      <p><Link to="/">← Carte et alertes</Link></p>
      <header className="supplier__head">
        <div>
          <h1>{s.name}</h1>
          <p className="muted">{s.supplier_id} · {s.label}</p>
        </div>
        <div className="score" style={{ ["--c" as string]: levelStyleVar(s.risk_level) }}>
          <b>{Math.round(s.risk_score)}</b><span>/ 100</span>
          <LevelBadge id={s.risk_level} levels={meta.risk_levels} />
        </div>
      </header>

      <div className="detail__grid">
        <section className="card">
          <h2>Fiche</h2>
          <dl className="facts">
            <dt>Catégorie</dt><dd>{s.category_label}</dd>
            <dt>Ville</dt><dd>{s.city}, {s.country} <span className="muted">(centre-ville)</span></dd>
            <dt>Région d'origine</dt><dd>{s.region}</dd>
            <dt>Part des achats</dt><dd>{formatNumber(s.purchase_share_pct, 2)} %</dd>
            <dt>Fournisseur unique</dt><dd>{s.sole_source ? "Oui" : "Non"}</dd>
            <dt>Route</dt>
            <dd>
              {route.length === 0 ? "Aucun détroit" : routeNames.map((c, i) => (
                <span key={route[i].chokepoint_id}>
                  {i > 0 && " → "}{c?.name_fr ?? route[i].chokepoint_id}
                </span>
              ))}
              <p className="muted">Route fictive et simplifiée, vers Casablanca.</p>
            </dd>
          </dl>
          <h2>Composantes du score</h2>
          {meta.components.map((c) => (
            <Bar key={c.id} label={c.label_fr} hint={c.description_fr} value={s[c.id]} />
          ))}
          <h2>Origine des événements</h2>
          <Bar label={`Ville (${s.events_local} événement(s))`} value={s.score_local} />
          <Bar label={`Route, hors événements déjà comptés près de la ville (${s.events_route})`} value={s.score_route} />
          <p className="muted">Version du score : {meta.score_version}.{meta.is_sample && " Formule d'illustration, pas le score v1."}</p>
          {s.top_driver && <p>{s.top_driver}</p>}
        </section>

        <section className="card">
          <WorldMap suppliers={[s]} chokepoints={chokepoints.filter((c) => route.some((r) => r.chokepoint_id === c.chokepoint_id))}
            levels={meta.risk_levels} selection={null} onSelect={() => {}} routeIds={route.map((r) => r.chokepoint_id)} />
          <Legend levels={meta.risk_levels} />
        </section>
      </div>

      <section className="card">
        <h2>Événements explicatifs {events && <span className="muted">({events.length})</span>}</h2>
        {events === null && <p className="muted">Chargement…</p>}
        {events?.length === 0 && <p className="muted">Aucun événement récent n'explique un score pour ce fournisseur.</p>}
        {cityEvents.length > 0 && (<>
          <h3>Près de la ville ({cityEvents.length})</h3>
          <ul className="events">{cityEvents.map((e) => <EventItem key={`c${e.global_event_id}`} e={e} chokepoints={chokepoints} />)}</ul>
        </>)}
        {cpEvents.length > 0 && (<>
          <h3>Près des détroits de la route ({cpEvents.length})</h3>
          <ul className="events">{cpEvents.map((e) => <EventItem key={`d${e.global_event_id}${e.chokepoint_id}`} e={e} chokepoints={chokepoints} />)}</ul>
        </>)}
        {events && events.length > 0 && (
          <p className="muted">GDELT n'a pas de titre d'article dans la table des événements : seul le lien source est affiché.</p>
        )}
      </section>
    </main>
  );
}

function Bar({ label, value, hint }: { label: string; value: number; hint?: string }) {
  return (
    <div className="bar">
      <div className="bar__label" title={hint}><span>{label}</span><b>{formatNumber(value, 1)}</b></div>
      {hint && <div className="muted bar__hint">{hint}</div>}
      <div className="bar__track"><div className="bar__fill" style={{ width: `${Math.min(100, Math.max(0, value))}%` }} /></div>
    </div>
  );
}
