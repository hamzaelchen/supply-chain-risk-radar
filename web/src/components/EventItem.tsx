import type { ChokepointScore, SupplierEvent } from "../data/types";
import { cameoRootLabel, quadLabel } from "../lib/cameo";
import { formatDay, formatNumber, sourceDomain } from "../lib/format";

export function EventItem({ e, chokepoints }: { e: SupplierEvent; chokepoints: ChokepointScore[] }) {
  const cp = e.chokepoint_id ? chokepoints.find((c) => c.chokepoint_id === e.chokepoint_id) : null;
  const where = e.link_type === "city" ? "près de la ville du fournisseur" : `près de ${cp?.name_fr ?? e.chokepoint_id}`;
  const kind = cameoRootLabel(e.event_root_code) ?? quadLabel(e.quad_class);
  const domain = sourceDomain(e.source_url);
  return (
    <li className="event">
      <div className="event__head">
        <strong>{e.action_geo_name ?? "Lieu n/d"}</strong>
        <span className="muted">
          {where}, à {formatNumber(e.distance_km)} km
        </span>
      </div>
      <div className="event__meta">
        <span>{formatDay(e.event_date)}</span>
        {kind && <span>{kind}{e.event_root_code ? ` (CAMEO ${e.event_root_code})` : ""}</span>}
        <span>{formatNumber(e.num_mentions)} mention(s)</span>
        <span>{formatNumber(e.num_sources)} source(s)</span>
        {e.avg_tone !== null && <span>ton {formatNumber(e.avg_tone, 1)}</span>}
        {e.goldstein_scale !== null && <span>Goldstein {formatNumber(e.goldstein_scale, 1)}</span>}
      </div>
      {e.event_family_label_fr && (
        <div className="event__rule">
          <span className="tag">Classé par règle CAMEO</span> {e.event_family_label_fr}
        </div>
      )}
      {e.llm_category && (
        <div className="event__llm">
          <span className="tag tag--llm">Classé par LLM</span> {e.llm_category}
          {e.llm_summary && <p>{e.llm_summary}</p>}
        </div>
      )}
      <div className="event__src">
        {e.source_url ? (
          <a href={e.source_url} target="_blank" rel="noopener noreferrer">
            Article source{domain ? ` : ${domain}` : ""}
          </a>
        ) : (
          <span className="muted">Source n/d</span>
        )}
        <span className="muted"> · événement GDELT {e.global_event_id}</span>
      </div>
    </li>
  );
}
