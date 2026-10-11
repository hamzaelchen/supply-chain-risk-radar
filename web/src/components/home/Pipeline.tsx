import { useRef, useState } from "react";
import type { Metric, PipelineStats } from "../../data/home";
import { formatDay, formatNumber } from "../../lib/format";

interface Stage {
  id: string;
  name: string;
  does: string;
  guarantee: { title: string; text: string };
  /** Identifiants de mesures de pipeline_stats.json à montrer, dans cet ordre. */
  show: string[];
}

/** Le texte explique ; tous les chiffres viennent de pipeline_stats.json (aucun nombre écrit ici). */
const STAGES: Stage[] = [
  {
    id: "bronze",
    name: "Bronze",
    does: "Reçoit les lignes GDELT telles qu'elles sont publiées, avec leur lignage : fichier source, lot de 15 minutes, heure réelle du téléchargement. Rien n'est corrigé ni rejeté ici.",
    guarantee: {
      title: "Pas de ligne écrite deux fois",
      text: "Le flux reprend à son checkpoint : un lot déjà commité n'est pas réécrit, même après un arrêt brutal. Un manifeste dit, lot par lot, ce qui est publié, absent chez GDELT ou en échec.",
    },
    show: ["rows", "batches", "days_loaded", "first_day", "last_day", "batches_missing", "batches_failed"],
  },
  {
    id: "silver",
    name: "Silver",
    does: "Donne un type à chaque colonne, vérifie les plages (latitude, ton, codes) et dédoublonne sur l'identifiant d'événement : le premier arrivé gagne.",
    guarantee: {
      title: "Idempotence et quarantaine",
      text: "Un passage interrompu rejoue la même fenêtre sans rien insérer deux fois. Une ligne invalide va en quarantaine avec sa raison : elle n'est ni corrigée ni perdue, et le compte bronze = silver + quarantaine se vérifie.",
    },
    show: ["rows", "quarantine", "duplicates", "conflicting_duplicates", "unaccounted", "silver_runs", "silver_runs_failed", "silver_duration"],
  },
  {
    id: "proximity",
    name: "Proximité",
    does: "Rattache chaque événement localisé à une ville de fournisseur (150 km) et aux détroits (rayon propre à chacun), avec un préfiltre exact sur la latitude.",
    guarantee: {
      title: "Incrémental, rejouable, vérifié",
      text: "Le job ne lit que les événements nouveaux et n'écrit qu'en ajout. Il a été comparé à un calcul indépendant en Python pur sur un vrai cycle GDELT, et à quatre autres méthodes de jointure.",
    },
    show: ["supplier_pairs", "chokepoint_pairs", "proximity_runs", "proximity_runs_failed", "proximity_duration"],
  },
  {
    id: "gold",
    name: "Gold",
    does: "dbt compare chaque ville et chaque détroit à sa propre normale des 28 derniers jours, puis combine événements locaux, ton, route et criticité en un score de 0 à 100.",
    guarantee: {
      title: "Testé avant d'être publié",
      text: "Les tests dbt portent sur les clés, les bornes du score et la cohérence des tables. Un test en échec arrête la chaîne : rien n'est exporté.",
    },
    show: ["suppliers", "chokepoints", "dbt_tests_passed", "dbt_tests_total", "dbt_duration"],
  },
  {
    id: "export",
    name: "Export",
    does: "Écrit, par instantané, les cinq fichiers du contrat du site (un meta.json et quatre Parquet) et valide leurs règles avant de rendre la main.",
    guarantee: {
      title: "Un contrat, pas un chiffre douteux",
      text: "Colonnes, bornes, absence de double comptage : un export invalide échoue au lieu d'afficher un chiffre faux. Le site, de son côté, refuse un fichier qui ne respecte pas le contrat.",
    },
    show: ["snapshots", "files", "bytes"],
  },
];

function duration(s: number): string {
  if (s < 60) return `${formatNumber(s, 1)} s`;
  const m = Math.floor(s / 60);
  const r = Math.round(s - m * 60);
  if (m < 60) return `${m} min ${String(r).padStart(2, "0")} s`;
  const total = Math.round(s / 60);
  return `${Math.floor(total / 60)} h ${String(total % 60).padStart(2, "0")} min`;
}

function bytes(n: number): string {
  if (n < 1024 * 1024) return `${formatNumber(n / 1024, 0)} Ko`;
  return `${formatNumber(n / (1024 * 1024), 1)} Mo`;
}

export function formatMetric(m: Metric): string {
  if (typeof m.value === "string") return m.kind === "date" ? formatDay(m.value) : m.value;
  switch (m.kind) {
    case "seconds":
      return duration(m.value);
    case "bytes":
      return bytes(m.value);
    default:
      return formatNumber(m.value);
  }
}

function MetricTile({ m }: { m: Metric | undefined }) {
  if (!m) return null;
  return (
    <div className="metric">
      <dt>{m.label_fr}</dt>
      <dd>{formatMetric(m)}</dd>
    </div>
  );
}

export function Pipeline({ stats, error }: { stats: PipelineStats | null; error: string | null }) {
  const [current, setCurrent] = useState(0);
  const tabs = useRef<(HTMLButtonElement | null)[]>([]);
  const stage = STAGES[current];
  const found = stats?.stages.find((s) => s.id === stage.id)?.metrics ?? [];
  const byId = new Map(found.map((m) => [m.id, m]));

  const move = (to: number) => {
    const i = (to + STAGES.length) % STAGES.length;
    setCurrent(i);
    tabs.current[i]?.focus();
  };

  return (
    <div className="pipeline">
      <div className="pipeline__rail" role="tablist" aria-label="Étapes du pipeline">
        {STAGES.map((s, i) => (
          <button
            key={s.id}
            ref={(el) => {
              tabs.current[i] = el;
            }}
            type="button"
            role="tab"
            id={`stage-${s.id}`}
            aria-selected={i === current}
            aria-controls="stage-panel"
            tabIndex={i === current ? 0 : -1}
            className={`pipeline__step${i === current ? " is-on" : ""}${i < current ? " is-done" : ""}`}
            onClick={() => setCurrent(i)}
            onKeyDown={(e) => {
              if (e.key === "ArrowRight") move(i + 1);
              else if (e.key === "ArrowLeft") move(i - 1);
              else if (e.key === "Home") move(0);
              else if (e.key === "End") move(STAGES.length - 1);
              else return;
              e.preventDefault();
            }}
          >
            <span className="pipeline__dot" aria-hidden="true">{i + 1}</span>
            <span className="pipeline__label">{s.name}</span>
          </button>
        ))}
      </div>

      <div id="stage-panel" role="tabpanel" aria-labelledby={`stage-${stage.id}`} className="pipeline__panel" key={stage.id}>
        <div className="pipeline__text">
          <h3>{stage.name}</h3>
          <p>{stage.does}</p>
          <div className="guarantee">
            <p className="guarantee__title">Garantie : {stage.guarantee.title}</p>
            <p>{stage.guarantee.text}</p>
          </div>
        </div>
        <div className="pipeline__numbers">
          <h4>Les vrais chiffres</h4>
          {found.length > 0 ? (
            <dl className="metrics">
              {stage.show.map((id) => (
                <MetricTile key={id} m={byId.get(id)} />
              ))}
            </dl>
          ) : (
            <p className="muted">
              n/d{error ? ` : ${error}` : stats ? " : cette étape n'a pas de mesure dans le fichier." : "…"}
            </p>
          )}
          {stats && (
            <p className="pipeline__src">
              Lus dans le manifeste d'ingestion, <code>ops.pipeline_runs</code> et les tables du lac, le{" "}
              {formatDay(stats.generated_at)}.
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
