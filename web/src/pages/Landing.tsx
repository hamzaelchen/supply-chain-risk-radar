import { Link } from "react-router-dom";
import { ContainerShip, IconLake, IconNews, IconScore, PortCranes, RadarSweep, Waves } from "../components/Illustrations";
import { SiteFooter, SiteHeader } from "../components/Site";
import { useIndexState } from "../data/context";
import type { SnapshotIndex } from "../data/snapshots";
import { formatDay, formatNumber } from "../lib/format";

/** Précharge le code du radar (JavaScript du site seulement : DuckDB-WASM n'est demandé qu'une fois dans le radar). */
const warmRadar = () => {
  void import("./RadarLayout");
  void import("./Home");
};

function episodeButtonLabel(label: string): string {
  // « Épisode d'Ormuz (2 mars 2026) » -> « Voir l'épisode d'Ormuz (2 mars 2026) » : le texte vient de l'index.
  return `Voir l'${label.charAt(0).toLowerCase()}${label.slice(1)}`;
}

function Figures({ index }: { index: SnapshotIndex | null }) {
  const s = index?.stats;
  const items = [
    { v: s ? formatNumber(s.events_total) : "…", l: "événements GDELT traités", n: s ? `${formatNumber(s.gdelt_batches)} fichiers de 15 minutes` : "" },
    { v: s ? formatNumber(s.days_covered) : "…", l: "jours couverts", n: s ? `du ${formatDay(s.first_day)} au ${formatDay(s.last_day)}` : "" },
    { v: s ? formatNumber(s.suppliers) : "…", l: "fournisseurs suivis", n: "portefeuille fictif" },
    { v: s ? formatNumber(s.chokepoints) : "…", l: "détroits surveillés", n: "canaux, détroits et caps" },
  ];
  return (
    <section className="figures" aria-label="Chiffres clés">
      {items.map((it) => (
        <div key={it.l} className="figure">
          <b>{it.v}</b>
          <span>{it.l}</span>
          <small>{it.n}</small>
        </div>
      ))}
    </section>
  );
}

export function Landing() {
  const state = useIndexState();
  const index = state.status === "ready" ? state.index : null;
  const episode = index?.snapshots.find((s) => s.kind === "episode");
  const live = index?.snapshots.find((s) => s.kind === "live");

  return (
    <div className="landing">
      <SiteHeader />
      <header className="hero">
        <div className="hero__inner">
        <div className="hero__text">
          <p className="eyebrow">Projet de data engineering · Entreprise démo</p>
          <h1>Supply Chain Risk Radar</h1>
          <p className="lead">
            Le radar surveille l'actualité mondiale et signale les fournisseurs dont la route maritime ou la ville
            est exposée à un événement inhabituel.
          </p>
          <div className="cta">
            <Link
              className="btn btn--primary"
              to={episode ? { pathname: "/radar", search: `?snapshot=${episode.id}` } : "/radar"}
              onPointerEnter={warmRadar}
              onFocus={warmRadar}
            >
              {episode ? episodeButtonLabel(episode.label_fr) : "Ouvrir le radar"}
            </Link>
            {live && (
              <Link
                className="btn btn--ghost"
                to={{ pathname: "/radar", search: `?snapshot=${live.id}` }}
                onPointerEnter={warmRadar}
                onFocus={warmRadar}
              >
                Situation d'aujourd'hui
              </Link>
            )}
          </div>
          {state.status === "error" && (
            <p className="notice" role="alert">Les chiffres n'ont pas pu être lus : {state.message}</p>
          )}
        </div>
        <div className="hero__art">
          <RadarSweep />
        </div>
        </div>
        <div className="hero__sea" aria-hidden="true">
          <ContainerShip className="hero__ship" />
          <Waves />
        </div>
      </header>

      <main>
        <Figures index={index} />

        <section className="block block--problem" aria-labelledby="probleme">
          <div>
            <h2 id="probleme">Le problème</h2>
            <p>
              Un responsable achats découvre les crises trop tard : l'information est dans la presse du monde entier,
              mais personne ne peut lire des milliers d'articles par heure et se demander lesquels touchent un
              fournisseur précis ou le détroit par lequel passent ses marchandises.
            </p>
            <p>
              Le radar fait ce tri : il lit les événements recensés par GDELT, les rattache à la ville et à la route
              de chaque fournisseur, et ne remonte que ce qui sort de l'ordinaire pour cet endroit.
            </p>
          </div>
          <PortCranes className="block__art" />
        </section>

        <section className="block" aria-labelledby="comment">
          <h2 id="comment">Comment ça marche</h2>
          <ol className="steps">
            <li className="step">
              <IconNews />
              <h3><span className="step__n">1</span> L'actualité mondiale</h3>
              <p>
                GDELT publie un fichier d'événements tous les quarts d'heure (qui, quoi, où, quel ton). Le projet les
                télécharge tels quels et en garde la source.
              </p>
            </li>
            <li className="step">
              <IconLake />
              <h3><span className="step__n">2</span> Le lakehouse</h3>
              <p>
                Kafka, Spark et Iceberg stockent, typent et dédoublonnent ; dbt calcule les tables finales. Chaque
                chiffre remonte à un fichier téléchargé.
              </p>
            </li>
            <li className="step">
              <IconScore />
              <h3><span className="step__n">3</span> Un score par fournisseur</h3>
              <p>
                Le score, de 0 à 100, compare chaque ville et chaque détroit à sa propre normale des 28 derniers
                jours : une grande ville n'alerte pas pour son bruit habituel.
              </p>
            </li>
          </ol>
        </section>

        <section className="block honesty" aria-labelledby="honnete">
          <h2 id="honnete">Ce qui est réel, ce qui ne l'est pas</h2>
          <ul>
            <li><strong>Portefeuille fictif.</strong> Les 100 fournisseurs de l'« Entreprise démo » sont inventés (noms, parts d'achats, routes).</li>
            <li><strong>Événements réels.</strong> Chaque événement vient d'un fichier GDELT réellement téléchargé.</li>
            <li><strong>Pas de cause établie.</strong> Le site montre ce que les articles recensent près d'un lieu ; il n'établit pas pourquoi cela arrive.</li>
          </ul>
        </section>

        <section className="closing">
          <h2>Voir le radar</h2>
          <div className="cta cta--center">
            <Link className="btn btn--primary" to={episode ? { pathname: "/radar", search: `?snapshot=${episode.id}` } : "/radar"}>
              {episode ? episodeButtonLabel(episode.label_fr) : "Ouvrir le radar"}
            </Link>
            {live && (
              <Link className="btn btn--ghost" to={{ pathname: "/radar", search: `?snapshot=${live.id}` }}>
                Situation d'aujourd'hui
              </Link>
            )}
          </div>
        </section>
      </main>
      <SiteFooter />
    </div>
  );
}
