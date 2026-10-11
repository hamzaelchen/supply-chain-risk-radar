import { lazy, Suspense } from "react";
import { Link } from "react-router-dom";
import { Architecture } from "../components/home/Architecture";
import { Pipeline } from "../components/home/Pipeline";
import { IconLate, IconStack, IconWhere } from "../components/home/ProblemIcons";
import { Reveal } from "../components/home/Reveal";
import { ContainerShip, PortCranes, RadarSweep, Waves } from "../components/Illustrations";
import { SiteFooter, SiteHeader, scrollToSection } from "../components/Site";
import { useIndexState } from "../data/context";
import { usePipelineStats, usePreview, type Preview } from "../data/home";
import type { Hub, SnapshotIndex } from "../data/snapshots";
import { formatDay, formatNumber } from "../lib/format";
import { useInView } from "../lib/hooks";

// Fond de carte et d3-geo : chargés quand la section démo approche de l'écran, jamais DuckDB-WASM.
const DemoMap = lazy(() => import("../components/home/DemoMap"));

/** Précharge le code du radar (JavaScript du site seulement : DuckDB-WASM n'est demandé qu'une fois dans le radar). */
const warmRadar = () => {
  void import("./RadarLayout");
  void import("./Home");
};

const PROBLEM = [
  {
    Icon: IconStack,
    title: "Trop d'information",
    text: "GDELT recense des milliers d'événements par heure. Personne ne les lit tous pour y trouver ce qui touche un fournisseur.",
  },
  {
    Icon: IconLate,
    title: "Trop tard",
    text: "Une crise arrive souvent au responsable achats par la presse, quand les commandes sont déjà en mer.",
  },
  {
    Icon: IconWhere,
    title: "Pas le bon lieu",
    text: "Savoir que « quelque chose se passe » n'aide pas : il faut le relier à la ville du fournisseur et au détroit de sa route.",
  },
];

const DECISIONS = [
  {
    topic: "Stockage",
    choice: "Tables Apache Iceberg derrière un catalogue REST.",
    rejected: "Catalogue Hadoop, Hive Metastore, JDBC direct.",
    why: "Le protocole REST est neutre : Spark, DuckDB, dbt et PyIceberg parlent au même catalogue. Le catalogue Hadoop n'offre pas de commit atomique sur S3.",
  },
  {
    topic: "Lecture incrémentale",
    choice: "Un curseur sur la date d'ingestion, rangé dans la table silver.",
    rejected: "La lecture par snapshots Iceberg.",
    why: "Le curseur survit à l'expiration des snapshots et à la compaction, se rejoue à la main par une simple date, et se lit tel quel depuis DuckDB.",
  },
  {
    topic: "Jointure géographique",
    choice: "Produit croisé avec un préfiltre exact sur la latitude.",
    rejected: "H3, et des grilles de cellules.",
    why: "Les cinq méthodes comparées donnent les mêmes paires sur des événements réels ; le préfiltre est le plus rapide et n'ajoute aucune dépendance. H3 était plus lent et irrégulier.",
  },
  {
    topic: "Score",
    choice: "Une anomalie par rapport à la normale de chaque ville et de chaque détroit (28 jours).",
    rejected: "Le nombre brut d'événements, ou un seuil fixe.",
    why: "Le brut mesure la couverture médiatique d'une ville, pas son risque : une grande ville aurait toujours l'air plus exposée qu'une petite.",
  },
  {
    topic: "Sources",
    choice: "GDELT comme seule source d'événements.",
    rejected: "Le trafic maritime d'IMF PortWatch.",
    why: "Les conditions d'utilisation de l'IMF interdisent le téléchargement automatisé en masse sans permission, ce que ferait une planification. Le « trafic maritime » a donc disparu du score.",
  },
];

const LIMITS = [
  { title: "Portefeuille fictif", text: "Les 100 fournisseurs de l'Entreprise démo sont inventés (noms, parts d'achats, routes). Seuls les événements sont réels." },
  { title: "Classification par règles", text: "Les événements sont retenus par des règles sur les codes CAMEO, sans LLM : une règle ne voit pas le contexte, des faux positifs sont possibles." },
  { title: "Cause non établie", text: "Le site montre ce que les articles recensent près d'un lieu. Il n'établit pas pourquoi cela arrive." },
  { title: "Données rafraîchies à la main", text: "Le planning toutes les 15 minutes existe mais n'est pas activé : les données sont régénérées à la demande, et le jour en cours n'est pas noté." },
];

function Kpis({ preview, index }: { preview: Preview | null; index: SnapshotIndex | null }) {
  const k = preview?.kpis;
  const f = k?.focus_chokepoint;
  const s = index?.stats;
  const items = [
    {
      v: k ? `${k.alerts} sur ${k.suppliers}` : "…",
      l: "fournisseurs en alerte ou critiques",
      n: k ? `${formatNumber(k.alert_purchase_share_pct, 1)} % des achats, dont ${k.sole_source_alerts} fournisseurs uniques` : "",
    },
    {
      v: f ? formatNumber(f.events_recent) : "…",
      l: "événements autour d'Ormuz",
      n: f ? `dans un rayon de ${formatNumber(f.radius_km)} km, ${f.suppliers_exposed} fournisseurs exposés` : "",
    },
    {
      v: s ? formatNumber(s.events_total) : "…",
      l: "événements GDELT traités",
      n: s ? `${formatNumber(s.gdelt_batches)} fichiers de 15 minutes` : "",
    },
    {
      v: s ? formatNumber(s.days_covered) : "…",
      l: "jours couverts",
      n: s ? `du ${formatDay(s.first_day)} au ${formatDay(s.last_day)}` : "",
    },
  ];
  return (
    <dl className="kpi-grid" aria-label="Chiffres clés">
      {items.map((it) => (
        <div key={it.l} className="kpi">
          <dt>{it.l}</dt>
          <dd>{it.v}</dd>
          <small>{it.n}</small>
        </div>
      ))}
    </dl>
  );
}

export function Landing() {
  const state = useIndexState();
  const index = state.status === "ready" ? state.index : null;
  const episode = index?.snapshots.find((s) => s.kind === "episode");
  const live = index?.snapshots.find((s) => s.kind === "live");
  const radarTo = episode ? { pathname: "/radar", search: `?snapshot=${episode.id}` } : "/radar";

  const [demoRef, demoNear] = useInView<HTMLElement>("300px 0px 300px 0px");
  const preview = usePreview(demoNear);
  const stats = usePipelineStats();
  const hub = (index?.hub ?? null) as Hub | null;

  return (
    <div className="landing">
      <SiteHeader home />
      <header className="hero">
        <div className="hero__inner">
          <div className="hero__text">
            <p className="eyebrow">Projet de data engineering · Entreprise démo</p>
            <h1>Supply Chain Risk Radar</h1>
            <p className="lead">
              Un radar qui lit l'actualité mondiale et signale les fournisseurs dont la ville ou la route maritime
              est touchée par un événement inhabituel.
            </p>
            <div className="cta">
              <Link className="btn btn--primary" to={radarTo} onPointerEnter={warmRadar} onFocus={warmRadar}>
                Voir l'épisode d'Ormuz
              </Link>
              <button type="button" className="btn btn--ghost" onClick={() => scrollToSection("architecture")}>
                Voir l'architecture
              </button>
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
        <Reveal as="section" className="block" aria-labelledby="probleme" id="probleme-section">
          <p className="kicker">1 · Le problème</p>
          <h2 id="probleme">Un responsable achats découvre les crises trop tard</h2>
          <ul className="problem">
            {PROBLEM.map(({ Icon, title, text }, i) => (
              <Reveal as="li" key={title} className="pitem" delay={i * 90}>
                <Icon />
                <h3>{title}</h3>
                <p>{text}</p>
              </Reveal>
            ))}
          </ul>
          <div className="problem__art" aria-hidden="true">
            <PortCranes />
          </div>
        </Reveal>

        <section className="block block--wide" id="demo" aria-labelledby="demo-title" ref={demoRef}>
          <Reveal>
            <p className="kicker">2 · La démo</p>
            <h2 id="demo-title">{episode ? episode.label_fr : "L'épisode d'Ormuz"}</h2>
            <p className="block__intro">
              Chaque trait rejoint Casablanca, le siège de l'Entreprise démo, en passant par les détroits de la route
              du fournisseur. Les routes en alerte sont plus épaisses et colorées.
            </p>
          </Reveal>
          <Reveal className="demo__frame">
            {preview.status === "ready" ? (
              <Suspense fallback={<div className="demo__placeholder" role="status">Chargement de la carte…</div>}>
                <DemoMap preview={preview.value} hub={hub} />
              </Suspense>
            ) : (
              <div className="demo__placeholder" role="status">
                {preview.status === "error" ? `Aperçu indisponible : ${preview.message}` : "Chargement de la carte…"}
              </div>
            )}
          </Reveal>
          <Reveal>
            <Kpis preview={preview.status === "ready" ? preview.value : null} index={index} />
            <p className="demo__note muted">
              {episode ? episode.description_fr : ""}
            </p>
            <div className="cta">
              <Link className="btn btn--primary" to={radarTo} onPointerEnter={warmRadar} onFocus={warmRadar}>
                Ouvrir le radar
              </Link>
              {live && (
                <Link className="btn btn--ghost" to={{ pathname: "/radar", search: `?snapshot=${live.id}` }}>
                  Situation d'aujourd'hui
                </Link>
              )}
            </div>
          </Reveal>
        </section>

        <section className="block block--wide" id="architecture" aria-labelledby="archi-title">
          <Reveal>
            <p className="kicker">3 · L'architecture</p>
            <h2 id="archi-title">Du fichier GDELT au navigateur</h2>
            <p className="block__intro">
              Un lakehouse local complet (Kafka, Spark, Iceberg, dbt) produit des fichiers Parquet ; le site les lit
              directement dans le navigateur, sans serveur.
            </p>
          </Reveal>
          <Reveal>
            <Architecture />
          </Reveal>
        </section>

        <section className="block block--wide" id="pipeline" aria-labelledby="pipe-title">
          <Reveal>
            <p className="kicker">4 · Le pipeline, étape par étape</p>
            <h2 id="pipe-title">Ce que chaque étape garantit, avec ses vrais chiffres</h2>
          </Reveal>
          <Reveal>
            <Pipeline
              stats={stats.status === "ready" ? stats.value : null}
              error={stats.status === "error" ? stats.message : null}
            />
          </Reveal>
        </section>

        <section className="block" id="decisions" aria-labelledby="dec-title">
          <Reveal>
            <p className="kicker">5 · Les décisions techniques</p>
            <h2 id="dec-title">Ce qui a été choisi, ce qui a été écarté, pourquoi</h2>
          </Reveal>
          <ul className="decisions">
            {DECISIONS.map((d, i) => (
              <Reveal as="li" key={d.topic} className="decision" delay={(i % 3) * 80}>
                <p className="decision__topic">{d.topic}</p>
                <dl>
                  <div><dt>Choix</dt><dd>{d.choice}</dd></div>
                  <div><dt>Écarté</dt><dd>{d.rejected}</dd></div>
                  <div><dt>Pourquoi</dt><dd>{d.why}</dd></div>
                </dl>
              </Reveal>
            ))}
          </ul>
        </section>

        <section className="block" id="limites" aria-labelledby="lim-title">
          <Reveal>
            <p className="kicker">6 · Limites honnêtes</p>
            <h2 id="lim-title">Ce que ce projet ne prétend pas faire</h2>
          </Reveal>
          <ul className="limits">
            {LIMITS.map((l, i) => (
              <Reveal as="li" key={l.title} className="limit" delay={(i % 2) * 80}>
                <h3>{l.title}</h3>
                <p>{l.text}</p>
              </Reveal>
            ))}
          </ul>
        </section>
      </main>
      <SiteFooter />
    </div>
  );
}
