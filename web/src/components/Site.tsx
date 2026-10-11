import { Link } from "react-router-dom";
import { RadarMark } from "./Illustrations";

export const REPO_URL = "https://github.com/hamzaelchen/supply-chain-risk-radar";

/**
 * Le routage est par hash (#/radar) : un lien « #architecture » casserait la route.
 * Les sections de l'accueil se joignent donc par défilement, pas par ancre.
 */
export function scrollToSection(id: string) {
  document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
}

const SECTIONS: { id: string; label: string }[] = [
  { id: "demo", label: "Démo" },
  { id: "architecture", label: "Architecture" },
  { id: "pipeline", label: "Pipeline" },
  { id: "decisions", label: "Décisions" },
];

export function SiteHeader({ home = false }: { home?: boolean }) {
  return (
    <div className="topbar">
      <Link to="/" className="brand" aria-label="Supply Chain Risk Radar, accueil">
        <RadarMark className="brand__mark" />
        <span>Supply Chain Risk Radar</span>
      </Link>
      <nav aria-label="Navigation">
        {home ? (
          <>
            {SECTIONS.map((s) => (
              <button key={s.id} type="button" className="nav__section" onClick={() => scrollToSection(s.id)}>
                {s.label}
              </button>
            ))}
            <Link to="/radar">Radar</Link>
          </>
        ) : (
          <Link to="/">Accueil</Link>
        )}
        <a href={REPO_URL} target="_blank" rel="noopener noreferrer">Code</a>
      </nav>
    </div>
  );
}

export function SiteFooter() {
  return (
    <footer className="footer">
      <div className="footer__inner">
        <p>
          <strong>Hamza El Alami</strong> · projet de data engineering ·{" "}
          <a href={REPO_URL} target="_blank" rel="noopener noreferrer">code source sur GitHub</a>
        </p>
        <p>
          Sources : événements{" "}
          <a href="https://www.gdeltproject.org/" target="_blank" rel="noopener noreferrer">GDELT 2.0</a> ·
          coordonnées des villes et des détroits{" "}
          <a href="https://www.wikidata.org/" target="_blank" rel="noopener noreferrer">Wikidata</a> (CC0) ·
          portefeuille fournisseurs : fictif. Logos : paquet{" "}
          <a href="https://simpleicons.org/" target="_blank" rel="noopener noreferrer">simple-icons</a>, marques de
          leurs propriétaires.
        </p>
      </div>
    </footer>
  );
}
