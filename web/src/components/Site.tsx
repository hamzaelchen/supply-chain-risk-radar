import { Link } from "react-router-dom";
import { RadarMark } from "./Illustrations";

export const REPO_URL = "https://github.com/hamzaelchen/supply-chain-risk-radar";

export function SiteHeader({ radar = false }: { radar?: boolean }) {
  return (
    <div className="topbar">
      <Link to="/" className="brand" aria-label="Supply Chain Risk Radar, accueil">
        <RadarMark className="brand__mark" />
        <span>Supply Chain Risk Radar</span>
      </Link>
      <nav aria-label="Navigation">
        {radar ? <Link to="/">Accueil</Link> : <Link to="/radar">Radar</Link>}
        <a href={REPO_URL} target="_blank" rel="noopener noreferrer">Code</a>
      </nav>
    </div>
  );
}

export function SiteFooter() {
  return (
    <footer className="footer">
      <p>
        Événements : <a href="https://www.gdeltproject.org/" target="_blank" rel="noopener noreferrer">GDELT 2.0</a> ·
        coordonnées des villes et des détroits :{" "}
        <a href="https://www.wikidata.org/" target="_blank" rel="noopener noreferrer">Wikidata</a> (CC0) ·
        portefeuille fournisseurs : fictif.
      </p>
      <p>
        Projet de <strong>Hamza El Alami</strong> ·{" "}
        <a href={REPO_URL} target="_blank" rel="noopener noreferrer">code source sur GitHub</a>
      </p>
    </footer>
  );
}
