import { HashRouter, Route, Routes } from "react-router-dom";
import { Banner } from "./components/Banner";
import { useDataState } from "./data/context";
import { Home } from "./pages/Home";
import { SupplierPage } from "./pages/SupplierPage";

export function App() {
  const state = useDataState();
  return (
    <>
      <Banner meta={state.status === "loading" ? null : state.meta} />
      {state.status === "loading" && <p className="page muted" role="status">Chargement…</p>}
      {state.status === "engine" && (
        <main className="page" role="status">
          <h1>Radar de risque supply chain</h1>
          <p className="muted">Chargement du moteur de requêtes (DuckDB-WASM, environ 8 Mo la première fois) puis des données…</p>
        </main>
      )}
      {state.status === "error" && (
        <main className="page" role="alert">
          <h1>Données inutilisables</h1>
          <p>{state.message}</p>
          <p className="muted">Le site n'affiche rien plutôt qu'un chiffre douteux. Voir web/CONTRACT.md.</p>
        </main>
      )}
      {state.status === "ready" && (
        <HashRouter>
          <Routes>
            <Route path="/" element={<Home data={state.data} />} />
            <Route path="/fournisseur/:id" element={<SupplierPage data={state.data} />} />
            <Route path="*" element={<main className="page"><h1>Page introuvable</h1></main>} />
          </Routes>
        </HashRouter>
      )}
      <footer className="footer muted">
        Événements : <a href="https://www.gdeltproject.org/" target="_blank" rel="noopener noreferrer">GDELT 2.0</a> ·
        coordonnées des villes et détroits : Wikidata (CC0) · portefeuille : fictif.
      </footer>
    </>
  );
}
