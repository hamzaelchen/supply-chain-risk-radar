import { lazy, Suspense } from "react";
import { Route, Routes } from "react-router-dom";
import { Landing } from "./pages/Landing";

// Le radar (carte, d3, DuckDB-WASM) est un chunk à part : la page d'accueil ne le télécharge pas.
const RadarLayout = lazy(() => import("./pages/RadarLayout"));
const Home = lazy(() => import("./pages/Home"));
const SupplierPage = lazy(() => import("./pages/SupplierPage"));

export function App() {
  return (
    <Suspense fallback={<p className="page muted" role="status">Chargement…</p>}>
      <Routes>
        <Route path="/" element={<Landing />} />
        <Route element={<RadarLayout />}>
          <Route path="/radar" element={<Home />} />
          <Route path="/fournisseur/:id" element={<SupplierPage />} />
        </Route>
        <Route path="*" element={<main className="page"><h1>Page introuvable</h1><p><a href="#/">Retour à l'accueil</a></p></main>} />
      </Routes>
    </Suspense>
  );
}
