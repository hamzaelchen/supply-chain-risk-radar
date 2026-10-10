import { useEffect, useState } from "react";
import type { Meta } from "../data/types";
import { ageBetween, formatUtc } from "../lib/format";

export function Banner({ meta }: { meta: Meta | null }) {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 60_000);
    return () => clearInterval(t);
  }, []);

  return (
    <header className="banner" role="banner">
      <p className="banner__scope">
        <strong>Entreprise démo</strong>, données fictives pour le portefeuille, événements réels GDELT
      </p>
      {meta && (
        <p className="banner__fresh">
          Mise à jour : <time dateTime={meta.generated_at}>{formatUtc(meta.generated_at)}</time>
          <span className="sep" aria-hidden="true">·</span>
          dernier fichier GDELT téléchargé : {formatUtc(meta.latest_fetched_at)} (il y a{" "}
          {ageBetween(meta.latest_fetched_at, now)})
        </p>
      )}
      {meta?.is_sample && (
        <p className="banner__sample" role="note">
          <span className="tag">EXEMPLE</span> {meta.sample_notice}
        </p>
      )}
    </header>
  );
}
