import { WorldMap } from "../WorldMap";
import { Legend } from "../Legend";
import type { Preview } from "../../data/home";
import type { Hub } from "../../data/snapshots";

/**
 * Aperçu de la carte pour l'accueil : les mêmes routes animées vers le siège que dans le radar, mais
 * figées (pas de zoom ni de glisser) et lues dans preview.json, sans DuckDB-WASM.
 * Chargé à la demande (d3-geo et le fond de carte n'alourdissent pas le premier affichage).
 */
export default function DemoMap({ preview, hub }: { preview: Preview; hub: Hub | null }) {
  return (
    <div className="demo__map">
      <WorldMap
        static
        suppliers={preview.suppliers}
        chokepoints={preview.chokepoints}
        levels={preview.risk_levels}
        selection={null}
        onSelect={() => undefined}
        routes={preview.routes}
        hub={hub}
        showRadius
      />
      <Legend levels={preview.risk_levels} />
    </div>
  );
}
