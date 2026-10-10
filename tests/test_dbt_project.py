"""Le projet dbt compile (modèles, macros, sources, seeds, tests) sans toucher au lac de données."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_dbt_parse_succeeds_and_finds_every_model():
    dbt = Path(sys.executable).parent / "dbt"
    result = subprocess.run(
        [str(dbt), "parse", "--profiles-dir", ".", "--project-dir", ".", "--no-partial-parse"],
        cwd=ROOT / "transform",
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]
    manifest = (ROOT / "transform/target/manifest.json").read_text(encoding="utf-8")
    for model in ("supplier_scores", "supplier_events", "chokepoint_scores", "supplier_routes",
                  "supplier_scores_daily", "stg_events", "int_supplier_event_links"):  # fmt: skip
        assert f"model.scrr.{model}" in manifest, model
