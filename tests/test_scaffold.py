from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXPECTED_DIRS = [
    "ingestion", "streaming", "lakehouse", "transform", "orchestration",
    "llm", "api", "web", "tests", "docs", "data/raw",
]


def test_expected_directories_exist():
    missing = [d for d in EXPECTED_DIRS if not (ROOT / d).is_dir()]
    assert not missing, f"dossiers manquants : {missing}"


def test_raw_data_is_gitignored():
    gitignore = (ROOT / ".gitignore").read_text()
    assert "data/raw/*" in gitignore
