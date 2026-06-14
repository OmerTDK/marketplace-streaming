"""Phase 0 tests: IP hygiene, schema consistency, SQL structure checks."""

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Terms that must never appear in this public repo (employer IP / internal names).
FORBIDDEN_TERMS = [
    "cloover",
    "bawag",
    "schufa",
    "crif",
    "bubble_sync",
    "eif",
    "viola",
    "credibur",
    "omer@cloover",
]

# Files and directories to skip in IP hygiene scan.
SKIP_DIRS = {".git", ".venv", ".ruff_cache", ".pytest_cache", "__pycache__"}
# Exclude the test file itself — it necessarily contains the forbidden terms as
# string literals in the definition list. The scan targets non-test source files.
SKIP_FILES = {"uv.lock", "test_smoke.py"}


def _repo_text_files() -> list[Path]:
    """Return all text files in the repo, skipping binary and cache dirs."""
    result = []
    for path in REPO_ROOT.rglob("*"):
        if path.is_dir():
            continue
        if any(skip in path.parts for skip in SKIP_DIRS):
            continue
        if path.name in SKIP_FILES:
            continue
        try:
            path.read_text(encoding="utf-8")
            result.append(path)
        except (UnicodeDecodeError, PermissionError):
            pass
    return result


def test_python_version() -> None:
    assert sys.version_info >= (3, 12)


def test_no_employer_ip_in_repo() -> None:
    """No employer-specific terms may appear anywhere in the repo."""
    violations = []
    for path in _repo_text_files():
        text = path.read_text(encoding="utf-8").lower()
        for term in FORBIDDEN_TERMS:
            if term.lower() in text:
                violations.append(f"{path.relative_to(REPO_ROOT)}: contains '{term}'")
    assert not violations, "Employer IP found in repo:\n" + "\n".join(violations)


def test_sql_sources_exist() -> None:
    """Phase 0 requires both SQL DDL skeleton files."""
    assert (REPO_ROOT / "sql" / "01_sources.sql").exists(), "sql/01_sources.sql missing"
    assert (REPO_ROOT / "sql" / "02_mvs.sql").exists(), "sql/02_mvs.sql missing"


def test_adrs_exist() -> None:
    """Phase 0 requires ADR-0001 and ADR-0002."""
    adr_dir = REPO_ROOT / "docs" / "adr"
    assert (adr_dir / "0001-streaming-engine.md").exists(), "ADR-0001 missing"
    assert (adr_dir / "0002-architecture.md").exists(), "ADR-0002 missing"


def test_docker_compose_has_six_services() -> None:
    """docker-compose.yml must declare the six Phase 0 services."""
    compose_file = REPO_ROOT / "docker-compose.yml"
    assert compose_file.exists(), "docker-compose.yml missing"
    text = compose_file.read_text(encoding="utf-8")
    expected_services = [
        "redpanda:",
        "redpanda-init:",
        "risingwave:",
        "clickhouse:",
        "generator:",
        "dagster:",
    ]
    for service in expected_services:
        assert service in text, f"docker-compose.yml missing service: {service}"


def test_sql_sources_declare_watermark() -> None:
    """All four Kafka sources must declare a WATERMARK clause."""
    sources_sql = (REPO_ROOT / "sql" / "01_sources.sql").read_text(encoding="utf-8").upper()
    watermark_count = sources_sql.count("WATERMARK FOR")
    assert watermark_count >= 4, (
        f"Expected at least 4 WATERMARK FOR clauses in 01_sources.sql, found {watermark_count}"
    )


def test_sql_mvs_declare_tumble_or_hop() -> None:
    """Materialized view SQL must use TUMBLE or HOP window functions."""
    mvs_sql = (REPO_ROOT / "sql" / "02_mvs.sql").read_text(encoding="utf-8").upper()
    assert "TUMBLE(" in mvs_sql, "02_mvs.sql missing TUMBLE window function"
    assert "HOP(" in mvs_sql, "02_mvs.sql missing HOP window function"


def test_fault_injection_config_exists() -> None:
    """Default fault injection config must be present and well-formed."""
    import json

    config_path = REPO_ROOT / "shared" / "fault_injection.json"
    assert config_path.exists(), "shared/fault_injection.json missing"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert "active" in config, "fault_injection.json missing 'active' key"
    assert config["active"] is False, "fault_injection.json must default to active=false"


def test_no_co_authored_by_in_commits() -> None:
    """Verify the constraint is documented — not a git log check (CI has no history)."""
    claude_md = (REPO_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    assert "Co-Authored-By" in claude_md, "CLAUDE.md should document the no-Co-Authored-By rule"


def test_clickhouse_final_requirement_documented() -> None:
    """ClickHouse init.sql must document the FINAL requirement."""
    init_sql = (REPO_ROOT / "clickhouse" / "init.sql").read_text(encoding="utf-8").upper()
    assert "FINAL" in init_sql, (
        "clickhouse/init.sql must document the FINAL requirement for ReplacingMergeTree queries"
    )


def test_adr_0001_mentions_risingwave_and_flink() -> None:
    """ADR-0001 must document both RisingWave (chosen) and Flink (rejected)."""
    adr = (REPO_ROOT / "docs" / "adr" / "0001-streaming-engine.md").read_text(encoding="utf-8")
    assert re.search(r"risingwave", adr, re.IGNORECASE), "ADR-0001 missing RisingWave"
    assert re.search(r"flink", adr, re.IGNORECASE), "ADR-0001 missing Flink alternative"
    assert re.search(r"upgrade path", adr, re.IGNORECASE), "ADR-0001 missing upgrade path"
