"""Phase 4 tests: demo scripts, hygiene files, README polish.

All tests are fast-lane (no containers, no network). The demo script's
--dry-run path is invoked via subprocess to confirm it exits 0 and is
importable in the CI environment.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_license_exists() -> None:
    assert (REPO_ROOT / "LICENSE").exists(), "LICENSE file missing at repo root"


def test_security_md_exists() -> None:
    assert (REPO_ROOT / "SECURITY.md").exists(), "SECURITY.md missing at repo root"


def test_adr_0006_exists() -> None:
    assert (REPO_ROOT / "docs" / "adr" / "0006-demo-and-results.md").exists(), "ADR-0006 missing"


def test_demo_script_exists() -> None:
    assert (REPO_ROOT / "scripts" / "demo.py").exists(), "scripts/demo.py missing"


def test_measure_results_script_exists() -> None:
    assert (REPO_ROOT / "scripts" / "measure_results.py").exists(), (
        "scripts/measure_results.py missing"
    )


def test_demo_script_dry_run() -> None:
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "demo.py"), "--dry-run"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"demo.py --dry-run exited {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "DRY RUN" in result.stdout, "dry-run output missing DRY RUN marker"


def test_measure_results_dry_run() -> None:
    import json

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "measure_results.py"), "--dry-run"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"measure_results.py --dry-run exited {result.returncode}\nstderr: {result.stderr}"
    )
    data = json.loads(result.stdout)
    assert "throughput_events_per_sec" in data
    assert "mv_correctness_diverged_windows" in data


def test_dependabot_covers_pip() -> None:
    dependabot = (REPO_ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    assert "pip" in dependabot, ".github/dependabot.yml does not cover pip ecosystem"


def test_readme_has_results_section() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "## Results" in readme, "README.md missing Results section"
    assert "50 events/sec" in readme, "README.md results section missing throughput number"
    assert "140 windows" in readme, "README.md results section missing window count"


def test_readme_has_quickstart() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "Quickstart" in readme or "Quick start" in readme, "README.md missing Quickstart section"
    assert "docker compose up" in readme, "README.md Quickstart missing docker compose command"


def test_readme_has_hardest_decision() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "watermark" in readme.lower(), "README.md missing watermark design decision writeup"
    assert "late" in readme.lower(), "README.md missing late-event tradeoff discussion"


def test_readme_has_ci_badge() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "actions/workflows/ci.yml/badge.svg" in readme, "README.md missing CI badge"


def test_makefile_has_demo_target() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "demo:" in makefile, "Makefile missing 'demo' target"
