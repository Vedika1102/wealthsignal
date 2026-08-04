import hashlib
import json
from pathlib import Path

import pytest

from wealthsignal_pipeline.cloud_bootstrap import validate_cloud1_contract


FROZEN_COHORT_HASH = "23617b83308e9b073212f9eb493e57921877eacc887f2fcdd923cf3b9ebfc3ff"


def _write_contract(tmp_path: Path) -> tuple[Path, Path, Path]:
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    packages = []
    for index in range(30):
        package = f"package-{index:02d}"
        filename = f"{package}_form13f.zip"
        content = package.encode()
        (source_dir / filename).write_bytes(content)
        packages.append(
            {
                "package": package,
                "source_url": f"https://www.sec.gov/files/{filename}",
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "package_count": 30,
                "prospective_2026q3_included": False,
                "packages": packages,
            }
        ),
        encoding="utf-8",
    )
    cohort_path = tmp_path / "cohort.json"
    repository_cohort = json.loads(
        Path("docs/ai-governance/forecast-protocol-v2-manager-cohort.json").read_text(encoding="utf-8")
    )
    cohort_path.write_text(json.dumps(repository_cohort), encoding="utf-8")
    return source_dir, manifest_path, cohort_path


def test_cloud1_contract_accepts_exact_frozen_inputs(tmp_path: Path) -> None:
    source_dir, manifest, cohort = _write_contract(tmp_path)

    result = validate_cloud1_contract(
        source_dir, source_manifest_path=manifest, cohort_path=cohort
    )

    assert result["package_count"] == 30
    assert result["cohort_count"] == 50
    assert result["cohort_sha256"] == FROZEN_COHORT_HASH
    assert result["prospective_source_count"] == 0


def test_cloud1_contract_rejects_unexpected_prospective_source(tmp_path: Path) -> None:
    source_dir, manifest, cohort = _write_contract(tmp_path)
    (source_dir / "2026q3_form13f.zip").write_bytes(b"prohibited")

    with pytest.raises(ValueError, match="unexpected=.*2026q3"):
        validate_cloud1_contract(
            source_dir, source_manifest_path=manifest, cohort_path=cohort
        )
