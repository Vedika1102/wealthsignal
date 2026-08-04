"""Validate the checksum-frozen Protocol V2 Databricks bootstrap contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def validate_cloud1_contract(
    source_dir: str | Path,
    *,
    source_manifest_path: str | Path,
    cohort_path: str | Path,
) -> dict[str, object]:
    """Validate exact source allowlisting, package hashes, and cohort identity."""

    source_root = Path(source_dir)
    manifest = json.loads(Path(source_manifest_path).read_text(encoding="utf-8"))
    cohort = json.loads(Path(cohort_path).read_text(encoding="utf-8"))
    packages = manifest["packages"]
    expected = {f"{row['package']}_form13f.zip": row for row in packages}
    actual = {path.name: path for path in source_root.iterdir() if path.is_file()}

    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    if missing or unexpected:
        raise ValueError(f"source allowlist mismatch: missing={missing}, unexpected={unexpected}")
    if manifest["package_count"] != len(packages) or len(packages) != 30:
        raise ValueError("Protocol V2 requires exactly 30 development packages")
    if manifest.get("prospective_2026q3_included"):
        raise ValueError("prospective 2026q3 source is prohibited")

    verified = []
    for filename, row in sorted(expected.items()):
        path = actual[filename]
        size = path.stat().st_size
        digest = _sha256_file(path)
        if size != row["size_bytes"] or digest != row["sha256"]:
            raise ValueError(f"checksum contract failed for {row['package']}")
        verified.append(row["package"])

    ordered_ciks = cohort["main_ordered_ciks"]
    cohort_digest = hashlib.sha256(_canonical_json(ordered_ciks).encode("utf-8")).hexdigest()
    if cohort_digest != cohort["main_ordered_ciks_sha256"]:
        raise ValueError("manager cohort checksum mismatch")
    if cohort_digest != "23617b83308e9b073212f9eb493e57921877eacc887f2fcdd923cf3b9ebfc3ff":
        raise ValueError("manager cohort does not match the frozen Cloud 1 contract")

    return {
        "package_count": len(verified),
        "verified_packages": verified,
        "cohort_count": len(ordered_ciks),
        "cohort_sha256": cohort_digest,
        "prospective_source_count": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--source-manifest", required=True)
    parser.add_argument("--cohort", required=True)
    args = parser.parse_args()
    result = validate_cloud1_contract(
        args.source_dir,
        source_manifest_path=args.source_manifest,
        cohort_path=args.cohort,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
