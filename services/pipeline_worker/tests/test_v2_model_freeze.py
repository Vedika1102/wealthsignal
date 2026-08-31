import json
from pathlib import Path

from wealthsignal_pipeline.v2_model_freeze import (
    canonical_sha256,
    normalized_file_sha256,
    validate_model_freeze,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def test_committed_v2_model_freeze_is_internally_consistent() -> None:
    assert validate_model_freeze(REPOSITORY_ROOT) == []


def test_normalized_hash_is_independent_of_checkout_line_endings(tmp_path: Path) -> None:
    lf = tmp_path / "lf.txt"
    crlf = tmp_path / "crlf.txt"
    lf.write_bytes(b"alpha\nbeta\n")
    crlf.write_bytes(b"alpha\r\nbeta\r\n")

    assert normalized_file_sha256(lf) == normalized_file_sha256(crlf)


def test_protocol_hash_algorithms_remain_explicitly_distinct() -> None:
    protocol = (
        REPOSITORY_ROOT / "docs/ai-governance/forecast-comparison-protocol-v2.md"
    ).read_text(encoding="utf-8")
    manifest = json.loads(
        (
            REPOSITORY_ROOT
            / "docs/ai-governance/forecast-protocol-v2-model-freeze.json"
        ).read_text(encoding="utf-8")
    )

    canonical_string_hash = canonical_sha256(protocol.replace("\r\n", "\n"))
    assert canonical_string_hash == manifest["protocol_hashes"]["cloud4_canonical_json_string_sha256"]
    assert canonical_string_hash != manifest["protocol_hashes"]["git_normalized_file_sha256"]
