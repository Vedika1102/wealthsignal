import pytest

from wealthsignal_pipeline.cloud3_gold import predicate_split_manifest, select_candidate_cap


def test_select_candidate_cap_applies_frozen_tolerance() -> None:
    assert select_candidate_cap({100: 0.8000, 250: 0.8010, 500: 0.8024}) == 100
    assert select_candidate_cap({100: 0.8000, 250: 0.8030, 500: 0.8031}) == 250


def test_select_candidate_cap_rejects_non_frozen_study() -> None:
    with pytest.raises(ValueError, match="exactly"):
        select_candidate_cap({100: 0.8, 500: 0.9})


def test_split_manifest_uses_predicates_and_excludes_prospective_quarter() -> None:
    manifest = predicate_split_manifest(["2023-12-31", "2024-03-31", "2026-03-31"])
    assert [fold["evaluation_target_quarter"] for fold in manifest["folds"]] == [
        "2024-03-31", "2026-03-31"
    ]
    assert "example_ids" not in str(manifest)
    assert manifest["folds"][0]["train_predicate"] == "target_report_period < DATE '2024-03-31'"


def test_cloud3_source_avoids_serverless_unsupported_persist() -> None:
    from wealthsignal_pipeline import cloud3_gold

    source = open(cloud3_gold.__file__, encoding="utf-8").read()
    assert ".persist(" not in source
    assert ".cache(" not in source
