from wealthsignal_pipeline.cloud3_gate import validate_report_contract


def _report() -> dict[str, object]:
    quarters = [
        "2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31", "2025-03-31",
        "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31",
    ]
    return {
        "validation_coverage_by_cap": {"100": 0.976, "250": 0.978, "500": 0.981},
        "selected_candidate_cap": 500,
        "prospective_q2_2026_truth_accessed": False,
        "split_manifest": {
            "folds": [
                {
                    "fold_id": f"validation-{index}",
                    "evaluation_target_quarter": quarter,
                    "train_predicate": f"target_report_period < DATE '{quarter}'",
                    "evaluation_predicate": f"target_report_period = DATE '{quarter}'",
                }
                for index, quarter in enumerate(quarters, 1)
            ]
        },
    }


def test_gate_accepts_frozen_report_contract() -> None:
    assert validate_report_contract(_report()) == []


def test_gate_rejects_non_validation_selected_cap() -> None:
    report = _report()
    report["selected_candidate_cap"] = 250
    assert "selected_candidate_cap_mismatch" in validate_report_contract(report)


def test_gate_declares_databricks_standalone_import_fallback() -> None:
    from wealthsignal_pipeline import cloud3_gate

    source = open(cloud3_gate.__file__, encoding="utf-8").read()
    assert "except ImportError" in source
    assert "from cloud3_gold import" in source
