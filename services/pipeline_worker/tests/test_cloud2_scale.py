import pytest

from wealthsignal_pipeline.cloud2_scale import run_scale_validation, scale_go_no_go


def test_scale_gate_passes_only_clean_prefix_and_quality() -> None:
    passed, reasons = scale_go_no_go({"prospective_rows": 0, "portfolio_weight_max_abs_error": 1e-15})
    assert passed
    assert reasons == []


def test_scale_gate_blocks_prefix_or_leakage_failures() -> None:
    passed, reasons = scale_go_no_go(
        {"missing_in_scaled": 1, "prospective_rows": 2, "portfolio_weight_max_abs_error": 1e-4}
    )
    assert not passed
    assert reasons == ["missing_in_scaled", "prospective_rows", "portfolio_weight_max_abs_error"]


def test_scale_gate_rejects_non_frozen_prefix_configuration() -> None:
    with pytest.raises(ValueError, match="frozen"):
        run_scale_validation(50, (25,))
