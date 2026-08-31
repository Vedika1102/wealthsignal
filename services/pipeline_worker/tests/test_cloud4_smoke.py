from wealthsignal_pipeline.cloud4_smoke import SMOKE_MAX_ROWS, validate_smoke_contract


def test_smoke_contract_accepts_bounded_reload() -> None:
    assert validate_smoke_contract(
        source_rows=SMOKE_MAX_ROWS, train_rows=16_000, evaluation_rows=4_000,
        reload_rows=4_000, prospective_rows=0, label_classes=2,
    ) == []


def test_smoke_contract_rejects_unbounded_or_prospective_input() -> None:
    reasons = validate_smoke_contract(
        source_rows=SMOKE_MAX_ROWS + 1, train_rows=1, evaluation_rows=1,
        reload_rows=0, prospective_rows=1, label_classes=1,
    )
    assert reasons == [
        "source_row_bound_failed", "write_reload_count_mismatch",
        "prospective_guard_failed", "logistic_label_class_count_mismatch",
    ]


def test_smoke_source_is_engineering_only_and_bounded() -> None:
    from wealthsignal_pipeline import cloud4_smoke

    source = open(cloud4_smoke.__file__, encoding="utf-8").read()
    assert "SMOKE_MAX_ROWS = 20_000" in source
    assert 'SMOKE_HISTORY_END = "2023-12-31"' in source
    assert '"engineering_smoke_only": True' in source
    assert 'mlflow.set_tracking_uri("databricks")' in source
    assert "VectorAssembler" in source
    assert "LinearRegression" in source
    assert "LogisticRegression" in source
    assert "saveAsTable(SMOKE_TABLE)" in source
