from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal


@dataclass(slots=True)
class FilingReference:
    """Minimal metadata used to identify a specific SEC filing."""

    cik: str
    accession_number: str
    filing_date: date | None = None
    report_period: date | None = None
    form_type: str = "13F-HR"
    filer_name: str | None = None


@dataclass(slots=True)
class Holding:
    """Normalized holding extracted from the 13F information table."""

    issuer_name: str
    title_of_class: str
    cusip: str
    value_thousands: int
    shares_or_principal: float
    share_type: str = ""
    put_call: Literal["PUT", "CALL", ""] = ""
    investment_discretion: str = ""
    other_manager: str | None = None
    voting_authority_sole: int = 0
    voting_authority_shared: int = 0
    voting_authority_none: int = 0
    official_issuer_name: str | None = None
    official_class_title: str | None = None
    ticker: str | None = None
    official_list_match: bool = False
    official_list_source: str | None = None

    @property
    def market_value_usd(self) -> int:
        return self.value_thousands * 1000


@dataclass(slots=True)
class ParsedInformationTable:
    """Structured representation of a 13F filing's holdings table."""

    filing: FilingReference
    holdings: list[Holding]


@dataclass(slots=True)
class SubmissionFiling:
    """A single filing entry extracted from the SEC submissions JSON feed."""

    cik: str
    accession_number: str
    form_type: str
    filing_date: date | None = None
    report_period: date | None = None
    primary_document: str = ""
    primary_doc_description: str = ""


@dataclass(slots=True)
class FilingArtifacts:
    """Resolved file locations for a filing inside the SEC archive folder."""

    filing: SubmissionFiling
    filing_index_json_url: str
    filing_folder_url: str
    primary_document_url: str
    information_table_url: str | None = None
    information_table_filename: str | None = None


@dataclass(slots=True)
class IngestFailure:
    """One filing-level ingest failure captured during a batch run."""

    stage: str
    message: str
    accession_number: str | None = None


@dataclass(slots=True)
class IngestBatchResult:
    """Structured result for a multi-filing ingest attempt."""

    cik: str
    parsed_filings: list[ParsedInformationTable] = field(default_factory=list)
    failures: list[IngestFailure] = field(default_factory=list)


@dataclass(slots=True)
class PositionDelta:
    """Quarter-over-quarter change for a single holding key."""

    holding_key: str
    issuer_name: str
    cusip: str
    old_value_thousands: int
    new_value_thousands: int
    old_shares: float
    new_shares: float
    old_weight: float
    new_weight: float
    is_new_position: bool
    is_exited_position: bool
    share_type: str = ""
    value_delta_thousands: int = 0
    shares_delta: float = 0.0
    value_pct_change: float | None = None
    shares_pct_change: float | None = None
    rank_delta: int | None = None
    previous_rank: int | None = None
    current_rank: int | None = None
    official_issuer_name: str | None = None


@dataclass(slots=True)
class FilingDelta:
    """Collection of deltas between two quarters for the same filer."""

    current_filing: FilingReference
    previous_filing: FilingReference
    positions: list[PositionDelta] = field(default_factory=list)


@dataclass(slots=True)
class PositionFeatures:
    """Feature vector for a single position-change event."""

    holding_key: str
    issuer_name: str
    cusip: str
    sector: str
    current_value_thousands: int
    previous_value_thousands: int
    current_weight: float
    previous_weight: float
    weight_delta: float
    abs_weight_delta: float
    value_delta_thousands: int
    abs_value_delta_thousands: int
    value_pct_change: float | None
    shares_pct_change: float | None
    is_new_position: bool
    is_exited_position: bool
    current_rank: int | None
    previous_rank: int | None
    entered_top10: bool
    exited_top10: bool
    entered_top20: bool
    exited_top20: bool
    turnover_ratio: float
    change_share_of_turnover: float


@dataclass(slots=True)
class MaterialityAssessment:
    """Explainable first-pass materiality output."""

    holding_key: str
    issuer_name: str
    cusip: str
    sector: str
    score: int
    severity: str
    should_alert: bool
    reasons: list[str]
    feature_snapshot: PositionFeatures


@dataclass(slots=True)
class ClientHolding:
    """Synthetic wealth-client holding for downstream impact scoring."""

    cusip: str
    issuer_name: str
    sector: str
    weight: float


@dataclass(slots=True)
class ClientPortfolio:
    """Synthetic client portfolio used for overlap-based impact scoring."""

    client_id: str
    client_name: str
    strategy: str
    holdings: list[ClientHolding]


@dataclass(slots=True)
class ClientImpact:
    """Impact score linking one alert candidate to one client portfolio."""

    client_id: str
    client_name: str
    strategy: str
    cusip: str
    issuer_name: str
    sector: str
    direct_weight: float
    sector_weight: float
    impact_score: int
    impact_label: str


@dataclass(slots=True)
class PersistedAlert:
    """Stored alert record for API and audit retrieval."""

    alert_id: int
    current_accession_number: str
    previous_accession_number: str
    holding_key: str
    issuer_name: str
    cusip: str
    sector: str
    score: int
    severity: str
    should_alert: bool
    reasons: list[str]
    current_weight: float
    previous_weight: float
    weight_delta: float
    current_rank: int | None
    previous_rank: int | None
    turnover_ratio: float


@dataclass(slots=True)
class PersistedFeatureRow:
    """Stored feature row used for weak labeling and baseline model training."""

    current_accession_number: str
    previous_accession_number: str
    holding_key: str
    issuer_name: str
    cusip: str
    sector: str
    current_value_thousands: int
    previous_value_thousands: int
    current_weight: float
    previous_weight: float
    weight_delta: float
    abs_weight_delta: float
    value_delta_thousands: int
    abs_value_delta_thousands: int
    value_pct_change: float | None
    shares_pct_change: float | None
    is_new_position: bool
    is_exited_position: bool
    current_rank: int | None
    previous_rank: int | None
    entered_top10: bool
    exited_top10: bool
    entered_top20: bool
    exited_top20: bool
    turnover_ratio: float
    change_share_of_turnover: float
    rule_score: int
    weak_label: int


@dataclass(slots=True)
class ModelRunSummary:
    """Metadata for a baseline model training run."""

    run_id: int
    model_name: str
    training_samples: int
    positive_count: int
    feature_names: list[str]
    coefficients: list[float]
    intercept: float
    metrics: dict[str, float]
    comparison_group_id: str | None = None
    best_params: dict[str, object] | None = None
    calibration_curve: list[dict[str, float]] | None = None
    shap_feature_importance: list[dict[str, float]] | None = None
    artifact_path: str | None = None
    is_best_model: bool = False


@dataclass(slots=True)
class ModelPrediction:
    """Per-event baseline model output."""

    run_id: int
    current_accession_number: str
    holding_key: str
    issuer_name: str
    cusip: str
    probability: float
    predicted_label: int
    weak_label: int
    rule_score: int


@dataclass(slots=True)
class RecommendationPrecedent:
    """Historical precedent similar to a current material position change."""

    current_accession_number: str
    previous_accession_number: str
    issuer_name: str
    cusip: str
    sector: str
    abs_weight_delta: float
    value_delta_thousands: int
    rule_score: int
    weak_label: int
    similarity: float


@dataclass(slots=True)
class ClientRecommendation:
    """Ranked alert recommendation for one client portfolio."""

    client_id: str
    client_name: str
    strategy: str
    current_accession_number: str
    holding_key: str
    issuer_name: str
    cusip: str
    sector: str
    alert_score: int
    alert_severity: str
    relevance_score: int
    content_similarity: float
    direct_weight: float
    sector_weight: float
    precedents: list[RecommendationPrecedent]
    rationale: list[str]


@dataclass(slots=True)
class PersistedRecommendation:
    """Stored recommendation record exposed by the API."""

    recommendation_id: int
    alert_id: int
    client_id: str
    client_name: str
    strategy: str
    current_accession_number: str
    holding_key: str
    issuer_name: str
    cusip: str
    sector: str
    alert_score: int
    alert_severity: str
    relevance_score: int
    content_similarity: float
    direct_weight: float
    sector_weight: float
    precedents: list[dict[str, object]]
    rationale: list[str]
