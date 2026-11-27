# src/darwin_cleaner/results.py

"""
Result value objects: the engine only produces these objects in memory and writes
no files.

Contains no business identifiers such as source / clean_type / date, and no
output paths or checkpoint information. The upper layer writes data to parquet
and adds business fields and path information itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

Status = Literal["pending", "success", "failed", "skipped"]


@dataclass(frozen=True)
class CleaningStats:
    """
    Cleaning statistics.

    Each statistic must be written by an explicit rule; fields without a writer
    should not appear in the report, to avoid "always zero" fake metrics.
    """

    rows_in: int = 0
    rows_out: int = 0
    rows_removed: int = 0
    rows_modified: int = 0
    duplicates_removed: int = 0
    nulls_filled: Mapping[str, int] = field(default_factory=dict)
    nulls_backfilled: Mapping[str, int] = field(default_factory=dict)
    outliers_corrected: Mapping[str, int] = field(default_factory=dict)
    computed_fields: Mapping[str, int] = field(default_factory=dict)
    validated_fields: Mapping[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-writable dict."""
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True)
class ValidationIssue:
    """Sample row-level violation."""

    row_index: int
    field: str
    rule: str
    value: Any
    expected: str
    action: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-writable dict."""
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True)
class CleanMeta:
    """
    Metadata about the data itself (no business identifiers, paths, write times,
    or checkpoint lookups).

    data_hash is computed from DataFrame content and is independent of the
    persistence format / compression parameters; rule_signature is composed of
    the contract + rules + middleware registry fingerprints.
    """

    output_key: str
    rows: int
    columns: tuple[str, ...]
    data_hash: str
    rule_signature: str
    engine_version: str
    stats: CleaningStats = field(default_factory=CleaningStats)
    warnings: tuple[str, ...] = ()
    degraded: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-writable dict."""
        return {
            "output_key": self.output_key,
            "rows": self.rows,
            "columns": list(self.columns),
            "data_hash": self.data_hash,
            "rule_signature": self.rule_signature,
            "engine_version": self.engine_version,
            "stats": self.stats.to_dict(),
            "warnings": list(self.warnings),
            "degraded": self.degraded,
        }


@dataclass
class CleanReport:
    """Process audit report for a single fork; whether to persist it is decided by the upper layer."""

    output_key: str
    status: Status = "pending"
    stats: CleaningStats = field(default_factory=CleaningStats)

    validation_errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float = 0.0
    degraded: bool = False
    max_validation_samples: int = 100

    def start(self) -> None:
        """Record the start time."""
        self.started_at = datetime.now(timezone.utc).isoformat()

    def finish(self) -> None:
        """Record the end time and duration."""
        self.finished_at = datetime.now(timezone.utc).isoformat()
        if self.started_at is not None:
            start = datetime.fromisoformat(self.started_at)
            end = datetime.fromisoformat(self.finished_at)
            self.duration_seconds = (end - start).total_seconds()

    def add_validation_error(self, issue: ValidationIssue) -> None:
        """Add a row-level violation sample, discarding it once the limit is reached."""
        if len(self.validation_errors) < self.max_validation_samples:
            self.validation_errors.append(issue)

    def add_warning(self, msg: str) -> None:
        """Add a warning."""
        self.warnings.append(msg)

    def add_error(self, msg: str) -> None:
        """Add an error."""
        self.errors.append(msg)

    def bump_stat(self, name: str, amount: int = 1) -> None:
        """Increment an integer statistic (unknown fields are ignored)."""
        current = getattr(self.stats, name, None)
        if isinstance(current, int):
            self.stats = replace(self.stats, **{name: current + amount})

    def bump_mapping(self, name: str, key: str, amount: int = 1) -> None:
        """Increment a mapping statistic (unknown fields are ignored)."""
        current = getattr(self.stats, name, None)
        if isinstance(current, Mapping):
            updated = dict(current)
            updated[key] = updated.get(key, 0) + amount
            self.stats = replace(self.stats, **{name: updated})

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-writable dict."""
        return {
            "output_key": self.output_key,
            "status": self.status,
            "stats": self.stats.to_dict(),
            "validation_errors": [i.to_dict() for i in self.validation_errors],
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "degraded": self.degraded,
        }


@dataclass(frozen=True)
class CleanOutput:
    """In-memory output of a single fork. A 0-row output is handled as an error path and produces no special status."""

    output_key: str
    status: Status
    data: "pd.DataFrame | None" = None
    meta: CleanMeta | None = None
    report: CleanReport | None = None


@dataclass(frozen=True)
class CleanResult:
    """Overall result of one CleanRequest."""

    outputs: tuple[CleanOutput, ...] = ()
    status: Status = "pending"
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float = 0.0

    @property
    def success(self) -> bool:
        """Return True when all forks succeed."""
        return bool(self.outputs) and all(o.status == "success" for o in self.outputs)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-writable dict."""
        return {
            "outputs": [
                {
                    "output_key": o.output_key,
                    "status": o.status,
                    "meta": o.meta.to_dict() if o.meta else None,
                    "report": o.report.to_dict() if o.report else None,
                }
                for o in self.outputs
            ],
            "status": self.status,
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
        }


__all__ = [
    "Status",
    "CleaningStats",
    "ValidationIssue",
    "CleanMeta",
    "CleanReport",
    "CleanOutput",
    "CleanResult",
]
