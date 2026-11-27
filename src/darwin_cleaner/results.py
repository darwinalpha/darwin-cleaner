# src/darwin_cleaner/results.py

"""
结果值对象：引擎只在内存中产出这些对象，不写任何文件。

不含 source / clean_type / date 等业务标识，也不含输出路径与断点信息。
上层负责把 data 写成 parquet，并自行补充业务字段与路径信息。
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
    清洗统计。

    每项统计必须由明确的规则写入；无写入方的字段不应出现在报告中，
    以免形成"永远为 0"的假指标。
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
        """序列化为可写 JSON 的字典。"""
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True)
class ValidationIssue:
    """行级违规样本。"""

    row_index: int
    field: str
    rule: str
    value: Any
    expected: str
    action: str

    def to_dict(self) -> dict[str, Any]:
        """序列化为可写 JSON 的字典。"""
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True)
class CleanMeta:
    """
    数据自身元信息（不含业务标识、路径、写入时间、断点反查）。

    data_hash 基于 DataFrame 内容计算，与落盘格式/压缩参数无关；
    rule_signature 由契约 + 规则 + middleware 注册表指纹组成。
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
        """序列化为可写 JSON 的字典。"""
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
    """单个分叉的过程审计报告；是否落盘由上层决定。"""

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
        """记录开始时间。"""
        self.started_at = datetime.now(timezone.utc).isoformat()

    def finish(self) -> None:
        """记录结束时间与耗时。"""
        self.finished_at = datetime.now(timezone.utc).isoformat()
        if self.started_at is not None:
            start = datetime.fromisoformat(self.started_at)
            end = datetime.fromisoformat(self.finished_at)
            self.duration_seconds = (end - start).total_seconds()

    def add_validation_error(self, issue: ValidationIssue) -> None:
        """添加行级违规样本，超过上限后丢弃。"""
        if len(self.validation_errors) < self.max_validation_samples:
            self.validation_errors.append(issue)

    def add_warning(self, msg: str) -> None:
        """添加一条告警。"""
        self.warnings.append(msg)

    def add_error(self, msg: str) -> None:
        """添加一条错误。"""
        self.errors.append(msg)

    def bump_stat(self, name: str, amount: int = 1) -> None:
        """递增一个整数统计项（未知字段忽略）。"""
        current = getattr(self.stats, name, None)
        if isinstance(current, int):
            self.stats = replace(self.stats, **{name: current + amount})

    def bump_mapping(self, name: str, key: str, amount: int = 1) -> None:
        """递增一个映射统计项（未知字段忽略）。"""
        current = getattr(self.stats, name, None)
        if isinstance(current, Mapping):
            updated = dict(current)
            updated[key] = updated.get(key, 0) + amount
            self.stats = replace(self.stats, **{name: updated})

    def to_dict(self) -> dict[str, Any]:
        """序列化为可写 JSON 的字典。"""
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
    """单个分叉的内存产出。0 行输出按错误路径处理，不产生 special status。"""

    output_key: str
    status: Status
    data: "pd.DataFrame | None" = None
    meta: CleanMeta | None = None
    report: CleanReport | None = None


@dataclass(frozen=True)
class CleanResult:
    """一次 CleanRequest 的整体结果。"""

    outputs: tuple[CleanOutput, ...] = ()
    status: Status = "pending"
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float = 0.0

    @property
    def success(self) -> bool:
        """所有分叉都成功时返回 True。"""
        return bool(self.outputs) and all(o.status == "success" for o in self.outputs)

    def to_dict(self) -> dict[str, Any]:
        """序列化为可写 JSON 的字典。"""
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
