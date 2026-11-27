# src/darwin_cleaner/__init__.py

"""
darwin-cleaner: a contract- and rule-driven single-file cleaning engine.

Scope: read a single parquet -> row forks / column forks -> run middleware by
order -> return data and meta. No business concepts such as source / clean_type;
no configuration loading, file discovery, scheduling, checkpoints, CLI, or
persistence to disk.
"""

from __future__ import annotations

from darwin_cleaner.contract import (
    Contract,
    FieldSpec,
    FillSpec,
    SortDirection,
    SortSpec,
    TypeSpec,
    ViolationAction,
)
from darwin_cleaner.context import CleanContext, Logger
from darwin_cleaner.engine import CleanPlan, CleanStep, CleaningEngine, clean
from darwin_cleaner.exceptions import (
    CleanerConfigError,
    CleanerDataEmptyError,
    CleanerDataNotFoundError,
    CleanerError,
    CleanerRuleError,
    CleanerValidationError,
)
from darwin_cleaner.middleware import (
    ActionFn,
    MiddlewareFn,
    MiddlewareRegistry,
    MiddlewareRegistryImpl,
    MiddlewareOverride,
    MiddlewareSpec,
    builtin_middlewares,
    load_middlewares_from_dir,
    middleware,
)
from darwin_cleaner.request import CleanOptions, CleanRequest, InputData, ReadOptions
from darwin_cleaner.results import (
    CleanMeta,
    CleanOutput,
    CleanReport,
    CleanResult,
    CleaningStats,
    Status,
    ValidationIssue,
)
from darwin_cleaner.rules import (
    ForkPlan,
    JsonExtractSpec,
    RowForkSpec,
    Rules,
    TransformKind,
    TransformSpec,
)

__version__ = "0.4.2"

__all__ = [
    "__version__",
    "Contract",
    "FieldSpec",
    "FillSpec",
    "SortDirection",
    "SortSpec",
    "TypeSpec",
    "ViolationAction",
    "CleanContext",
    "Logger",
    "CleanPlan",
    "CleanStep",
    "CleaningEngine",
    "clean",
    "CleanerError",
    "CleanerConfigError",
    "CleanerRuleError",
    "CleanerDataNotFoundError",
    "CleanerDataEmptyError",
    "CleanerValidationError",
    "ActionFn",
    "MiddlewareFn",
    "MiddlewareRegistry",
    "MiddlewareRegistryImpl",
    "MiddlewareOverride",
    "MiddlewareSpec",
    "middleware",
    "builtin_middlewares",
    "load_middlewares_from_dir",
    "CleanOptions",
    "CleanRequest",
    "InputData",
    "ReadOptions",
    "CleanMeta",
    "CleanOutput",
    "CleanReport",
    "CleanResult",
    "CleaningStats",
    "Status",
    "ValidationIssue",
    "ForkPlan",
    "JsonExtractSpec",
    "RowForkSpec",
    "Rules",
    "TransformKind",
    "TransformSpec",
]
