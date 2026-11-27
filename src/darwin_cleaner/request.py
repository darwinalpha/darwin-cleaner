# src/darwin_cleaner/request.py

"""
Request value objects: the upper layer packages "a single discovered file, parsed
rules, and contracts" into a CleanRequest.

The engine does not read configuration, discover files, read checkpoints, or
decide output paths.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, TYPE_CHECKING, Union

from darwin_cleaner.contract import Contract
from darwin_cleaner.middleware import MiddlewareOverride
from darwin_cleaner.rules import ForkPlan, Rules

if TYPE_CHECKING:
    import pandas as pd

InputData = Union[Path, "pd.DataFrame"]


@dataclass(frozen=True)
class ReadOptions:
    """Read options. Column pruning is derived by the engine from the contract and can also be explicitly overridden by the caller."""

    columns: tuple[str, ...] | None = None
    storage_options: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class CleanOptions:
    """Cleaning options."""

    strict: bool = True
    hash_algorithm: str = "sha256"
    collect_validation_samples: int = 100
    read: ReadOptions = field(default_factory=ReadOptions)
    middleware_overrides: Mapping[str, MiddlewareOverride] = field(default_factory=dict)
    """
    Override enabled / params of registered middleware by name (per-request tuning).
    Fields with a None value keep the registered value; unknown names are reported
    as errors during preflight.
    Typical use: temporarily enable the disabled-by-default dedupe for this request,
    or override warn_on_missing for output.
    """


@dataclass(frozen=True)
class CleanRequest:
    """
    A single cleaning request.

    input:       a single parquet path, or an in-memory DataFrame passed directly.
    contracts:   output name -> resolved contract (only the outputs needed for this run).
    rules:       resolved rules.
    fork_plans:  optional, explicit fork plans; when empty, derived from
                 rules.filters / rules.outputs.
    """

    input: InputData
    contracts: Mapping[str, Contract]
    rules: Rules
    options: CleanOptions = field(default_factory=CleanOptions)
    fork_plans: tuple[ForkPlan, ...] = ()


__all__ = [
    "InputData",
    "ReadOptions",
    "CleanOptions",
    "CleanRequest",
]
