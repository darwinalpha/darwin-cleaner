# src/darwin_cleaner/context.py

"""
Execution context: middleware reads the contract, rules, and report from ctx.

Contains no business identifiers (source / clean_type / date / filename),
and no IO options (output_path / overwrite / check_only / csv_mode).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING

from darwin_cleaner.contract import Contract
from darwin_cleaner.results import CleanReport
from darwin_cleaner.rules import Rules

if TYPE_CHECKING:
    import pandas as pd


class Logger(Protocol):
    """Minimal logging protocol to avoid binding to a specific logging library."""

    def debug(self, msg: str, *args: Any) -> None: ...

    def info(self, msg: str, *args: Any) -> None: ...

    def warning(self, msg: str, *args: Any) -> None: ...

    def error(self, msg: str, *args: Any) -> None: ...


@dataclass
class CleanContext:
    """
    Runtime context for a single fork.

    output_key is only an identifier for this output, used by the report;
    it carries no business semantics.
    """

    output_key: str
    contract: Contract
    rules: Rules
    report: CleanReport
    logger: Logger | None = None
    data: "pd.DataFrame | None" = None


__all__ = ["Logger", "CleanContext"]
