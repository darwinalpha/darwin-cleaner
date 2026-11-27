# src/darwin_cleaner/context.py

"""
执行上下文：middleware 从 ctx 读取契约、规则与报告。

不含任何业务标识（source / clean_type / date / filename），
不含 IO 选项（output_path / overwrite / check_only / csv_mode）。
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
    """最小日志协议，避免绑定具体日志库。"""

    def debug(self, msg: str, *args: Any) -> None: ...

    def info(self, msg: str, *args: Any) -> None: ...

    def warning(self, msg: str, *args: Any) -> None: ...

    def error(self, msg: str, *args: Any) -> None: ...


@dataclass
class CleanContext:
    """
    单个分叉的运行时上下文。

    output_key 只是本次产出的标识，供报告使用，不携带业务语义。
    """

    output_key: str
    contract: Contract
    rules: Rules
    report: CleanReport
    logger: Logger | None = None
    data: "pd.DataFrame | None" = None


__all__ = ["Logger", "CleanContext"]
