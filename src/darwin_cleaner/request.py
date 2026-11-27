# src/darwin_cleaner/request.py

"""
请求值对象：上层把"已经发现单个文件、解析好的规则与契约"打包成 CleanRequest。

引擎不读配置、不发现文件、不读断点、不决定输出路径。
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
    """读取选项。列裁剪由引擎按契约推导，也可由调用方显式覆盖。"""

    columns: tuple[str, ...] | None = None
    storage_options: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class CleanOptions:
    """清洗选项。"""

    strict: bool = True
    hash_algorithm: str = "sha256"
    collect_validation_samples: int = 100
    read: ReadOptions = field(default_factory=ReadOptions)
    middleware_overrides: Mapping[str, MiddlewareOverride] = field(default_factory=dict)
    """
    按 name 覆盖已注册 middleware 的 enabled / params（请求级微调）。
    值为 None 的字段沿用注册值；未知 name 在 preflight 报错。
    典型用法：本次请求临时开启默认关闭的 dedupe，或覆盖 output 的 warn_on_missing。
    """


@dataclass(frozen=True)
class CleanRequest:
    """
    一次清洗请求。

    input:       单个 parquet 路径，或直接传入的内存 DataFrame。
    contracts:   输出名 -> 已解析契约（只需本次要生成的输出）。
    rules:       已解析规则。
    fork_plans:  可选，显式分叉计划；为空时由 rules.filters / rules.outputs 派生。
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
