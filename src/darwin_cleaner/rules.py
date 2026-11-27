# src/darwin_cleaner/rules.py

"""
规则值对象：描述"原始字段如何映射到契约"。

由上层解析后注入，替代 middleware 内的全局配置读取。
本模块不含任何数据源标识、业务类型标识。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

TransformKind = Literal["value", "factor", "action"]


@dataclass(frozen=True)
class TransformSpec:
    """
    字段转换声明，kind 决定读取哪个参数。

    value  —— 常量赋值，可创建新列
    factor —— 乘以系数，要求字段已存在
    action —— 调用注册的转换动作，要求字段已存在
    """

    kind: TransformKind
    value: Any = None
    factor: float | None = None
    action: str | None = None


@dataclass(frozen=True)
class JsonExtractSpec:
    """从 dict / JSON 列按路径提取字段。"""

    column: str
    path: tuple[str, ...] = ()


@dataclass(frozen=True)
class RowForkSpec:
    """
    行分叉组。

    field 指向的列不存在时不过滤（全量通过）。
    regex 与 exclude_regex 可同时存在：两者都满足才算命中。
    outputs 是输出名（对应请求中 contracts 的键），不是业务类型。
    """

    key: str
    outputs: tuple[str, ...] = ()
    field: str | None = None
    regex: str | None = None
    exclude_regex: str | None = None


@dataclass(frozen=True)
class ForkPlan:
    """单个分叉的显式执行规格。"""

    key: str
    output_names: tuple[str, ...]
    row_filter: RowForkSpec | None = None


@dataclass(frozen=True)
class Rules:
    """一组通用清洗规则。"""

    mapping: Mapping[str, str] = field(default_factory=dict)
    value_map: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    transform: Mapping[str, TransformSpec] = field(default_factory=dict)
    json_extract: Mapping[str, JsonExtractSpec] = field(default_factory=dict)
    filters: Mapping[str, RowForkSpec] = field(default_factory=dict)
    outputs: tuple[str, ...] = ()

    def to_fork_plans(self) -> tuple[ForkPlan, ...]:
        """
        把 filters / outputs 解析为有序分叉计划。

        - 有 filters：每个非空 outputs 的 RowForkSpec 生成一个计划（保持插入顺序）；
        - 无 filters 但有 outputs：退化为单组 "all" 的纯列分叉。
        """
        plans: list[ForkPlan] = []
        if self.filters:
            for key, spec in self.filters.items():
                if spec.outputs:
                    plans.append(ForkPlan(key=key, output_names=spec.outputs, row_filter=spec))
        elif self.outputs:
            plans.append(ForkPlan(key="all", output_names=self.outputs, row_filter=None))
        return tuple(plans)


__all__ = [
    "TransformKind",
    "TransformSpec",
    "JsonExtractSpec",
    "RowForkSpec",
    "ForkPlan",
    "Rules",
]
