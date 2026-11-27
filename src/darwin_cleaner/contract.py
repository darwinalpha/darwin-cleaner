# src/darwin_cleaner/contract.py

"""
契约值对象：描述"一份数据应该长什么样"。

这些类型由上层解析后注入引擎，引擎不负责读取文件。
所有对象均为 frozen，执行期间不可变。

注意：Contract.name 只是诊断用的标签，不是 clean_type / source 等业务概念。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

ViolationAction = Literal["null", "flag", "reject"]
SortDirection = Literal["ascending", "descending"]


@dataclass(frozen=True)
class FillSpec:
    """字段填充规则。"""

    strategy: Literal["ffill", "value", "none"] = "ffill"
    max_days: int = 5
    value: Any = None


@dataclass(frozen=True)
class TypeSpec:
    """类型定义。"""

    name: str
    converter: str | None = None
    min: float | None = None
    max: float | None = None
    rel_diff: float = 0.003
    abs_diff: float = 0.01
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FieldSpec:
    """
    单个字段的契约。

    type_def 保留原始定义字典，便于上层做兼容与诊断；
    常用约束同时提升为显式字段，供规则直接读取。
    """

    name: str
    type: str
    type_def: Mapping[str, Any] = field(default_factory=dict)
    help: str = ""
    fill: FillSpec | None = None
    formula: str | None = None
    consistency: tuple[str, ...] = ()
    on_violation: ViolationAction = "null"
    required: bool | None = None
    """
    该列是否必须存在于输出数据中（三态）。

    True  —— 缺失即报错；
    False —— 显式允许缺失，补空列；
    None  —— 未声明，由 Contract 推导：sort / keys 字段为 True，其余为 False。

    最终取值请用 Contract.is_required(field) 获取。
    """


@dataclass(frozen=True)
class SortSpec:
    """单字段排序声明；多个 SortSpec 的顺序即排序优先级。"""

    field: str
    direction: SortDirection = "ascending"


@dataclass(frozen=True)
class Contract:
    """
    通用契约视图：字段、类型、输出列、排序。

    fields / types 由上层完成合并后注入，本库只接收合并后的不可变视图。
    """

    name: str = ""
    fields: Mapping[str, FieldSpec] = field(default_factory=dict)
    types: Mapping[str, TypeSpec] = field(default_factory=dict)
    output: tuple[str, ...] = ()
    keys: tuple[str, ...] = ()
    """
    区分唯一行的列（复合键）。空元组表示不区分唯一行，允许重复行。

    非空时这些列必须出现在 output 中；其 required 由 is_required() 推导为 True。
    是否/如何去重由 dedupe middleware 的 params 控制（默认关闭）。
    """
    sort: tuple[SortSpec, ...] = ()
    enabled: bool = True
    help: str = ""

    def output_specs(self) -> tuple[FieldSpec, ...]:
        """按 output 顺序返回字段定义。"""
        return tuple(self.fields[name] for name in self.output if name in self.fields)

    def is_required(self, field: str) -> bool:
        """
        字段的最终必填性。

        显式声明优先；未声明时，参与 sort 或 keys 的字段推导为 True，其余为 False。
        """
        spec = self.fields.get(field)
        if spec is not None and spec.required is not None:
            return spec.required
        if field in self.keys:
            return True
        if any(s.field == field for s in self.sort):
            return True
        return False

    def dedupe_keys(self) -> tuple[str, ...]:
        """
        返回契约声明的去重键。

        仅供上层与 dedupe middleware 参考；dedupe 的 params.keys 优先级更高。
        """
        return self.keys


__all__ = [
    "ViolationAction",
    "SortDirection",
    "FillSpec",
    "TypeSpec",
    "FieldSpec",
    "SortSpec",
    "Contract",
]
