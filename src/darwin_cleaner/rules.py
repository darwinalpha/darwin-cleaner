# src/darwin_cleaner/rules.py

"""
Rule value objects: describe "how raw fields map to the contract".

Parsed by the upper layer and injected, replacing global configuration reads
inside middleware. This module contains no data source identifiers or business
type identifiers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

TransformKind = Literal["value", "factor", "action"]


@dataclass(frozen=True)
class TransformSpec:
    """
    Field transform declaration; kind determines which parameter is read.

    value  -- assign a constant, can create a new column
    factor -- multiply by a factor, the field must already exist
    action -- call a registered transform action, the field must already exist
    """

    kind: TransformKind
    value: Any = None
    factor: float | None = None
    action: str | None = None


@dataclass(frozen=True)
class JsonExtractSpec:
    """Extract fields from a dict / JSON column by path."""

    column: str
    path: tuple[str, ...] = ()


@dataclass(frozen=True)
class RowForkSpec:
    """
    Row fork group.

    If the column pointed to by field does not exist, no filtering is applied
    (all rows pass). regex and exclude_regex can both be present: a row matches
    only when both are satisfied. outputs are output names (keys of contracts in
    the request), not business types.
    """

    key: str
    outputs: tuple[str, ...] = ()
    field: str | None = None
    regex: str | None = None
    exclude_regex: str | None = None


@dataclass(frozen=True)
class ForkPlan:
    """Explicit execution specification for a single fork."""

    key: str
    output_names: tuple[str, ...]
    row_filter: RowForkSpec | None = None


@dataclass(frozen=True)
class Rules:
    """A set of generic cleaning rules."""

    mapping: Mapping[str, str] = field(default_factory=dict)
    value_map: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    transform: Mapping[str, TransformSpec] = field(default_factory=dict)
    json_extract: Mapping[str, JsonExtractSpec] = field(default_factory=dict)
    filters: Mapping[str, RowForkSpec] = field(default_factory=dict)
    outputs: tuple[str, ...] = ()

    def to_fork_plans(self) -> tuple[ForkPlan, ...]:
        """
        Parse filters / outputs into an ordered list of fork plans.

        - With filters: each RowForkSpec with non-empty outputs produces one plan
          (insertion order preserved);
        - Without filters but with outputs: fall back to a single column-only
          fork named "all".
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
