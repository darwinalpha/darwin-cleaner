# src/darwin_cleaner/contract.py

"""
Contract value objects: describe "what a piece of data should look like".

These types are parsed by the upper layer and injected into the engine;
the engine is not responsible for reading files.
All objects are frozen and immutable during execution.

Note: Contract.name is only a label for diagnostics, not a business concept
such as clean_type / source.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

ViolationAction = Literal["null", "flag", "reject"]
SortDirection = Literal["ascending", "descending"]


@dataclass(frozen=True)
class FillSpec:
    """Field fill rule."""

    strategy: Literal["ffill", "value", "none"] = "ffill"
    max_days: int = 5
    value: Any = None


@dataclass(frozen=True)
class TypeSpec:
    """Type definition."""

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
    Contract for a single field.

    type_def keeps the original definition dict for upper-layer compatibility
    and diagnostics; commonly used constraints are also promoted to explicit
    fields so that rules can read them directly.
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
    Whether this column must be present in the output data (tri-state).

    True  -- missing is an error;
    False -- missing is explicitly allowed, and an empty column is added;
    None  -- not declared; derived by Contract: True for sort / keys fields,
             False for all others.

    Use Contract.is_required(field) for the final value.
    """


@dataclass(frozen=True)
class SortSpec:
    """Single-field sort declaration; the order of multiple SortSpecs is the sort priority."""

    field: str
    direction: SortDirection = "ascending"


@dataclass(frozen=True)
class Contract:
    """
    Generic contract view: fields, types, output columns, sorting.

    fields / types are merged by the upper layer and then injected; this
    library only accepts the merged immutable view.
    """

    name: str = ""
    fields: Mapping[str, FieldSpec] = field(default_factory=dict)
    types: Mapping[str, TypeSpec] = field(default_factory=dict)
    output: tuple[str, ...] = ()
    keys: tuple[str, ...] = ()
    """
    Columns that distinguish unique rows (composite key). An empty tuple means
    unique rows are not distinguished and duplicate rows are allowed.

    When non-empty, these columns must appear in output; their required status
    is derived as True by is_required(). Whether/how to deduplicate is controlled
    by the params of the dedupe middleware (disabled by default).
    """
    sort: tuple[SortSpec, ...] = ()
    enabled: bool = True
    help: str = ""

    def output_specs(self) -> tuple[FieldSpec, ...]:
        """Return field definitions in output order."""
        return tuple(self.fields[name] for name in self.output if name in self.fields)

    def is_required(self, field: str) -> bool:
        """
        Final required status of a field.

        An explicit declaration takes precedence; otherwise fields participating
        in sort or keys are derived as True, and all others as False.
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
        Return the deduplication keys declared by the contract.

        For reference by the upper layer and the dedupe middleware only;
        dedupe's params.keys takes precedence.
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
