# src/darwin_cleaner/engine.py

"""
Cleaning engine entry point.

Scope: read a single parquet -> fork -> run the chain by order -> return data
and meta. Out of scope: configuration loading, file discovery/merging,
scheduling, checkpoints, writing parquet/meta, CLI exit codes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, TYPE_CHECKING

import pandas as pd

from darwin_cleaner.context import CleanContext
from darwin_cleaner.exceptions import (
    CleanerConfigError,
    CleanerDataEmptyError,
    CleanerDataNotFoundError,
    CleanerValidationError,
)
from darwin_cleaner.middleware import MiddlewareRegistry, MiddlewareRegistryImpl
from darwin_cleaner.request import CleanRequest
from darwin_cleaner.results import (
    CleanMeta,
    CleanOutput,
    CleanReport,
    CleanResult,
)

if TYPE_CHECKING:
    pass

ENGINE_VERSION = "0.4.2"


@dataclass(frozen=True)
class CleanStep:
    """One step in the execution plan (ordered by order)."""

    name: str
    order: int
    version: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CleanPlan:
    """Auditable execution plan for a single fork."""

    output_key: str
    steps: tuple[CleanStep, ...]
    read_columns: tuple[str, ...]
    warnings: tuple[str, ...] = ()


def _input_hash(df: pd.DataFrame, algorithm: str) -> str:
    """Compute a stable hash from DataFrame content (independent of format/compression)."""
    import hashlib

    h = hashlib.new(algorithm)
    payload = pd.util.hash_pandas_object(df, index=True).values.tobytes()
    h.update(payload)
    return f"{algorithm}:{h.hexdigest()}"


def _signature(*parts: Any) -> str:
    """Generate a stable combined fingerprint from contract / rules / registry fingerprints."""
    import hashlib
    import json

    blob = json.dumps(parts, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


class CleaningEngine:
    """
    Cleaning engine.

    Usage:
        registry = MiddlewareRegistryImpl()
        registry.builtins()
        engine = CleaningEngine(registry)
        result = engine.clean(request)

    All outputs are produced in memory; the upper layer decides how to persist them.
    """

    def __init__(self, registry: MiddlewareRegistry) -> None:
        self._registry = registry

    def build_plan(self, request: CleanRequest) -> tuple[CleanPlan, ...]:
        """Build and validate the execution plan without executing or reading data. Can be used for review and debugging."""
        self._preflight(request)
        resolved = self._registry.resolve(request.options.middleware_overrides)
        steps = tuple(
            CleanStep(name=s.name, order=s.order, version=s.version, params=dict(s.params))
            for s in resolved
        )
        return tuple(
            CleanPlan(
                output_key=name,
                steps=steps,
                read_columns=request.contracts[name].output,
            )
            for name in request.contracts
        )

    def clean(self, request: CleanRequest) -> CleanResult:
        """
        Execute cleaning.

        Flow: preflight validation -> read a single input -> row fork / column fork ->
        run the chain by order for each fork -> generate meta -> aggregate.

        Exceptions are raised through darwin_cleaner.exceptions and are not mapped
        to exit codes.
        """
        started = datetime.now(timezone.utc).isoformat()
        warnings: list[str] = []
        errors: list[str] = []
        outputs: list[CleanOutput] = []

        try:
            self._preflight(request)
            df = self._read_input(request)
            resolved = self._registry.resolve(request.options.middleware_overrides)
            signature = _signature(
                self._contract_signature(request),
                self._rules_signature(request),
                self._registry.signature(request.options.middleware_overrides),
            )
            plans = request.rules.to_fork_plans()
            if not plans:
                # With no filters / outputs, fall back to a single column fork: produce all injected contracts
                from darwin_cleaner.rules import ForkPlan

                plans = (ForkPlan(key="all", output_names=tuple(request.contracts)),)

            for plan in plans:
                group = self._apply_row_filter(df, plan)
                for output_name in plan.output_names:
                    out = self._run_one(
                        group.copy(), request, output_name, resolved, signature
                    )
                    outputs.append(out)
        except (CleanerConfigError, CleanerDataNotFoundError, CleanerDataEmptyError) as exc:
            errors.append(str(exc))
        except Exception as exc:
            errors.append(f"Cleaning failed: {exc}")

        finished = datetime.now(timezone.utc).isoformat()
        status = "failed" if errors else "success"
        return CleanResult(
            outputs=tuple(outputs),
            status=status,
            warnings=tuple(warnings),
            errors=tuple(errors),
            started_at=started,
            finished_at=finished,
            duration_seconds=0.0,
        )

    # ------------------------------------------------------------
    # preflight
    # ------------------------------------------------------------

    def _preflight(self, request: CleanRequest) -> None:
        """Validate the request's self-consistency before execution; raises CleanerConfigError on failure."""
        if not request.contracts:
            raise CleanerConfigError("contracts must not be empty")
        for name, contract in request.contracts.items():
            if not contract.output:
                raise CleanerConfigError(f"Contract {name} must have a non-empty output")
            missing_def = [f for f in contract.output if f not in contract.fields]
            if missing_def:
                raise CleanerConfigError(
                    f"Output fields of contract {name} are not defined in fields: {missing_def}"
                )
            missing_keys = [k for k in contract.keys if k not in contract.output]
            if missing_keys:
                raise CleanerConfigError(
                    f"Keys fields of contract {name} are not in output: {missing_keys}"
                )
        for plan in request.rules.to_fork_plans():
            unknown = [n for n in plan.output_names if n not in request.contracts]
            if unknown:
                raise CleanerConfigError(
                    f"Fork {plan.key} references unknown output names: {unknown}"
                )

    # ------------------------------------------------------------
    # Read and fork
    # ------------------------------------------------------------

    def _read_input(self, request: CleanRequest) -> pd.DataFrame:
        """
        Read a single parquet or accept an in-memory DataFrame.

        Column pruning is only used to optimize parquet reads: the intersection of
        required columns and actual file columns is taken, avoiding errors from
        reading non-existent columns. In-memory DataFrames are not pruned --
        steps such as field mapping and formulas may depend on intermediate columns
        outside output, and pruning would break the cleaning chain.
        """
        source = request.input
        if isinstance(source, pd.DataFrame):
            df = source.copy()
        else:
            columns = request.options.read.columns
            if columns is None:
                columns = self._derive_columns(request)
            read_cols = None
            if columns:
                try:
                    import pyarrow.parquet as pq

                    available = set(pq.read_schema(source).names)
                    read_cols = [c for c in columns if c in available]
                except Exception:
                    read_cols = None
            try:
                df = pd.read_parquet(source, columns=read_cols)
            except Exception as exc:
                raise CleanerDataNotFoundError(f"Failed to read parquet: {source}: {exc}") from exc

        if df.empty:
            raise CleanerDataEmptyError("Input data has 0 rows")
        return df

    def _derive_columns(self, request: CleanRequest) -> tuple[str, ...]:
        """Derive columns to read from the output / keys / sort of the contracts involved and the rule fields."""
        needed: list[str] = []
        for name in request.contracts:
            contract = request.contracts[name]
            for field in list(contract.output) + list(contract.keys):
                if field not in needed:
                    needed.append(field)
            for s in contract.sort:
                if s.field not in needed:
                    needed.append(s.field)
        for field in list(request.rules.mapping) + list(request.rules.transform):
            if field not in needed:
                needed.append(field)
        for spec in request.rules.filters.values():
            if spec.field and spec.field not in needed:
                needed.append(spec.field)
        return tuple(needed)

    @staticmethod
    def _apply_row_filter(df: pd.DataFrame, plan: Any) -> pd.DataFrame:
        """Filter rows by RowForkSpec; no configuration means keep all rows."""
        spec = plan.row_filter
        if spec is None or not spec.field:
            return df
        if not spec.regex and not spec.exclude_regex:
            return df
        if spec.field not in df.columns:
            return df
        series = df[spec.field].astype(str)
        if spec.regex:
            mask = series.str.match(spec.regex, na=False)
        else:
            mask = ~series.str.match(spec.exclude_regex, na=False)
        return df[mask].reset_index(drop=True)

    # ------------------------------------------------------------
    # Single-fork execution
    # ------------------------------------------------------------

    def _run_one(
        self,
        df: pd.DataFrame,
        request: CleanRequest,
        output_name: str,
        resolved: tuple[Any, ...],
        signature: str,
    ) -> CleanOutput:
        """Run the full chain for a single fork and produce a CleanOutput."""
        contract = request.contracts[output_name]
        report = CleanReport(
            output_key=output_name,
            max_validation_samples=request.options.collect_validation_samples,
        )
        ctx = CleanContext(
            output_key=output_name,
            contract=contract,
            rules=request.rules,
            report=report,
        )
        report.start()
        report.bump_stat("rows_in", len(df))

        try:
            current = df
            for spec in resolved:
                try:
                    result = spec.fn(current, ctx, spec.params)
                except (CleanerConfigError, CleanerValidationError):
                    raise
                except Exception as exc:
                    if request.options.strict:
                        raise CleanerValidationError(
                            f"middleware {spec.name} failed: {exc}"
                        ) from exc
                    report.add_warning(f"middleware {spec.name} failed: {exc}")
                    report.degraded = True
                    continue
                if result is not None:
                    current = result

            if current.empty:
                raise CleanerDataEmptyError(f"Output {output_name} has 0 rows")

            report.bump_stat("rows_out", len(current))
            report.status = "success"
            meta = CleanMeta(
                output_key=output_name,
                rows=len(current),
                columns=tuple(current.columns),
                data_hash=_input_hash(current, request.options.hash_algorithm),
                rule_signature=signature,
                engine_version=ENGINE_VERSION,
                stats=report.stats,
                warnings=tuple(report.warnings),
                degraded=report.degraded,
            )
            return CleanOutput(
                output_key=output_name,
                status="success",
                data=current,
                meta=meta,
                report=report,
            )
        except (CleanerConfigError, CleanerDataEmptyError, CleanerValidationError) as exc:
            report.status = "failed"
            report.add_error(str(exc))
            return CleanOutput(
                output_key=output_name,
                status="failed",
                data=None,
                report=report,
            )
        finally:
            report.finish()

    # ------------------------------------------------------------
    # Fingerprints
    # ------------------------------------------------------------

    @staticmethod
    def _contract_signature(request: CleanRequest) -> list[dict[str, Any]]:
        result = []
        for name in sorted(request.contracts):
            c = request.contracts[name]
            result.append(
                {
                    "name": name,
                    "output": list(c.output),
                    "keys": list(c.keys),
                    "sort": [[s.field, s.direction] for s in c.sort],
                    "fields": {
                        k: {
                            "type": v.type,
                            "required": v.required,
                            "formula": v.formula,
                            "fill": str(v.fill) if v.fill else None,
                            "consistency": list(v.consistency),
                            "on_violation": v.on_violation,
                        }
                        for k, v in sorted(c.fields.items())
                    },
                    "types": {
                        k: [v.converter, v.min, v.max, v.rel_diff, v.abs_diff]
                        for k, v in sorted(c.types.items())
                    },
                }
            )
        return result

    @staticmethod
    def _rules_signature(request: CleanRequest) -> dict[str, Any]:
        r = request.rules
        return {
            "mapping": dict(r.mapping),
            "value_map": {k: dict(v) for k, v in r.value_map.items()},
            "transform": {
                k: [s.kind, s.value, s.factor, s.action] for k, s in r.transform.items()
            },
            "filters": {
                k: [s.field, s.regex, s.exclude_regex, list(s.outputs)]
                for k, s in r.filters.items()
            },
            "outputs": list(r.outputs),
        }


def clean(
    request: CleanRequest,
    *,
    registry: MiddlewareRegistry | None = None,
) -> CleanResult:
    """Convenience function: run one cleaning pass with the default (built-in) registry."""
    if registry is None:
        registry = MiddlewareRegistryImpl()
        registry.builtins()
    return CleaningEngine(registry).clean(request)


__all__ = ["CleanStep", "CleanPlan", "CleaningEngine", "clean"]
