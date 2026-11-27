# src/darwin_cleaner/engine.py

"""
清洗引擎入口。

职责边界：读取单个 parquet -> 分叉 -> 按 order 跑链 -> 返回数据与 meta。
不做：配置加载、文件发现/合并、调度、断点、写 parquet/meta、CLI 退出码。
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

ENGINE_VERSION = "0.4.0.dev0"


@dataclass(frozen=True)
class CleanStep:
    """执行计划中的一步（按 order 排列）。"""

    name: str
    order: int
    version: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CleanPlan:
    """单个分叉的可审查执行计划。"""

    output_key: str
    steps: tuple[CleanStep, ...]
    read_columns: tuple[str, ...]
    warnings: tuple[str, ...] = ()


def _input_hash(df: pd.DataFrame, algorithm: str) -> str:
    """基于 DataFrame 内容计算稳定 hash（与格式/压缩无关）。"""
    import hashlib

    h = hashlib.new(algorithm)
    payload = pd.util.hash_pandas_object(df, index=True).values.tobytes()
    h.update(payload)
    return f"{algorithm}:{h.hexdigest()}"


def _signature(*parts: Any) -> str:
    """对契约 / 规则 / 注册表指纹生成稳定组合指纹。"""
    import hashlib
    import json

    blob = json.dumps(parts, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


class CleaningEngine:
    """
    清洗引擎。

    使用方式：
        registry = MiddlewareRegistryImpl()
        registry.builtins()
        engine = CleaningEngine(registry)
        result = engine.clean(request)

    所有产出都在内存中；上层自行决定如何持久化。
    """

    def __init__(self, registry: MiddlewareRegistry) -> None:
        self._registry = registry

    def build_plan(self, request: CleanRequest) -> tuple[CleanPlan, ...]:
        """构建并校验执行计划，不执行、不读数据。可用于审查与调试。"""
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
        执行清洗。

        流程：preflight 校验 -> 读取单个输入 -> 行分叉/列分叉 ->
        逐分叉按 order 跑链 -> 生成 meta -> 汇总。

        异常通过 darwin_cleaner.exceptions 抛出，不映射为退出码。
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
                # 无 filters / outputs 时，退化为单组列分叉：产出全部已注入契约
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
            errors.append(f"清洗失败: {exc}")

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
        """执行前校验请求的自洽性；失败即抛 CleanerConfigError。"""
        if not request.contracts:
            raise CleanerConfigError("contracts 不能为空")
        for name, contract in request.contracts.items():
            if not contract.output:
                raise CleanerConfigError(f"契约 {name} 的 output 不能为空")
            missing_def = [f for f in contract.output if f not in contract.fields]
            if missing_def:
                raise CleanerConfigError(
                    f"契约 {name} 的 output 字段未在 fields 中定义: {missing_def}"
                )
            missing_keys = [k for k in contract.keys if k not in contract.output]
            if missing_keys:
                raise CleanerConfigError(
                    f"契约 {name} 的 keys 字段不在 output 中: {missing_keys}"
                )
        for plan in request.rules.to_fork_plans():
            unknown = [n for n in plan.output_names if n not in request.contracts]
            if unknown:
                raise CleanerConfigError(
                    f"分叉 {plan.key} 引用了未知输出名: {unknown}"
                )

    # ------------------------------------------------------------
    # 读取与分叉
    # ------------------------------------------------------------

    def _read_input(self, request: CleanRequest) -> pd.DataFrame:
        """
        读取单个 parquet 或接收内存 DataFrame。

        列裁剪只用于 parquet 读取优化：需求列与文件实际列取交集，
        避免读取不存在的列报错。内存 DataFrame 不裁剪——字段映射、公式等
        步骤可能依赖 output 之外的中间列，裁剪会破坏清洗链。
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
                raise CleanerDataNotFoundError(f"读取 parquet 失败: {source}: {exc}") from exc

        if df.empty:
            raise CleanerDataEmptyError("输入数据为 0 行")
        return df

    def _derive_columns(self, request: CleanRequest) -> tuple[str, ...]:
        """按本次涉及契约的 output / keys / sort 与规则字段推导读取列。"""
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
        """按 RowForkSpec 过滤行；无配置则全量。"""
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
    # 单分叉执行
    # ------------------------------------------------------------

    def _run_one(
        self,
        df: pd.DataFrame,
        request: CleanRequest,
        output_name: str,
        resolved: tuple[Any, ...],
        signature: str,
    ) -> CleanOutput:
        """对单个分叉跑完整链，产出 CleanOutput。"""
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
                            f"middleware {spec.name} 失败: {exc}"
                        ) from exc
                    report.add_warning(f"middleware {spec.name} 失败: {exc}")
                    report.degraded = True
                    continue
                if result is not None:
                    current = result

            if current.empty:
                raise CleanerDataEmptyError(f"输出 {output_name} 为 0 行")

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
    # 指纹
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
    """便捷函数：使用默认（内置）注册表执行一次清洗。"""
    if registry is None:
        registry = MiddlewareRegistryImpl()
        registry.builtins()
    return CleaningEngine(registry).clean(request)


__all__ = ["CleanStep", "CleanPlan", "CleaningEngine", "clean"]
