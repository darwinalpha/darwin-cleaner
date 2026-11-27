# src/darwin_cleaner/middleware.py

"""
Middleware 注册与调度（order 机制 + 声明式参数 + 启停开关）。

- 注册表是实例级的；模块导入不产生注册副作用。
- 无 clean_type / source 作用域：启用的 middleware 对本次清洗全部生效，
  按 (order, 注册序号) 稳定排序执行。
- MiddlewareSpec 携带 params（声明式参数）与 enabled（启停开关）。
  fn 统一签名 (df, ctx, params) -> df | None；用户可用 option dataclass
  在 fn 内部把 params 转成强类型视图，便于校验与默认值。
- 库内置 12 个通用步骤，其中 dedupe 默认 enabled=False；其余 11 个默认启用。
- load_middlewares_from_dir(path) 从用户目录加载带 @middleware 标记的函数，
  它是代码加载 helper，不是数据文件发现；是否注册由调用方决定。
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, TypeAlias, TYPE_CHECKING

from darwin_cleaner.context import CleanContext
from darwin_cleaner.exceptions import CleanerConfigError, CleanerRuleError

if TYPE_CHECKING:
    import pandas as pd

MiddlewareFn: TypeAlias = Callable[
    ["pd.DataFrame", CleanContext, Mapping[str, Any]], "pd.DataFrame | None"
]
ActionFn: TypeAlias = Callable[["pd.Series"], "pd.Series"]


@dataclass(frozen=True)
class MiddlewareSpec:
    """
    一条已注册的规则。

    params 是声明式参数，引擎原样传给 fn，并计入 rule_signature；
    enabled=False 的 spec 在 resolve() 阶段被跳过，不进入执行计划。
    """

    name: str
    order: int
    fn: MiddlewareFn
    version: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)
    enabled: bool = True

    def enabled_with(self, enabled: bool) -> "MiddlewareSpec":
        """返回一个启停状态不同的副本，其余字段不变。"""
        return replace(self, enabled=enabled)

    def with_params(self, params: Mapping[str, Any]) -> "MiddlewareSpec":
        """返回一个参数不同的副本，其余字段不变。"""
        return replace(self, params=params)


@dataclass(frozen=True)
class MiddlewareOverride:
    """
    请求级的 middleware 覆盖项，用于全局注册之外的按请求微调。

    enabled / params 为 None 表示"不覆盖"，沿用注册值。
    """

    enabled: bool | None = None
    params: Mapping[str, Any] | None = None


_MARKER_ATTR = "__darwin_cleaner_middleware__"


def middleware(
    order: int = 100,
    *,
    version: str = "",
    params: Mapping[str, Any] | None = None,
    enabled: bool = True,
) -> Callable[[MiddlewareFn], MiddlewareFn]:
    """装饰器：给函数附加 order / version / params / enabled 标记，不执行注册。"""

    def deco(fn: MiddlewareFn) -> MiddlewareFn:
        setattr(
            fn,
            _MARKER_ATTR,
            {
                "order": order,
                "version": version,
                "params": dict(params or {}),
                "enabled": enabled,
            },
        )
        return fn

    return deco


def _spec_from_fn(fn: MiddlewareFn, name: str | None = None) -> MiddlewareSpec:
    """从带 @middleware 标记的函数构造 MiddlewareSpec。"""
    marker = getattr(fn, _MARKER_ATTR, None)
    if marker is None:
        raise CleanerRuleError(f"middleware 函数缺少 @middleware 标记: {fn!r}")
    return MiddlewareSpec(
        name=name or fn.__name__,
        order=marker["order"],
        fn=fn,
        version=marker["version"],
        params=dict(marker["params"]),
        enabled=marker["enabled"],
    )


def builtin_middlewares() -> tuple[MiddlewareSpec, ...]:
    """
    返回库内置的 12 个通用步骤（按 order 升序）。

    实际函数定义在 darwin_cleaner.steps 中，此处延迟导入以避免循环依赖。
    """
    from darwin_cleaner.steps import BUILTIN_FNS

    return tuple(_spec_from_fn(fn) for fn in BUILTIN_FNS)


def load_middlewares_from_dir(path: Path) -> tuple[MiddlewareSpec, ...]:
    """
    扫描目录下带 @middleware 标记的函数并返回 MiddlewareSpec。

    只做代码加载，不写注册表；调用方决定把哪些 spec 注册进 registry。
    同名冲突不在本函数吞掉：注册阶段由 registry 立即拒绝并抛出 CleanerRuleError，
    上层应在启动时捕获该异常，告警后终止进程。
    """
    path = Path(path)
    if not path.is_dir():
        raise CleanerConfigError(f"middleware 目录不存在: {path}")

    specs: list[MiddlewareSpec] = []
    for py_file in sorted(path.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        module_name = f"_darwin_cleaner_ext_{py_file.stem}_{abs(hash(str(py_file)))}"
        spec_obj = importlib.util.spec_from_file_location(module_name, py_file)
        if spec_obj is None or spec_obj.loader is None:
            raise CleanerConfigError(f"无法加载 middleware 模块: {py_file}")
        module = importlib.util.module_from_spec(spec_obj)
        sys.modules[module_name] = module
        try:
            spec_obj.loader.exec_module(module)
        except Exception as exc:
            raise CleanerConfigError(f"加载 middleware 模块失败: {py_file}: {exc}") from exc
        finally:
            sys.modules.pop(module_name, None)

        for attr_name in sorted(vars(module)):
            obj = getattr(module, attr_name)
            if callable(obj) and hasattr(obj, _MARKER_ATTR):
                specs.append(_spec_from_fn(obj, name=attr_name))
    return tuple(specs)


def _apply_overrides(
    spec: MiddlewareSpec, overrides: Mapping[str, MiddlewareOverride]
) -> MiddlewareSpec:
    """按覆盖项派生新 spec。"""
    override = overrides.get(spec.name)
    if override is None:
        return spec
    if override.enabled is not None:
        spec = spec.enabled_with(override.enabled)
    if override.params is not None:
        spec = spec.with_params(override.params)
    return spec


def _stable_fingerprint(payload: Any) -> str:
    """对可序列化对象生成稳定指纹。"""
    blob = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class MiddlewareRegistryImpl:
    """MiddlewareRegistry 的默认实现：实例级注册、按 order 稳定排序。"""

    def __init__(self) -> None:
        self._specs: dict[str, MiddlewareSpec] = {}
        self._actions: dict[str, ActionFn] = {}

    def register(self, spec: MiddlewareSpec | MiddlewareFn) -> None:
        """
        注册一条规则；传入被 @middleware 打标的函数时自动读取标记。

        同名冲突在注册时立即抛 CleanerRuleError（注册阶段即失败启动），
        不覆盖、不静默跳过。
        """
        if not isinstance(spec, MiddlewareSpec):
            spec = _spec_from_fn(spec)
        if spec.name in self._specs:
            raise CleanerRuleError(f"middleware 名称冲突: {spec.name}")
        self._specs[spec.name] = spec

    def builtins(self) -> None:
        """显式注册库内置的 12 个步骤（dedupe 默认 enabled=False）。"""
        builtin = builtin_middlewares()
        seen: set[str] = set()
        for spec in builtin:
            if spec.name in seen:
                raise CleanerRuleError(f"内置 middleware 名称重复: {spec.name}")
            seen.add(spec.name)
            self.register(spec)

    def register_action(self, name: str, fn: ActionFn) -> None:
        """注册一个 transform 动作。"""
        if name in self._actions:
            raise CleanerRuleError(f"transform action 名称冲突: {name}")
        self._actions[name] = fn

    @property
    def actions(self) -> Mapping[str, ActionFn]:
        """已注册的 transform 动作（只读视图）。"""
        return dict(self._actions)

    def resolve(
        self, overrides: Mapping[str, MiddlewareOverride] | None = None
    ) -> tuple[MiddlewareSpec, ...]:
        """
        返回实际生效的 middleware，按 (order, 注册序号) 稳定排序。

        - enabled=False 的 spec 被跳过，不进入执行计划；
        - overrides 按 name 覆盖 enabled / params，值为 None 表示沿用注册值；
        - overrides 中的未知 name 抛 CleanerRuleError。
        """
        overrides = overrides or {}
        unknown = set(overrides) - set(self._specs)
        if unknown:
            raise CleanerRuleError(f"未知的 middleware 覆盖项: {sorted(unknown)}")

        ordered = sorted(
            enumerate(self._specs.values()),
            key=lambda pair: (pair[1].order, pair[0]),
        )
        result: list[MiddlewareSpec] = []
        for _, spec in ordered:
            spec = _apply_overrides(spec, overrides)
            if spec.enabled:
                result.append(spec)
        return tuple(result)

    def describe(
        self, overrides: Mapping[str, MiddlewareOverride] | None = None
    ) -> tuple[Mapping[str, Any], ...]:
        """返回实际生效规则链的可读描述（含 params），供执行计划审查。"""
        return tuple(
            {
                "name": s.name,
                "order": s.order,
                "version": s.version,
                "params": dict(s.params),
            }
            for s in self.resolve(overrides)
        )

    def signature(
        self, overrides: Mapping[str, MiddlewareOverride] | None = None
    ) -> str:
        """返回实际生效规则链的稳定指纹（含 params 与 enabled）。"""
        payload = {
            "middlewares": [dict(d) for d in self.describe(overrides)],
            "actions": sorted(self._actions),
        }
        return _stable_fingerprint(payload)


class MiddlewareRegistry(Protocol):
    """规则注册表协议。"""

    def register(self, spec: MiddlewareSpec | MiddlewareFn) -> None: ...

    def builtins(self) -> None: ...

    def register_action(self, name: str, fn: ActionFn) -> None: ...

    def resolve(
        self, overrides: Mapping[str, MiddlewareOverride] | None = None
    ) -> tuple[MiddlewareSpec, ...]: ...

    def describe(
        self, overrides: Mapping[str, MiddlewareOverride] | None = None
    ) -> tuple[Mapping[str, Any], ...]: ...

    def signature(
        self, overrides: Mapping[str, MiddlewareOverride] | None = None
    ) -> str: ...


__all__ = [
    "MiddlewareFn",
    "ActionFn",
    "MiddlewareSpec",
    "MiddlewareOverride",
    "middleware",
    "builtin_middlewares",
    "load_middlewares_from_dir",
    "MiddlewareRegistry",
    "MiddlewareRegistryImpl",
]
