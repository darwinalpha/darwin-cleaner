# src/darwin_cleaner/middleware.py

"""
Middleware registration and dispatch (order mechanism + declarative params + enable/disable switches).

- The registry is instance-level; importing the module has no registration side effects.
- No clean_type / source scoping: enabled middleware applies to the entire cleaning
  run, executed in stable sort order by (order, registration index).
- MiddlewareSpec carries params (declarative params) and enabled (on/off switch).
  fn has a uniform signature (df, ctx, params) -> df | None; users can use an
  option dataclass to convert params into a strongly typed view inside fn for
  validation and default values.
- The library ships 12 generic built-in steps; dedupe is disabled by default,
  the other 11 are enabled by default.
- load_middlewares_from_dir(path) loads functions marked with @middleware from a
  user directory; it is a code-loading helper, not data file discovery; whether
  to register them is up to the caller.
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
    A registered rule.

    params are declarative parameters, passed verbatim to fn by the engine and
    included in rule_signature; specs with enabled=False are skipped during
    resolve() and do not enter the execution plan.
    """

    name: str
    order: int
    fn: MiddlewareFn
    version: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)
    enabled: bool = True

    def enabled_with(self, enabled: bool) -> "MiddlewareSpec":
        """Return a copy with a different enabled state, all other fields unchanged."""
        return replace(self, enabled=enabled)

    def with_params(self, params: Mapping[str, Any]) -> "MiddlewareSpec":
        """Return a copy with different params, all other fields unchanged."""
        return replace(self, params=params)


@dataclass(frozen=True)
class MiddlewareOverride:
    """
    Request-level middleware override for per-request tuning beyond global registration.

    enabled / params set to None means "no override", keeping the registered value.
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
    """Decorator: attach order / version / params / enabled markers to a function; does not register it."""

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
    """Construct a MiddlewareSpec from a function marked with @middleware."""
    marker = getattr(fn, _MARKER_ATTR, None)
    if marker is None:
        raise CleanerRuleError(f"middleware function is missing the @middleware marker: {fn!r}")
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
    Return the 12 generic built-in steps shipped with the library (ascending order).

    The actual functions are defined in darwin_cleaner.steps; imported lazily here
    to avoid circular dependencies.
    """
    from darwin_cleaner.steps import BUILTIN_FNS

    return tuple(_spec_from_fn(fn) for fn in BUILTIN_FNS)


def load_middlewares_from_dir(path: Path) -> tuple[MiddlewareSpec, ...]:
    """
    Scan a directory for functions marked with @middleware and return MiddlewareSpecs.

    Only loads code; does not write to the registry; the caller decides which
    specs to register. Name conflicts are not swallowed here: the registry rejects
    them immediately at registration time with a CleanerRuleError; the upper layer
    should catch this exception at startup, log a warning, and terminate.
    """
    path = Path(path)
    if not path.is_dir():
        raise CleanerConfigError(f"middleware directory does not exist: {path}")

    specs: list[MiddlewareSpec] = []
    for py_file in sorted(path.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        module_name = f"_darwin_cleaner_ext_{py_file.stem}_{abs(hash(str(py_file)))}"
        spec_obj = importlib.util.spec_from_file_location(module_name, py_file)
        if spec_obj is None or spec_obj.loader is None:
            raise CleanerConfigError(f"Cannot load middleware module: {py_file}")
        module = importlib.util.module_from_spec(spec_obj)
        sys.modules[module_name] = module
        try:
            spec_obj.loader.exec_module(module)
        except Exception as exc:
            raise CleanerConfigError(f"Failed to load middleware module: {py_file}: {exc}") from exc
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
    """Derive a new spec according to overrides."""
    override = overrides.get(spec.name)
    if override is None:
        return spec
    if override.enabled is not None:
        spec = spec.enabled_with(override.enabled)
    if override.params is not None:
        spec = spec.with_params(override.params)
    return spec


def _stable_fingerprint(payload: Any) -> str:
    """Generate a stable fingerprint for a serializable object."""
    blob = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class MiddlewareRegistryImpl:
    """Default implementation of MiddlewareRegistry: instance-level registration, stable sort by order."""

    def __init__(self) -> None:
        self._specs: dict[str, MiddlewareSpec] = {}
        self._actions: dict[str, ActionFn] = {}

    def register(self, spec: MiddlewareSpec | MiddlewareFn) -> None:
        """
        Register a rule; when passed a function marked with @middleware, the marker
        is read automatically.

        A name conflict immediately raises CleanerRuleError at registration time
        (startup fails at registration), with no overwrite and no silent skip.
        """
        if not isinstance(spec, MiddlewareSpec):
            spec = _spec_from_fn(spec)
        if spec.name in self._specs:
            raise CleanerRuleError(f"middleware name conflict: {spec.name}")
        self._specs[spec.name] = spec

    def builtins(self) -> None:
        """Explicitly register the 12 built-in steps shipped with the library (dedupe disabled by default)."""
        builtin = builtin_middlewares()
        seen: set[str] = set()
        for spec in builtin:
            if spec.name in seen:
                raise CleanerRuleError(f"Duplicate built-in middleware name: {spec.name}")
            seen.add(spec.name)
            self.register(spec)

    def register_action(self, name: str, fn: ActionFn) -> None:
        """Register a transform action."""
        if name in self._actions:
            raise CleanerRuleError(f"transform action name conflict: {name}")
        self._actions[name] = fn

    @property
    def actions(self) -> Mapping[str, ActionFn]:
        """Registered transform actions (read-only view)."""
        return dict(self._actions)

    def resolve(
        self, overrides: Mapping[str, MiddlewareOverride] | None = None
    ) -> tuple[MiddlewareSpec, ...]:
        """
        Return the middleware actually in effect, in stable sort order by
        (order, registration index).

        - Specs with enabled=False are skipped and do not enter the execution plan;
        - overrides replace enabled / params by name; a None value keeps the
          registered value;
        - unknown names in overrides raise CleanerRuleError.
        """
        overrides = overrides or {}
        unknown = set(overrides) - set(self._specs)
        if unknown:
            raise CleanerRuleError(f"Unknown middleware override: {sorted(unknown)}")

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
        """Return a readable description of the effective rule chain (including params) for execution plan review."""
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
        """Return a stable fingerprint of the effective rule chain (including params and enabled)."""
        payload = {
            "middlewares": [dict(d) for d in self.describe(overrides)],
            "actions": sorted(self._actions),
        }
        return _stable_fingerprint(payload)


class MiddlewareRegistry(Protocol):
    """Rule registry protocol."""

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
