"""Middleware registry: order sorting, switches, overrides, duplicate names and fingerprints."""

import pytest

import pandas as pd

from darwin_cleaner import (
    MiddlewareOverride,
    MiddlewareRegistryImpl,
    middleware,
)
from darwin_cleaner.context import CleanContext
from darwin_cleaner.exceptions import CleanerRuleError
from darwin_cleaner.results import CleanReport
from darwin_cleaner.rules import Rules
from darwin_cleaner import Contract


def test_builtins_are_twelve_and_dedupe_disabled():
    r = MiddlewareRegistryImpl()
    r.builtins()
    all_specs = r.resolve({"dedupe": MiddlewareOverride(enabled=True)})
    assert len(all_specs) == 12
    default = r.resolve()
    assert len(default) == 11
    assert "dedupe" not in [s.name for s in default]
    assert [s.order for s in default] == sorted(s.order for s in default)


def test_duplicate_name_raises():
    r = MiddlewareRegistryImpl()

    @middleware(order=1)
    def custom(df, ctx, params):
        return df

    r.register(custom)
    with pytest.raises(CleanerRuleError):
        r.register(custom)


def test_unknown_override_raises():
    r = MiddlewareRegistryImpl()
    r.builtins()
    with pytest.raises(CleanerRuleError):
        r.resolve({"no_such_mw": MiddlewareOverride(enabled=True)})


def test_override_params_change_signature():
    r = MiddlewareRegistryImpl()
    r.builtins()
    sig1 = r.signature()
    sig2 = r.signature({"output": MiddlewareOverride(params={"warn_on_missing": True})})
    assert sig1 != sig2


def test_disabled_middleware_excluded_from_plan_signature():
    r = MiddlewareRegistryImpl()
    r.builtins()
    sig_with = r.signature()
    sig_without = r.signature({"output": MiddlewareOverride(enabled=False)})
    assert sig_with != sig_without


def test_custom_middleware_runs_with_params():
    r = MiddlewareRegistryImpl()

    @middleware(order=5, params={"tag": "a"})
    def tagger(df: pd.DataFrame, ctx: CleanContext, params):
        df["tag"] = params["tag"]
        return df

    r.register(tagger)
    specs = r.resolve()
    assert len(specs) == 1
    assert specs[0].params["tag"] == "a"

    df = pd.DataFrame({"x": [1]})
    ctx = CleanContext(output_key="o", contract=Contract(), rules=Rules(), report=CleanReport(output_key="o"))
    out = specs[0].fn(df, ctx, specs[0].params)
    assert out["tag"].iloc[0] == "a"


def test_load_middlewares_from_dir(tmp_path):
    mod = tmp_path / "extra.py"
    mod.write_text(
        "from darwin_cleaner import middleware\n"
        "@middleware(order=7)\n"
        "def my_rule(df, ctx, params):\n"
        "    return df\n",
        encoding="utf-8",
    )
    from darwin_cleaner import load_middlewares_from_dir

    specs = load_middlewares_from_dir(tmp_path)
    assert len(specs) == 1
    assert specs[0].name == "my_rule"
    assert specs[0].order == 7
