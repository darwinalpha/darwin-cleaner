"""Error handling: strict fail-fast, lenient degradation and config error propagation."""

import pandas as pd

from darwin_cleaner import (
    CleanOptions,
    CleanRequest,
    Contract,
    FieldSpec,
    MiddlewareRegistryImpl,
    Rules,
)
from darwin_cleaner import middleware
from darwin_cleaner.context import CleanContext
from darwin_cleaner.engine import CleaningEngine
from darwin_cleaner.exceptions import CleanerRuleError


def make_contract():
    return Contract(
        name="t",
        fields={
            "symbol": FieldSpec(name="symbol", type="str"),
            "x": FieldSpec(name="x", type="float"),
        },
        output=("symbol", "x"),
    )


def make_df():
    return pd.DataFrame({"symbol": ["a", "b"], "x": [1.0, 2.0]})


def add_failing_middleware(registry):
    @middleware(order=80)
    def boom(df: pd.DataFrame, ctx: CleanContext, params):
        raise RuntimeError("boom")
    registry.register(boom)


def test_strict_mode_marks_failed():
    r = MiddlewareRegistryImpl()
    r.builtins()
    add_failing_middleware(r)
    result = CleaningEngine(r).clean(
        CleanRequest(
            input=make_df(),
            contracts={"t": make_contract()},
            rules=Rules(),
            options=CleanOptions(strict=True),
        )
    )
    assert result.outputs[0].status == "failed"
    assert any("boom" in e for e in result.outputs[0].report.errors)
    assert result.outputs[0].data is None


def test_lenient_mode_marks_degraded():
    r = MiddlewareRegistryImpl()
    r.builtins()
    add_failing_middleware(r)
    result = CleaningEngine(r).clean(
        CleanRequest(
            input=make_df(),
            contracts={"t": make_contract()},
            rules=Rules(),
            options=CleanOptions(strict=False),
        )
    )
    assert result.outputs[0].status == "success"
    assert result.outputs[0].meta.degraded is True
    assert any("boom" in w for w in result.outputs[0].report.warnings)


def test_config_error_propagates_even_in_lenient_mode():
    r = MiddlewareRegistryImpl()
    r.builtins()
    result = CleaningEngine(r).clean(
        CleanRequest(
            input=make_df(),
            contracts={"t": make_contract()},
            rules=Rules(),
            options=CleanOptions(
                strict=False,
                middleware_overrides={
                    "dedupe": __import__("darwin_cleaner", fromlist=["MiddlewareOverride"]).MiddlewareOverride(enabled=True)
                },
            ),
        )
    )
    assert result.outputs[0].status == "failed"
    assert any("keys" in e for e in result.outputs[0].report.errors)
