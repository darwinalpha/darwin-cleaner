"""内置步骤测试：逐个验证关键行为。"""

import pandas as pd
import pytest

from darwin_cleaner import Contract, FieldSpec, SortSpec, TransformSpec, TypeSpec, Rules
from darwin_cleaner.context import CleanContext
from darwin_cleaner.exceptions import CleanerConfigError
from darwin_cleaner.results import CleanReport
from darwin_cleaner.steps import (
    consistency,
    dedupe,
    field_mapping,
    field_transform,
    fill,
    formula_compute,
    output,
    sort,
    type_conversion,
    type_validate,
    value_mapping,
)


def make_ctx(contract: Contract, rules: Rules | None = None) -> CleanContext:
    return CleanContext(
        output_key="t",
        contract=contract,
        rules=rules or Rules(),
        report=CleanReport(output_key="t"),
    )


def test_field_mapping():
    df = pd.DataFrame({"a": [1]})
    out = field_mapping(df, make_ctx(Contract(), Rules(mapping={"a": "b"})), {})
    assert "b" in out.columns and "a" not in out.columns


def test_value_mapping():
    df = pd.DataFrame({"s": ["Suspended", "Normal"]})
    out = value_mapping(
        df, make_ctx(Contract(), Rules(value_map={"s": {"Suspended": "停牌"}})), {}
    )
    assert out["s"].tolist() == ["停牌", "Normal"]


def test_field_transform_factor_and_value():
    rules = Rules(
        transform={
            "volume": TransformSpec(kind="factor", factor=100),
            "duration": TransformSpec(kind="value", value=240),
        }
    )
    df = pd.DataFrame({"volume": [2.0]})
    out = field_transform(df, make_ctx(Contract(), rules), {})
    assert out["volume"].iloc[0] == 200.0
    assert out["duration"].iloc[0] == 240


def test_field_transform_unknown_action_raises():
    rules = Rules(transform={"s": TransformSpec(kind="action", action="nope")})
    with pytest.raises(CleanerConfigError):
        field_transform(pd.DataFrame({"s": ["x"]}), make_ctx(Contract(), rules), {})


def test_type_conversion():
    c = Contract(
        fields={"x": FieldSpec(name="x", type="int")},
        types={"int": TypeSpec(name="int", converter="numeric")},
    )
    out = type_conversion(pd.DataFrame({"x": ["1", "2"]}), make_ctx(c), {})
    assert pd.api.types.is_numeric_dtype(out["x"])


def test_fill_grouped_ffill_and_stats():
    c = Contract(
        fields={"v": FieldSpec(name="v", type="float", fill=__import__(
            "darwin_cleaner", fromlist=["FillSpec"]
        ).FillSpec(strategy="ffill", max_days=2))}
    )
    df = pd.DataFrame({"symbol": ["a", "a", "b"], "v": [1.0, None, None]})
    ctx = make_ctx(c)
    out = fill(df, ctx, {})
    assert out["v"].iloc[1] == 1.0
    assert ctx.report.stats.nulls_filled["v"] == 1


def test_formula_compute_all_null():
    c = Contract(fields={"c": FieldSpec(name="c", type="float", formula="a + b")})
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [None, None]})
    ctx = make_ctx(c)
    out = formula_compute(df, ctx, {})
    assert out["c"].tolist() == [4, 6]
    assert ctx.report.stats.computed_fields["c"] == 2


def test_formula_compute_partial_null_only_warns():
    c = Contract(fields={"c": FieldSpec(name="c", type="float", formula="a + b")})
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [None, 99]})
    ctx = make_ctx(c)
    out = formula_compute(df, ctx, {})
    assert out["c"].iloc[0] is None or pd.isna(out["c"].iloc[0])
    assert ctx.report.warnings


def test_consistency_masks_bad_rows():
    c = Contract(fields={"close": FieldSpec(name="close", type="float", consistency=("high_ge_low",))})
    df = pd.DataFrame({"high": [10, 5], "low": [8, 6], "close": [9, 5.5]})
    ctx = make_ctx(c)
    out = consistency(df, ctx, {})
    assert pd.isna(out["close"].iloc[1])
    assert ctx.report.stats.outliers_corrected["high_ge_low"] == 1


def test_type_validate_masks_out_of_range():
    c = Contract(
        fields={"x": FieldSpec(name="x", type="pct")},
        types={"pct": TypeSpec(name="pct", min=0, max=100)},
    )
    df = pd.DataFrame({"x": [-1, 50, 101, float("inf")]})
    ctx = make_ctx(c)
    out = type_validate(df, ctx, {})
    assert out["x"].iloc[0] is None or pd.isna(out["x"].iloc[0])
    assert out["x"].iloc[1] == 50
    assert pd.isna(out["x"].iloc[2])
    assert pd.isna(out["x"].iloc[3])


def test_dedupe_default_keys_and_strategies():
    c = Contract(
        fields={"k": FieldSpec(name="k", type="str"), "v": FieldSpec(name="v", type="int")},
        output=("k", "v"),
        keys=("k",),
    )
    df = pd.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]})
    ctx = make_ctx(c)
    out = dedupe(df, ctx, {"strategy": "last"})
    assert len(out) == 2
    assert out[out["k"] == "a"]["v"].iloc[0] == 2
    assert ctx.report.stats.duplicates_removed == 1


def test_dedupe_error_strategy_raises():
    c = Contract(
        fields={"k": FieldSpec(name="k", type="str")},
        output=("k",),
        keys=("k",),
    )
    with pytest.raises(CleanerConfigError):
        dedupe(pd.DataFrame({"k": ["a", "a"]}), make_ctx(c), {"strategy": "error"})


def test_output_required_missing_raises_optional_fills():
    c = Contract(
        fields={
            "symbol": FieldSpec(name="symbol", type="str"),
            "note": FieldSpec(name="note", type="str"),
        },
        output=("symbol", "note"),
        keys=("symbol",),
    )
    with pytest.raises(CleanerConfigError):
        output(pd.DataFrame({"note": ["x"]}), make_ctx(c), {})

    out = output(pd.DataFrame({"symbol": ["s"]}), make_ctx(c), {})
    assert "note" in out.columns and pd.isna(out["note"].iloc[0])


def test_output_warn_on_missing_param():
    c = Contract(
        fields={"a": FieldSpec(name="a", type="str"), "b": FieldSpec(name="b", type="str")},
        output=("a", "b"),
    )
    ctx = make_ctx(c)
    output(pd.DataFrame({"a": ["x"]}), ctx, {"warn_on_missing": True})
    assert any("b" in w for w in ctx.report.warnings)


def test_sort_multiple_fields():
    c = Contract(sort=(SortSpec("symbol"), SortSpec("date", direction="descending")))
    df = pd.DataFrame({"symbol": ["b", "a", "a"], "date": ["2026-01-01", "2026-01-01", "2026-01-02"]})
    out = sort(df, make_ctx(c), {})
    assert out["symbol"].tolist() == ["a", "a", "b"]
    assert out["date"].iloc[0] == "2026-01-02"
