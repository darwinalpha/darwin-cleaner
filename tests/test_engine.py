"""引擎端到端测试：分叉、错误处理、parquet 读取、指纹。"""

import pandas as pd
import pytest

from darwin_cleaner import (
    CleanOptions,
    CleanRequest,
    CleaningEngine,
    Contract,
    FieldSpec,
    MiddlewareOverride,
    MiddlewareRegistryImpl,
    RowForkSpec,
    Rules,
    SortSpec,
    TypeSpec,
)


def make_contract(name="t", **overrides):
    base = dict(
        name=name,
        fields={
            "symbol": FieldSpec(name="symbol", type="str"),
            "trade_date": FieldSpec(name="trade_date", type="str"),
            "close": FieldSpec(name="close", type="float"),
        },
        types={"float": TypeSpec(name="float", converter="numeric", min=0)},
        output=("symbol", "trade_date", "close"),
        sort=(SortSpec("symbol"),),
    )
    base.update(overrides)
    return Contract(**base)


def make_engine():
    r = MiddlewareRegistryImpl()
    r.builtins()
    return CleaningEngine(r)


def sample_df():
    return pd.DataFrame(
        {
            "raw_symbol": ["sh.600000", "sz.000001", "sh.600000"],
            "trade_date": ["2026-01-03", "2026-01-02", "2026-01-03"],
            "close": ["10.5", "20.3", "10.5"],
        }
    )


def test_end_to_end_with_mapping_and_dedupe():
    opts = CleanOptions(
        middleware_overrides={
            "dedupe": MiddlewareOverride(enabled=True, params={"strategy": "last", "warn": True})
        }
    )
    result = make_engine().clean(
        CleanRequest(
            input=sample_df(),
            contracts={"t": make_contract(keys=("symbol", "trade_date"))},
            rules=Rules(mapping={"raw_symbol": "symbol"}),
            options=opts,
        )
    )
    assert result.status == "success"
    assert result.success
    assert len(result.outputs) == 1
    out = result.outputs[0]
    assert out.status == "success"
    assert len(out.data) == 2
    assert out.meta.stats.duplicates_removed == 1
    assert out.meta.rows == 2
    assert out.meta.data_hash.startswith("sha256:")
    assert out.meta.rule_signature
    assert any("dedupe" in w for w in out.report.warnings)


def test_row_fork_and_column_fork():
    """一份输入按行分叉成两组，每组产出两个不同列集的输出。"""
    df = pd.DataFrame(
        {
            "code": ["600000", "000001", "510300"],
            "name": ["浦发", "平安", "ETF"],
            "close": ["10", "20", "3"],
        }
    )
    c1 = Contract(
        name="stock",
        fields={
            "code": FieldSpec(name="code", type="str"),
            "close": FieldSpec(name="close", type="float"),
        },
        types={"float": TypeSpec(name="float", converter="numeric")},
        output=("code", "close"),
    )
    c2 = Contract(
        name="meta",
        fields={
            "code": FieldSpec(name="code", type="str"),
            "name": FieldSpec(name="name", type="str"),
        },
        output=("code", "name"),
    )
    rules = Rules(
        filters={
            "stk": RowForkSpec(key="stk", field="code", regex="^(60|00)", outputs=("stock", "meta")),
            "etf": RowForkSpec(key="etf", field="code", regex="^51", outputs=("stock", "meta")),
        }
    )
    result = make_engine().clean(
        CleanRequest(input=df, contracts={"stock": c1, "meta": c2}, rules=rules)
    )
    assert result.status == "success"
    assert len(result.outputs) == 4
    keys = [(o.output_key, len(o.data)) for o in result.outputs]
    assert keys == [("stock", 2), ("meta", 2), ("stock", 1), ("meta", 1)]


def test_zero_row_output_fails():
    c = make_contract()
    # 行分叉发生在字段映射之前，需使用原始列名
    rules = Rules(
        mapping={"raw_symbol": "symbol"},
        filters={"none": RowForkSpec(key="none", field="raw_symbol", regex="^ZZZ", outputs=("t",))},
    )
    result = make_engine().clean(CleanRequest(input=sample_df(), contracts={"t": c}, rules=rules))
    assert result.status == "success"
    assert result.outputs[0].status == "failed"
    assert any("0 行" in e for e in result.outputs[0].report.errors)


def test_required_missing_column_fails():
    c = make_contract(keys=("symbol",))
    df = sample_df().drop(columns=["raw_symbol"])
    result = make_engine().clean(
        CleanRequest(input=df, contracts={"t": c}, rules=Rules())
    )
    assert result.outputs[0].status == "failed"
    assert any("required" in e or "缺失" in e for e in result.outputs[0].report.errors)


def test_empty_input_returns_failed_result():
    empty = pd.DataFrame({"a": []})
    result = make_engine().clean(
        CleanRequest(input=empty, contracts={"t": make_contract()}, rules=Rules())
    )
    assert result.status == "failed"
    assert result.outputs == ()
    assert any("0 行" in e for e in result.errors)


def test_parquet_input(tmp_path):
    path = tmp_path / "raw.parquet"
    sample_df().to_parquet(path, index=False)
    result = make_engine().clean(
        CleanRequest(
            input=path,
            contracts={"t": make_contract(keys=("symbol", "trade_date"))},
            rules=Rules(mapping={"raw_symbol": "symbol"}),
        )
    )
    assert result.status == "success"
    assert len(result.outputs[0].data) == 3


def test_signature_changes_with_overrides():
    engine = make_engine()
    mapping = Rules(mapping={"raw_symbol": "symbol"})
    req = CleanRequest(input=sample_df(), contracts={"t": make_contract()}, rules=mapping)
    s1 = engine.clean(req).outputs[0].meta.rule_signature
    req2 = CleanRequest(
        input=sample_df(),
        contracts={"t": make_contract()},
        rules=mapping,
        options=CleanOptions(middleware_overrides={"output": MiddlewareOverride(enabled=False)}),
    )
    s2 = engine.clean(req2).outputs[0].meta.rule_signature
    assert s1 != s2


def test_build_plan_is_side_effect_free():
    engine = make_engine()
    req = CleanRequest(input=sample_df(), contracts={"t": make_contract()}, rules=Rules())
    plan = engine.build_plan(req)
    assert len(plan) == 1
    assert [s.name for s in plan[0].steps][-1] == "sort"
