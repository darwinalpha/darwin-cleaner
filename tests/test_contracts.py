"""Contract value objects: tri-state required, unique-row keys and output order."""

from darwin_cleaner import Contract, FieldSpec, SortSpec, TypeSpec


def make_contract(**overrides):
    base = dict(
        name="t",
        fields={
            "symbol": FieldSpec(name="symbol", type="str"),
            "trade_date": FieldSpec(name="trade_date", type="str"),
            "close": FieldSpec(name="close", type="float"),
            "note": FieldSpec(name="note", type="str"),
        },
        types={"float": TypeSpec(name="float", converter="numeric")},
        output=("symbol", "trade_date", "close", "note"),
    )
    base.update(overrides)
    return Contract(**base)


def test_required_defaults_to_false():
    c = make_contract()
    assert c.is_required("close") is False
    assert c.is_required("note") is False


def test_sort_and_keys_auto_required():
    c = make_contract(
        keys=("symbol", "trade_date"),
        sort=(SortSpec(field="close", direction="descending"),),
    )
    assert c.is_required("symbol") is True
    assert c.is_required("trade_date") is True
    assert c.is_required("close") is True
    assert c.is_required("note") is False


def test_explicit_required_overrides_derivation():
    c = make_contract(
        fields={
            "symbol": FieldSpec(name="symbol", type="str", required=False),
            "trade_date": FieldSpec(name="trade_date", type="str"),
            "close": FieldSpec(name="close", type="float"),
            "note": FieldSpec(name="note", type="str"),
        },
        keys=("symbol",),
    )
    assert c.is_required("symbol") is False  # an explicit declaration takes precedence
    assert c.is_required("trade_date") is False


def test_output_specs_follow_output_order():
    c = make_contract(output=("close", "symbol"))
    assert [s.name for s in c.output_specs()] == ["close", "symbol"]


def test_dedupe_keys_returns_contract_keys():
    c = make_contract(keys=("symbol", "trade_date"))
    assert c.dedupe_keys() == ("symbol", "trade_date")
