"""Basic usage: clean an in-memory DataFrame through the default middleware chain.

Usage:
    python examples/01_basic.py
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

import pandas as pd  # noqa: E402

from darwin_cleaner import (  # noqa: E402
    CleaningEngine,
    CleanRequest,
    Contract,
    FieldSpec,
    MiddlewareRegistryImpl,
    Rules,
    TypeSpec,
)


def main() -> None:
    df = pd.DataFrame(
        {
            "raw_symbol": ["sh.600000", "sz.000001"],
            "trade_date": ["2026-01-03", "2026-01-02"],
            "close": ["10.5", "20.3"],
        }
    )

    contract = Contract(
        name="quote",
        fields={
            "symbol": FieldSpec(name="symbol", type="str"),
            "trade_date": FieldSpec(name="trade_date", type="str"),
            "close": FieldSpec(name="close", type="float"),
        },
        types={"float": TypeSpec(name="float", converter="numeric", min=0)},
        output=("symbol", "trade_date", "close"),
    )
    rules = Rules(mapping={"raw_symbol": "symbol"})

    registry = MiddlewareRegistryImpl()
    registry.builtins()
    result = CleaningEngine(registry).clean(
        CleanRequest(input=df, contracts={"quote": contract}, rules=rules)
    )

    print(f"status: {result.status}")
    for out in result.outputs:
        print(f"output: {out.output_key} ({out.status})")
        print(out.data.to_string(index=False))
        print(f"data hash: {out.meta.data_hash}")
        print(f"rule signature: {out.meta.rule_signature}")


if __name__ == "__main__":
    main()
