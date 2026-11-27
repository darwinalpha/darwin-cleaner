"""Read a single parquet file and inspect the generated meta.

Usage:
    python examples/02_parquet.py
"""

from __future__ import annotations

import sys
import tempfile
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
    SortSpec,
)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="dc_example_") as tmp:
        path = Path(tmp) / "raw.parquet"
        pd.DataFrame(
            {
                "symbol": ["sh.600000", "sz.000001"],
                "trade_date": ["2026-01-03", "2026-01-02"],
                "close": [10.5, 20.3],
            }
        ).to_parquet(path, index=False)

        contract = Contract(
            name="quote",
            fields={
                "symbol": FieldSpec(name="symbol", type="str"),
                "trade_date": FieldSpec(name="trade_date", type="str"),
                "close": FieldSpec(name="close", type="float"),
            },
            output=("symbol", "trade_date", "close"),
            sort=(SortSpec(field="trade_date"),),
        )

        registry = MiddlewareRegistryImpl()
        registry.builtins()
        result = CleaningEngine(registry).clean(
            CleanRequest(input=path, contracts={"quote": contract}, rules=Rules())
        )

        for out in result.outputs:
            print(f"output: {out.output_key} ({out.status})")
            print(out.data.to_string(index=False))
            meta = out.meta
            print(f"rows: {meta.rows}, columns: {meta.columns}")
            print(f"data hash: {meta.data_hash}")
            print(f"rule signature: {meta.rule_signature}")
            print(f"engine version: {meta.engine_version}")
        # The temp parquet is removed here. In a real app the caller
        # decides where to persist the cleaned data.


if __name__ == "__main__":
    main()
