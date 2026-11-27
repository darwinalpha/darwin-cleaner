"""Enable the built-in dedupe step (off by default) via a request override.

Usage:
    python examples/05_dedupe.py
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
    CleanOptions,
    CleanRequest,
    Contract,
    FieldSpec,
    MiddlewareOverride,
    MiddlewareRegistryImpl,
    Rules,
    SortSpec,
)


def main() -> None:
    df = pd.DataFrame(
        {
            "symbol": ["sh.600000", "sz.000001", "sh.600000"],
            "trade_date": ["2026-01-03", "2026-01-02", "2026-01-03"],
            "close": [10.5, 20.3, 11.0],
        }
    )

    contract = Contract(
        name="quote",
        fields={
            "symbol": FieldSpec(name="symbol", type="str"),
            "trade_date": FieldSpec(name="trade_date", type="str"),
            "close": FieldSpec(name="close", type="float"),
        },
        output=("symbol", "trade_date", "close"),
        keys=("symbol", "trade_date"),  # keys are implicitly required
        sort=(SortSpec(field="symbol"),),
    )

    registry = MiddlewareRegistryImpl()
    registry.builtins()
    print("default enabled steps:", [s.name for s in registry.resolve()])

    result = CleaningEngine(registry).clean(
        CleanRequest(
            input=df,
            contracts={"quote": contract},
            rules=Rules(),
            options=CleanOptions(
                middleware_overrides={
                    "dedupe": MiddlewareOverride(
                        enabled=True,
                        params={"scope": "keys", "strategy": "last", "warn": True},
                    ),
                }
            ),
        )
    )

    for out in result.outputs:
        print(f"\noutput: {out.output_key} ({out.status})")
        print(out.data.to_string(index=False))
        print(f"duplicates removed: {out.meta.stats.duplicates_removed}")
        print(f"warnings: {out.report.warnings}")


if __name__ == "__main__":
    main()
