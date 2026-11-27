"""Row forks and column forks: one raw file, several semantic outputs.

A row fork filters rows by a regex on a raw column; a column fork projects
different columns from the same row group. Forks are executed before the
middleware chain, so filter fields must use raw column names.

Usage:
    python examples/03_forks.py
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
    RowForkSpec,
    Rules,
)


def make_contract(fields: tuple[str, ...]) -> Contract:
    return Contract(
        name=",".join(fields),
        fields={name: FieldSpec(name=name, type="str") for name in fields},
        output=fields,
    )


def main() -> None:
    df = pd.DataFrame(
        {
            "code": ["sh.600000", "sh.510300", "sz.000001", "sz.159915"],
            "name": ["Pudong Bank", "CSI300 ETF", "Ping An Bank", "ChiNext ETF"],
        }
    )

    # Row fork: split stock codes from ETF codes by regex on the raw column.
    rules = Rules(
        filters={
            "stock": RowForkSpec(
                key="stock",
                field="code",
                regex=r"^(sh\.60|sz\.00)",
                outputs=("stock",),
            ),
            "etf": RowForkSpec(
                key="etf",
                field="code",
                regex=r"^(sh\.51|sz\.15)",
                outputs=("etf",),
            ),
        }
    )
    contracts = {
        "stock": make_contract(("code", "name")),
        "etf": make_contract(("code", "name")),
    }

    registry = MiddlewareRegistryImpl()
    registry.builtins()
    result = CleaningEngine(registry).clean(
        CleanRequest(input=df, contracts=contracts, rules=rules)
    )

    print(f"status: {result.status}")
    for out in result.outputs:
        print(f"\noutput: {out.output_key} ({out.status})")
        print(out.data.to_string(index=False))


if __name__ == "__main__":
    main()
