"""Custom middleware: declarative params, directory loading and overrides.

Usage:
    python examples/04_middleware.py
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
    TypeSpec,
    load_middlewares_from_dir,
    middleware,
)


@middleware(order=80, version="1.0", params={"column": "note", "value": "checked"})
def add_note(df: pd.DataFrame, ctx, params: dict) -> pd.DataFrame:
    """A custom step that adds a constant column; params are declarative."""
    df[params["column"]] = params["value"]
    return df


def main() -> None:
    df = pd.DataFrame({"close": [10.5, -1.0, 20.3]})

    contract = Contract(
        name="quote",
        fields={
            "close": FieldSpec(name="close", type="float"),
            "note": FieldSpec(name="note", type="str"),
        },
        output=("close", "note"),
        types={"float": TypeSpec(name="float", converter="numeric")},
    )

    registry = MiddlewareRegistryImpl()
    registry.builtins()
    registry.register(add_note)

    # Load user middlewares from a directory; registration is explicit.
    specs = load_middlewares_from_dir(HERE / "custom")
    for spec in specs:
        registry.register(spec)
    print(f"loaded user middlewares: {[s.name for s in specs]}")

    result = CleaningEngine(registry).clean(
        CleanRequest(
            input=df,
            contracts={"quote": contract},
            rules=Rules(),
            options=CleanOptions(
                middleware_overrides={
                    # Change a registered middleware's params for this request.
                    "add_note": MiddlewareOverride(
                        params={"column": "note", "value": "verified"}
                    ),
                }
            ),
        )
    )

    print(f"status: {result.status}")
    for out in result.outputs:
        if out.status != "success":
            print(f"output {out.output_key} failed: {out.report.errors}")
            continue
        print(out.data.to_string(index=False))
        print(f"rule signature: {out.meta.rule_signature}")


if __name__ == "__main__":
    main()
