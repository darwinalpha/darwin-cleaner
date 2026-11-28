# darwin-cleaner

[English](https://github.com/darwinalpha/darwin-cleaner/blob/main/README.md) · [简体中文](https://github.com/darwinalpha/darwin-cleaner/blob/main/README.cn.md)

A contract- and rules-driven **single-file cleaning engine**: read one parquet file (or an in-memory DataFrame) → split rows / columns → run middlewares by `order` → return data and meta in memory.

Python >= 3.10.

```bash
pip install darwin-cleaner
```

## Scope

This library only transforms data. Upstream and downstream concerns stay outside:

| In the library | Outside (caller's responsibility) |
|---|---|
| Read a single parquet / accept a DataFrame | Merge multiple files, discover files, check freshness |
| Row forks / column forks | Load `cleaner.json` / `clean.d/*.json` / adapter configs |
| 12 built-in middlewares (`dedupe` disabled by default) | Write parquet / csv / meta / checkpoints |
| Contract and rules validation, stats and report generation | CLI, scheduling, exit codes, business identifiers |

See [docs/design.md](docs/design.md) for the design and [docs/clean-stage-code-walkthrough.md](docs/clean-stage-code-walkthrough.md) for a walkthrough of the original clean-stage code. Run the [examples](examples/README.md) to see the engine in action.

---

## 1. Quick start

```python
from pathlib import Path
from darwin_cleaner import (
    CleaningEngine, CleanRequest, CleanOptions, Contract, FieldSpec,
    MiddlewareOverride, MiddlewareRegistryImpl, Rules, TypeSpec,
)

registry = MiddlewareRegistryImpl()
registry.builtins()  # 12 built-in steps; dedupe is off by default

contract = Contract(
    name="stk_ohlcv",
    fields={
        "symbol": FieldSpec(name="symbol", type="str"),
        "trade_date": FieldSpec(name="trade_date", type="str"),
        "close": FieldSpec(name="close", type="float"),
    },
    types={"float": TypeSpec(name="float", converter="numeric", min=0)},
    output=("symbol", "trade_date", "close"),
    keys=("symbol", "trade_date"),  # columns in keys are implicitly required
)

engine = CleaningEngine(registry)
result = engine.clean(CleanRequest(
    input=Path("raw.parquet"),
    contracts={"stk_ohlcv": contract},
    rules=Rules(),
    options=CleanOptions(
        middleware_overrides={
            "dedupe": MiddlewareOverride(
                enabled=True,
                params={"scope": "keys", "strategy": "last"},
            ),
        },
    ),
))

for out in result.outputs:
    if out.status == "success":
        df = out.data        # pandas.DataFrame
        meta = out.meta      # content hash + rule fingerprint + stats
        report = out.report  # audit trail
        # Persistence is the caller's responsibility.
```

---

## 2. Core concepts

- **Contract** — fields, types, output columns, unique-row keys and sort order. The caller parses config files and injects contracts; the engine never reads configuration.
- **FieldSpec.required** — tri-state. `True`: a missing column is an error. `False`: explicitly optional (filled with nulls). `None`: not declared; the engine infers it — columns used in `sort` or `keys` become `True`, all others `False`.
- **Rules** — `mapping` / `value_map` / `transform` / `filters` / `outputs`, etc. They replace global config lookups inside middlewares.
- **Middleware** — unified signature `(df, ctx, params) -> df | None`. `order` decides execution position, `params` are declarative (and part of the rule fingerprint), `enabled` is the on/off switch, and `MiddlewareOverride` can change both per request.
- **Order bands** — `10-19` preprocessing, `20-29` dtype, `30-39` fill, `40-49` formulas, `50-69` validation / dedupe, `90-99` output.

---

## 3. Built-in middlewares

| Order | Name | Purpose | Default |
|---|---|---|---|
| 10 | `field_mapping` | Rename fields | on |
| 12 | `value_mapping` | Replace values | on |
| 15 | `field_transform` | Constants / scale / actions | on |
| 20 | `type_conversion` | Normalize dtypes | on |
| 30 | `fill` | Group-wise forward fill | on |
| 40 | `formula_compute` | Compute all-null columns from formulas | on |
| 45 | `formula_validate` | Validate formulas with dual tolerance | on |
| 50 | `consistency` | Cross-field consistency | on |
| 60 | `type_validate` | Null out inf / out-of-range values | on |
| 70 | `dedupe` | Deduplicate by keys | **off** |
| 90 | `output` | Select columns per contract | on |
| 95 | `sort` | Sort per contract | on |

`builtins()` registers all 12. Disabled middlewares are skipped at the registry level: they do not enter the plan and are never called. User middlewares can be loaded from a directory with `load_middlewares_from_dir(path)` and registered explicitly. Duplicate names raise `CleanerRuleError` at registration time.

---

## 4. Testing

```bash
PYTHONPATH=src pytest
```

The suite covers contracts, fork plans, the registry (built-ins, switches, overrides, conflicts, fingerprints, directory loading), all 12 built-in steps, end-to-end engine paths (row + column forks, parquet input, zero-row failure, missing required columns) and strict / lenient error modes.

---

## 5. License

MIT
