# darwin-cleaner examples

[English](README.md) · [简体中文](README.cn.md)

Runnable examples for the single-file cleaning engine. Each script is self-contained: it inserts `src/` into `sys.path`, so no installation is required.

## Run

From the repository root:

```bash
python examples/demo.py       # run every example
python examples/01_basic.py   # run one example
```

After installation, any script can also be run directly with `python examples/01_basic.py`.

## Examples

| Script | What it shows |
|---|---|
| `01_basic.py` | In-memory DataFrame input, field mapping, type conversion and output contract |
| `02_parquet.py` | Reading a single parquet file, content hash and rule fingerprint in meta |
| `03_forks.py` | Row forks (regex on raw columns) and column forks (multiple contracts per row group) |
| `04_middleware.py` | Custom middleware with declarative params and loading middlewares from a directory |
| `05_dedupe.py` | Enabling the built-in `dedupe` step and choosing a dedupe strategy |

The `custom/` directory holds a sample user middleware module used by `04_middleware.py`.

All scripts only produce in-memory results. Writing parquet, csv or meta files is the caller's responsibility.
