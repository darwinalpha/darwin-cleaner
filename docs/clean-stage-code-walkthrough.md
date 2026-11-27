# darwin-fetch Clean Stage: Code Logic Walkthrough

[English](clean-stage-code-walkthrough.md) · [简体中文](clean-stage-code-walkthrough.cn.md)

> This document is a read-only walkthrough of all clean-stage code in `darwin-fetch/`, intended to provide a complete logical baseline for the boundary design and extraction/migration of the new `darwin-cleaner` library.
> Scope: entry scripts, Shell batch scheduling, the core execution engine, contract loading, the middleware rule chain, all configuration, and upstream/downstream coupling points.
> All line numbers / function names / configuration keys in this document come from the source code; content prefixed with "Note" is the author's assessment, not a code fact.

---

## 0. One-sentence overview

The clean stage does one thing: **read a fetch done breakpoint → locate the raw parquet it points to → row-fork / column-fork that raw into several `clean_type`s according to the adapter configuration → run a config-driven middleware rule chain for each fork → write `data/clean/{ct}/{source}/{date_key}/{filename}.parquet` + a same-named `.json` meta + a clean breakpoint**.

Business logic hardly lives in the main flow; it lives in the middleware. The main flow only handles "read → fork → run chain → write".

---

## 1. File inventory and responsibilities

### 1.1 Python core (`src/darwin_fetch/cleaners/`)

| File | Lines | Responsibility |
|---|---|---|
| `cleaner.py` | ~496 | Cleaning scheduler. Loads cleaner.json, resolves the done path, reads done contents, decides the clean_type list, creates CleanTask, aggregates results and writes the clean breakpoint |
| `task.py` | ~504 | Single raw-file executor. Reads parquet, forks by filter/outputs, drives the middleware chain and writes parquet + meta |
| `contract.py` | ~274 | Contract reading (shared by Cleaner/Selector/Load). Loads stk_base + clean.d/{ct}.json and returns a frozen `CleanContract` |
| `context.py` | ~120 | Runtime context `CleanContext`, the ctx of the middleware signature `(df, ctx)`; `fork()` derives fork contexts |
| `result.py` | ~176 | `CleanOutput` (single output) / `CleanResult` (whole result for one raw); `finalize()` aggregates status and exit code |
| `report.py` | ~166 | `CleanReport`, the detailed audit record of a single clean_type (statistics, row-level violations, warnings, timing) |
| `exceptions.py` | ~60 | 5 custom exceptions + mapping to exit_code |
| `actions/print_clean_types.py` | ~57 | CLI action `--list_types`, prints available cleaning types |
| `middlewares/base.py` | ~139 | `@middleware` registration decorator, global registry, `get_middlewares()` sorted collection |
| `middlewares/__init__.py` | ~50 | imports all middleware modules to trigger registration |
| `middlewares/steps.py` | ~833 | Global generic rule chain (11 steps; the file header comment says "10 steps", which is outdated — 11 are actually registered) |
| `middlewares/stk_ohlcv.py` | ~51 | Template placeholder, currently registers no middleware |
| `middlewares/stk_limit.py` | ~44 | limit-up / limit-down invalid values set to NaN (specific to stk_limit) |
| `middlewares/gold_sge.py` | ~43 | keep only the gold main contract Au99.99 (specific to gold_sge) |
| `middlewares/trd_calendar.py` | ~120 | trading-calendar UTC millisecond boundaries + previous trading day (specific to trd_calendar) |
| `middlewares/stk_code.py` | ~126 | market / exchange inference for A-share codes (specific to stk_code) |
| `middlewares/stk_hkcode.py` | ~38 | market normalization for HK stock codes (specific to stk_hkcode) |
| `middlewares/ftshare.py` | ~68 | ftshare source backfills trade_date / symbol (global mw judged by ctx.source) |
| `middlewares/json_extract.py` | ~63 | extract fields from a JSON column by path (global config-driven) |
| `middlewares/ohlcvault.py` | ~49 | ohlcvault source backfills symbol (global mw judged by ctx.source) |

### 1.2 Scripts and utilities (`scripts/`)

| File | Responsibility |
|---|---|
| `scripts/clean.py` | single-task CLI entry: bootstrap → Cleaner → parse_cli → clean → write_done → return exit_code |
| `scripts/clean_batch.sh` | batch scheduling: iterate over fetch dones, multi-condition filtering and call the wrapper in parallel |
| `scripts/lib/clean_common.sh` | common batch logic: argument parsing, push-down of `find -regex` filters, incremental checks, single-task execution and statistics |
| `scripts/utils/clean_wrapper.sh` | single-task wrapper: idempotence check, tmp done + atomic mv, routing success/skip/fail by exit_code |
| `scripts/utils/clean_types.py` | reverse lookup: find (source, raw_type) pairs for a clean_type and list all clean_types |
| `scripts/utils/clean_parquet_scan.py` | scan `data/clean/` and output (date, ct, max_mtime) TSV for freshness determination on the select side |
| `scripts/utils/dump_clean_config.py` | debugging: preload the config tree in clean.py order and print it (including `_merge` / `_imports` expansion results) |

### 1.3 Configuration

| File | Responsibility |
|---|---|
| `cleaners/cleaner.json` | global config: CLI parameter definitions and output base_dir |
| `cleaners/clean.d/stk_base.json` | base contract: 50+ field definitions + 16 type definitions (converter/min/max/formula/consistency) |
| `cleaners/clean.d/*.json` (32 files: stk_base + stk_meta as the two bases + 30 concrete contracts) | per-clean_type contract: output columns, sort, field overrides and exchange |
| `adapters/{source}/adapter_clean.json` (5 files) | source-side mapping: mapping / transform / value_map / json_extract / types fork rules |

### 1.4 Key upstream/downstream coupling files

| File | Coupling point |
|---|---|
| `utils/bootstrap.py` | provides `PACKAGE_ROOT`/`PROJECT_ROOT`/`add_config`/`get_config`/`try_config`/`to_rel` |
| `selectors/config.py` | directly `from darwin_fetch.cleaners import contract as clean_contract`, reusing `CleanContract`/`FieldSpec` |
| `selectors/selector.py` | reads `cleaner.output.base_dir` to locate data/clean |
| `selectors/stages/normalize.py` | directly scans the `data/clean/{ct}/{source}/{date_key}/` directory structure |
| `darwin-common/store/parquet/*.py` | the meta suffix of generic parquet storage is `.meta.json`, while clean writes `.json` |

---

## 2. Architecture and data flow

```
fetch done (data/done/fetch/{source}/{raw_type}/{date_key}/{date}.json)
   │  contains status / raw_data_path / rows / data_hash
   ▼
[scripts/clean.py]  bootstrap → Cleaner() → parse_cli → cleaner.clean()
   ▼
[Cleaner.clean]
   ├─ _parse_done_path()  → source / raw_type / date_key / filename / date
   ├─ read done JSON      → raw_path / raw_rows / raw_hash; status=empty → skip directly (exit 5)
   ├─ add_config(adapter_clean.json)
   ├─ _resolve_clean_types() → clean_type list (filter / outputs / bare list)
   ├─ clean_contract.ensure_loaded(ct) × N
   └─ _clean_one() → CleanTask(ctx).run() → List[CleanReport]
   ▼
[CleanTask.run]
   ├─ load_data()         → pd.read_parquet(raw_path)
   ├─ _resolve_groups()   → {group: {field, regex, exclude_regex, outputs}}
   └─ for each group _run_group():
        ├─ _apply_filter()      → row fork (field/regex/exclude_regex, may overlap)
        └─ for each ct in group outputs _fork_one(sub.copy(), ct):
             ├─ ctx.fork(ct, output_path)
             ├─ _run_middlewares() → get_middlewares(ct) executed in order
             └─ save_data()       → tmp write + rename → .parquet + .json meta
   ▼
[CleanResult.finalize] aggregate outputs → status / exit_code
   ▼
[scripts/clean.py]  write_done(result) → clean breakpoint (tmp + rename)
```

**Key design**: one raw file may produce multiple clean_types; one clean_type is one independent output file, runs the complete middleware chain and has its own report.

---

## 3. Configuration system (three-layer config tree)

All configuration is mounted to the global config tree through `darwin-common`'s `add_config()` and read via `get_config("a.b.c")` / `try_config(*paths)`. Three key expansion mechanisms:

- `_imports`: a file immediately imports other config (e.g. `stk_ohlcv.json` importing `stk_base.json`)
- `_merge`: `"${_self.xxx}"` references other nodes in the same config tree for deep merging (e.g. the hk contract merging the stk_ohlcv contract)
- `_mount`: specifies the root path at which this JSON is mounted in the config tree (e.g. `cleaner.clean_type`, `adapter.tushare`)

Load order (completed in clean.py by Cleaner and each module as needed):

1. `bootstrap()` → library default `config/settings.json` + `conf.d/${APP_ENV}.json` + user `config/settings.json`
2. `Cleaner.__init__` → `cleaner.json` (`_mount: cleaner`, containing cli_args and output.base_dir)
3. `Cleaner.clean` → `adapters/{source}/adapter_clean.json` (`_mount: adapter.{source}`)
4. `clean_contract.ensure_loaded(ct)` → `stk_base.json` + `clean.d/{ct}.json` (both `_mount: cleaner.clean_type`)

### 3.1 cleaner.json

```
_mount: cleaner
cli_args:  fetch_done(required) / clean_type / overwrite / check / csv / outfile / done_path / list_types(action=print_clean_types)
output.base_dir: data/clean
```

### 3.2 Four key configuration sections of adapter_clean.json

Taking tushare as an example (`_mount: adapter.tushare`):

```jsonc
{
  "clean": {
    "field": {
      "mapping":   { "ts_code": "symbol", "pre_close": "close_adj", ... },  // source field → contract field
      "transform": { "volume": {"factor":100}, "amount": {"factor":1000}, ... } // unit / constant / action
    },
    "types": {
      "stock_daily": ["stk_ohlcv"],                    // format ① bare list
      "stock_market_daily": {                          // format ② dict + outputs
        "outputs": ["stk_market"],
        "mapping": {"_merge":"${_self.clean.field.mapping}", ...},
        "transform": {...},
        "value_map": {"market_name": {...}}            // value mapping: source value → standard value
      },
      "fund_code": {                                   // format ③ dict + filter (row fork)
        "filter": {
          "etf": {"field":"name", "regex":"^(?!.*联接)(?=.*ETF).*$", "outputs":["etf_code"]}
        },
        "mapping": {...}, "value_map": {...}
      },
      "stock_market_hist": {"_merge":"${_self.clean.types.stock_market_daily}"}, // reuse another raw_type
      ...
    }
  }
}
```

`value_map` is configured (all of tushare/baostock/ftshare/ohlcvault/tickflow have it), as is `json_extract` (used by ftshare's stock_code).
The `exclude_regex` field is read in `_apply_filter` of task.py, but is not actually used in any of the 5 existing adapter configs.

### 3.3 Inheritance of clean.d contracts

- `stk_base.json`: `_mount: cleaner.clean_type`, contents hanging under the `base` key → `cleaner.clean_type.base.fields` / `.types`
- ordinary contracts (e.g. `stk_ohlcv.json`): `_imports` brings in base, contents hanging under the same-named key → `cleaner.clean_type.stk_ohlcv`
- derived contracts (`stk_ohlcv_hk/us.json`): `_imports` brings in `stk_ohlcv.json`, uses `_merge: ${_self.stk_ohlcv}` to inherit everything and overrides only `exchange` (HKEX/NYSE)
- `stk_meta.json`: `_imports` brings in `stk_base.json`, extends `base.fields` (adding cal_date/is_open/trade_date_prev/various ms fields etc., 23 fields) and mounts to `cleaner.clean_type.base` (deep-merged with stk_base's base → all clean_types can use these meta fields)
- 3 contracts carry an `exchange` field: `stk_hkcode`(HKEX), `stk_ohlcv_hk`(HKEX), `stk_ohlcv_us`(NYSE). This field is currently read mainly by `clean_parquet_scan.py` for date-range expansion.

---

## 4. Entry and scheduling layer logic

### 4.1 scripts/clean.py (single-task entry)

```
main(args):
1. bootstrap()                   failure → return 1
2. Cleaner()                     loads cleaner.json, failure → return 1
3. parse_cli_safe(args, "cleaner.cli_args", ...)
   - parameter definitions come from cli_args in cleaner.json
   - --list_types triggers the action print_clean_types → prints and sys.exit(0)
   - final_args is None → return the action's exit_code
4. cleaner.clean(final_args)     → CleanResult (throws no exceptions internally)
   - KeyboardInterrupt → return 130
5. if --check: print(json.dumps(result.to_dict())), do not write done
   otherwise cleaner.write_done(result); if writing fails and the result was originally successful → change exit_code=1
6. return result.exit_code
```

### 4.2 Cleaner.clean() (core scheduling)

Key fields of the input final_args: `fetch_done`(required) / `clean_type`(only run a single type) / `overwrite` / `csv` / `check` / `outfile` / `done_path`(the tmp path passed by the wrapper).

```
result initialization: done_path / fetch_done_path / started_at / start_ts
try:
  1. _parse_done_path(fetch_done)
     - find the "fetch" segment in the path; the following four segments are source/raw_type/date_key/filename
     - date = the last segment of date_key split by "_" (single day = itself; a range takes the end date)
     - validation: no "fetch" segment in the path, or too few segments → CleanerConfigError
  2. read done JSON
     - status == "empty" → record warning, status=skipped/exit 5/success=True, return directly
     - raw_data_path ← raw_data_path || data_path || raw_path (three keys accepted)
     - missing → CleanerConfigError; if raw_path is relative, join PROJECT_ROOT
     - file does not exist → CleanerDataNotFoundError
     - result.raw_rows = rows; result.raw_hash = data_hash
  3. add_config(adapters/{source}/adapter_clean.json); absent → CleanerConfigError
  4. source_config = adapter.{source}.clean
     clean_types = _resolve_clean_types(source_config, raw_type, only_clean_type)
     - supports three formats: bare list / dict.filter / dict.outputs
     - only_clean_type not in the list → CleanerConfigError
     - empty → CleanerDataNotFoundError
  5. for ct in clean_types: clean_contract.ensure_loaded(ct)
  6. outputs = _clean_one(...)  → internally CleanTask.run() → reports → list of CleanOutput
  7. result.finalize()
except CleanerDataNotFoundError → skipped/exit 5/success=True
except CleanerConfigError        → failed/exit 2/success=False
except Exception                 → failed/exit 1/success=False
finally: finished_at / duration_seconds
return result
```

**Note**: `CleanerDataNotFoundError` is treated as "skip" at the Cleaner layer (success=True, exit 5) and also as "skip" at the CleanTask layer. This means "no data / no contract" is not considered a failure.

### 4.3 _resolve_clean_types (deciding the clean_type list)

```
types_map = adapter.{source}.clean.types
type_cfg = types_map[raw_type]
if type_cfg is a list            → clean_types = that list (old format)
elif type_cfg is a dict:
    filters = type_cfg.filter
    outputs = type_cfg.outputs
    if filters is a non-empty dict       → iterate over each group's outputs and merge (row fork)
    elif outputs is a non-empty list     → clean_types = outputs (pure column fork)
if only_clean_type:
    not in clean_types → CleanerConfigError; otherwise return only it
return clean_types
```

Note: this determination is consistent with `clean_types.py::_clean_types_of` (used by the tool's reverse lookup).

### 4.4 Cleaner.write_done() (writing the clean breakpoint)

```
1. no fetch_done_path → return
2. target path:
   - an externally specified done_path (the wrapper's tmp) takes priority
   - otherwise replace the "fetch" segment in the fetch_done path with "clean"
3. skipped protection: if result.status=skipped and an existing breakpoint has status=success → do not overwrite (preserve the fact of the first success)
4. atomic write: write {clean_done}.tmp.{pid} → json.dump(result.to_dict()) → replace
   clean up tmp on exception
```

### 4.5 scripts/clean_batch.sh (batch scheduling main flow)

```
main:
1. clean_init()  → bootstrap_init + FAIL_FAST_FLAG_FILE
2. cli_parse_common (generic params such as --notify)
3. cli_parse_clean_common (clean-specific params)
4. resolve_clean_type_pairs
   - if --clean_type CT is given: call clean_types.py CT to get (source,raw_type) pairs → CLEAN_TYPE_PAIRS
5. list_pending_fetch_dones → pending[]
6. dry_run? print (only the first 20 when >20) then exit
7. worker_run:
   - tasks_file = pending
   - handler = clean_worker_handler → clean_run_one
   - max-parallel N (default 5)
   - skip-codes 5,6 (skip codes are not counted as failures)
   - fail-fast-code 2 (a config error triggers fail_fast)
```

**Key points of argument parsing** (`cli_parse_clean_common`):
- filter params: `--source/--no_source`, `--raw_type/--no_raw_type`, `--date/--no_date` (handled by the common layer), `--include_dates`, `--pattern/--no_pattern`, `--clean_type`
- mode params (choose one of three; multiple → error): `--all` (full coverage) / `--newer` (fetch done newer than clean done) / `--change` (compare data_hash, precise)
- others: `--limit` (first N per (source,raw_type)), `--max-parallel`, `--fail_fast`, `--check`, `--dry_run/--fake`
- default mode: run clean dones that do not exist or have status=failed (first run + retry of failures)

### 4.6 clean_common.sh's filtering architecture

Core idea: **everything except include_dates / newer / change / limit is pushed down to `find -regex`** (stated explicitly in the file header comment).

- `_regex_or_group`: comma-separated multiple values → `(a|b)` ERE group
- `_clean_type_regex_group`: CLEAN_TYPE_PAIRS (`src/rt,src/rt`) → `(src1/rt1|src2/rt2)`
- `_ere_escape`: escape ERE metacharacters (used for fixed patterns)
- `_pattern_token_to_regex`: 4 pattern syntaxes
  - default (no prefix) → fixed substring `.*xxx.*` (grep -F semantics)
  - `re:XXX` → extended regex `.*XXX.*`
  - `glob:XXX` → after escaping, `*`→`.`, `?`→`.` (approximate glob)
  - `exact:XXX` → `^XXX$`, exact whole path
  - multiple PATs comma-separated, OR semantics (any hit passes)
- `_fetch_done_regex_args` / `_fetch_done_no_regex_args`: build positive / negative find arguments
  - positive: the first segment is `.*/fetch/`, building path levels by source/raw_type/clean_type/date, with pattern as a separate -regex (AND)
  - negative: `! -regex` excludes source/raw_type/date/pattern respectively
- `list_pending_fetch_dones`:
  - `--change` goes through `_change_candidates` (newer filter first, then compare hash, see below)
  - `--limit` uses one find + an associative array counting by `(source,raw_type)`
  - default/--all/--newer: one find + while post-processing
  - all branches exclude `fail.*` and `skip.*` files
  - clean done determination: `clean_done_is_done` = a real done exists **or** `skip.{basename}` exists (i.e. the skip marker also counts as done)

**--change incremental logic** (`_change_candidates`):
1. collect the mtime of fetch done and clean done
2. awk computes newer candidates (clean does not exist or fetch is newer)
3. jq extracts the candidates' fetch `.data_hash` and the corresponding clean `.input.raw_hash`
4. awk compares: clean absent → process; both hashes non-empty and equal → skip; otherwise process
5. the temporary directory `_CHANGE_TMP_DIR` is cleaned up by `change_cleanup`

**--include_dates logic** (range-inclusion semantics, inexpressible with regex, post-processing):
- `_parse_date_key_range`: date_key contains `_` → split into start/end; otherwise a single day
- `_validate_include_dates`: each date must be 8 digits (YYYY-MM-DD allowed)
- `_match_include_dates`: a target date falling in the closed interval `[start,end]` is a hit

**clean_run_one (single task)**:
```
1. fail_fast flag exists → return 6
2. args = (--fetch_done X --done_path {clean_done})
   - --check → add --check
   - --clean_type → add --clean_type
   - --all/--newer/--change and not check → add --overwrite
3. check mode: bash wrapper >/dev/null 2>>LOG; otherwise >>LOG 2>&1
4. stats_record(rc, fetch_done)
5. log by rc: 0 success / 1 failed / 2 config error / 5 skip(no data) / 6 skip(exists) / other unknown
6. --fail_fast and rc==2 → touch FAIL_FAST_FLAG_FILE
7. return rc
```

### 4.7 clean_wrapper.sh (idempotent wrapper)

**Responsibility**: idempotence check + tmp done + atomic mv + cmd write + routing by exit_code.

```
Argument parsing:
  --done_path → REAL_DONE (required, otherwise exit 2)
  --overwrite → OVERWRITE=true (passed through to clean.py)
  --fake      → FAKE=true (passed through)
  --check     → CHECK=true (not passed through to clean.py; short-circuited by the wrapper)
  the rest → PASSTHRU_ARGS

Idempotence check (when FAKE/OVERWRITE/CHECK are all not true):
  - REAL_DONE exists and status in (success, skipped) → exit 6
  - REAL_DONE status=failed → continue and rerun
  - SKIP_DONE (skip.{base}) exists → exit 5

TMP_DONE = {REAL_DONE}.tmp.{pid}
call clean.py: python CLEAN_PY "${PASSTHRU_ARGS[@]}" --done_path "$TMP_DONE" → EXIT_CODE
SELF_CMD = _build_self_cmd ORIG_ARGS

Routing:
  CHECK → delete TMP_DONE, exit EXIT_CODE (no done/fail/skip/manifest is written)
  0|6   → write cmd to manifest, mv TMP_DONE → REAL_DONE, delete fail/skip
           (FAKE only deletes tmp; when there is no tmp, write a minimal done)
  5     → delete tmp, touch SKIP_DONE, delete REAL_DONE/FAIL_DONE
  130   → delete tmp, no marker
  other → mv tmp → FAIL_DONE (or write a minimal fail manifest), delete REAL_DONE/SKIP_DONE
exit EXIT_CODE
```

Note: this is a typical example of an "upstream/downstream transaction" — the breakpoint / wrapper layer can be kept entirely outside the new library.

---

## 5. Core execution layer logic

### 5.1 CleanContext (context.py)

A dataclass carrying all information for one CleanTask execution:

- task identifiers: `source / raw_type / clean_type / date / date_key / filename`
- paths: `raw_path / output_path`
- runtime: `report / logger`
- CLI options: `overwrite / csv_mode / check_only / outfile / only_clean_type`
- back-references: `fetch_done_path` (the clean_done written into meta), `done_path` (the wrapper tmp)

Convenience properties and derivation:
- `final_output_path`: when `outfile` is non-empty, the relative path is joined to `output_path.parent`, otherwise `output_path`
- `fork(clean_type, output_path)`: derives a fork context. Shared fields are inherited as-is (including done_path), while `clean_type/output_path/report` are newly set (report=None)

### 5.2 CleanTask (task.py)

**Constructor**: if `ctx.clean_type` is non-empty (the legacy single-type call), pre-create `self.report` bound to ctx; otherwise `self.report=None` (multi-output mode, each fork creates its own report).

**run()**:
```
1. raw_df = load_data()
   - file does not exist → CleanerDataNotFoundError; read_parquet failure → wrapped in the same exception
   - df.empty → CleanerDataEmptyError
2. groups = _resolve_groups()
   - read adapter.{source}.clean.types.{raw_type}
   - list → wrapped as {"outputs": list}
   - if filter (dict) exists → take {field, regex, exclude_regex, outputs} for each group (outputs required to be non-empty)
   - otherwise if outputs (list) exists → {"all": {field:None, regex:None, outputs:list}}
   - neither → {} → run() throws CleanerDataNotFoundError
3. for group: _run_group(raw_df, group_key, group_cfg) → reports
exception fallback: NotFound/Empty → skipped report; others → failed report
```

**_apply_filter(df, spec) (row fork)**:
```
field = spec.field; regex/exclude = spec.regex/spec.exclude_regex
- no field, or both empty → return everything
- field not in columns → return everything
- regex non-empty → df[field].astype(str).str.match(regex, na=False) keeps matches
- otherwise exclude non-empty → keeps non-matches
- reset_index(drop=True)
```
Note: the same row may enter multiple groups (row overlap across groups); when `regex` and `exclude` are both configured, only `regex` takes effect (`elif`).

**_run_group (column fork within a group)**:
```
sub = _apply_filter(raw_df, group_cfg)   # row filtering
for ct in group_cfg.outputs:
    - only_clean_type and ct != it → skip
    - report = self._fork_one(sub.copy(), ct)   # use a copy for each ct so output column filtering does not contaminate other cts in the group
```

**_fork_one(df, ct) (complete chain for a single contract)**:
```
1. derive output_path = data/clean/{ct}/{source}/{date_key}/{filename}{.csv|.parquet}
2. fork_ctx = ctx.fork(ct, output_path)
3. fork_ctx.report = CleanReport(ct, source, raw_type, date)
4. report.start(); report.input_rows = len(df)
5. supplement the report: input_path = raw_path, input_size_bytes = stat, raw_meta_path = raw_path.with_suffix(".meta.json") (if present)
6. df = _run_middlewares(df, fork_ctx)
7. save_data(df, fork_ctx); report.status = "success"
8. exception routing:
   - NotFound/Empty/OutputExists → status=skipped + add_warning
   - ConfigError/ValidationError/others → status=failed + add_error
9. finally: report.finish()
return report
```

**_run_middlewares(df, ctx)**:
```
middlewares = get_middlewares(ctx.clean_type)
none → warning "no middleware applies to clean_type=X", return the original df
for mw in middlewares:
    try: result = mw(df, ctx); if result is not None: df = result
    except CleanerConfigError: raise           # config error bubbles up → the whole ct failed
    except CleanerValidationError: raise       # validation error bubbles up → the whole ct failed
    except Exception as e:
        ctx.report.add_warning("middleware X failed: e")   # other exceptions downgraded to warning, keep running
return df
```
**This is extremely important behavior**: all middleware exceptions other than ConfigError/ValidationError are swallowed and downgraded to warnings, and the chain continues. This means that when a rule is written incorrectly or throws at runtime, data may continue downstream uncleaned while the status remains success. This is one of the gaps the new library most needs to fix.

**save_data(df, ctx)**:
```
path = ctx.final_output_path
1. check_only → only set report.output_path/output_meta_path/output_rows, write no file, return
2. path.parent.mkdir; if path exists and not overwrite → CleanerOutputExistsError
3. report.output_rows = len(df)
4. atomic write: tmp = {path}.tmp.{pid}
   - csv_mode → df.to_csv(tmp, encoding="utf-8-sig")
   - otherwise → df.to_parquet(tmp, index=False, compression="zstd")
   - tmp.replace(path); clean up tmp on exception
5. report.output_path / output_size_bytes
6. _write_meta(df, {path}.json, path, ctx)
```

**_write_meta (meta.json contents)**:
```
{
  clean_type, source, raw_type, date, date_key,
  data_path: to_rel(output_path),
  rows, columns: list(df.columns),
  cleaned_at: datetime.now().isoformat(),
  data_hash: sha256(file byte stream, "sha256:" prefix),
  data_size_bytes,
  clean_done: fetch_done_path.replace("/fetch/", "/clean/"),
  cleaning_stats: {
     rows_removed, duplicates_removed, nulls_filled,
     nulls_backfilled, outliers_corrected
  }
}
```
Note: the meta file name is `{filename}.json` (`path.with_suffix(".json")`), while darwin-common generic storage uses `.meta.json`; the two differ.

### 5.3 CleanContract (contract.py)

A frozen dataclass with fields: `clean_type / fields(Dict[str,FieldSpec]) / output(List[str]) / sort(Dict) / enabled / help`.
- `FieldSpec`: `name / type / type_def (contents of base.types[type]) / help`
- `output_specs()`: returns a list of FieldSpec in output order

`load_contract(ct)`:
```
1. ensure_loaded(ct) → add_config(stk_base.json) + add_config(clean.d/{ct}.json)
2. type_config = cleaner.clean_type.{ct}; empty → CleanerConfigError
3. base_config = cleaner.clean_type.base; base.fields / base.types empty → CleanerConfigError
4. merged_fields = base.fields ∪ ct.fields (ct overrides same names; same-name dicts shallow-merged with {**base, **override})
5. field_specs = {name: FieldSpec(name, type, base.types[type], help)}
   - every field must have a type; the type must be defined in base.types, otherwise CleanerConfigError
6. output = ct.output; must be a non-empty list, otherwise CleanerConfigError
   - every output column must be in field_specs, otherwise CleanerConfigError
7. sort = ct.sort (dict, otherwise CleanerConfigError)
8. enabled = bool(ct.enabled default True); help = ct.help
9. return CleanContract(...)
```

**add_config idempotence patch**: darwin-common's `add_config` merges every time (not idempotent); contract.py uses a module-level `_LOADED: set[str]` to record loaded file paths, ensuring add_config runs only once per process. This is a global-state coupling point that must be handled during extraction.

`list_clean_types()`: lists all json file names under clean.d that do not end with `_base` (without loading contracts).

### 5.4 CleanResult / CleanOutput (result.py)

`CleanOutput` (single output): `clean_type / status / data_path / meta_path / error`
- the `success` property: `status in ("success", "empty", "skipped")` is considered successful
- `to_dict()`: status / data_path / meta_path / error

`CleanResult` (whole result for a single raw):
- task identifiers, input information (fetch_done_path / raw_path / raw_hash / raw_rows), outputs (Dict[ct, CleanOutput]), errors/warnings and timestamps
- `done_path` is not written into to_dict (it is meta-information about "where to write")
- `finalize()` aggregation rules:
  - no outputs → failed/exit 1
  - any output failed (`not o.success`) → failed/exit 1
  - all skipped → skipped/exit 6
  - others (some success/empty) → success/exit 0

### 5.5 CleanReport (report.py)

A detailed audit object (division of labor with CleanOutput: CleanOutput is a condensed summary, CleanReport is the detailed process). Fields:
- input: input_path / input_rows / input_size_bytes / raw_meta_path
- output: output_path / output_rows / output_size_bytes / output_meta_path / report_path
- statistics: rows_removed / rows_modified / duplicates_removed / nulls_filled(Dict) / nulls_backfilled(Dict) / outliers_corrected(Dict) / computed_fields(Dict) / validated_fields(Dict)
- row-level violations: validation_errors(List[Dict]); `add_validation_error()` stores at most 200 entries, `to_dict()` truncates to 100
- warnings / errors
- timing: started_at / finished_at / duration_seconds

Methods: `start()` / `finish()` / `add_validation_error(row_index, symbol, trade_date, field, rule, value, expected, action="set_to_null")` / `add_warning` / `add_error` / `to_dict()` / `save(path)`

**Key gaps (to be fixed in the new library)**:
1. `report.save()` is **never called** in the entire clean stage → the detailed audit report is never persisted
2. `duplicates_removed` / `nulls_backfilled` / `validation_errors` have no writers → they are always empty
3. `add_validation_error()` appears only at its definition and is never called by any middleware → the row-level violation recording mechanism is effectively inert

### 5.6 exceptions.py (exception → exit_code mapping)

| Exception | exit_code | Meaning |
|---|---|---|
| `CleanerError` (base class, default) | 1 | generic business failure |
| `CleanerConfigError` | 2 | configuration error |
| `CleanerDataNotFoundError` | 5 | source data absent (skip) |
| `CleanerDataEmptyError` | 5 | source data empty (skip) |
| `CleanerValidationError` | 1 | validation failed |
| `CleanerOutputExistsError` | 6 | output already exists (skip) |

---

## 6. Middleware rule chain (core)

### 6.1 Registration and scheduling (base.py)

- `@middleware(order=100, clean_type=None)`: appends `{fn, name, order, clean_type}` to the global `_MIDDLEWARES` list
- `get_middlewares(clean_type)`:
  - filter: `ct is None or ct == clean_type` (global + specific)
  - sorted by `(order, registration index)` (same order → registration order, stable)
  - returns a list of fns
- `list_middlewares(clean_type=None)`: for debugging, lists the registry

`__init__.py` imports all modules to trigger registration; `task.py`'s top-level `import darwin_fetch.cleaners.middlewares` also triggers registration. A new middleware file must be imported in `__init__.py` to take effect.

### 6.2 Global rule chain (steps.py + 3 global specific modules)

**order segment convention**: 10-19 preprocessing / 20-29 type conversion and filling / 30-49 computation and validation / 50-69 consistency and type validation / 90-99 output.

The complete chain sorted by actual order (including the other modules):

| order | Name | Source | Logic |
|---|---|---|---|
| 10 | `field_mapping` | steps | rename columns per `adapter.{source}.clean.types.{raw_type}.mapping` (or the fallback `field.mapping`); only rename columns that already exist |
| 11 | `json_extract` | json_extract.py | extract from dict/JSON columns into new columns by path list per `types.{raw_type}.json_extract` |
| 12 | `value_mapping` | steps | apply `.replace(mapping)` to columns per `value_map: {field: {source_value: standard_value}}` |
| 15 | `field_transform` | steps | three spec forms: `{value:constant}` (does not require the column to exist, assigns directly) / `{factor:multiplier}` (multiplication) / `{action:name}` (calls TRANSFORM_ACTIONS); unknown action → warning and skip |
| 16 | `ftshare_fill_trade_date` | ftshare.py | only `ctx.source=="ftshare"`: when there is no trade_date, derive YYYYMMDD from ts_millis in UTC+8 if present, otherwise fill with ctx.date |
| 16 | `ftshare_fill_symbol` | ftshare.py | ftshare only: when there is no symbol, fill from ctx.filename (for hist types the filename is the symbol) |
| 16 | `ohlcvault_fill_symbol` | ohlcvault.py | ohlcvault only: when there is no symbol, fill from ctx.filename |
| 16 | `stk_code_exchange` | stk_code.py | specific to stk_code: when there is no exchange, extract and uppercase it from the symbol suffix (000001.SZ→SZ) |
| 17 | `gold_sge_filter_main` | gold_sge.py | specific to gold_sge: keep only symbol ∈ {Au99.99}, record rows_removed |
| 17 | `stk_code_market_name` | stk_code.py | specific to stk_code: when market+exchange are present, map out market_name by the {(主板,SH):沪市主板,...} table; keep market when there is no match |
| 17 | `hk_code_normalize` | stk_hkcode.py | specific to stk_hkcode: market→港股主板/创业板, hardcode exchange=HK/area=香港/is_hs=none, do not set industry (output fills None) |
| 17 | `trade_calendar_prev` | trd_calendar.py | specific to trd_calendar: group by exchange, use bisect to take the most recent date before each cal_date with is_open=1 as trade_date_prev; skip if already present and all non-null (tushare provides pretrade_date) |
| 18 | `stk_limit_null_markers` | stk_limit.py | specific to stk_limit: up_limit/down_limit values ≤0 or >100000 → NaN, record outliers_corrected |
| 18 | `trade_calendar_timestamps` | trd_calendar.py | specific to trd_calendar: compute day_begin_ms/day_end_ms/trade_begin_ms/trade_end_ms from the exchange's session table (SSE/SZSE 09:30-15:00 UTC+8 / HKEX 09:30-16:00 UTC+8 / XNYS 09:30-16:00 UTC-5); on non-trading days the trade boundaries are None |
| 18 | `stk_code_market_name_infer` | stk_code.py | specific to stk_code: when there is no market_name column, infer from the symbol prefix (60/68→沪市主板/科创板 etc.), do not overwrite existing tushare values |
| 20 | `type_conversion` | steps | convert dtype per `fields[field].type → types[type].converter`: numeric (bool→Int64, others pd.to_numeric coerce) / string (astype str) / datetime / timestamp_ms (Int64); skip unregistered converters |
| 30 | `fill` | steps | per field fill.strategy (default ffill) + fill.max_days (default 5); if a symbol column exists, use groupby(symbol).ffill(limit); record nulls_filled |
| 40 | `formula_compute` | steps | per field formula: field absent → create None; all-null → df.eval and record computed_fields; partially missing → warning, no computation (to avoid masking problems) |
| 45 | `formula_validate` | steps | validate existing values per field formula: extract dependency fields (regex + keyword-table exclusion); if missing → warning and skip; inf → warning and skip; bool fields → skip; only when both diff and rel_diff exceed tolerance (default rel 0.003 / abs 0.01, overridable per field) record validated_fields + warning |
| 50 | `consistency` | steps | look up CONSISTENCY_RULES by the field's consistency rule name (high≥low / high≥open / high≥close / low≤open / low≤close); for violating rows set that field to None, recording outliers_corrected + warning |
| 60 | `type_validate` | steps | per field type constraints: first set inf/±inf to null, then out-of-min values to null, then out-of-max values to null, recording outliers_corrected |
| 90 | `output` | steps | validate all output columns are in fields (otherwise CleanerConfigError); fill missing fields with None; `return df[output_fields]` (select columns in contract order) |
| 95 | `sort` | steps | per contract sort (dict key order = priority, value ascending/descending), filter out absent fields, `sort_values(...).reset_index(drop=True)` |

### 6.3 The three auxiliary tables in steps.py

**TRANSFORM_ACTIONS** (7 named actions):
- `symbol_to_standard`: `sh.600000`/`sz.000001` → `600000.SH`/`000001.SZ` (move the prefix to a suffix and uppercase it)
- `date_to_compact`: `2026-10-09`/`2026/10/09` → `20261009`
- `datetime_to_compact`: `2026-10-08 09:30:00` → `20261008093000000` (17 digits; if already purely numeric, zfill(17))
- `zero_to_nan`: 0 → NaN (compare after to_numeric, then mask)
- `symbol_suffix`: 6 pure digits → with a market suffix (60/68/900→.SH, 00/30/200/201→.SZ, 8/4/92→.BJ)
- `us_symbol_suffix`: `AAPL` → `AAPL.US` (an existing .US stays unchanged)
- `float_to_compact`: float64 `20261105.0` → `"20261105"` (used for ohlcvault's out/delist_date)

**CONSISTENCY_RULES** (5 rules): see order 50 in the table above.

**Formula-related**:
- `_eval_formula`: `df.eval(formula)`, scalar results wrapped into a Series
- `_extract_formula_fields`: regex `\b([a-zA-Z_]\w*)\b` extracts identifiers, minus the keyword table (and/or/not/in/is/if/else/True/False/None/abs/min/max/round/int/float/str/bool/where/index/columns)

### 6.4 Timing pattern of source-specific middleware

All source-specific middleware are deliberately inserted after `field_mapping(10)` and before `type_conversion(20)`, because:
- at this point source fields have been renamed to contract field names (e.g. code→symbol)
- at this point the fields are still of str type (convenient for string operations such as suffix extraction / regex mapping)
- they precede the dtype conversion at 20, avoiding type changes breaking string logic

`gold_sge`'s filtering (17) and `stk_limit`'s nulling (18) are also within this window.

---

## 7. Utility script logic

### 7.1 clean_types.py (reverse lookup tool)
- config load order: cleaner.json → clean.d/*.json → adapters/*/adapter_clean.json (`_load_all_configs`)
- `find_pairs(cts)`: iterate over `adapter.*.clean.types`, find those whose outputs/bare-list/filter union contains the target ct, and return `["source/raw_type"]` sorted and deduplicated
- `list_all_clean_types()`: take the union of all cts declared by adapters; only fall back to cleaner.clean_type contract declarations if nothing is found
- CLI: `python clean_types.py stk_ohlcv[,stk_valuation]` (look up pairs) / `--list_types` (list all)
- called by `clean_common.sh::resolve_clean_type_pairs` (--clean_type reverse lookup of (source,raw_type))

### 7.2 clean_parquet_scan.py
- scan `data/clean/{ct}/{source}/{date_key}/*.parquet` and output `{date}\t{ct}\t{max_mtime}`
- `max_mtime` = the maximum mtime of all parquet files in the directory (epoch seconds)
- date_key single day → date itself; a range (`19901219_20260731`) → expand into trading days with `sessions_in_range(start, end, exchange)` (exchange read from the `exchange` in clean.d/{ct}.json, default SSE)
- multiple date_keys covering the same date → take max_mtime; only output (date, ct) pairs that have data; sorted date DESC / ct ASC

### 7.3 dump_clean_config.py
- preload the config tree in the same order as clean.py (optional --no_adapter to see contracts only)
- print the specified prefix or the whole tree, `--pretty` for indentation, `--sort` to sort keys
- used to inspect the actual config after `_merge`/`_imports` expansion

---

## 8. Upstream/downstream coupling checklist (must be handled when extracting darwin-cleaner)

1. **Path constant coupling**: `Cleaner`/`CleanTask`/`contract`/bootstrap all use `PACKAGE_ROOT` (=darwin-fetch/src/darwin_fetch) and `PROJECT_ROOT` (=darwin-fetch/). The new library needs its own path source or explicit injection.
2. **Global config state**: `add_config` is not idempotent and contract.py uses the `_LOADED` patch; the config tree is process-level global, and contracts/mapping/transform all rely on `get_config("cleaner.clean_type.X")` / `try_config("adapter.{source}.clean.types.X")` reads. If the new library is to be a pure-function service, config reads must become explicit parameter injection.
3. **Direct selectors import**: `selectors/config.py` has `from darwin_fetch.cleaners import contract as clean_contract` at the top; `CleanContract`/`FieldSpec` are reused by the Select layer. The new library must decide whether to keep providing these two types or move contract reading down into a shared library.
4. **`cleaner.output.base_dir` config key**: both Cleaner and selectors/selector.py read it to locate data/clean.
5. **Meta suffix inconsistency**: clean writes `{filename}.json`, while darwin-common generic parquet storage uses `.meta.json`. The select/normalize side reads only parquet, not meta, but generic toolchains may mix them.
6. **Done path convention**: `_parse_done_path` hardcodes that the path must contain a `fetch` segment with the following four segments fixed; `write_done` hardcodes replacing `fetch` with `clean`; meta's `clean_done` uses the string `replace("/fetch/", "/clean/")`. These are all "upstream/downstream transactions" and, by the user's convention, stay out of the new library's boundary.
7. **clean.d contract ownership**: the user has explicitly stated that clean.d contracts do not enter the new library (they are cleaning requirements for concrete data, not cleaning rules). Contract loading happens on the Cleaner/selectors side; the new library receives "parsed contracts / rules" as input.
8. **Wrapper / batch scheduling**: `clean_batch.sh`, `clean_common.sh` and `clean_wrapper.sh` belong to the breakpoint / wrapper layer and, by the user's convention, stay in darwin-fetch.
9. **clean_parquet_scan.py**: a read-side scanning tool that, by the user's convention, stays out of the new library's boundary.
10. **Hardcoded trading calendar**: the session tables in `trd_calendar.py` (09:30-15:00 UTC+8 etc.) and the board-prefix tables in `stk_code.py` are hardcoded business knowledge and are candidates for configurability / replacement by a specialist library.
11. **Middleware exception downgrade**: `_run_middlewares` swallows non-Config/Validation exceptions and only records a warning; this is a behavioral semantic that must be explicitly defined for the new library during migration (fail-fast vs warn-continue).

---

## 9. Known issues and gaps (code facts)

| # | Issue | Location | Impact |
|---|---|---|---|
| 1 | `report.save()` is never called | defined in report.py, no call anywhere in the project | the detailed audit report is never persisted and cannot be investigated afterward |
| 2 | `duplicates_removed` / `nulls_backfilled` / `validation_errors` have no writers | report.py | these items in meta's cleaning_stats are always 0/empty |
| 3 | non-config middleware exceptions are downgraded to warnings | task.py `_run_middlewares` | when a rule fails, data may remain uncleaned yet status=success |
| 4 | the file header comment "10 steps" does not match the actual 11 steps | steps.py header | documentation drift |
| 5 | `stk_ohlcv.py` is a template placeholder registering no middleware | stk_ohlcv.py | no impact, but misleading |
| 6 | `add_config` is non-idempotent and relies on the `_LOADED` patch | contract.py | process-level global state, difficult test isolation |
| 7 | the `command` field mentioned in comments is commented out and not written to the breakpoint | cleaner.py / result.py | cannot directly obtain the rerun command from the breakpoint |
| 8 | `_apply_filter` only applies regex when both regex and exclude_regex are configured | task.py | potential configuration misuse |
| 9 | meta suffix `.json` is inconsistent with darwin-common's `.meta.json` | task.py vs darwin-common | risk of mixing generic toolchains |
| 10 | the `exclude_regex` config field is read in code but unused in existing adapter configs | task.py | dead configuration |

---

## 10. Logical boundary division for darwin-cleaner (based on the four points confirmed by the user)

**Boundaries confirmed by the user**:
1. The new library = a config- and contract-driven cleaning engine: it performs row forking, column forking, applies cleaning rules and validation on one or more parquet files (multiple files in a single directory may be merged), and returns data + meta. **Upstream/downstream transactions are outside the boundary** (reading config, clean_parquet_scan.py, etc.).
2. Cleaning via middleware, supporting dynamic insertion of new rules (loading of source-specific middleware).
3. Agreement with the summary of advanced principles (contracts first, validation as testing, assetization and lineage, idempotence + versioning, specialization of the trading calendar, incrementality and caching, naming and migration governance).
4. `clean.d` contracts do not enter the new library — they are cleaning requirements for concrete data, not cleaning rules; the upper layer reads clean.d contracts and then calls the darwin-cleaner engine to perform the cleaning.

**Mapping this to the existing code**:

| Existing component | Assignment | Reason |
|---|---|---|
| forking / chain execution / data writing + meta generation in `task.py` | **new library core** | the cleaning engine itself |
| the registration mechanism in `middlewares/base.py` + global rules in `steps.py` + each source-specific middleware | **new library core** | rules are engine capabilities; dynamic insertion is guaranteed by the registration mechanism |
| `context.py` / `result.py` (clean-result data classes) / `report.py` / `exceptions.py` | **new library core** (done-related fields may be trimmed) | execution-time data model |
| the `CleanContract`/`FieldSpec` data structures in `contract.py` | **new library core** (as input contract types) | contracts are the engine's input model |
| the mapping/transform/value_map/json_extract/filter config formats of `adapter_clean.json` | **defined and interpreted by the new library** | these are declarations of "cleaning rules", not concrete data contracts |
| the concrete fields/outputs/formulas of `clean.d/*.json` and `stk_base.json` | **not in the new library** (injected by the upper layer) | explicit user convention: cleaning requirements for concrete data |
| done path resolution / breakpoint reading / writing in `cleaner.py` | **stays in darwin-fetch** | upstream/downstream transactions |
| `clean_batch.sh` / `clean_common.sh` / `clean_wrapper.sh` | **stays in darwin-fetch** | breakpoint / wrapper scheduling layer |
| the `clean.py` CLI entry | **stays in darwin-fetch** (a thin wrapper calling the new library) | transaction boundary |
| `clean_types.py` / `clean_parquet_scan.py` / `dump_clean_config.py` | **stays in darwin-fetch** | tools / read-side scanning |
| config reading (`bootstrap`/`add_config` global state) | **stays in darwin-fetch; the new library receives parsed config** | explicit user convention: reading config is outside the boundary |

**Suggested shape of the new library's external interface (description only, no implementation code)**:
- a "cleaning service / pure function": input = (raw parquet path or DataFrame + parsed contract objects + source-side rule config + fork declarations), output = (cleaned DataFrame/parquet + meta + complete report)
- the middleware registry supports external injection of new rules (decorator or explicit registration API), preserving the dual order + clean_type filtering semantics
- an explicit error policy: config/validation errors fail fast; other rule exceptions must at least have an observable record (avoiding the current silent downgrade)
- meta/report field names follow the existing ones for smooth migration, while filling in the write paths for `duplicates_removed`/`nulls_backfilled`/`validation_errors`

---

## 11. Appendix

### 11.1 Full clean_type list (clean.d directory, 32 JSON files)

`etf_adj, etf_code, etf_corp, etf_ohlcv, etf_turnover, gold_sge, stk_adj, stk_base(base), stk_bidask, stk_code, stk_corp, stk_flow, stk_hkcode, stk_index_basic, stk_index_code, stk_index_ohlcv, stk_limit, stk_market, stk_market_flow, stk_meta(base), stk_ohlcv, stk_ohlcv_hk, stk_ohlcv_us, stk_pool, stk_pool_member, stk_scale, stk_st, stk_suspend, stk_turnover, stk_valuation, stk_xm, trd_calendar`

Among these, `stk_base`/`stk_meta` are base contracts (not independent cleaning types), and `stk_ohlcv_hk/us` are derived from `stk_ohlcv` via `_merge`.

### 11.2 Exit code table

| Code | Meaning | Trigger |
|---|---|---|
| 0 | success | all outputs successful |
| 1 | business failure | any output failed / generic exception / validation failure |
| 2 | config error | CleanerConfigError / Cleaner creation failure |
| 3 | temporary error | mentioned in the clean.py docstring (not explicitly returned in the implementation) |
| 5 | skip (no data / no contract / skip marker) | status=empty / CleanerDataNotFound / wrapper skip |
| 6 | skip (output exists / all skipped) | CleanerOutputExists / finalize all skipped / wrapper idempotence hit |
| 130 | user interruption | KeyboardInterrupt |

### 11.3 Example timeline of a typical execution

The fetch done for `tushare/stock_daily/20260929` → raw is the `stock_daily` daily parquet:
1. Cleaner resolves source=tushare, raw_type=stock_daily, ct list = [stk_ohlcv] (bare list)
2. ensure_loaded(stk_ohlcv) → load the stk_base + stk_ohlcv contracts
3. CleanTask reads raw → groups={"all": {outputs:[stk_ohlcv]}} → all rows
4. _fork_one: output_path = data/clean/stk_ohlcv/tushare/20260929/20260929.parquet
5. middleware chain (stk_ohlcv has no specific rules): field_mapping(10, ts_code→symbol etc.) → json_extract(11, skipped with no config) → value_mapping(12) → field_transform(15, volume×100/amount×1000) → type_conversion(20) → fill(30) → formula_compute(40, computes when close_adj/change/change_pct/amplitude are all null) → formula_validate(45) → consistency(50, high≥low etc.) → type_validate(60) → output(90, select 13 columns) → sort(95, trade_date desc/symbol asc)
6. save_data writes zstd parquet + meta.json (containing data_hash and cleaning_stats)
7. CleanResult.finalize → success/exit 0; write_done writes the clean breakpoint
8. the wrapper receives exit 0 → mv tmp done → real clean done
