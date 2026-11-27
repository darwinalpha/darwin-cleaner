# darwin-fetch clean 阶段代码逻辑梳理

English · [简体中文](clean-stage-code-walkthrough.cn.md)

> 本文是对 `darwin-fetch/` 中 clean 阶段全部代码的只读梳理，目的是为 `darwin-cleaner` 新库的边界设计与剥离迁移提供完整的逻辑底稿。
> 范围：入口脚本、Shell 批量调度、核心执行引擎、契约加载、middleware 规则链、全部配置、上下游耦合点。
> 文中所有行号/函数名/配置键均来自源码原文；"注"开头的内容为梳理判断，非代码事实。

---

## 0. 一句话总览

clean 阶段做的一件事：**读一个 fetch done 断点 → 找到它指向的 raw parquet → 按 adapter 配置把这份 raw 行分叉/列分叉成若干 clean_type → 对每个分叉跑一条配置驱动的 middleware 规则链 → 写出 `data/clean/{ct}/{source}/{date_key}/{filename}.parquet` + 同名 `.json` meta + clean 断点**。

业务逻辑几乎不在主流程里，而在 middleware 中；主流程只负责"读 → 分叉 → 跑链 → 写"。

---

## 1. 文件清单与职责

### 1.1 Python 核心（`src/darwin_fetch/cleaners/`）

| 文件 | 行数 | 职责 |
|---|---|---|
| `cleaner.py` | ~496 | 清洗调度器。加载 cleaner.json、解析 done 路径、读 done 内容、决定 clean_type 列表、创建 CleanTask、汇总结果、写 clean 断点 |
| `task.py` | ~504 | 单 raw 文件执行器。读 parquet、按 filter/outputs 分叉、驱动 middleware 链、写 parquet + meta |
| `contract.py` | ~274 | 契约读取（Cleaner/Selector/Load 共享）。加载 stk_base + clean.d/{ct}.json，返回冻结的 `CleanContract` |
| `context.py` | ~120 | 运行时上下文 `CleanContext`，middleware 签名 `(df, ctx)` 的 ctx，`fork()` 派生分叉上下文 |
| `result.py` | ~176 | `CleanOutput`（单产出）/ `CleanResult`（单 raw 整体结果），`finalize()` 聚合状态与退出码 |
| `report.py` | ~166 | `CleanReport`，单 clean_type 的详细审计记录（统计、行级违规、警告、计时） |
| `exceptions.py` | ~60 | 5 个自定义异常 + 到 exit_code 的映射 |
| `actions/print_clean_types.py` | ~57 | CLI action `--list_types`，打印可用清洗类型 |
| `middlewares/base.py` | ~139 | `@middleware` 注册装饰器、全局注册表、`get_middlewares()` 排序收集 |
| `middlewares/__init__.py` | ~50 | 导入全部 middleware 模块以触发注册 |
| `middlewares/steps.py` | ~833 | 全局通用规则链（11 个步骤，文件头注释写"10 步"已过时，实际注册 11 个） |
| `middlewares/stk_ohlcv.py` | ~51 | 模板占位，当前不注册任何 middleware |
| `middlewares/stk_limit.py` | ~44 | 涨跌停价无效值置 NaN（专属 stk_limit） |
| `middlewares/gold_sge.py` | ~43 | 只保留黄金主力合约 Au99.99（专属 gold_sge） |
| `middlewares/trd_calendar.py` | ~120 | 交易日历 UTC 毫秒边界 + 上一交易日（专属 trd_calendar） |
| `middlewares/stk_code.py` | ~126 | A股代码的市场/交易所推断（专属 stk_code） |
| `middlewares/stk_hkcode.py` | ~38 | 港股代码市场规范化（专属 stk_hkcode） |
| `middlewares/ftshare.py` | ~68 | ftshare 源补 trade_date / symbol（按 ctx.source 判断的全局 mw） |
| `middlewares/json_extract.py` | ~63 | 从 JSON 列按路径提取字段（全局配置驱动） |
| `middlewares/ohlcvault.py` | ~49 | ohlcvault 源补 symbol（按 ctx.source 判断的全局 mw） |

### 1.2 脚本与工具（`scripts/`）

| 文件 | 职责 |
|---|---|
| `scripts/clean.py` | 单任务 CLI 入口：bootstrap → Cleaner → parse_cli → clean → write_done → 返回 exit_code |
| `scripts/clean_batch.sh` | 批量调度：遍历 fetch done，多条件过滤，并行调 wrapper |
| `scripts/lib/clean_common.sh` | batch 的公共逻辑：参数解析、find -regex 下推筛选、增量判定、单任务执行、统计 |
| `scripts/utils/clean_wrapper.sh` | 单任务包装器：幂等检查、tmp done + 原子 mv、按 exit_code 分流 success/skip/fail |
| `scripts/utils/clean_types.py` | 反查：给 clean_type 找 (source, raw_type) 对；列出所有 clean_type |
| `scripts/utils/clean_parquet_scan.py` | 扫描 `data/clean/`，输出 (date, ct, max_mtime) TSV，供 select 侧新鲜度判定 |
| `scripts/utils/dump_clean_config.py` | 调试：按 clean.py 顺序预加载配置树并打印（含 `_merge`/`_imports` 展开结果） |

### 1.3 配置

| 文件 | 职责 |
|---|---|
| `cleaners/cleaner.json` | 全局配置：CLI 参数定义、输出 base_dir |
| `cleaners/clean.d/stk_base.json` | 基础契约：50+ 字段定义 + 16 种类型定义（converter/min/max/formula/consistency） |
| `cleaners/clean.d/*.json`（32 份：stk_base + stk_meta 两份基础 + 30 份具体契约） | 每个 clean_type 的契约：output 列、sort、字段覆盖、exchange |
| `adapters/{source}/adapter_clean.json`（5 份） | 源侧映射：mapping / transform / value_map / json_extract / types 分叉规则 |

### 1.4 关键上下游耦合文件

| 文件 | 耦合点 |
|---|---|
| `utils/bootstrap.py` | 提供 `PACKAGE_ROOT`/`PROJECT_ROOT`/`add_config`/`get_config`/`try_config`/`to_rel` |
| `selectors/config.py` | 直接 `from darwin_fetch.cleaners import contract as clean_contract`，复用 `CleanContract`/`FieldSpec` |
| `selectors/selector.py` | 读 `cleaner.output.base_dir` 定位 data/clean |
| `selectors/stages/normalize.py` | 直接扫描 `data/clean/{ct}/{source}/{date_key}/` 目录结构 |
| `darwin-common/store/parquet/*.py` | 通用 parquet 存储的 meta 后缀是 `.meta.json`，而 clean 自己写的是 `.json` |

---

## 2. 架构与数据流

```
fetch done (data/done/fetch/{source}/{raw_type}/{date_key}/{date}.json)
   │  含 status / raw_data_path / rows / data_hash
   ▼
[scripts/clean.py]  bootstrap → Cleaner() → parse_cli → cleaner.clean()
   ▼
[Cleaner.clean]
   ├─ _parse_done_path()  → source / raw_type / date_key / filename / date
   ├─ 读 done JSON        → raw_path / raw_rows / raw_hash；status=empty 直接跳过(exit 5)
   ├─ add_config(adapter_clean.json)
   ├─ _resolve_clean_types() → clean_type 列表（filter/outputs/裸list）
   ├─ clean_contract.ensure_loaded(ct) × N
   └─ _clean_one() → CleanTask(ctx).run() → List[CleanReport]
   ▼
[CleanTask.run]
   ├─ load_data()         → pd.read_parquet(raw_path)
   ├─ _resolve_groups()   → {group: {field, regex, exclude_regex, outputs}}
   └─ 对每组 _run_group():
        ├─ _apply_filter()      → 行分叉（field/regex/exclude_regex，可重叠）
        └─ 对组 outputs 每 ct _fork_one(sub.copy(), ct):
             ├─ ctx.fork(ct, output_path)
             ├─ _run_middlewares() → get_middlewares(ct) 按 order 依次执行
             └─ save_data()       → tmp 写 + rename → .parquet + .json meta
   ▼
[CleanResult.finalize] 聚合 outputs → status / exit_code
   ▼
[scripts/clean.py]  write_done(result) → clean 断点（tmp + rename）
```

**关键设计**：一个 raw 文件可产出多个 clean_type；一个 clean_type 就是一个独立输出文件，跑完整 middleware 链，有独立的 report。

---

## 3. 配置体系（三层配置树）

所有配置通过 `darwin-common` 的 `add_config()` 挂载到全局配置树，用 `get_config("a.b.c")` / `try_config(*paths)` 读取。三个关键展开机制：

- `_imports`：文件级立即导入其他配置（如 `stk_ohlcv.json` 引入 `stk_base.json`）
- `_merge`：`"${_self.xxx}"` 引用同配置树其他节点做深合并（如 hk 契约合并 stk_ohlcv 契约）
- `_mount`：指定该 JSON 挂载到配置树的根路径（如 `cleaner.clean_type`、`adapter.tushare`）

加载顺序（在 clean.py 中由 Cleaner 与各模块按需完成）：

1. `bootstrap()` → 库默认 `config/settings.json` + `conf.d/${APP_ENV}.json` + 用户 `config/settings.json`
2. `Cleaner.__init__` → `cleaner.json`（`_mount: cleaner`，含 cli_args 和 output.base_dir）
3. `Cleaner.clean` → `adapters/{source}/adapter_clean.json`（`_mount: adapter.{source}`）
4. `clean_contract.ensure_loaded(ct)` → `stk_base.json` + `clean.d/{ct}.json`（都 `_mount: cleaner.clean_type`）

### 3.1 cleaner.json

```
_mount: cleaner
cli_args:  fetch_done(必填) / clean_type / overwrite / check / csv / outfile / done_path / list_types(action=print_clean_types)
output.base_dir: data/clean
```

### 3.2 adapter_clean.json 的四种关键配置节

以 tushare 为例（`_mount: adapter.tushare`）：

```jsonc
{
  "clean": {
    "field": {
      "mapping":   { "ts_code": "symbol", "pre_close": "close_adj", ... },  // 源字段 → 契约字段
      "transform": { "volume": {"factor":100}, "amount": {"factor":1000}, ... } // 单位/常量/动作
    },
    "types": {
      "stock_daily": ["stk_ohlcv"],                    // 格式① 裸 list
      "stock_market_daily": {                          // 格式② dict + outputs
        "outputs": ["stk_market"],
        "mapping": {"_merge":"${_self.clean.field.mapping}", ...},
        "transform": {...},
        "value_map": {"market_name": {...}}            // 值映射：源值 → 标准值
      },
      "fund_code": {                                   // 格式③ dict + filter（行分叉）
        "filter": {
          "etf": {"field":"name", "regex":"^(?!.*联接)(?=.*ETF).*$", "outputs":["etf_code"]}
        },
        "mapping": {...}, "value_map": {...}
      },
      "stock_market_hist": {"_merge":"${_self.clean.types.stock_market_daily}"}, // 复用其他 raw_type
      ...
    }
  }
}
```

配 `value_map`（tushare/baostock/ftshare/ohlcvault/tickflow 均有）和 `json_extract`（ftshare 的 stock_code 用）。
`exclude_regex` 字段在 task.py 的 `_apply_filter` 里被读取，但现有 5 份 adapter 配置中未实际使用。

### 3.3 clean.d 契约的继承

- `stk_base.json`：`_mount: cleaner.clean_type`，内容挂在 `base` 键下 → `cleaner.clean_type.base.fields` / `.types`
- 普通契约（如 `stk_ohlcv.json`）：`_imports` 引入 base，内容挂在同名键下 → `cleaner.clean_type.stk_ohlcv`
- 派生契约（`stk_ohlcv_hk/us.json`）：`_imports` 引入 `stk_ohlcv.json`，用 `_merge: ${_self.stk_ohlcv}` 全量继承，只覆盖 `exchange`（HKEX/NYSE）
- `stk_meta.json`：`_imports` 引入 `stk_base.json`，扩展 `base.fields`（加 cal_date/is_open/trade_date_prev/各种 ms 字段等 23 个），挂到 `cleaner.clean_type.base`（与 stk_base 的 base 深合并 → 所有 ct 都能用这些元字段）
- 有 3 份契约带 `exchange` 字段：`stk_hkcode`(HKEX)、`stk_ohlcv_hk`(HKEX)、`stk_ohlcv_us`(NYSE)。该字段目前主要被 `clean_parquet_scan.py` 读取用于日期范围展开

---

## 4. 入口与调度层逻辑

### 4.1 scripts/clean.py（单任务入口）

```
main(args):
1. bootstrap()                   失败 → return 1
2. Cleaner()                     加载 cleaner.json，失败 → return 1
3. parse_cli_safe(args, "cleaner.cli_args", ...)
   - 参数定义来自 cleaner.json 的 cli_args
   - --list_types 会触发 action print_clean_types → 打印后 sys.exit(0)
   - final_args is None → 返回 action 的 exit_code
4. cleaner.clean(final_args)     → CleanResult（内部不抛异常）
   - KeyboardInterrupt → return 130
5. 若 --check：print(json.dumps(result.to_dict()))，不写 done
   否则 cleaner.write_done(result)；写失败且原本成功 → 改 exit_code=1
6. return result.exit_code
```

### 4.2 Cleaner.clean()（核心调度）

入参 final_args 关键字段：`fetch_done`(必填) / `clean_type`(只跑单类型) / `overwrite` / `csv` / `check` / `outfile` / `done_path`(wrapper 传的 tmp 路径)。

```
result 初始化：done_path / fetch_done_path / started_at / start_ts
try:
  1. _parse_done_path(fetch_done)
     - 找路径里的 "fetch" 段，后四段依次是 source/raw_type/date_key/filename
     - date = date_key 按 "_" 切的最后一段（单日=自身；区间取末日）
     - 校验：路径不含 fetch 段、或长度不足 → CleanerConfigError
  2. 读 done JSON
     - status == "empty" → 记 warning，status=skipped/exit 5/success=True，直接 return
     - raw_data_path ← raw_data_path || data_path || raw_path（三个键兼容）
     - 缺失 → CleanerConfigError；raw_path 相对则拼 PROJECT_ROOT
     - 文件不存在 → CleanerDataNotFoundError
     - result.raw_rows = rows；result.raw_hash = data_hash
  3. add_config(adapters/{source}/adapter_clean.json)；不存在 → CleanerConfigError
  4. source_config = adapter.{source}.clean
     clean_types = _resolve_clean_types(source_config, raw_type, only_clean_type)
     - 支持三种格式：裸 list / dict.filter / dict.outputs
     - only_clean_type 不在列表里 → CleanerConfigError
     - 空 → CleanerDataNotFoundError
  5. for ct in clean_types: clean_contract.ensure_loaded(ct)
  6. outputs = _clean_one(...)  → 内部 CleanTask.run() → reports → CleanOutput 列表
  7. result.finalize()
except CleanerDataNotFoundError → skipped/exit 5/success=True
except CleanerConfigError        → failed/exit 2/success=False
except Exception                 → failed/exit 1/success=False
finally: finished_at / duration_seconds
return result
```

**注意**：`CleanerDataNotFoundError` 在 Cleaner 层被当作"跳过"（success=True, exit 5），在 CleanTask 层也是"跳过"。这表示"没数据/没契约"不算失败。

### 4.3 _resolve_clean_types（决定 clean_type 列表）

```
types_map = adapter.{source}.clean.types
type_cfg = types_map[raw_type]
if type_cfg 是 list            → clean_types = 该 list（旧格式）
elif type_cfg 是 dict:
    filters = type_cfg.filter
    outputs = type_cfg.outputs
    if filters 非空 dict       → 遍历每组 outputs 合并（行分叉）
    elif outputs 非空 list     → clean_types = outputs（纯列分叉）
if only_clean_type:
    不在 clean_types 里 → CleanerConfigError；否则只返回它
return clean_types
```

注：这里的判定与 `clean_types.py::_clean_types_of` 保持一致（工具反查用）。

### 4.4 Cleaner.write_done()（写 clean 断点）

```
1. 无 fetch_done_path → return
2. 目标路径：
   - 外部指定 done_path（wrapper 的 tmp）优先
   - 否则把 fetch_done 路径里的 "fetch" 段替换为 "clean"
3. skipped 保护：若 result.status=skipped 且已有断点 status=success → 不覆盖（保留首次成功事实）
4. 原子写：写 {clean_done}.tmp.{pid} → json.dump(result.to_dict()) → replace
   异常则清理 tmp
```

### 4.5 scripts/clean_batch.sh（批量调度主流程）

```
main:
1. clean_init()  → bootstrap_init + FAIL_FAST_FLAG_FILE
2. cli_parse_common（--notify 等通用参数）
3. cli_parse_clean_common（clean 专属参数）
4. resolve_clean_type_pairs
   - 若给 --clean_type CT：调 clean_types.py CT 得到 (source,raw_type) 对 → CLEAN_TYPE_PAIRS
5. list_pending_fetch_dones → pending[]
6. dry_run? 打印（>20 只打前 20）后退出
7. worker_run:
   - tasks_file = pending
   - handler = clean_worker_handler → clean_run_one
   - max-parallel N（默认 5）
   - skip-codes 5,6（跳过码不计数为失败）
   - fail-fast-code 2（配置错误触发 fail_fast）
```

**参数解析要点**（`cli_parse_clean_common`）：
- 过滤类：`--source/--no_source`、`--raw_type/--no_raw_type`、`--date/--no_date`（由 common 层处理）、`--include_dates`、`--pattern/--no_pattern`、`--clean_type`
- 模式类（三选一，多选报错）：`--all`（全量覆盖）/ `--newer`（fetch done 比 clean done 新）/ `--change`（比 data_hash，精确）
- 其他：`--limit`（每 (source,raw_type) 前 N 个）、`--max-parallel`、`--fail_fast`、`--check`、`--dry_run/--fake`
- 默认模式：跑 clean done 不存在或 status=failed 的（首次 + 失败重试）

### 4.6 clean_common.sh 的筛选架构

核心思路：**除 include_dates / newer / change / limit 外，全部下推到 `find -regex`**（文件头部注释明确写了这一点）。

- `_regex_or_group`：逗号分隔多值 → `(a|b)` ERE 组
- `_clean_type_regex_group`：CLEAN_TYPE_PAIRS（`src/rt,src/rt`）→ `(src1/rt1|src2/rt2)`
- `_ere_escape`：转义 ERE 元字符（fixed pattern 用）
- `_pattern_token_to_regex`：4 种 pattern 语法
  - 默认（无前缀）→ fixed 子串 `.*xxx.*`（grep -F 语义）
  - `re:XXX` → 扩展正则 `.*XXX.*`
  - `glob:XXX` → 转义后 `*`→`.`、`?`→`.`（近似 glob）
  - `exact:XXX` → `^XXX$` 整路径精确
  - 多个 PAT 逗号分隔，OR 语义（任一命中即通过）
- `_fetch_done_regex_args` / `_fetch_done_no_regex_args`：拼正向/负向 find 参数
  - 正向：第一段是 `.*/fetch/`，按 source/raw_type/clean_type/date 拼路径层级，pattern 单独一条 -regex（AND）
  - 负向：`! -regex` 分别排除 source/raw_type/date/pattern
- `list_pending_fetch_dones`：
  - `--change` 走 `_change_candidates`（先 newer 过滤再比 hash，见下）
  - `--limit` 走一次 find + 关联数组按 `(source,raw_type)` 计数
  - 默认/--all/--newer：一次 find + while 后处理
  - 所有分支都排除 `fail.*` 和 `skip.*` 文件
  - clean done 判定：`clean_done_is_done` = 真实 done 存在 **或** `skip.{basename}` 存在（即 skip 标记也视为完成）

**--change 增量逻辑**（`_change_candidates`）：
1. 收集 fetch done 的 mtime 和 clean done 的 mtime
2. awk 算出 newer 候选（clean 不存在 或 fetch 更新）
3. jq 提取候选的 fetch `.data_hash` 和对应 clean `.input.raw_hash`
4. awk 比对：clean 不存在 → 处理；两边 hash 都非空且相等 → 跳过；否则处理
5. 临时目录 `_CHANGE_TMP_DIR` 由 `change_cleanup` 清理

**--include_dates 逻辑**（区间包含语义，regex 无法表达，后处理）：
- `_parse_date_key_range`：date_key 含 `_` → 拆成 start/end；否则单日
- `_validate_include_dates`：每个日期必须是 8 位数字（允许 YYYY-MM-DD）
- `_match_include_dates`：目标日期落在 `[start,end]` 闭区间内即命中

**clean_run_one（单任务）**：
```
1. fail_fast 标志存在 → return 6
2. args = (--fetch_done X --done_path {clean_done})
   - --check → 加 --check
   - --clean_type → 加 --clean_type
   - --all/--newer/--change 且非 check → 加 --overwrite
3. check 模式：bash wrapper >/dev/null 2>>LOG；否则 >>LOG 2>&1
4. stats_record(rc, fetch_done)
5. 按 rc 打日志：0 success / 1 failed / 2 config error / 5 skip(no data) / 6 skip(exists) / 其他 unknown
6. --fail_fast 且 rc==2 → touch FAIL_FAST_FLAG_FILE
7. return rc
```

### 4.7 clean_wrapper.sh（幂等包装器）

**职责**：幂等检查 + tmp done + 原子 mv + cmd 写入 + 按 exit_code 分流。

```
参数解析：
  --done_path → REAL_DONE（必填，否则 exit 2）
  --overwrite → OVERWRITE=true（透传 clean.py）
  --fake      → FAKE=true（透传）
  --check     → CHECK=true（不透传 clean.py，wrapper 短路）
  其余 → PASSTHRU_ARGS

幂等检查（FAKE/OVERWRITE/CHECK 都非 true 时）：
  - REAL_DONE 存在且 status in (success, skipped) → exit 6
  - REAL_DONE status=failed → 继续重跑
  - SKIP_DONE（skip.{base}）存在 → exit 5

TMP_DONE = {REAL_DONE}.tmp.{pid}
调 clean.py：python CLEAN_PY "${PASSTHRU_ARGS[@]}" --done_path "$TMP_DONE" → EXIT_CODE
SELF_CMD = _build_self_cmd ORIG_ARGS

分流：
  CHECK → 删 TMP_DONE，exit EXIT_CODE（不写任何 done/fail/skip/manifest）
  0|6   → 写 cmd 到 manifest，mv TMP_DONE → REAL_DONE，删 fail/skip
           （FAKE 只删 tmp；无 tmp 时写最小 done）
  5     → 删 tmp，touch SKIP_DONE，删 REAL_DONE/FAIL_DONE
  130   → 删 tmp，不标记
  其他  → mv tmp → FAIL_DONE（或写最小 fail manifest），删 REAL_DONE/SKIP_DONE
exit EXIT_CODE
```

注：这是"上下游事务"的典型代表——断点/wrapper 层完全可以在新库之外保留。

---

## 5. 核心执行层逻辑

### 5.1 CleanContext（context.py）

数据类，一次 CleanTask 执行的全部信息载体：

- 任务标识：`source / raw_type / clean_type / date / date_key / filename`
- 路径：`raw_path / output_path`
- 运行时：`report / logger`
- CLI 选项：`overwrite / csv_mode / check_only / outfile / only_clean_type`
- 反向指向：`fetch_done_path`（写 meta 的 clean_done）、`done_path`（wrapper tmp）

便捷属性与派生：
- `final_output_path`：`outfile` 非空时，相对路径拼到 `output_path.parent`，否则 `output_path`
- `fork(clean_type, output_path)`：派生分叉上下文。共享字段原样继承（含 done_path），`clean_type/output_path/report` 新设（report=None）

### 5.2 CleanTask（task.py）

**构造函数**：若 `ctx.clean_type` 非空（旧式单类型调用），预建 `self.report` 绑到 ctx；否则 `self.report=None`（多输出模式，各分叉自建 report）。

**run()**：
```
1. raw_df = load_data()
   - 文件不存在 → CleanerDataNotFoundError；read_parquet 失败 → 同异常包装
   - df.empty → CleanerDataEmptyError
2. groups = _resolve_groups()
   - 读 adapter.{source}.clean.types.{raw_type}
   - list → 包成 {"outputs": list}
   - 有 filter(dict) → 每组取 {field, regex, exclude_regex, outputs}（要求 outputs 非空）
   - 否则有 outputs(list) → {"all": {field:None, regex:None, outputs:list}}
   - 都没有 → {} → run() 抛 CleanerDataNotFoundError
3. for group: _run_group(raw_df, group_key, group_cfg) → reports
异常兜底：NotFound/Empty → skipped report；其他 → failed report
```

**_apply_filter(df, spec)（行分叉）**：
```
field = spec.field；regex/exclude = spec.regex/spec.exclude_regex
- 无 field 或两者都空 → 返回全量
- field 不在 columns → 返回全量
- regex 非空 → df[field].astype(str).str.match(regex, na=False) 保留匹配
- 否则 exclude 非空 → 保留不匹配
- reset_index(drop=True)
```
注：同一行可进入多个 group（组间行重叠）；`regex` 与 `exclude` 同时配置时只生效 `regex`（`elif`）。

**_run_group（组内列分叉）**：
```
sub = _apply_filter(raw_df, group_cfg)   # 行过滤
for ct in group_cfg.outputs:
    - only_clean_type 且 ct != 它 → skip
    - report = self._fork_one(sub.copy(), ct)   # 每个 ct 用 copy，避免 output 筛列污染同组其他 ct
```

**_fork_one(df, ct)（单契约完整链路）**：
```
1. 派生 output_path = data/clean/{ct}/{source}/{date_key}/{filename}{.csv|.parquet}
2. fork_ctx = ctx.fork(ct, output_path)
3. fork_ctx.report = CleanReport(ct, source, raw_type, date)
4. report.start(); report.input_rows = len(df)
5. 补报告：input_path = raw_path、input_size_bytes = stat、raw_meta_path = raw_path.with_suffix(".meta.json")（若存在）
6. df = _run_middlewares(df, fork_ctx)
7. save_data(df, fork_ctx); report.status = "success"
8. 异常分流：
   - NotFound/Empty/OutputExists → status=skipped + add_warning
   - ConfigError/ValidationError/其他 → status=failed + add_error
9. finally: report.finish()
return report
```

**_run_middlewares(df, ctx)**：
```
middlewares = get_middlewares(ctx.clean_type)
无 → warning "没有 middleware 对 clean_type=X 生效"，返回原 df
for mw in middlewares:
    try: result = mw(df, ctx); if result is not None: df = result
    except CleanerConfigError: raise           # 配置错误冒泡 → 整个 ct failed
    except CleanerValidationError: raise       # 校验错误冒泡 → 整个 ct failed
    except Exception as e:
        ctx.report.add_warning("middleware X 失败: e")   # 其他异常降级为 warning，继续跑
return df
```
**这是极其重要的行为**：除 ConfigError/ValidationError 外的所有 middleware 异常都被吞掉降级为 warning，链继续。意味着一个规则写错/运行异常时，数据可能带着未清洗的状态继续往下写，而状态仍是 success。这是新库最需要修正的缺口之一。

**save_data(df, ctx)**：
```
path = ctx.final_output_path
1. check_only → 只设 report.output_path/output_meta_path/output_rows，不写文件，return
2. path.parent.mkdir；若 path 存在且非 overwrite → CleanerOutputExistsError
3. report.output_rows = len(df)
4. 原子写：tmp = {path}.tmp.{pid}
   - csv_mode → df.to_csv(tmp, encoding="utf-8-sig")
   - 否则    → df.to_parquet(tmp, index=False, compression="zstd")
   - tmp.replace(path)；异常清理 tmp
5. report.output_path / output_size_bytes
6. _write_meta(df, {path}.json, path, ctx)
```

**_write_meta（meta.json 内容）**：
```
{
  clean_type, source, raw_type, date, date_key,
  data_path: to_rel(output_path),
  rows, columns: list(df.columns),
  cleaned_at: datetime.now().isoformat(),
  data_hash: sha256(文件字节流, "sha256:" 前缀),
  data_size_bytes,
  clean_done: fetch_done_path.replace("/fetch/", "/clean/"),
  cleaning_stats: {
     rows_removed, duplicates_removed, nulls_filled,
     nulls_backfilled, outliers_corrected
  }
}
```
注：meta 文件名是 `{filename}.json`（`path.with_suffix(".json")`），而 darwin-common 通用存储用 `.meta.json`，两者不同。

### 5.3 CleanContract（contract.py）

冻结的 dataclass，字段：`clean_type / fields(Dict[str,FieldSpec]) / output(List[str]) / sort(Dict) / enabled / help`。
- `FieldSpec`：`name / type / type_def(base.types[type] 内容) / help`
- `output_specs()`：按 output 顺序返回 FieldSpec 列表

`load_contract(ct)`：
```
1. ensure_loaded(ct) → add_config(stk_base.json) + add_config(clean.d/{ct}.json)
2. type_config = cleaner.clean_type.{ct}；空 → CleanerConfigError
3. base_config = cleaner.clean_type.base；base.fields / base.types 为空 → CleanerConfigError
4. merged_fields = base.fields ∪ ct.fields（ct 覆盖同名；同名 dict 用 {**base, **override} 浅合并）
5. field_specs = {name: FieldSpec(name, type, base.types[type], help)}
   - 每字段必须有 type；type 必须在 base.types 定义，否则 CleanerConfigError
6. output = ct.output；非空 list，否则 CleanerConfigError
   - output 每列必须在 field_specs，否则 CleanerConfigError
7. sort = ct.sort（dict，否则 CleanerConfigError）
8. enabled = bool(ct.enabled 默认 True)；help = ct.help
9. return CleanContract(...)
```

**add_config 幂等性补丁**：darwin-common 的 `add_config` 每次都会 merge（不幂等），contract.py 用模块级 `_LOADED: set[str]` 记录已加载文件路径，保证同一进程内只 add_config 一次。这是剥离时必须处理的全局状态耦合点。

`list_clean_types()`：列出 clean.d 下所有非 `_base` 结尾的 json 文件名（不加载契约）。

### 5.4 CleanResult / CleanOutput（result.py）

`CleanOutput`（单产出）：`clean_type / status / data_path / meta_path / error`
- `success` 属性：`status in ("success", "empty", "skipped")` 视为成功
- `to_dict()`：status / data_path / meta_path / error

`CleanResult`（单 raw 整体）：
- 任务标识、输入信息（fetch_done_path / raw_path / raw_hash / raw_rows）、outputs(Dict[ct, CleanOutput])、errors/warnings、时间
- `done_path` 不写进 to_dict（它是"写哪里"的元信息）
- `finalize()` 聚合规则：
  - 无 outputs → failed/exit 1
  - 任一 output 失败（`not o.success`）→ failed/exit 1
  - 全部 skipped → skipped/exit 6
  - 其他（有 success/empty）→ success/exit 0

### 5.5 CleanReport（report.py）

详细审计对象（与 CleanOutput 分工：CleanOutput 是精简摘要，CleanReport 是详细过程）。字段：
- 输入：input_path / input_rows / input_size_bytes / raw_meta_path
- 输出：output_path / output_rows / output_size_bytes / output_meta_path / report_path
- 统计：rows_removed / rows_modified / duplicates_removed / nulls_filled(Dict) / nulls_backfilled(Dict) / outliers_corrected(Dict) / computed_fields(Dict) / validated_fields(Dict)
- 行级违规：validation_errors(List[Dict])，`add_validation_error()` 最多存 200 条，`to_dict()` 截断到 100 条
- warnings / errors
- 计时：started_at / finished_at / duration_seconds

方法：`start()` / `finish()` / `add_validation_error(row_index, symbol, trade_date, field, rule, value, expected, action="set_to_null")` / `add_warning` / `add_error` / `to_dict()` / `save(path)`

**关键缺口（需在新库修复）**：
1. `report.save()` 在整个 clean 阶段**从未被调用** → 详细审计报告永远不落盘
2. `duplicates_removed` / `nulls_backfilled` / `validation_errors` 没有任何写入方 → 永远为空
3. `add_validation_error()` 只在定义处出现，没有 middleware 调用它 → 行级违规记录机制形同虚设

### 5.6 exceptions.py（异常 → exit_code 映射）

| 异常 | exit_code | 语义 |
|---|---|---|
| `CleanerError`（基类，默认） | 1 | 通用业务失败 |
| `CleanerConfigError` | 2 | 配置错误 |
| `CleanerDataNotFoundError` | 5 | 源数据不存在（跳过） |
| `CleanerDataEmptyError` | 5 | 源数据为空（跳过） |
| `CleanerValidationError` | 1 | 校验不通过 |
| `CleanerOutputExistsError` | 6 | 输出已存在（跳过） |

---

## 6. Middleware 规则链（核心）

### 6.1 注册与调度（base.py）

- `@middleware(order=100, clean_type=None)`：把 `{fn, name, order, clean_type}` 追加到全局 `_MIDDLEWARES` 列表
- `get_middlewares(clean_type)`：
  - 过滤：`ct is None or ct == clean_type`（全局 + 专属）
  - 按 `(order, 注册序号)` 排序（同 order 按注册先后，稳定）
  - 返回 fn 列表
- `list_middlewares(clean_type=None)`：调试用，列出注册表

`__init__.py` 导入全部模块触发注册；`task.py` 顶部 `import darwin_fetch.cleaners.middlewares` 也触发注册。新增 middleware 文件必须在 `__init__.py` 加 import 才生效。

### 6.2 全局规则链（steps.py + 3 个全局专属模块）

**order 分段约定**：10-19 预处理 / 20-29 类型转换填充 / 30-49 计算校验 / 50-69 一致性类型校验 / 90-99 输出。

按实际 order 排序的完整链（含其他模块）：

| order | 名称 | 来源 | 逻辑 |
|---|---|---|---|
| 10 | `field_mapping` | steps | 按 `adapter.{source}.clean.types.{raw_type}.mapping`（或 fallback `field.mapping`）rename 列，只 rename 已存在的列 |
| 11 | `json_extract` | json_extract.py | 按 `types.{raw_type}.json_extract` 从 dict/JSON 列按 path 列表提取到新列 |
| 12 | `value_mapping` | steps | 按 `value_map: {字段: {源值: 标准值}}` 对列 `.replace(mapping)` |
| 15 | `field_transform` | steps | 三种 spec：`{value:常量}`（不要求列存在，直接赋值）/`{factor:倍数}`（乘法）/`{action:名称}`（调用 TRANSFORM_ACTIONS）；未知 action → warning 跳过 |
| 16 | `ftshare_fill_trade_date` | ftshare.py | 仅 `ctx.source=="ftshare"`：无 trade_date 时，有 ts_millis 则按 UTC+8 推出 YYYYMMDD，否则填 ctx.date |
| 16 | `ftshare_fill_symbol` | ftshare.py | 仅 ftshare：无 symbol 时用 ctx.filename 填（hist 类型 filename 即 symbol） |
| 16 | `ohlcvault_fill_symbol` | ohlcvault.py | 仅 ohlcvault：无 symbol 时用 ctx.filename 填 |
| 16 | `stk_code_exchange` | stk_code.py | 专属 stk_code：无 exchange 时从 symbol 后缀提取大写（000001.SZ→SZ） |
| 17 | `gold_sge_filter_main` | gold_sge.py | 专属 gold_sge：只保留 symbol ∈ {Au99.99}，记录 rows_removed |
| 17 | `stk_code_market_name` | stk_code.py | 专属 stk_code：有 market+exchange 时按 {(主板,SH):沪市主板,...} 映射出 market_name，未命中保留 market |
| 17 | `hk_code_normalize` | stk_hkcode.py | 专属 stk_hkcode：market→港股主板/创业板，硬编码 exchange=HK/area=香港/is_hs=none，industry 不设（output 补 None） |
| 17 | `trade_calendar_prev` | trd_calendar.py | 专属 trd_calendar：按 exchange 分组，用 bisect 取每个 cal_date 之前最近 is_open=1 的日期作 trade_date_prev；若已存在且全非空则跳过（tushare 自带 pretrade_date） |
| 18 | `stk_limit_null_markers` | stk_limit.py | 专属 stk_limit：up_limit/down_limit 的 ≤0 或 >100000 → NaN，记 outliers_corrected |
| 18 | `trade_calendar_timestamps` | trd_calendar.py | 专属 trd_calendar：按 exchange 的时段表（SSE/SZSE 09:30-15:00 UTC+8 / HKEX 09:30-16:00 UTC+8 / XNYS 09:30-16:00 UTC-5）算 day_begin_ms/day_end_ms/trade_begin_ms/trade_end_ms；非交易日 trade 边界为 None |
| 18 | `stk_code_market_name_infer` | stk_code.py | 专属 stk_code：无 market_name 列时，按 symbol 前缀推断（60/68→沪市主板/科创板等），不覆盖 tushare 已有值 |
| 20 | `type_conversion` | steps | 按 `fields[field].type → types[type].converter` 转 dtype：numeric(bool→Int64, 其他 pd.to_numeric coerce) / string(astype str) / datetime / timestamp_ms(Int64)；未注册 converter 跳过 |
| 30 | `fill` | steps | 按字段 fill.strategy（默认 ffill）+ fill.max_days（默认 5），有 symbol 列则 groupby(symbol).ffill(limit)，记 nulls_filled |
| 40 | `formula_compute` | steps | 按字段 formula：字段不存在→建 None；全空→df.eval 计算并记 computed_fields；部分缺失→warning 不计算（避免掩盖问题） |
| 45 | `formula_validate` | steps | 按字段 formula 验证已有值：提取依赖字段（正则+关键字表排除），缺失→warning 跳过；inf→warning 跳过；bool 字段跳过；diff 与 rel_diff 双超容差（默认 rel 0.003 / abs 0.01，字段级可覆盖）才记 validated_fields + warning |
| 50 | `consistency` | steps | 按字段 consistency 规则名查 CONSISTENCY_RULES（high≥low / high≥open / high≥close / low≤open / low≤close），违规行把该字段置 None，记 outliers_corrected + warning |
| 60 | `type_validate` | steps | 按字段 type 的约束：先 inf/±inf 置空，再 min 越界置空，再 max 越界置空，记 outliers_corrected |
| 90 | `output` | steps | 校验 output 列都在 fields（否则 CleanerConfigError）；缺失字段补 None；`return df[output_fields]`（按契约顺序筛列） |
| 95 | `sort` | steps | 按契约 sort（dict key 顺序=优先级，值 ascending/descending），过滤不存在字段，`sort_values(...).reset_index(drop=True)` |

### 6.3 steps.py 的三个辅助表

**TRANSFORM_ACTIONS**（7 个命名动作）：
- `symbol_to_standard`：`sh.600000`/`sz.000001` → `600000.SH`/`000001.SZ`（前缀移到后缀并大写）
- `date_to_compact`：`2026-10-09`/`2026/10/09` → `20261009`
- `datetime_to_compact`：`2026-10-08 09:30:00` → `20261008093000000`（17 位；已纯数字则 zfill(17)）
- `zero_to_nan`：0 → NaN（先 to_numeric 比较再 mask）
- `symbol_suffix`：6 位纯数字 → 带市场后缀（60/68/900→.SH，00/30/200/201→.SZ，8/4/92→.BJ）
- `us_symbol_suffix`：`AAPL` → `AAPL.US`（已有 .US 不变）
- `float_to_compact`：float64 的 `20261105.0` → `"20261105"`（用于 ohlcvault 的 out/delist_date）

**CONSISTENCY_RULES**（5 条）：见上表 order 50。

**公式相关**：
- `_eval_formula`：`df.eval(formula)`，标量结果包成 Series
- `_extract_formula_fields`：正则 `\b([a-zA-Z_]\w*)\b` 提取标识符，减去关键字表（and/or/not/in/is/if/else/True/False/None/abs/min/max/round/int/float/str/bool/where/index/columns）

### 6.4 专属 middleware 的时机选择规律

所有专属 middleware 都刻意插在 `field_mapping(10)` 之后、`type_conversion(20)` 之前，因为：
- 此时源字段已被 rename 成契约字段名（如 code→symbol）
- 此时字段还是 str 类型（便于字符串操作如后缀提取/正则映射）
- 早于 20 的 dtype 转换，避免类型变化破坏字符串逻辑

`gold_sge` 的过滤(17) 和 `stk_limit` 的置空(18) 也都在这个窗口内。

---

## 7. 工具脚本逻辑

### 7.1 clean_types.py（反查工具）
- 加载配置顺序：cleaner.json → clean.d/*.json → adapters/*/adapter_clean.json（`_load_all_configs`）
- `find_pairs(cts)`：遍历 `adapter.*.clean.types`，找 outputs/裸list/filter 并集里含目标 ct 的，返回 `["source/raw_type"]` 排序去重
- `list_all_clean_types()`：取所有 adapter 声明过的 ct 并集；扫不到才回退 cleaner.clean_type 契约声明
- CLI：`python clean_types.py stk_ohlcv[,stk_valuation]`（查对）/ `--list_types`（列全部）
- 被 `clean_common.sh::resolve_clean_type_pairs` 调用（--clean_type 反向找 (source,raw_type)）

### 7.2 clean_parquet_scan.py
- 扫 `data/clean/{ct}/{source}/{date_key}/*.parquet`，输出 `{date}\t{ct}\t{max_mtime}`
- `max_mtime` = 目录下所有 parquet 的最大 mtime（epoch 秒）
- date_key 单日 → date 自身；范围（`19901219_20260731`）→ 用 `sessions_in_range(start, end, exchange)` 展开成交易日（exchange 从 clean.d/{ct}.json 的 exchange 读，默认 SSE）
- 多 date_key 覆盖同一 date → 取 max_mtime；只输出有数据的 (date, ct)；排序 date DESC / ct ASC

### 7.3 dump_clean_config.py
- 按 clean.py 相同顺序预加载配置树（可选 --no_adapter 只看契约）
- 打印指定 prefix 或整树，`--pretty` 缩进、`--sort` 排序 key
- 用于查看 `_merge`/`_imports` 展开后的实际配置

---

## 8. 上下游耦合点清单（剥离 darwin-cleaner 时必须处理）

1. **路径常量耦合**：`Cleaner`/`CleanTask`/`contract`/bootstrap 都用 `PACKAGE_ROOT`（=darwin-fetch/src/darwin_fetch）、`PROJECT_ROOT`（=darwin-fetch/）。新库需要自己的路径来源或改为显式注入。
2. **全局配置状态**：`add_config` 不幂等，contract.py 用 `_LOADED` 补丁；配置树是进程级全局，契约/mapping/transform 全靠 `get_config("cleaner.clean_type.X")` / `try_config("adapter.{source}.clean.types.X")` 读取。新库若要做纯函数服务，需把配置读取改为显式参数注入。
3. **selectors 直接 import**：`selectors/config.py` 顶部 `from darwin_fetch.cleaners import contract as clean_contract`，`CleanContract`/`FieldSpec` 被 Select 层复用。新库需决定是继续提供这两个类型，还是把契约读取下沉为共享库。
4. **`cleaner.output.base_dir` 配置键**：Cleaner 与 selectors/selector.py 都读它来定位 data/clean。
5. **meta 后缀不一致**：clean 自己写 `{filename}.json`，darwin-common 通用 parquet 存储用 `.meta.json`。select/normalize 侧不读 meta 只读 parquet，但通用工具链可能混用。
6. **done 路径约定**：`_parse_done_path` 硬编码路径里必须有 `fetch` 段且后四段固定；`write_done` 硬编码把 `fetch` 替换成 `clean`；meta 的 `clean_done` 用字符串 `replace("/fetch/", "/clean/")`。这些都是"上下游事务"，按用户约定不进新库边界。
7. **clean.d 契约归属**：用户已明确 `clean.d` 契约不进新库（它是具体数据的清洗要求，不是清洗规则）。契约加载发生在 Cleaner/selectors 侧，新库接收"已解析的契约/规则"作为输入。
8. **wrapper/批量调度**：`clean_batch.sh`、`clean_common.sh`、`clean_wrapper.sh` 属于断点/wrapper 层，按用户约定留在 darwin-fetch。
9. **clean_parquet_scan.py**：属读侧扫描工具，按用户约定不进新库边界。
10. **交易日历硬编码**：`trd_calendar.py` 的时段表（09:30-15:00 UTC+8 等）和 `stk_code.py` 的板块前缀表都是硬编码业务知识，属于可配置化/专业库替换的候选。
11. **middleware 异常降级**：`_run_middlewares` 吞掉非 Config/Validation 异常只记 warning，这是行为语义，迁移时需明确定义新库的错误策略（fail-fast vs warn-continue）。

---

## 9. 已知问题与缺口（代码事实）

| # | 问题 | 位置 | 影响 |
|---|---|---|---|
| 1 | `report.save()` 从未被调用 | report.py 定义，全项目无调用 | 详细审计报告不落盘，无法事后排查 |
| 2 | `duplicates_removed` / `nulls_backfilled` / `validation_errors` 无写入方 | report.py | meta 里的 cleaning_stats 这几项恒为 0/空 |
| 3 | middleware 非配置类异常被降级为 warning | task.py `_run_middlewares` | 规则失败时数据可能未清洗却 status=success |
| 4 | 文件头注释"10 步"与实际 11 个步骤不符 | steps.py 头部 | 文档漂移 |
| 5 | `stk_ohlcv.py` 是模板占位，未注册任何 middleware | stk_ohlcv.py | 无影响，但易误导 |
| 6 | `add_config` 不幂等靠 `_LOADED` 补丁 | contract.py | 进程级全局状态，测试隔离困难 |
| 7 | 注释中提到的 `command` 字段被注释掉不写断点 | cleaner.py / result.py | 断点里无法直接拿到重跑命令 |
| 8 | `_apply_filter` 的 regex 与 exclude_regex 同时配置时只生效 regex | task.py | 潜在配置误用 |
| 9 | meta 后缀 `.json` 与 darwin-common `.meta.json` 不一致 | task.py vs darwin-common | 通用工具链混用风险 |
| 10 | `exclude_regex` 配置字段在代码里被读取但现有 adapter 配置未使用 | task.py | 死配置 |

---

## 10. 面向 darwin-cleaner 的逻辑边界划分（基于用户确认的四点约定）

**用户确认的边界**：
1. 新库 = 配置和契约驱动的清洗引擎：对一个或多个（single 目录多文件可合并）parquet 做行分叉、列分叉、施加清洗规则与校验，返回数据 + meta。**上下游事务不在边界内**（读取配置、clean_parquet_scan.py 等）。
2. middleware 方式清洗，支持动态插入新规则（专属源侧 middleware 加载）。
3. 认同先进理念归纳（契约先行、校验即测试、资产化与血缘、幂等+版本化、交易日历专业化、增量与缓存、命名与迁移治理）。
4. `clean.d` 契约不进新库——它是具体数据的清洗要求，不是清洗规则；上层读取 clean.d 契约后调用 darwin-cleaner 引擎执行清洗。

**据此映射到现有代码**：

| 现有组件 | 归属 | 理由 |
|---|---|---|
| `task.py` 的分叉/跑链/写数据 + meta 生成 | **新库核心** | 清洗引擎本体 |
| `middlewares/base.py` 注册机制 + `steps.py` 全局规则 + 各专属 middleware | **新库核心** | 规则即引擎能力；动态插入由注册机制保证 |
| `context.py` / `result.py`（清洗结果数据类）/ `report.py` / `exceptions.py` | **新库核心**（可裁剪 done 相关字段） | 执行期数据模型 |
| `contract.py` 的 `CleanContract`/`FieldSpec` 数据结构 | **新库核心**（作为输入契约类型） | 契约是引擎的输入模型 |
| `adapter_clean.json` 的 mapping/transform/value_map/json_extract/filter 配置格式 | **新库定义并解释** | 这是"清洗规则"的声明，不是具体数据契约 |
| `clean.d/*.json`、`stk_base.json` 的具体字段/输出/公式 | **不进新库**（上层注入） | 用户明确约定：具体数据的清洗要求 |
| `cleaner.py` 的 done 路径解析/读断点/写断点 | **留 darwin-fetch** | 上下游事务 |
| `clean_batch.sh` / `clean_common.sh` / `clean_wrapper.sh` | **留 darwin-fetch** | 断点/wrapper 调度层 |
| `clean.py` CLI 入口 | **留 darwin-fetch**（薄封装调新库） | 事务边界 |
| `clean_types.py` / `clean_parquet_scan.py` / `dump_clean_config.py` | **留 darwin-fetch** | 工具/读侧扫描 |
| `config` 读取（`bootstrap`/`add_config` 全局状态） | **留 darwin-fetch，新库接收已解析配置** | 用户明确约定：读取配置不在边界内 |

**建议的新库对外接口形态（不写实现代码，仅描述）**：
- 一个"清洗服务/纯函数"：输入 =（raw parquet 路径或 DataFrame + 已解析的契约对象 + 源侧规则配置 + 分叉声明），输出 =（清洗后的 DataFrame/parquet + meta + 完整报告）
- middleware 注册表支持外部注入新规则（装饰器或显式注册 API），保持 order + clean_type 双重筛选语义
- 明确错误策略：配置/校验错误 fail-fast；其他规则异常至少要有可观测记录（避免现状的静默降级）
- meta/报告字段沿用现有命名以平滑迁移，同时补齐 `duplicates_removed`/`nulls_backfilled`/`validation_errors` 的写入路径

---

## 11. 附录

### 11.1 clean_type 全清单（clean.d 目录，32 份 JSON）

`etf_adj, etf_code, etf_corp, etf_ohlcv, etf_turnover, gold_sge, stk_adj, stk_base(基础), stk_bidask, stk_code, stk_corp, stk_flow, stk_hkcode, stk_index_basic, stk_index_code, stk_index_ohlcv, stk_limit, stk_market, stk_market_flow, stk_meta(基础), stk_ohlcv, stk_ohlcv_hk, stk_ohlcv_us, stk_pool, stk_pool_member, stk_scale, stk_st, stk_suspend, stk_turnover, stk_valuation, stk_xm, trd_calendar`

其中 `stk_base`/`stk_meta` 是基础契约（非独立清洗类型），`stk_ohlcv_hk/us` 通过 `_merge` 派生自 `stk_ohlcv`。

### 11.2 退出码总表

| 码 | 含义 | 触发处 |
|---|---|---|
| 0 | 成功 | 所有 output 成功 |
| 1 | 业务失败 | 任一 output failed / 通用异常 / 校验失败 |
| 2 | 配置错误 | CleanerConfigError / Cleaner 创建失败 |
| 3 | 临时错误 | clean.py docstring 提及（未在实现中明确返回） |
| 5 | 跳过（无数据/无契约/skip 标记） | status=empty / CleanerDataNotFound / wrapper skip |
| 6 | 跳过（输出已存在/全 skipped） | CleanerOutputExists / finalize 全 skipped / wrapper 幂等命中 |
| 130 | 用户中断 | KeyboardInterrupt |

### 11.3 一次典型执行的时间线示例

`tushare/stock_daily/20260929` 的 fetch done → raw 是 `stock_daily` 日线 parquet：
1. Cleaner 解析出 source=tushare, raw_type=stock_daily, ct 列表 = [stk_ohlcv]（裸 list）
2. ensure_loaded(stk_ohlcv) → 加载 stk_base + stk_ohlcv 契约
3. CleanTask 读 raw → groups={"all": {outputs:[stk_ohlcv]}} → 全量行
4. _fork_one：output_path = data/clean/stk_ohlcv/tushare/20260929/20260929.parquet
5. middleware 链（stk_ohlcv 无专属规则）：field_mapping(10, ts_code→symbol 等) → json_extract(11, 无配置跳过) → value_mapping(12) → field_transform(15, volume×100/amount×1000) → type_conversion(20) → fill(30) → formula_compute(40, close_adj/change/change_pct/amplitude 全空时算) → formula_validate(45) → consistency(50, high≥low 等) → type_validate(60) → output(90, 筛 13 列) → sort(95, trade_date desc/symbol asc)
6. save_data 写 zstd parquet + meta.json（含 data_hash、cleaning_stats）
7. CleanResult.finalize → success/exit 0；write_done 写 clean 断点
8. wrapper 收到 exit 0 → mv tmp done → 真实 clean done
