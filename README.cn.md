# darwin-cleaner

[English](https://github.com/darwinalpha/darwin-cleaner/blob/main/README.md) · 简体中文

契约与规则驱动的**单文件清洗引擎**：读取一个 parquet（或内存 DataFrame）→ 行分叉 / 列分叉 → 按 order 跑 middleware → 在内存中返回数据与 meta。

Python >= 3.10。

```bash
pip install darwin-cleaner
```

## 边界

本库只做数据变换，不做上下游事务：

| 库内 | 库外（上层应用） |
|---|---|
| 读单个 parquet / 接收 DataFrame | 多文件合并、文件发现、新鲜度判定 |
| 行分叉 / 列分叉 | 加载 `cleaner.json` / `clean.d/*.json` / adapter 配置 |
| 12 个内置 middleware（含默认关闭的 `dedupe`） | 写 parquet / csv / meta / 断点 |
| 契约与规则校验、统计与报告生成 | CLI、调度、退出码、业务标识 |

详细设计见 [docs/design.cn.md](docs/design.cn.md)，现有代码梳理见 [docs/clean-stage-code-walkthrough.cn.md](docs/clean-stage-code-walkthrough.cn.md)。可运行示例见 [examples](examples/README.cn.md)。

---

## 1. 快速开始

```python
from pathlib import Path
from darwin_cleaner import (
    CleaningEngine, CleanRequest, CleanOptions, Contract, FieldSpec,
    MiddlewareOverride, MiddlewareRegistryImpl, Rules, TypeSpec,
)

registry = MiddlewareRegistryImpl()
registry.builtins()  # 12 个内置步骤，dedupe 默认关闭

contract = Contract(
    name="stk_ohlcv",
    fields={
        "symbol": FieldSpec(name="symbol", type="str"),
        "trade_date": FieldSpec(name="trade_date", type="str"),
        "close": FieldSpec(name="close", type="float"),
    },
    types={"float": TypeSpec(name="float", converter="numeric", min=0)},
    output=("symbol", "trade_date", "close"),
    keys=("symbol", "trade_date"),  # 参与 keys 的列自动 required
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
        meta = out.meta      # 内容 hash + 规则指纹 + 统计
        report = out.report  # 过程审计
        # 写盘由上层负责
```

---

## 2. 核心概念

- **Contract**：字段、类型、输出列、唯一行键、排序。由上层解析配置后注入，引擎不读配置文件。
- **FieldSpec.required**：三态。`True` 缺失即报错；`False` 显式允许缺失（补空列）；`None` 未声明时由引擎推导——参与 `sort` / `keys` 的列自动为 `True`，其余为 `False`。
- **Rules**：`mapping` / `value_map` / `transform` / `filters` / `outputs` 等，替代 middleware 内的全局配置读取。
- **Middleware**：统一签名 `(df, ctx, params) -> df | None`。`order` 决定执行顺序，`params` 是声明式参数（计入规则指纹），`enabled` 是开关；`MiddlewareOverride` 可按请求覆盖。
- **order 分段**：`10-19` 预处理、`20-29` dtype、`30-39` 填充、`40-49` 公式、`50-69` 校验/去重、`90-99` 输出。

---

## 3. 内置 middleware

| order | 名称 | 作用 | 默认 |
|---|---|---|---|
| 10 | `field_mapping` | 字段改名 | 开 |
| 12 | `value_mapping` | 值替换 | 开 |
| 15 | `field_transform` | 常量 / 系数 / 动作 | 开 |
| 20 | `type_conversion` | dtype 归一 | 开 |
| 30 | `fill` | 分组 ffill | 开 |
| 40 | `formula_compute` | 全空列按公式计算 | 开 |
| 45 | `formula_validate` | 双容差公式校验 | 开 |
| 50 | `consistency` | 字段间一致性 | 开 |
| 60 | `type_validate` | inf / min / max 越界置空 | 开 |
| 70 | `dedupe` | 按 keys 去重 | **关** |
| 90 | `output` | 按契约选列 | 开 |
| 95 | `sort` | 按契约排序 | 开 |

`builtins()` 会注册全部 12 个。关闭的 middleware 在注册表层被跳过：不进入执行计划，也不会被调用。用户 middleware 可通过 `load_middlewares_from_dir(path)` 从目录加载，再显式注册。同名 middleware 在注册时抛 `CleanerRuleError`。

---

## 4. 测试

```bash
PYTHONPATH=src pytest
```

测试覆盖：契约三态推导、分叉计划、注册表（内置/开关/覆盖/冲突/指纹/目录加载）、12 个内置步骤逐个、引擎端到端（行+列分叉、parquet、零行、必填列缺失）、strict/宽松错误模式。

---

## 5. 许可证

MIT
