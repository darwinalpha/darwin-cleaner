# darwin-cleaner 示例

English · [简体中文](README.cn.md)

单文件清洗引擎的可运行示例。每个脚本都是自包含的：会自行把 `src/` 加入 `sys.path`，无需先安装库。

## 运行

在项目根目录执行：

```bash
python examples/demo.py       # 运行全部示例
python examples/01_basic.py   # 运行单个示例
```

安装库之后，也可以直接用 `python examples/01_basic.py` 运行任意脚本。

## 示例清单

| 脚本 | 演示内容 |
|---|---|
| `01_basic.py` | 内存 DataFrame 输入、字段映射、类型转换、输出契约 |
| `02_parquet.py` | 读取单个 parquet、meta 中的内容 hash 与规则指纹 |
| `03_forks.py` | 行分叉（对原始列做正则）与列分叉（同一行组对应多个契约） |
| `04_middleware.py` | 带声明式参数的自定义 middleware，以及从目录加载 middleware |
| `05_dedupe.py` | 开启内置 `dedupe` 步骤并选择去重策略 |

`custom/` 目录存放供 `04_middleware.py` 使用的用户 middleware 示例模块。

所有脚本只产出内存结果。写 parquet / csv / meta 文件是上层调用方的责任。
