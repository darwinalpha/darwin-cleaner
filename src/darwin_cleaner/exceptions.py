# src/darwin_cleaner/exceptions.py

"""
darwin-cleaner 异常层次。

设计约定：
    - 异常只表达"发生了什么"，不携带 CLI 退出码；上层自行映射到退出码。
    - 配置/规则类异常表示调用方传入的契约或规则有问题，应在执行前（preflight）抛出。
    - 数据类异常表示输入数据不符合清洗前提。
    - 校验类异常表示严格模式下数据未通过校验。
"""

from __future__ import annotations


class CleanerError(Exception):
    """清洗引擎通用异常基类。"""


class CleanerConfigError(CleanerError):
    """契约定义不合法，或调用方传入的规则无法解析。"""


class CleanerRuleError(CleanerError):
    """规则未注册、名称冲突，或规则的 action / 依赖不存在。"""


class CleanerDataNotFoundError(CleanerError):
    """调用方传入的输入源不存在或不可读。"""


class CleanerDataEmptyError(CleanerError):
    """输入数据或任一输出为 0 行；与校验失败一样按错误路径处理。"""


class CleanerValidationError(CleanerError):
    """严格模式下数据未通过规则校验。"""


__all__ = [
    "CleanerError",
    "CleanerConfigError",
    "CleanerRuleError",
    "CleanerDataNotFoundError",
    "CleanerDataEmptyError",
    "CleanerValidationError",
]
