# src/darwin_cleaner/exceptions.py

"""
darwin-cleaner exception hierarchy.

Design conventions:
    - Exceptions only express "what happened" and carry no CLI exit code; the
      upper layer maps them to exit codes.
    - Configuration/rule exceptions indicate a problem with the contract or
      rules passed by the caller and should be raised before execution (preflight).
    - Data exceptions indicate that input data does not meet cleaning prerequisites.
    - Validation exceptions indicate that data failed validation in strict mode.
"""

from __future__ import annotations


class CleanerError(Exception):
    """Generic base exception for the cleaning engine."""


class CleanerConfigError(CleanerError):
    """Invalid contract definition, or rules passed by the caller cannot be parsed."""


class CleanerRuleError(CleanerError):
    """Rule not registered, name conflict, or the rule's action / dependency does not exist."""


class CleanerDataNotFoundError(CleanerError):
    """The input source passed by the caller does not exist or is unreadable."""


class CleanerDataEmptyError(CleanerError):
    """Input data or any output has 0 rows; handled through the error path like a validation failure."""


class CleanerValidationError(CleanerError):
    """Data failed rule validation in strict mode."""


__all__ = [
    "CleanerError",
    "CleanerConfigError",
    "CleanerRuleError",
    "CleanerDataNotFoundError",
    "CleanerDataEmptyError",
    "CleanerValidationError",
]
