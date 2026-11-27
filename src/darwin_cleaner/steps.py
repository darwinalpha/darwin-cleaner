# src/darwin_cleaner/steps.py

"""
The 12 generic cleaning steps built into the library.

Migrated and hardened from darwin-fetch's middlewares/steps.py:
- params are read from ctx.rules / ctx.contract / params, no longer from the
  global configuration tree;
- errors are raised in strict mode instead of silently degrading to warnings;
- added dedupe(70), disabled by default.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, TYPE_CHECKING

import pandas as pd

from darwin_cleaner.context import CleanContext
from darwin_cleaner.exceptions import CleanerConfigError
from darwin_cleaner.middleware import middleware

if TYPE_CHECKING:
    pass

DEFAULT_REL_DIFF = 0.003
DEFAULT_ABS_DIFF = 0.01

_FORMULA_IDENT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")
_FORMULA_KEYWORDS = {
    "and", "or", "not", "in", "is", "if", "else", "True", "False", "None",
    "abs", "min", "max", "round", "int", "float", "str", "bool",
    "where", "index", "columns",
}

CONSISTENCY_RULES = {
    "high_ge_low": ("high", ">=", "low"),
    "high_ge_open": ("high", ">=", "open"),
    "high_ge_close": ("high", ">=", "close"),
    "low_le_open": ("low", "<=", "open"),
    "low_le_close": ("low", "<=", "close"),
}


# ============================================================
# Transform actions
# ============================================================

def _symbol_to_standard(s: pd.Series) -> pd.Series:
    def norm(x: Any) -> Any:
        if pd.isna(x):
            return x
        text = str(x).strip()
        m = re.match(r"^(sh|sz|bj)\.(\w+)$", text, re.IGNORECASE)
        return f"{m.group(2)}.{m.group(1).upper()}" if m else text

    return s.astype(str).apply(norm)


def _date_to_compact(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace("-", "", regex=False)
        .str.replace("/", "", regex=False)
    )


def _zero_to_nan(s: pd.Series) -> pd.Series:
    s_num = pd.to_numeric(s, errors="coerce")
    return s.mask(s_num == 0, None)


def _datetime_to_compact(s: pd.Series) -> pd.Series:
    def norm(x: Any) -> Any:
        if pd.isna(x):
            return x
        text = str(x).strip()
        if text.isdigit():
            return text.zfill(17)
        dt = pd.to_datetime(text, errors="coerce")
        if pd.isna(dt):
            return x
        return dt.strftime("%Y%m%d%H%M%S") + "000"

    return s.apply(norm)


def _symbol_suffix(s: pd.Series) -> pd.Series:
    def norm(x: Any) -> Any:
        if pd.isna(x):
            return x
        code = str(x).strip()
        if "." in code:
            return code
        if code.startswith(("60", "68", "900")):
            return f"{code}.SH"
        if code.startswith(("00", "30", "200", "201")):
            return f"{code}.SZ"
        if code.startswith(("8", "4", "92")):
            return f"{code}.BJ"
        return code

    return s.apply(norm)


def _us_symbol_suffix(s: pd.Series) -> pd.Series:
    def norm(x: Any) -> Any:
        if pd.isna(x):
            return x
        code = str(x).strip()
        return code if code.endswith(".US") else f"{code}.US"

    return s.apply(norm)


def _float_to_compact(s: pd.Series) -> pd.Series:
    def norm(x: Any) -> Any:
        if pd.isna(x):
            return None
        try:
            return str(int(x))
        except (ValueError, OverflowError):
            return None

    return s.apply(norm)


DEFAULT_ACTIONS: dict[str, Any] = {
    "symbol_to_standard": _symbol_to_standard,
    "date_to_compact": _date_to_compact,
    "datetime_to_compact": _datetime_to_compact,
    "zero_to_nan": _zero_to_nan,
    "symbol_suffix": _symbol_suffix,
    "us_symbol_suffix": _us_symbol_suffix,
    "float_to_compact": _float_to_compact,
}


_CONVERTERS = {
    "numeric": lambda s: s.astype("Int64") if s.dtype == bool else pd.to_numeric(s, errors="coerce"),
    "string": lambda s: s.astype(str),
    "datetime": lambda s: pd.to_datetime(s, errors="coerce"),
    "timestamp_ms": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
}


def _eval_formula(df: pd.DataFrame, formula: str) -> pd.Series:
    result = df.eval(formula)
    if isinstance(result, pd.Series):
        return result
    return pd.Series(result, index=df.index)


def _extract_formula_fields(formula: str) -> set[str]:
    if not formula:
        return set()
    return set(_FORMULA_IDENT_RE.findall(formula)) - _FORMULA_KEYWORDS


# ============================================================
# 12 built-in steps
# ============================================================

@middleware(order=10)
def field_mapping(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Rename fields according to ctx.rules.mapping."""
    mapping = ctx.rules.mapping
    if not mapping:
        return df
    rename = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=rename)


@middleware(order=12)
def value_mapping(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Replace values according to ctx.rules.value_map."""
    value_map = ctx.rules.value_map
    if not value_map:
        return df
    for field, mapping in value_map.items():
        if field in df.columns:
            df[field] = df[field].replace(mapping)
    return df


@middleware(order=15)
def field_transform(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Apply constant assignment / multiplication / actions according to ctx.rules.transform."""
    actions = dict(DEFAULT_ACTIONS)
    # Allow injecting custom actions via params["actions"] (lightweight alternative to strongly typed Options)
    actions.update(params.get("actions", {}) or {})

    for field, spec in ctx.rules.transform.items():
        if spec.kind == "value":
            df[field] = spec.value
            continue
        if field not in df.columns:
            continue
        if spec.kind == "factor":
            if spec.factor is None:
                raise CleanerConfigError(f"transform factor is missing the factor value: {field}")
            df[field] = df[field] * spec.factor
        elif spec.kind == "action":
            if not spec.action:
                raise CleanerConfigError(f"transform action is missing the action name: {field}")
            action = actions.get(spec.action)
            if action is None:
                raise CleanerConfigError(f"Unknown transform action: {spec.action}")
            df[field] = action(df[field])
    return df


@middleware(order=20)
def type_conversion(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Unify dtypes using the converter of each contract field type."""
    for field, spec in ctx.contract.fields.items():
        if field not in df.columns:
            continue
        type_spec = ctx.contract.types.get(spec.type)
        conv_name = type_spec.converter if type_spec else "string"
        handler = _CONVERTERS.get(conv_name)
        if handler is not None:
            df[field] = handler(df[field])
    return df


@middleware(order=30)
def fill(df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]) -> pd.DataFrame:
    """Fill values by each field's fill rule, grouped accordingly; statistics go to nulls_filled."""
    group_field = params.get("group_field", "symbol")
    for field, spec in ctx.contract.fields.items():
        if spec.fill is None or field not in df.columns:
            continue
        strategy = spec.fill.strategy
        if strategy == "none":
            continue
        if strategy == "value":
            before = int(df[field].isna().sum())
            df[field] = df[field].fillna(spec.fill.value)
            filled = before - int(df[field].isna().sum())
            if filled:
                ctx.report.bump_mapping("nulls_filled", field, filled)
        elif strategy == "ffill":
            if group_field not in df.columns:
                continue
            before = int(df[field].isna().sum())
            df[field] = df.groupby(group_field)[field].ffill(limit=spec.fill.max_days)
            filled = before - int(df[field].isna().sum())
            if filled:
                ctx.report.bump_mapping("nulls_filled", field, filled)
    return df


@middleware(order=40)
def formula_compute(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Compute fully empty columns from formula; partial missing values only produce a warning."""
    for field, spec in ctx.contract.fields.items():
        if not spec.formula:
            continue
        if field not in df.columns:
            df[field] = None
        null_count = int(df[field].isna().sum())
        total = len(df)
        if null_count == 0:
            continue
        if null_count == total:
            try:
                df[field] = _eval_formula(df, spec.formula)
                ctx.report.bump_mapping("computed_fields", field, int(df[field].notna().sum()))
            except Exception as exc:
                ctx.report.add_warning(f"Failed to compute {field}: {exc}")
        else:
            ctx.report.add_warning(
                f"{field}: {null_count}/{total} rows missing, not computed (partial missing)"
            )
    return df


@middleware(order=45)
def formula_validate(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Dual-tolerance validation: a value is non-conforming only when both rel_diff and abs_diff exceed their limits."""
    for field, spec in ctx.contract.fields.items():
        if not spec.formula or field not in df.columns:
            continue
        required = _extract_formula_fields(spec.formula)
        missing = required - set(df.columns)
        if missing:
            ctx.report.add_warning(f"{field}: formula dependency fields missing: {sorted(missing)}")
            continue

        type_spec = ctx.contract.types.get(spec.type)
        rel_tol = type_spec.rel_diff if type_spec else DEFAULT_REL_DIFF
        abs_tol = type_spec.abs_diff if type_spec else DEFAULT_ABS_DIFF

        valid = df[field].notna()
        if not bool(valid.any()):
            continue
        try:
            expected = _eval_formula(df, spec.formula)
        except Exception as exc:
            ctx.report.add_warning(f"{field}: formula evaluation failed: {exc}")
            continue
        if not isinstance(expected, pd.Series):
            expected = pd.Series(expected, index=df.index)

        if bool(expected.isin([float("inf"), float("-inf")]).any()):
            ctx.report.add_warning(f"{field}: formula produced inf, skipping validation")
            continue
        if pd.api.types.is_bool_dtype(expected) or pd.api.types.is_bool_dtype(df[field]):
            continue

        diff = (df[field] - expected).abs()
        denom = expected.abs().replace(0, pd.NA)
        rel_diff = (diff / denom).fillna(0.0)
        bad = valid & (rel_diff > rel_tol) & (diff > abs_tol)
        bad_count = int(bad.sum())
        if bad_count:
            ctx.report.bump_mapping("validated_fields", field, bad_count)
            ctx.report.add_warning(
                f"{field}: {bad_count} rows do not match the formula (rel_tol={rel_tol}, abs_tol={abs_tol})"
            )
    return df


@middleware(order=50)
def consistency(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Cross-field consistency rules (e.g. high >= low); violations are set to null."""
    for field, spec in ctx.contract.fields.items():
        for rule_name in spec.consistency:
            rule = CONSISTENCY_RULES.get(rule_name)
            if rule is None:
                ctx.report.add_warning(f"Unknown consistency rule: {rule_name}")
                continue
            a, op, b = rule
            if a not in df.columns or b not in df.columns:
                continue
            both = df[a].notna() & df[b].notna()
            if op == ">=":
                bad = (df[a] < df[b]) & both
            elif op == "<=":
                bad = (df[a] > df[b]) & both
            else:
                continue
            bad_count = int(bad.sum())
            if bad_count:
                ctx.report.bump_mapping("outliers_corrected", rule_name, bad_count)
                ctx.report.add_warning(f"Consistency rule {rule_name} violated: {bad_count} rows")
                df[field] = df[field].mask(bad, None)
    return df


@middleware(order=60)
def type_validate(
    df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]
) -> pd.DataFrame:
    """Set out-of-range inf / min / max values to null."""
    for field, spec in ctx.contract.fields.items():
        if field not in df.columns:
            continue
        type_spec = ctx.contract.types.get(spec.type)
        series = df[field]

        inf_mask = series.isin([float("inf"), float("-inf")])
        n_inf = int(inf_mask.sum())
        if n_inf:
            ctx.report.bump_mapping("outliers_corrected", f"{field}.inf", n_inf)
            df[field] = series.mask(inf_mask, None)
            series = df[field]

        if type_spec is None:
            continue
        if type_spec.min is not None:
            bad = series.notna() & (series < type_spec.min)
            n = int(bad.sum())
            if n:
                ctx.report.bump_mapping("outliers_corrected", f"{field}.min", n)
                df[field] = series.mask(bad, None)
                series = df[field]
        if type_spec.max is not None:
            bad = series.notna() & (series > type_spec.max)
            n = int(bad.sum())
            if n:
                ctx.report.bump_mapping("outliers_corrected", f"{field}.max", n)
                df[field] = series.mask(bad, None)
    return df


@middleware(order=70, enabled=False)
def dedupe(df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]) -> pd.DataFrame:
    """Deduplicate by keys; disabled by default; params define what constitutes a duplicate and how to handle it."""
    keys = tuple(params.get("keys") or ()) or ctx.contract.dedupe_keys()
    if not keys:
        raise CleanerConfigError("dedupe is enabled but no keys are specified: duplicate rows cannot be defined")
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise CleanerConfigError(f"dedupe keys column(s) do not exist: {missing}")

    scope = params.get("scope", "keys")
    strategy = params.get("strategy", "first")
    if strategy not in ("first", "last", "error"):
        raise CleanerConfigError(f"Unknown dedupe strategy: {strategy}")

    subset = list(df.columns) if scope == "all_columns" else list(keys)
    duplicated = df.duplicated(subset=subset, keep=False)
    n_dup = int(duplicated.sum())

    if strategy == "error":
        if n_dup:
            raise CleanerConfigError(f"Found {n_dup} duplicate rows (keys={keys})")
        return df

    before = len(df)
    result = df.drop_duplicates(subset=subset, keep="last" if strategy == "last" else "first")
    result = result.reset_index(drop=True)
    removed = before - len(result)
    if removed:
        ctx.report.bump_stat("duplicates_removed", removed)
        if params.get("warn", False):
            ctx.report.add_warning(f"dedupe removed {removed} duplicate rows (strategy={strategy})")
    return result


@middleware(order=90)
def output(df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]) -> pd.DataFrame:
    """Select columns per contract output; missing required columns raise an error, non-required ones are filled with empty values."""
    output_fields = ctx.contract.output
    missing_def = [f for f in output_fields if f not in ctx.contract.fields]
    if missing_def:
        raise CleanerConfigError(f"Output fields not defined in fields: {missing_def}")

    warn_on_missing = bool(params.get("warn_on_missing", False))
    for field in output_fields:
        if field in df.columns:
            continue
        if ctx.contract.is_required(field):
            raise CleanerConfigError(f"Required column missing: {field}")
        df[field] = None
        if warn_on_missing:
            ctx.report.add_warning(f"Non-required column missing, filled with empty values: {field}")
    return df[list(output_fields)]


@middleware(order=95)
def sort(df: pd.DataFrame, ctx: CleanContext, params: Mapping[str, Any]) -> pd.DataFrame:
    """Sort by multiple fields in contract sort."""
    specs = ctx.contract.sort
    if not specs:
        return df
    fields = [s.field for s in specs if s.field in df.columns]
    if not fields:
        return df
    ascending = [s.direction == "ascending" for s in specs if s.field in df.columns]
    return df.sort_values(fields, ascending=ascending).reset_index(drop=True)


BUILTIN_FNS = (
    field_mapping,
    value_mapping,
    field_transform,
    type_conversion,
    fill,
    formula_compute,
    formula_validate,
    consistency,
    type_validate,
    dedupe,
    output,
    sort,
)


__all__ = [
    "BUILTIN_FNS",
    "DEFAULT_ACTIONS",
    "field_mapping",
    "value_mapping",
    "field_transform",
    "type_conversion",
    "fill",
    "formula_compute",
    "formula_validate",
    "consistency",
    "type_validate",
    "dedupe",
    "output",
    "sort",
]
