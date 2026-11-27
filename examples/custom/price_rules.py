"""Sample user middleware module for examples/04_middleware.py.

A module in a middleware directory is discovered by load_middlewares_from_dir().
Every function decorated with @middleware is turned into a MiddlewareSpec; the
caller decides whether to register it.
"""

from __future__ import annotations

import pandas as pd

from darwin_cleaner import CleanContext, middleware


@middleware(order=55, version="1.0", params={"column": "close", "floor": 0.0})
def clamp_price(
    df: pd.DataFrame, ctx: CleanContext, params: dict
) -> pd.DataFrame:
    """Replace values below a floor with the floor value."""
    column = params["column"]
    if column in df.columns:
        before = df[column].copy()
        df[column] = df[column].clip(lower=params["floor"])
        changed = int((before != df[column]).sum())
        if changed:
            ctx.report.bump_stat("rows_modified", changed)
    return df
