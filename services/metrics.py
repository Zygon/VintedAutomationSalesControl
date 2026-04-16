from __future__ import annotations

from typing import Any

import pandas as pd


def safe_sum(df: pd.DataFrame, column: str) -> float:
    if df.empty or column not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def safe_count(df: pd.DataFrame) -> int:
    return 0 if df.empty else int(len(df))


def safe_avg(df: pd.DataFrame, column: str) -> float:
    if df.empty or column not in df.columns:
        return 0.0
    series = pd.to_numeric(df[column], errors="coerce").dropna()
    if series.empty:
        return 0.0
    return float(series.mean())


def status_breakdown(df: pd.DataFrame, status_column: str = "status") -> pd.DataFrame:
    if df.empty or status_column not in df.columns:
        return pd.DataFrame(columns=[status_column, "count"])

    result = (
        df.groupby(status_column, dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    return result


def time_series(
    df: pd.DataFrame,
    datetime_column: str,
    value_column: str | None = None,
    freq: str = "D",
) -> pd.DataFrame:
    if df.empty or datetime_column not in df.columns:
        columns = ["period", "count"] if not value_column else ["period", value_column]
        return pd.DataFrame(columns=columns)

    work = df.copy()
    work[datetime_column] = pd.to_datetime(work[datetime_column], errors="coerce", utc=True)
    work = work.dropna(subset=[datetime_column])
    if work.empty:
        columns = ["period", "count"] if not value_column else ["period", value_column]
        return pd.DataFrame(columns=columns)

    work["period"] = work[datetime_column].dt.to_period(freq).dt.to_timestamp()

    if value_column and value_column in work.columns:
        work[value_column] = pd.to_numeric(work[value_column], errors="coerce").fillna(0)
        return (
            work.groupby("period", as_index=False)[value_column]
            .sum()
            .sort_values("period")
        )

    return work.groupby("period", as_index=False).size().rename(columns={"size": "count"}).sort_values("period")


def format_currency(value: float) -> str:
    return f"{value:,.2f} EUR".replace(",", "X").replace(".", ",").replace("X", ".")
