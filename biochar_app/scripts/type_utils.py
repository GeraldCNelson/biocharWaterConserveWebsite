# biochar_app/scripts/type_utils.py
from __future__ import annotations

from typing import (
    Any,
    Optional,
    Sequence,
    Callable,
    Mapping,
    Union,
    cast,
    Protocol,
    TypeAlias,
    Literal
)

import pandas as pd


UnitSystem: TypeAlias = Literal["us", "metric"]

# Keep simple float constants (do not import from numpy stubs)
NAN: float = float("nan")
POS_INF: float = float("inf")
NEG_INF: float = float("-inf")


def df_cols(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    """
    Always return a DataFrame (helps df[cols] stub issues).
    Using .loc keeps this stable and type checkers like it.
    """
    return df.loc[:, list(cols)]


def to_float_series(s: Any) -> pd.Series:
    """
    Best-effort numeric conversion to a *pandas Series[float]*.

    Key idea: always coerce input into a Series first, so PyCharm doesn't infer
    ndarray/scalar unions from pd.to_numeric().
    """
    if isinstance(s, pd.Series):
        ser = s
    elif isinstance(s, pd.DataFrame):
        # If a DataFrame slipped in, take first column deterministically.
        ser = s.iloc[:, 0]
    else:
        # ndarray/list/scalar -> wrap into Series
        ser = pd.Series(s)

    out = pd.to_numeric(ser, errors="coerce")
    # Use dtype string to avoid "float|Series has no astype" nonsense
    return out.astype("float64")


def safe_tolist(x: Any) -> list[Any]:
    """
    Convert series/array/scalar to JSON-safe list without Optional issues.
    """
    if x is None:
        return []
    if isinstance(x, pd.Series):
        return x.tolist()
    if isinstance(x, pd.Index):
        return x.tolist()
    if isinstance(x, (list, tuple)):
        return list(x)
    # includes numpy arrays without importing numpy
    if hasattr(x, "tolist"):
        try:
            return list(x.tolist())
        except Exception:
            pass
    return [x]


def finite_min_max(block: pd.DataFrame) -> tuple[Optional[float], Optional[float]]:
    """
    Scalar min/max ignoring NaN/inf.

    Avoid np.isfinite entirely (numpy stubs cause PyCharm warnings).
    """
    if block.empty:
        return None, None

    # Ensure float dataframe
    df = block.copy()
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Replace +/-inf with NaN so min/max ignore them
    df = df.replace([POS_INF, NEG_INF], NAN)

    col_mins = df.min(numeric_only=True)
    col_maxs = df.max(numeric_only=True)

    # pandas stubs sometimes return scalar unions; guard explicitly
    if not isinstance(col_mins, pd.Series) or not isinstance(col_maxs, pd.Series):
        return None, None

    min_val = col_mins.min()
    max_val = col_maxs.max()

    if pd.isna(min_val) or pd.isna(max_val):
        return None, None
    return float(min_val), float(max_val)


def safe_timestamp(value: Any) -> Optional[pd.Timestamp]:
    """
    Return scalar Timestamp or None; avoids container types and type unions.
    """
    if value is None:
        return None

    # Reject container-like inputs early
    if isinstance(value, (pd.Series, pd.DataFrame, pd.DatetimeIndex, list, tuple)):
        return None
    if hasattr(value, "__array__"):
        return None

    ts = pd.to_datetime(value, errors="coerce")
    if isinstance(ts, pd.Timestamp) and not pd.isna(ts):
        return ts

    ts2 = pd.to_datetime(str(value), errors="coerce")
    return ts2 if isinstance(ts2, pd.Timestamp) and not pd.isna(ts2) else None


# -----------------------------------------------------------------------------
# Aggregation typing shims
# -----------------------------------------------------------------------------

AggFn = Callable[[pd.Series], Any]
AggSpec = Union[str, AggFn, list[Union[str, AggFn]]]
AggDict = Mapping[str, AggSpec]

class _Aggable(Protocol):
    """
    Protocol for "things that support .agg(spec)" in pandas:
    - DataFrame
    - GroupBy
    - Resampler
    - Rolling/Expanding
    etc.
    """

def agg(self, spec: Any) -> Any: ...


def df_agg(obj: Any, spec: AggDict) -> pd.DataFrame:
    """
    Typed wrapper around obj.agg(spec) for DataFrame/Resampler/GroupBy.
    """
    return cast(pd.DataFrame, obj.agg(spec))


def gb_agg(gb: Any, spec: AggDict) -> pd.DataFrame:
    """
    Typed wrapper around groupby.agg(spec).
    """
    return cast(pd.DataFrame, gb.agg(spec))