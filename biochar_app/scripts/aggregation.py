# aggregation.py
from typing import (
    Any, Callable, Dict, Iterable, Mapping, Tuple
)
import pandas as pd
from biochar_app.scripts.routes_utils import periods_to_list_of_dicts

PeriodBounds = Tuple[pd.Timestamp, pd.Timestamp]
# A mapping from period‐code → its (start, end) for a given year
PeriodSpecFn = Callable[[int], Mapping[str, PeriodBounds]]
# The function that computes raw/ratio stats for one slice
ComputeFn = Callable[
    [pd.DataFrame, str, str, str],
    Tuple[Dict[str, Any], Dict[str, Any]]
]

def build_summary(
    df: pd.DataFrame,
    year: int,
    get_period_specs: PeriodSpecFn,
    variables: Iterable[str],
    strips: Iterable[str],
    depths: Iterable[str],
    compute_fn: ComputeFn,
    zero_ratio_for: Iterable[str] = (),
) -> Dict[str, Dict]:
    """
    A generic driver that:
     - asks get_period_specs(year) for { code: (start_ts,end_ts) }
     - for each period code, slices df on that interval
     - for each var/strip/depth calls compute_fn → (raw,ratio)
     - zeroes out ratio if var in zero_ratio_for
     - returns nested dict[period][var][strip_depth] = {raw_statistics, ratio_statistics}
    """
    period_specs = get_period_specs(year)
    summary: Dict[str, Dict] = {}

    for code, (start, end) in period_specs.items():
        sub = df[(df.timestamp >= start) & (df.timestamp <= end)]
        stats_for_code: Dict[str, Dict] = {}

        for var in variables:
            stats_for_code[var] = {}
            for strip in strips:
                for depth in depths:
                    raw_stats, ratio_stats = compute_fn(sub, var, strip, depth)
                    if var in zero_ratio_for:
                        ratio_stats = {}
                    stats_for_code[var][f"{strip}_D{depth}"] = {
                        "raw_statistics": raw_stats,
                        "ratio_statistics": ratio_stats,
                    }

        summary[code] = stats_for_code

    return summary