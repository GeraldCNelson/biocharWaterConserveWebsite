import numpy as np
import pandas as pd

from biochar_app.scripts.research.analyze_ec_adjustment import (
    design_matrix,
    huber_irls,
    target_vector,
)


def test_huber_fit_limits_single_extreme_outlier():
    x_value = np.arange(20, dtype=float)
    design = np.column_stack([np.ones_like(x_value), x_value])
    outcome = 1.0 + 2.0 * x_value
    outcome[-1] += 500.0

    robust_slope = huber_irls(design, outcome)[1]
    ordinary_slope = np.linalg.lstsq(design, outcome, rcond=None)[0][1]

    assert abs(robust_slope - 2.0) < abs(ordinary_slope - 2.0)


def test_target_vector_encodes_pair_position_and_equal_year_weights():
    frame = pd.DataFrame(
        {
            "pair": ["S1/S2", "S3/S4"],
            "position": ["Top", "Bottom"],
            "year": [2023, 2026],
            "delta_vwc": [0.0, 1.0],
            "mean_vwc": [20.0, 30.0],
            "delta_temp": [0.0, 1.0],
            "mean_temp": [10.0, 20.0],
            "ante_delta_vwc": [0.0, 1.0],
            "ante_mean_vwc": [20.0, 30.0],
            "days_since_irrigation": [5.0, 10.0],
            "irrigation_day": [0.0, 1.0],
        }
    )
    design, _ = design_matrix(frame)
    target = pd.Series(
        target_vector(list(design.columns), "S3/S4", "Bottom"),
        index=design.columns,
    )

    assert target["pair_S3S4"] == 1
    assert target["position_Bottom"] == 1
    assert target["pair_x_Bottom"] == 1
    assert target["position_Middle"] == 0
    assert target[["year_2024", "year_2025", "year_2026"]].tolist() == [0.25, 0.25, 0.25]
