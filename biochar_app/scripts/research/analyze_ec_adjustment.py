#!/usr/bin/env python3
"""Analyze six-inch logger EC after moisture and temperature adjustment.

The analysis is intentionally pair based: S1/S2 and S3/S4.  It creates one
daily record for each pair and logger position, fits a Huber robust regression
to log(EC_biochar / EC_control), and uses a calendar-week block bootstrap for
uncertainty.  Laboratory chemistry is analyzed only at whole-strip level.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats


ROOT = Path(__file__).resolve().parents[3]
PARQUET_DIR = ROOT / "biochar_app/data-processed/parquet/summary/15min"
IRRIGATION = ROOT / "biochar_app/data-processed/management/irrigation/irrigation_clean.csv"
CHEMISTRY = ROOT / "biochar_app/data-processed/lab-tests/soil-tests-chem/csv-files/ward_master_soilchem_clean.csv"
OUTPUT_DIR = ROOT / "biochar_app/data-processed/research/ec_adjustment"

YEARS = (2023, 2024, 2025, 2026)
POSITIONS = ("T", "M", "B")
PAIRS = {"S1/S2": ("S1", "S2"), "S3/S4": ("S3", "S4")}
POSITION_LABELS = {"T": "Top", "M": "Middle", "B": "Bottom"}


def huber_irls(x: np.ndarray, y: np.ndarray, *, max_iter: int = 100) -> np.ndarray:
    """Huber M-estimator using iteratively reweighted least squares."""
    beta = np.linalg.lstsq(x, y, rcond=None)[0]
    for _ in range(max_iter):
        residual = y - x @ beta
        scale = 1.4826 * np.median(np.abs(residual - np.median(residual)))
        if not np.isfinite(scale) or scale < 1e-10:
            break
        cutoff = 1.345 * scale
        weights = np.ones_like(residual)
        large = np.abs(residual) > cutoff
        weights[large] = cutoff / np.abs(residual[large])
        root_w = np.sqrt(weights)
        updated = np.linalg.lstsq(x * root_w[:, None], y * root_w, rcond=None)[0]
        if np.max(np.abs(updated - beta)) < 1e-9:
            beta = updated
            break
        beta = updated
    return beta


def load_daily_pairs(*, growing_only: bool = True) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    wanted: list[str] = ["timestamp"]
    for strip in ("S1", "S2", "S3", "S4"):
        for position in POSITIONS:
            wanted.extend(
                [
                    f"EC_1_raw_{strip}_{position}",
                    f"VWC_1_raw_{strip}_{position}",
                    f"T_1_raw_{strip}_{position}",
                ]
            )

    for year in YEARS:
        frame = pd.read_parquet(PARQUET_DIR / f"{year}_15min.parquet", columns=wanted)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        if growing_only:
            frame = frame.loc[frame["timestamp"].dt.month.between(4, 10)].copy()
        frame["date"] = frame["timestamp"].dt.normalize()
        for pair, (biochar, control) in PAIRS.items():
            for position in POSITIONS:
                cols = {}
                for role, strip in (("biochar", biochar), ("control", control)):
                    for variable in ("EC", "VWC", "T"):
                        cols[f"{variable.lower()}_{role}"] = f"{variable}_1_raw_{strip}_{position}"
                daily = frame[["date", *cols.values()]].rename(columns={v: k for k, v in cols.items()})
                for column in cols:
                    daily[column] = pd.to_numeric(daily[column], errors="coerce")
                daily = daily.groupby("date", as_index=False).median(numeric_only=True)
                daily["year"] = year
                daily["pair"] = pair
                daily["position"] = POSITION_LABELS[position]
                records.append(daily)

    data = pd.concat(records, ignore_index=True)
    valid = (
        data["ec_biochar"].gt(0)
        & data["ec_control"].gt(0)
        & data["vwc_biochar"].between(0, 100)
        & data["vwc_control"].between(0, 100)
        & data["t_biochar"].between(-20, 60)
        & data["t_control"].between(-20, 60)
    )
    data = data.loc[valid].sort_values(["pair", "position", "date"]).copy()
    data["log_ec_ratio"] = np.log(data["ec_biochar"] / data["ec_control"])
    data["delta_vwc"] = data["vwc_biochar"] - data["vwc_control"]
    data["mean_vwc"] = (data["vwc_biochar"] + data["vwc_control"]) / 2
    data["delta_temp"] = data["t_biochar"] - data["t_control"]
    data["mean_temp"] = (data["t_biochar"] + data["t_control"]) / 2

    previous = data[["date", "pair", "position", "delta_vwc", "mean_vwc"]].copy()
    previous["date"] = previous["date"] + pd.Timedelta(days=1)
    previous = previous.rename(columns={"delta_vwc": "ante_delta_vwc", "mean_vwc": "ante_mean_vwc"})
    data = data.merge(previous, on=["date", "pair", "position"], how="left", validate="one_to_one")

    irrigation = pd.read_csv(IRRIGATION)
    irrigation["end_timestamp"] = pd.to_datetime(irrigation["end_timestamp"], errors="coerce")
    irrigation["start_timestamp"] = pd.to_datetime(irrigation["start_timestamp"], errors="coerce")
    irrigation["pair"] = irrigation["strip_group"].map({"S1_S2": "S1/S2", "S3_S4": "S3/S4"})
    irrigation = irrigation.dropna(subset=["pair", "end_timestamp"]).drop_duplicates(["pair", "event_id"])

    data["days_since_irrigation"] = np.nan
    data["irrigation_day"] = 0.0
    for pair, indices in data.groupby("pair").groups.items():
        events = irrigation.loc[irrigation["pair"].eq(pair)].sort_values("end_timestamp")
        ends = events["end_timestamp"].to_numpy(dtype="datetime64[ns]")
        starts = events["start_timestamp"].dt.normalize()
        event_ends = events["end_timestamp"].dt.normalize()
        for idx in indices:
            moment = np.datetime64(data.at[idx, "date"] + pd.Timedelta(hours=12))
            prior = ends[ends <= moment]
            if len(prior):
                days = (moment - prior[-1]) / np.timedelta64(1, "D")
                data.at[idx, "days_since_irrigation"] = min(float(days), 30.0)
            date = data.at[idx, "date"]
            if ((starts <= date) & (event_ends >= date)).any():
                data.at[idx, "irrigation_day"] = 1.0

    return data.dropna(
        subset=[
            "ante_delta_vwc",
            "ante_mean_vwc",
            "days_since_irrigation",
        ]
    ).reset_index(drop=True)


def design_matrix(
    data: pd.DataFrame, centers: dict[str, float] | None = None
) -> tuple[pd.DataFrame, dict[str, float]]:
    continuous = (
            "delta_vwc",
            "mean_vwc",
            "delta_temp",
            "mean_temp",
            "ante_delta_vwc",
            "ante_mean_vwc",
            "days_since_irrigation",
            "irrigation_day",
    )
    if centers is None:
        centers = {col: float(data[col].mean()) for col in continuous}
    out = pd.DataFrame(index=data.index)
    out["Intercept"] = 1.0
    out["pair_S3S4"] = data["pair"].eq("S3/S4").astype(float)
    out["position_Middle"] = data["position"].eq("Middle").astype(float)
    out["position_Bottom"] = data["position"].eq("Bottom").astype(float)
    for year in YEARS[1:]:
        out[f"year_{year}"] = data["year"].eq(year).astype(float)
    out["pair_x_Middle"] = out["pair_S3S4"] * out["position_Middle"]
    out["pair_x_Bottom"] = out["pair_S3S4"] * out["position_Bottom"]
    for column in centers:
        out[column] = data[column] - centers[column]
    out["mean_vwc_sq"] = out["mean_vwc"] ** 2
    out["mean_temp_sq"] = out["mean_temp"] ** 2
    return out, centers


def target_vector(columns: list[str], pair: str, position: str) -> np.ndarray:
    target = pd.Series(0.0, index=columns)
    target["Intercept"] = 1.0
    target["pair_S3S4"] = float(pair == "S3/S4")
    target["position_Middle"] = float(position == "Middle")
    target["position_Bottom"] = float(position == "Bottom")
    target["pair_x_Middle"] = target["pair_S3S4"] * target["position_Middle"]
    target["pair_x_Bottom"] = target["pair_S3S4"] * target["position_Bottom"]
    # Equal weighting of the four study years.
    for year in YEARS[1:]:
        target[f"year_{year}"] = 0.25
    return target.to_numpy()


def cluster_bootstrap(
    data: pd.DataFrame,
    x: pd.DataFrame,
    *,
    repetitions: int = 1000,
    seed: int = 20260926,
) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    blocks = data["date"].dt.to_period("W-SUN").astype(str).to_numpy()
    unique_blocks = np.unique(blocks)
    block_to_indices = {block: np.flatnonzero(blocks == block) for block in unique_blocks}
    coefficient_draws = []
    contrast_draws = []
    contrasts = [
        target_vector(list(x.columns), pair, position)
        for pair in PAIRS
        for position in ("Top", "Middle", "Bottom")
    ]
    x_values = x.to_numpy()
    y_values = data["log_ec_ratio"].to_numpy()
    for _ in range(repetitions):
        sampled_blocks = rng.choice(unique_blocks, size=len(unique_blocks), replace=True)
        indices = np.concatenate([block_to_indices[block] for block in sampled_blocks])
        beta = huber_irls(x_values[indices], y_values[indices])
        coefficient_draws.append(beta)
        contrast_draws.append([contrast @ beta for contrast in contrasts])
    return np.asarray(coefficient_draws), np.asarray(contrast_draws)


def summarize_temperature(data: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(20260926)
    rows = []
    for (pair, position), group in data.groupby(["pair", "position"], sort=False):
        values = group["delta_temp"].dropna().to_numpy()
        mean = float(values.mean())
        block_labels = group["date"].dt.to_period("W-SUN").astype(str).to_numpy()
        unique_blocks = np.unique(block_labels)
        block_values = {block: values[block_labels == block] for block in unique_blocks}
        draws = []
        for _ in range(2000):
            sampled_blocks = rng.choice(unique_blocks, size=len(unique_blocks), replace=True)
            draws.append(np.concatenate([block_values[block] for block in sampled_blocks]).mean())
        low, high = np.quantile(draws, [0.025, 0.975])
        rows.append(
            {
                "pair": pair,
                "position": position,
                "days": len(values),
                "mean_biochar_minus_control_c": mean,
                "ci95_low_c": low,
                "ci95_high_c": high,
                "median_c": float(np.median(values)),
            }
        )
    return pd.DataFrame(rows)


def analyze_chemistry() -> tuple[pd.DataFrame, pd.DataFrame]:
    chemistry = pd.read_csv(CHEMISTRY)
    chemistry["date_rec"] = pd.to_datetime(chemistry["date_rec"], errors="coerce")
    chemistry["strip"] = chemistry["strip"].str.extract(r"(\d+)").astype(int)
    variables = {
        "laboratory_ec": "ec_1_1",
        "sulfate_s": "sulfate_s_ppm_s",
        "sodium": "sodium_ppm_na",
        "potassium": "potassium_ppm_k",
        "nitrate_n": "nitrate_n_ppm",
        "olsen_phosphorus": "olsen_p_ppm_p",
    }
    ratios = []
    for date, group in chemistry.groupby("date_rec"):
        indexed = group.set_index("strip")
        for pair, (biochar, control) in {"S1/S2": (1, 2), "S3/S4": (3, 4)}.items():
            if biochar not in indexed.index or control not in indexed.index:
                continue
            row = {"date": date.date().isoformat(), "pair": pair}
            for label, column in variables.items():
                numerator = pd.to_numeric(indexed.loc[biochar, column], errors="coerce")
                denominator = pd.to_numeric(indexed.loc[control, column], errors="coerce")
                row[f"{label}_ratio"] = numerator / denominator if denominator > 0 else np.nan
            ratios.append(row)
    ratio_frame = pd.DataFrame(ratios)
    correlations = []
    for label in list(variables)[1:]:
        subset = ratio_frame[["laboratory_ec_ratio", f"{label}_ratio"]].dropna()
        rho, pvalue = stats.spearmanr(
            np.log(subset["laboratory_ec_ratio"]), np.log(subset[f"{label}_ratio"])
        )
        correlations.append(
            {"constituent": label, "n": len(subset), "spearman_rho": rho, "p_value": pvalue}
        )
    return ratio_frame, pd.DataFrame(correlations)


def compare_logger_with_chemistry(
    all_year_data: pd.DataFrame,
    centers: dict[str, float],
    beta: np.ndarray,
    columns: list[str],
    lab_ratios: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare shallow logger contrasts near sampling with whole-strip ions."""
    x_all, _ = design_matrix(all_year_data, centers=centers)
    continuous_terms = [
        "delta_vwc",
        "mean_vwc",
        "delta_temp",
        "mean_temp",
        "ante_delta_vwc",
        "ante_mean_vwc",
        "days_since_irrigation",
        "irrigation_day",
        "mean_vwc_sq",
        "mean_temp_sq",
    ]
    beta_series = pd.Series(beta, index=columns)
    environmental_component = x_all[continuous_terms].to_numpy() @ beta_series[continuous_terms].to_numpy()
    adjusted = all_year_data.copy()
    adjusted["environment_adjusted_log_ratio"] = adjusted["log_ec_ratio"] - environmental_component

    rows = []
    for lab in lab_ratios.itertuples(index=False):
        sample_date = pd.Timestamp(lab.date)
        window = adjusted.loc[
            adjusted["pair"].eq(lab.pair)
            & adjusted["date"].between(sample_date - pd.Timedelta(days=7), sample_date + pd.Timedelta(days=7))
        ]
        if window.empty:
            continue
        position_means = window.groupby("position")["environment_adjusted_log_ratio"].median()
        row = lab._asdict()
        row["logger_days_in_window"] = int(window["date"].nunique())
        row["logger_positions_in_window"] = int(position_means.size)
        row["adjusted_logger_ec_ratio"] = float(np.exp(position_means.mean()))
        rows.append(row)

    matched = pd.DataFrame(rows)
    correlations = []
    if not matched.empty:
        for label in ("sulfate_s", "sodium", "potassium", "nitrate_n", "olsen_phosphorus"):
            subset = matched[["adjusted_logger_ec_ratio", f"{label}_ratio"]].dropna()
            rho, pvalue = stats.spearmanr(
                np.log(subset["adjusted_logger_ec_ratio"]), np.log(subset[f"{label}_ratio"])
            )
            correlations.append(
                {"constituent": label, "n": len(subset), "spearman_rho": rho, "p_value": pvalue}
            )
    return matched, pd.DataFrame(correlations)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_daily_pairs(growing_only=True)
    x, centers = design_matrix(data)
    y = data["log_ec_ratio"].to_numpy()
    beta = huber_irls(x.to_numpy(), y)
    temperature_terms = ["delta_temp", "mean_temp", "mean_temp_sq"]
    x_without_temperature = x.drop(columns=temperature_terms)
    beta_without_temperature = huber_irls(x_without_temperature.to_numpy(), y)
    coefficient_draws, contrast_draws = cluster_bootstrap(data, x)

    coefficient_summary = pd.DataFrame(
        {
            "term": x.columns,
            "estimate_log_ratio": beta,
            "ci95_low": np.quantile(coefficient_draws, 0.025, axis=0),
            "ci95_high": np.quantile(coefficient_draws, 0.975, axis=0),
        }
    )
    coefficient_summary["multiplicative_effect"] = np.exp(coefficient_summary["estimate_log_ratio"])

    contrast_rows = []
    order = [(pair, position) for pair in PAIRS for position in ("Top", "Middle", "Bottom")]
    for index, (pair, position) in enumerate(order):
        estimate = target_vector(list(x.columns), pair, position) @ beta
        estimate_without_temperature = (
            target_vector(list(x_without_temperature.columns), pair, position)
            @ beta_without_temperature
        )
        adjusted_ratio = np.exp(estimate)
        moisture_only_ratio = np.exp(estimate_without_temperature)
        contrast_rows.append(
            {
                "pair": pair,
                "position": position,
                "adjusted_ec_ratio": adjusted_ratio,
                "ci95_low": np.exp(np.quantile(contrast_draws[:, index], 0.025)),
                "ci95_high": np.exp(np.quantile(contrast_draws[:, index], 0.975)),
                "moisture_only_ec_ratio": moisture_only_ratio,
                "change_from_adding_temperature_pct": 100 * (adjusted_ratio / moisture_only_ratio - 1),
            }
        )

    lab_ratios, lab_correlations = analyze_chemistry()
    all_year_data = load_daily_pairs(growing_only=False)
    logger_lab_matches, logger_ion_correlations = compare_logger_with_chemistry(
        all_year_data, centers, beta, list(x.columns), lab_ratios
    )
    pd.DataFrame(contrast_rows).to_csv(OUTPUT_DIR / "six_in_adjusted_ec_ratios.csv", index=False)
    coefficient_summary.to_csv(OUTPUT_DIR / "six_in_joint_model_coefficients.csv", index=False)
    summarize_temperature(data).to_csv(OUTPUT_DIR / "six_in_temperature_differences.csv", index=False)
    pd.DataFrame([centers]).to_csv(OUTPUT_DIR / "six_in_model_centering_values.csv", index=False)
    lab_ratios.to_csv(OUTPUT_DIR / "laboratory_strip_ratios.csv", index=False)
    lab_correlations.to_csv(OUTPUT_DIR / "laboratory_ec_ion_correlations.csv", index=False)
    logger_lab_matches.to_csv(OUTPUT_DIR / "six_in_logger_laboratory_matches.csv", index=False)
    logger_ion_correlations.to_csv(OUTPUT_DIR / "six_in_logger_ion_correlations.csv", index=False)
    print(f"Analyzed {len(data):,} daily pair-position records")
    print(pd.DataFrame(contrast_rows).to_string(index=False))
    print("\nTemperature differences")
    print(summarize_temperature(data).to_string(index=False))
    print("\nTemperature model terms")
    print(coefficient_summary.loc[coefficient_summary["term"].isin(["delta_temp", "mean_temp", "mean_temp_sq"])].to_string(index=False))
    print("\nAdjusted six-inch logger EC versus laboratory ions")
    print(logger_ion_correlations.to_string(index=False))


if __name__ == "__main__":
    main()
