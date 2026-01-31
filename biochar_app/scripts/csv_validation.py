import pandas as pd

REQUIRED_DATE_COLUMN = "date_rec"

def normalize_dates(df: pd.DataFrame, *, source: str = "") -> pd.DataFrame:
    """
    Enforce canonical date handling for Ward / biomass / soil CSVs.

    Rules:
    - date_rec MUST exist (hard failure if missing)
    - date_recd is dropped if present
    - date_rept is optional metadata (renamed to date_report)
    - date_rec is parsed to datetime.date
    """

    if REQUIRED_DATE_COLUMN not in df.columns:
        raise ValueError(
            f"[FATAL] Required column '{REQUIRED_DATE_COLUMN}' not found"
            f"{f' in {source}' if source else ''}. "
            "CSV ingestion halted."
        )

    # Drop known duplicates
    if "date_recd" in df.columns:
        df = df.drop(columns=["date_recd"])

    # Optional rename for clarity
    if "date_rept" in df.columns:
        df = df.rename(columns={"date_rept": "date_report"})

    # Enforce parseable dates
    df[REQUIRED_DATE_COLUMN] = pd.to_datetime(
        df[REQUIRED_DATE_COLUMN], errors="raise"
    ).dt.date

    return df