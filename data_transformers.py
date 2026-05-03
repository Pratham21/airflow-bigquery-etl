# ============================================================
# data_transformers.py — Reusable transformation functions for DAGs
# Author: Pratham Bharadwaj
# ============================================================

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column names, strip whitespace, drop duplicates."""
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    logger.info(f"Cleaned: removed {before - len(df):,} duplicates")
    return df


def add_date_parts(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Explode date column into year, month, quarter, week, weekday."""
    df[date_col] = pd.to_datetime(df[date_col])
    df[f"{date_col}_year"]    = df[date_col].dt.year
    df[f"{date_col}_month"]   = df[date_col].dt.month
    df[f"{date_col}_quarter"] = df[date_col].dt.quarter
    df[f"{date_col}_week"]    = df[date_col].dt.isocalendar().week.astype(int)
    df[f"{date_col}_weekday"] = df[date_col].dt.day_name()
    return df


def flag_churn_risk(df: pd.DataFrame, days_col: str,
                    thresholds: dict = None) -> pd.DataFrame:
    """Assign churn_risk_tier based on days since last activity."""
    if thresholds is None:
        thresholds = {"high": 90, "medium": 60, "low": 30}
    conditions = [
        df[days_col] > thresholds["high"],
        df[days_col] > thresholds["medium"],
        df[days_col] > thresholds["low"],
    ]
    df["churn_risk_tier"] = np.select(conditions,
                                       ["high_risk", "medium_risk", "low_risk"],
                                       default="active")
    logger.info(f"Churn tiers: {df['churn_risk_tier'].value_counts().to_dict()}")
    return df


def compute_rfm(df: pd.DataFrame, customer_col: str,
                date_col: str, amount_col: str) -> pd.DataFrame:
    """Compute RFM scores and segments per customer."""
    snapshot = pd.Timestamp.today()
    df[date_col] = pd.to_datetime(df[date_col])

    rfm = df.groupby(customer_col).agg(
        recency_days=(date_col, lambda x: (snapshot - x.max()).days),
        frequency=(date_col, "count"),
        monetary=(amount_col, "sum")
    ).reset_index()

    rfm["r_score"] = pd.qcut(rfm["recency_days"], q=5, labels=[5,4,3,2,1], duplicates="drop").astype(int)
    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), q=5, labels=[1,2,3,4,5], duplicates="drop").astype(int)
    rfm["m_score"] = pd.qcut(rfm["monetary"].rank(method="first"), q=5, labels=[1,2,3,4,5], duplicates="drop").astype(int)
    rfm["rfm_total"] = rfm["r_score"] + rfm["f_score"] + rfm["m_score"]
    rfm["rfm_segment"] = pd.cut(rfm["rfm_total"], bins=[0,6,9,12,15],
                                 labels=["Lost","At Risk","Loyal","Champion"])
    return rfm


def compute_funnel_conversion(df: pd.DataFrame, stage_col: str,
                               customer_col: str, stage_order: list) -> pd.DataFrame:
    """Compute step-by-step funnel conversion and drop-off rates."""
    records = []
    for i, stage in enumerate(stage_order):
        count = df[df[stage_col] == stage][customer_col].nunique()
        records.append({"stage": stage, "stage_order": i + 1, "users": count})

    funnel = pd.DataFrame(records)
    funnel["prev_users"]      = funnel["users"].shift(1)
    funnel["conversion_pct"]  = (funnel["users"] / funnel["prev_users"] * 100).round(1)
    funnel["drop_off_pct"]    = (100 - funnel["conversion_pct"]).round(1)
    funnel["pct_of_total"]    = (funnel["users"] / funnel["users"].iloc[0] * 100).round(1)
    return funnel.fillna({"conversion_pct": 100.0, "drop_off_pct": 0.0})
