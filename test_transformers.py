# ============================================================
# test_transformers.py — Unit tests for transformation functions
# Author: Pratham Bharadwaj
# Run: pytest tests/
# ============================================================

import pytest
import pandas as pd
import numpy as np
from plugins.transformers.data_transformers import (
    clean_dataframe,
    add_date_parts,
    flag_churn_risk,
    compute_funnel_conversion,
)


# ── FIXTURES ─────────────────────────────────────────────────

@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "Customer ID": [1, 2, 3, 4, 1],       # duplicate row 0 and 4
        "days_since_login": [10, 45, 75, 100, 10],
        "mrr": [500, 250, 1000, 150, 500],
    })


@pytest.fixture
def sample_events():
    return pd.DataFrame({
        "customer_id": [1, 1, 2, 2, 3],
        "event_date": ["2024-01-01", "2024-03-15", "2024-02-10",
                        "2024-04-01", "2024-01-20"],
        "amount": [100, 200, 150, 50, 300],
    })


@pytest.fixture
def sample_funnel():
    stages = (
        ["Lead Created"] * 100 +
        ["Demo Scheduled"] * 60 +
        ["Proposal Sent"] * 35 +
        ["Contract Signed"] * 20 +
        ["Closed Won"] * 12
    )
    return pd.DataFrame({
        "customer_id": range(len(stages)),
        "stage_name": stages,
    })


# ── TESTS: clean_dataframe ────────────────────────────────────

def test_clean_dataframe_lowercases_columns(sample_customers):
    df = clean_dataframe(sample_customers.copy())
    assert all(col == col.lower() for col in df.columns)


def test_clean_dataframe_removes_duplicates(sample_customers):
    df = clean_dataframe(sample_customers.copy())
    assert len(df) == 4   # 5 rows - 1 duplicate


def test_clean_dataframe_resets_index(sample_customers):
    df = clean_dataframe(sample_customers.copy())
    assert list(df.index) == list(range(len(df)))


# ── TESTS: add_date_parts ─────────────────────────────────────

def test_add_date_parts_creates_columns(sample_events):
    df = add_date_parts(sample_events.copy(), "event_date")
    for col in ["event_date_year", "event_date_month", "event_date_quarter",
                "event_date_week", "event_date_weekday"]:
        assert col in df.columns, f"Missing column: {col}"


def test_add_date_parts_correct_year(sample_events):
    df = add_date_parts(sample_events.copy(), "event_date")
    assert (df["event_date_year"] == 2024).all()


# ── TESTS: flag_churn_risk ────────────────────────────────────

def test_flag_churn_risk_tiers(sample_customers):
    df = clean_dataframe(sample_customers.copy())
    df = flag_churn_risk(df, "days_since_login")
    assert "churn_risk_tier" in df.columns
    tiers = set(df["churn_risk_tier"].unique())
    assert tiers.issubset({"active", "low_risk", "medium_risk", "high_risk"})


def test_flag_churn_risk_correct_assignment():
    df = pd.DataFrame({"days_since_login": [5, 35, 65, 95]})
    df = flag_churn_risk(df, "days_since_login")
    assert df.loc[0, "churn_risk_tier"] == "active"
    assert df.loc[1, "churn_risk_tier"] == "low_risk"
    assert df.loc[2, "churn_risk_tier"] == "medium_risk"
    assert df.loc[3, "churn_risk_tier"] == "high_risk"


def test_flag_churn_risk_custom_thresholds():
    df = pd.DataFrame({"days_since_login": [10, 25, 55]})
    df = flag_churn_risk(df, "days_since_login", thresholds={"high": 50, "medium": 20, "low": 5})
    assert df.loc[2, "churn_risk_tier"] == "high_risk"


# ── TESTS: compute_funnel_conversion ─────────────────────────

STAGES = ["Lead Created", "Demo Scheduled", "Proposal Sent",
          "Contract Signed", "Closed Won"]


def test_funnel_has_all_stages(sample_funnel):
    result = compute_funnel_conversion(sample_funnel, "stage_name", "customer_id", STAGES)
    assert len(result) == len(STAGES)


def test_funnel_first_stage_100pct(sample_funnel):
    result = compute_funnel_conversion(sample_funnel, "stage_name", "customer_id", STAGES)
    assert result.iloc[0]["pct_of_total"] == 100.0


def test_funnel_conversion_decreasing(sample_funnel):
    result = compute_funnel_conversion(sample_funnel, "stage_name", "customer_id", STAGES)
    users = result["users"].tolist()
    assert users == sorted(users, reverse=True), "Users should decrease through funnel"


def test_funnel_drop_off_non_negative(sample_funnel):
    result = compute_funnel_conversion(sample_funnel, "stage_name", "customer_id", STAGES)
    assert (result["drop_off_pct"].fillna(0) >= 0).all()
