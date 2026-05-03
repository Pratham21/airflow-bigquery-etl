# ============================================================
# competitor_insights_dag.py — Weekly Competitor Data Ingestion
# Author: Pratham Bharadwaj
# Schedule: Every Sunday at 5 AM UTC
# Use case: Competitive intelligence pipeline at eBay
#           Integrated 5+ vendor platforms, cut manual reporting by 40%
# ============================================================

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import requests

from plugins.hooks.bigquery_hook import BigQueryHook
from plugins.transformers.data_transformers import clean_dataframe, add_date_parts

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "pratham.bharadwaj",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}

BQ_PROJECT = "{{ var.value.gcp_project_id }}"
BQ_DATASET = "competitive_insights"

# Vendor platforms (mirrors eBay: Similarweb, Fox Intelligence, Dataforest)
VENDOR_CONFIGS = [
    {"name": "similarweb",    "endpoint": "{{ var.value.similarweb_api_url }}",    "token_var": "similarweb_token"},
    {"name": "fox_intel",     "endpoint": "{{ var.value.fox_intel_api_url }}",     "token_var": "fox_intel_token"},
    {"name": "dataforest",    "endpoint": "{{ var.value.dataforest_api_url }}",    "token_var": "dataforest_token"},
]


# ── TASK FUNCTIONS ───────────────────────────────────────────

def extract_vendor_data(vendor_name: str, endpoint: str, token_var: str, **context) -> None:
    """Extract competitive data from a single vendor API."""
    logger.info(f"Extracting from vendor: {vendor_name}")

    headers = {"Authorization": f"Bearer {{{{ var.value.{token_var} }}}}"}
    params  = {
        "week_ending": context["ds"],
        "metrics": "traffic,gmv,pricing,inventory",
    }

    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=90)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data.get("data", []))
        df["vendor_source"] = vendor_name
        logger.info(f"{vendor_name}: {len(df):,} records extracted")
        context["ti"].xcom_push(key=f"raw_{vendor_name}", value=df.to_json(orient="records"))
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to extract from {vendor_name}: {e}")
        # Push empty so downstream doesn't fail
        context["ti"].xcom_push(key=f"raw_{vendor_name}", value="[]")


def merge_vendor_data(**context) -> None:
    """Merge data from all vendors into a single DataFrame."""
    frames = []
    for vendor in VENDOR_CONFIGS:
        raw = context["ti"].xcom_pull(
            key=f"raw_{vendor['name']}",
            task_ids=f"extract_{vendor['name']}"
        )
        if raw and raw != "[]":
            df = pd.read_json(raw, orient="records")
            frames.append(df)

    if not frames:
        raise ValueError("No vendor data extracted — all sources failed")

    merged = pd.concat(frames, ignore_index=True)
    merged = clean_dataframe(merged)
    merged = add_date_parts(merged, "report_date") if "report_date" in merged.columns else merged
    merged["week_ending"] = context["ds"]

    logger.info(f"Merged {len(merged):,} rows from {len(frames)} vendors")
    context["ti"].xcom_push(key="merged_data", value=merged.to_json(orient="records"))


def compute_competitive_metrics(**context) -> None:
    """
    Compute week-over-week competitive metrics.
    Mirrors eBay exec deep-dives on Trading Cards, Fashion, live shopping.
    """
    raw = context["ti"].xcom_pull(key="merged_data", task_ids="merge_vendor_data")
    df = pd.read_json(raw, orient="records")

    # GMV trend by category
    if "category" in df.columns and "gmv" in df.columns:
        gmv_by_category = (
            df.groupby(["category", "vendor_source"])["gmv"]
            .sum()
            .reset_index()
            .rename(columns={"gmv": "total_gmv"})
        )
        gmv_by_category["week_ending"] = context["ds"]
        context["ti"].xcom_push(
            key="gmv_metrics",
            value=gmv_by_category.to_json(orient="records")
        )

    # Pricing index by competitor
    if "competitor" in df.columns and "avg_price" in df.columns:
        pricing = (
            df.groupby("competitor")["avg_price"]
            .mean()
            .reset_index()
            .rename(columns={"avg_price": "avg_pricing_index"})
        )
        pricing["week_ending"] = context["ds"]
        context["ti"].xcom_push(
            key="pricing_metrics",
            value=pricing.to_json(orient="records")
        )

    logger.info("Competitive metrics computed")


def load_insights_to_bigquery(**context) -> None:
    """Load all competitive metrics to BigQuery."""
    hook = BigQueryHook(project_id=BQ_PROJECT)

    # Load raw merged data
    raw = context["ti"].xcom_pull(key="merged_data", task_ids="merge_vendor_data")
    df_raw = pd.read_json(raw, orient="records")
    hook.load_dataframe(df_raw, BQ_DATASET, "vendor_raw_weekly", write_mode="WRITE_APPEND")

    # Load GMV metrics if available
    gmv_raw = context["ti"].xcom_pull(key="gmv_metrics", task_ids="compute_competitive_metrics")
    if gmv_raw:
        df_gmv = pd.read_json(gmv_raw, orient="records")
        hook.load_dataframe(df_gmv, BQ_DATASET, "gmv_by_category_weekly", write_mode="WRITE_APPEND")

    logger.info("Competitor insights loaded to BigQuery")


# ── DAG DEFINITION ───────────────────────────────────────────

with DAG(
    dag_id="competitor_insights_pipeline",
    default_args=DEFAULT_ARGS,
    description="Weekly competitor data ingestion from vendor APIs → BigQuery",
    schedule_interval="0 5 * * SUN",   # Every Sunday at 5 AM UTC
    start_date=days_ago(7),
    catchup=False,
    tags=["competitive-intelligence", "ebay", "bigquery", "weekly"],
) as dag:

    # One extract task per vendor — runs in parallel
    extract_tasks = []
    for vendor in VENDOR_CONFIGS:
        t = PythonOperator(
            task_id=f"extract_{vendor['name']}",
            python_callable=extract_vendor_data,
            op_kwargs={
                "vendor_name": vendor["name"],
                "endpoint":    vendor["endpoint"],
                "token_var":   vendor["token_var"],
            },
            provide_context=True,
        )
        extract_tasks.append(t)

    merge = PythonOperator(
        task_id="merge_vendor_data",
        python_callable=merge_vendor_data,
        provide_context=True,
    )

    metrics = PythonOperator(
        task_id="compute_competitive_metrics",
        python_callable=compute_competitive_metrics,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_insights_to_bigquery",
        python_callable=load_insights_to_bigquery,
        provide_context=True,
    )

    # Parallel extract → merge → metrics → load
    extract_tasks >> merge >> metrics >> load
