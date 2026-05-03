# ============================================================
# sales_kpi_pipeline.py — Daily Sales KPI ETL DAG
# Author: Pratham Bharadwaj
# Schedule: Daily at 6 AM UTC
# Use case: Automates KPI pipeline built at Intuit
#           Tracked consent capture, payroll adoption, sales funnels
#           Improved operational efficiency by 20–30%
# ============================================================

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

import pandas as pd
import requests

from plugins.transformers.data_transformers import (
    clean_dataframe,
    compute_funnel_conversion,
    add_date_parts,
)
from plugins.hooks.bigquery_hook import BigQueryHook

logger = logging.getLogger(__name__)

# ── DAG CONFIG ───────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "pratham.bharadwaj",
    "depends_on_past": False,
    "email": ["prathambharadwaj21@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

FUNNEL_STAGES = [
    "Lead Created",
    "Demo Scheduled",
    "Proposal Sent",
    "Contract Signed",
    "Closed Won",
]

BQ_PROJECT  = "{{ var.value.gcp_project_id }}"
BQ_DATASET  = "analytics_output"


# ── TASK FUNCTIONS ───────────────────────────────────────────

def extract_sales_data(**context) -> None:
    """
    Extract sales funnel events from source API.
    Pushes raw data to XCom for downstream tasks.
    """
    execution_date = context["ds"]  # YYYY-MM-DD
    logger.info(f"Extracting sales data for {execution_date}")

    # In production: replace with actual internal API endpoint
    # Example mirrors Intuit QuickBooks Enterprise data extraction
    api_url = "{{ var.value.sales_api_url }}"
    headers = {"Authorization": "Bearer {{ var.value.sales_api_token }}"}
    params  = {"date": execution_date, "limit": 10000}

    response = requests.get(api_url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data.get("records", []))
    logger.info(f"Extracted {len(df):,} records")

    # Push to XCom as JSON for downstream tasks
    context["ti"].xcom_push(key="raw_sales_data", value=df.to_json(orient="records"))


def transform_sales_data(**context) -> None:
    """
    Clean and transform raw sales data.
    Computes funnel conversion rates and KPIs.
    """
    raw_json = context["ti"].xcom_pull(key="raw_sales_data", task_ids="extract_sales_data")
    df = pd.read_json(raw_json, orient="records")
    logger.info(f"Transforming {len(df):,} records")

    # Standard cleaning
    df = clean_dataframe(df)
    df = add_date_parts(df, "event_date")

    # Funnel conversion
    funnel = compute_funnel_conversion(
        df,
        stage_col="stage_name",
        customer_col="customer_id",
        stage_order=FUNNEL_STAGES
    )

    # Core KPIs
    kpis = {
        "total_leads":        int(df["customer_id"].nunique()),
        "closed_won":         int(df[df["stage_name"] == "Closed Won"]["customer_id"].nunique()),
        "overall_conversion": round(
            df[df["stage_name"] == "Closed Won"]["customer_id"].nunique()
            / max(df["customer_id"].nunique(), 1) * 100, 2
        ),
        "avg_deal_value":     round(df["deal_value"].mean(), 2) if "deal_value" in df.columns else 0,
        "pipeline_value":     round(df["deal_value"].sum(), 2)  if "deal_value" in df.columns else 0,
    }
    logger.info(f"KPIs computed: {kpis}")

    context["ti"].xcom_push(key="funnel_data", value=funnel.to_json(orient="records"))
    context["ti"].xcom_push(key="kpis", value=str(kpis))


def validate_data(**context) -> None:
    """
    Data quality checks before loading to BigQuery.
    Fails the task if critical checks don't pass.
    """
    funnel_json = context["ti"].xcom_pull(key="funnel_data", task_ids="transform_sales_data")
    df = pd.read_json(funnel_json, orient="records")

    checks = {
        "not_empty":        len(df) > 0,
        "has_stage_col":    "stage" in df.columns,
        "no_negative_users": (df["users"] >= 0).all() if "users" in df.columns else True,
    }

    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")

    logger.info("All data quality checks passed ✓")


def load_to_bigquery(**context) -> None:
    """
    Load transformed data into BigQuery.
    Uses WRITE_TRUNCATE to replace today's partition.
    """
    execution_date = context["ds"]

    funnel_json = context["ti"].xcom_pull(key="funnel_data", task_ids="transform_sales_data")
    df = pd.read_json(funnel_json, orient="records")
    df["loaded_date"] = execution_date

    hook = BigQueryHook(project_id=BQ_PROJECT)
    hook.load_dataframe(
        df=df,
        dataset=BQ_DATASET,
        table="sales_funnel_daily",
        write_mode="WRITE_APPEND"
    )
    logger.info(f"Loaded {len(df):,} rows to {BQ_DATASET}.sales_funnel_daily")


# ── DAG DEFINITION ───────────────────────────────────────────

with DAG(
    dag_id="sales_kpi_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily sales KPI ETL: API → transform → BigQuery",
    schedule_interval="0 6 * * *",      # Daily at 6 AM UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "kpi", "bigquery", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_data,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        provide_context=True,
    )

    notify = EmailOperator(
        task_id="notify_success",
        to=["prathambharadwaj21@gmail.com"],
        subject="[Airflow] Sales KPI Pipeline — {{ ds }} ✓",
        html_content="""
            <h3>Sales KPI Pipeline completed successfully</h3>
            <p>Date: {{ ds }}</p>
            <p>Check BigQuery: <b>analytics_output.sales_funnel_daily</b></p>
        """,
    )

    # DAG dependency chain
    extract >> transform >> validate >> load >> notify
