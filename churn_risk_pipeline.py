# ============================================================
# churn_risk_pipeline.py — Weekly Churn Risk Scoring DAG
# Author: Pratham Bharadwaj
# Schedule: Every Monday at 7 AM UTC
# Use case: Proactive churn detection at Intuit
#           Scored 100K+ customers, maintained ~2% attrition target
# ============================================================

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

import pandas as pd

from plugins.transformers.data_transformers import flag_churn_risk, compute_rfm
from plugins.hooks.bigquery_hook import BigQueryHook

logger = logging.getLogger(__name__)

# ── DAG CONFIG ───────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "pratham.bharadwaj",
    "depends_on_past": False,
    "email": ["prathambharadwaj21@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

BQ_PROJECT = "{{ var.value.gcp_project_id }}"
BQ_DATASET = "analytics_output"

CHURN_THRESHOLDS = {"high": 90, "medium": 60, "low": 30}


# ── TASK FUNCTIONS ───────────────────────────────────────────

def extract_customer_data(**context) -> None:
    """Pull customer activity data from BigQuery for scoring."""
    hook = BigQueryHook(project_id=BQ_PROJECT)

    sql = """
        SELECT
            c.customer_id,
            c.account_name,
            c.plan_type,
            c.mrr,
            c.segment,
            c.csm_owner,
            DATE_DIFF(CURRENT_DATE(), MAX(l.login_date), DAY) AS days_since_login,
            COUNT(DISTINCT t.transaction_id)                   AS transactions_last_90d,
            SUM(t.amount)                                      AS spend_last_90d
        FROM `analytics.customers` c
        LEFT JOIN `analytics.logins` l
            ON c.customer_id = l.customer_id
            AND l.login_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        LEFT JOIN `analytics.transactions` t
            ON c.customer_id = t.customer_id
            AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        WHERE c.is_active = TRUE
        GROUP BY c.customer_id, c.account_name, c.plan_type,
                 c.mrr, c.segment, c.csm_owner
    """
    df = hook.run_query(sql)
    logger.info(f"Extracted {len(df):,} active customers")
    context["ti"].xcom_push(key="customer_data", value=df.to_json(orient="records"))


def score_churn_risk(**context) -> None:
    """Apply churn risk scoring and RFM segmentation."""
    raw = context["ti"].xcom_pull(key="customer_data", task_ids="extract_customer_data")
    df = pd.read_json(raw, orient="records")

    # Churn risk tier
    df = flag_churn_risk(df, days_col="days_since_login", thresholds=CHURN_THRESHOLDS)

    # Priority score — higher MRR at-risk customers flagged first
    df["priority_score"] = (
        df["mrr"].fillna(0) *
        df["churn_risk_tier"].map({"high_risk": 3, "medium_risk": 2, "low_risk": 1, "active": 0})
    )

    risk_summary = df["churn_risk_tier"].value_counts().to_dict()
    logger.info(f"Churn risk distribution: {risk_summary}")

    context["ti"].xcom_push(key="scored_data", value=df.to_json(orient="records"))
    context["ti"].xcom_push(key="risk_summary", value=str(risk_summary))


def check_high_risk_threshold(**context) -> str:
    """
    Branch operator — if high risk customers > 5% of base,
    trigger alert path. Otherwise proceed normally.
    """
    raw = context["ti"].xcom_pull(key="scored_data", task_ids="score_churn_risk")
    df = pd.read_json(raw, orient="records")

    total = len(df)
    high_risk = len(df[df["churn_risk_tier"] == "high_risk"])
    pct = high_risk / max(total, 1) * 100

    logger.info(f"High risk: {high_risk:,} / {total:,} ({pct:.1f}%)")

    if pct > 5.0:
        return "alert_high_churn"
    return "load_churn_scores"


def load_churn_scores(**context) -> None:
    """Load scored customers into BigQuery."""
    raw = context["ti"].xcom_pull(key="scored_data", task_ids="score_churn_risk")
    df = pd.read_json(raw, orient="records")
    df["scored_date"] = context["ds"]

    hook = BigQueryHook(project_id=BQ_PROJECT)
    hook.load_dataframe(df, BQ_DATASET, "churn_risk_scores", write_mode="WRITE_TRUNCATE")
    logger.info(f"Loaded {len(df):,} churn scores to BigQuery")


def alert_high_churn(**context) -> None:
    """Log alert when churn exceeds threshold — in prod would trigger Slack/PagerDuty."""
    summary = context["ti"].xcom_pull(key="risk_summary", task_ids="score_churn_risk")
    logger.warning(f"HIGH CHURN ALERT — risk summary: {summary}")
    # Production: integrate with Slack webhook or PagerDuty API here


# ── DAG DEFINITION ───────────────────────────────────────────

with DAG(
    dag_id="churn_risk_pipeline",
    default_args=DEFAULT_ARGS,
    description="Weekly churn risk scoring: BigQuery → score → reload",
    schedule_interval="0 7 * * MON",    # Every Monday at 7 AM UTC
    start_date=days_ago(7),
    catchup=False,
    max_active_runs=1,
    tags=["churn", "retention", "bigquery", "weekly"],
) as dag:

    extract = PythonOperator(
        task_id="extract_customer_data",
        python_callable=extract_customer_data,
        provide_context=True,
    )

    score = PythonOperator(
        task_id="score_churn_risk",
        python_callable=score_churn_risk,
        provide_context=True,
    )

    branch = BranchPythonOperator(
        task_id="check_high_risk_threshold",
        python_callable=check_high_risk_threshold,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_churn_scores",
        python_callable=load_churn_scores,
        provide_context=True,
    )

    alert = PythonOperator(
        task_id="alert_high_churn",
        python_callable=alert_high_churn,
        provide_context=True,
    )

    done = DummyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # DAG: extract → score → branch → load or alert → done
    extract >> score >> branch >> [load, alert] >> done
