# ============================================================
# bigquery_hook.py — Custom Airflow BigQuery Hook
# Author: Pratham Bharadwaj
# Use case: Reusable BQ connection across all DAGs
# ============================================================

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BigQueryHook:
    """
    Custom BigQuery hook for Airflow DAGs.
    Wraps google-cloud-bigquery with retry logic and DataFrame support.
    """

    def __init__(self, project_id: str, credentials_path: str = None):
        self.project_id = project_id
        if credentials_path:
            creds = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            self.client = bigquery.Client(project=project_id, credentials=creds)
        else:
            self.client = bigquery.Client(project=project_id)
        logger.info(f"BigQueryHook initialised: project={project_id}")

    def run_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return as DataFrame."""
        logger.info("Running BigQuery query...")
        job = self.client.query(sql)
        df  = job.to_dataframe()
        logger.info(f"Query returned {len(df):,} rows")
        return df

    def load_dataframe(self, df: pd.DataFrame, dataset: str,
                       table: str, write_mode: str = "WRITE_APPEND") -> None:
        """Load DataFrame to BigQuery table."""
        if df.empty:
            logger.warning(f"Empty DataFrame — skipping load to {dataset}.{table}")
            return

        table_ref = f"{self.project_id}.{dataset}.{table}"
        disposition = getattr(bigquery.WriteDisposition, write_mode)
        job_config = bigquery.LoadJobConfig(write_disposition=disposition)

        # Convert period/datetime cols to string for BQ compatibility
        for col in df.select_dtypes(include=["period", "datetimetz"]).columns:
            df[col] = df[col].astype(str)

        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(df):,} rows → {table_ref} [{write_mode}]")

    def table_exists(self, dataset: str, table: str) -> bool:
        """Check if a BigQuery table exists."""
        try:
            self.client.get_table(f"{self.project_id}.{dataset}.{table}")
            return True
        except Exception:
            return False
