# airflow-bigquery-etl

Production-style Apache Airflow DAGs for automated ETL pipelines — extracting from REST APIs and source systems, transforming with Python/Pandas, and loading into BigQuery on GCP.

Built on patterns from real data engineering work at **Intuit** (KPI automation, billing pipelines) and **Tesla** (energy operations, Airflow + Python, 70% dashboard efficiency improvement).

---

## 📁 Structure

```
airflow-bigquery-etl/
├── dags/
│   ├── sales_kpi_pipeline.py        # Daily sales KPI extraction → BigQuery
│   ├── churn_risk_pipeline.py       # Weekly churn risk scoring pipeline
│   └── competitor_insights_dag.py   # Competitor data ingestion (mirrors eBay work)
├── plugins/
│   ├── transformers/
│   │   └── data_transformers.py     # Reusable Pandas transformations
│   ├── hooks/
│   │   └── bigquery_hook.py         # Custom BigQuery hook
│   └── operators/
│       └── bigquery_operator.py     # Custom BigQuery load operator
├── config/
│   └── pipeline_config.yaml         # Centralized pipeline config
├── tests/
│   └── test_transformers.py         # Unit tests for transformation logic
├── docker-compose.yml               # Local Airflow setup
├── requirements.txt
└── README.md
```

---

## 🛠️ Tech Stack

![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-4285F4?style=flat&logo=google-cloud&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white)

---

## 🔄 Pipelines

### 1. Sales KPI Pipeline (`sales_kpi_pipeline.py`)
- Runs daily at 6 AM UTC
- Extracts sales funnel data from source API
- Computes conversion rates, pipeline health, revenue metrics
- Loads results to BigQuery for Tableau dashboard consumption
- Mirrors KPI automation built at Intuit (20–30% efficiency improvement)

### 2. Churn Risk Pipeline (`churn_risk_pipeline.py`)
- Runs weekly every Monday
- Pulls customer activity and login data from BigQuery
- Scores customers into churn risk tiers (high/medium/low/active)
- Exports flagged accounts to Salesforce for CSM follow-up
- Based on 90-day inactivity model used at Intuit (100K+ customers)

### 3. Competitor Insights Pipeline (`competitor_insights_dag.py`)
- Runs weekly
- Ingests competitive data from external vendor APIs
- Transforms and loads GMV/pricing trends to BigQuery
- Mirrors competitive intelligence pipeline built at eBay (40% manual reporting reduction)

---

## 🚀 Running Locally

### Prerequisites
- Docker & Docker Compose
- GCP service account JSON (for BigQuery access)

### Setup
```bash
# Clone the repo
git clone https://github.com/Pratham21/airflow-bigquery-etl.git
cd airflow-bigquery-etl

# Copy and configure environment
cp .env.example .env
# Add your GCP_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS path

# Start Airflow
docker-compose up -d

# Open Airflow UI
open http://localhost:8080
# Default login: airflow / airflow
```

---

## 📊 Architecture

```
Source Systems          Airflow DAG              BigQuery              Tableau
─────────────     ──────────────────────    ───────────────      ──────────────
REST APIs      →  Extract → Transform   →   analytics_output  →  Exec Dashboards
BQ Raw Tables     → Validate → Load         churn_scores
Vendor APIs       → Alert on failure        sales_kpis
```

---

*Built by Pratham Bharadwaj — Senior Analytics Engineer*  
🔗 [LinkedIn](https://linkedin.com/in/pratham-bharadwaj-47664371) · [Tableau Public](https://public.tableau.com/app/profile/pratham8634/vizzes)
