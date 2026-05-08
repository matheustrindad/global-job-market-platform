# Global Job Market Platform

A production-grade data platform that ingests, processes, and exposes global job market data through a REST API and interactive dashboard — built with Apache Airflow, PySpark, PostgreSQL Star Schema, FastAPI, and Streamlit.

## Architecture

```
Adzuna API (BR/US/GB/AT)          RemoteOK Scraper
         └──────────────┬──────────────┘
                        ↓
              Data Lake — raw/ (JSON)
                        ↓
             PySpark Batch Processing
              ├── Validation & cleaning
              ├── Seniority extraction
              └── Salary normalization (USD)
                        ↓
         ┌──────────────┴──────────────┐
      trusted/ (Parquet)         quarantine/
                        ↓
             PySpark Aggregations
                        ↓
           refined/ (3 Parquet files)
                        ↓
        PostgreSQL — Star Schema
                        ↓
              FastAPI (REST API)
                        ↓
          Streamlit Dashboard (dark mode)

     Apache Airflow orchestrates every step
     Docker Compose runs the full stack
```

## Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    │─────────────│
                    │ id (PK)     │
                    │ date        │
                    │ year        │
                    │ month       │
                    │ day         │
                    │ weekday     │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────────┐    ┌──────────────┐
│ dim_company │    │ fact_job_postings │    │ dim_location │
│─────────────│    │───────────────────│    │──────────────│
│ id (PK)     ├────│ id (PK)           ├────│ id (PK)      │
│ name        │    │ company_id (FK)   │    │ country      │
│ domain      │    │ location_id (FK)  │    │ city         │
└─────────────┘    │ date_id (FK)      │    └──────────────┘
                   │ title             │
                   │ salary_min        │
                   │ salary_max        │
                   │ salary_min_usd    │
                   │ salary_max_usd    │
                   │ seniority         │
                   │ is_remote         │
                   │ redirect_url      │
                   │ source            │
                   │ ingested_at       │
                   └───────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Requests, BeautifulSoup |
| Processing | Apache Spark 3.x (PySpark) |
| Orchestration | Apache Airflow 2.8 |
| Data Warehouse | PostgreSQL 13 — Star Schema |
| API | FastAPI + SQLAlchemy |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker Compose |
| CI/CD | GitHub Actions |

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `ingestion_dag` | 08:00 UTC | Adzuna API (4 countries) + RemoteOK scraper |
| `processing_dag` | 09:00 UTC | PySpark: raw JSON → trusted Parquet + quarantine |
| `refinement_dag` | 10:00 UTC | PySpark: trusted → 3 analytical aggregations |

## Refined Layer

Three pre-aggregated Parquet files ready for consumption:

- `salary_by_country_seniority.parquet` — avg salary (USD) by country and seniority level
- `top_skills_by_country.parquet` — most demanded skills extracted from job titles
- `jobs_volume_trend.parquet` — daily posting volume with company count and avg salary

## API Endpoints

```bash
# Health check
GET /health

# Job listings with filters
GET /jobs?country=us&seniority=senior&salary_min=100000&limit=50

# Average salary by country and seniority
GET /salaries?country=gb

# Top hiring companies
GET /companies?top_n=10&country=us

# Daily posting volume trend
GET /trends?days=30&country=br
```

Interactive API docs available at `http://localhost:8000/docs`

## How to Run

**Prerequisites:** Docker Desktop, Git

```bash
# 1. Clone the repository
git clone https://github.com/matheustrindad/global-job-market-platform.git
cd global-job-market-platform

# 2. Set up environment variables
cp .env.example .env
# Edit .env and add your ADZUNA_APP_ID and ADZUNA_APP_KEY

# 3. Start the full stack
docker-compose up -d

# 4. Wait ~30 seconds, then access:
#    Airflow UI    → http://localhost:8080  (airflow/airflow)
#    FastAPI docs  → http://localhost:8000/docs
#    Dashboard     → http://localhost:8501
```

## Environment Variables

```bash
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_app_key
FERNET_KEY=generated_by_cryptography
SECRET_KEY=generated_by_secrets
DATABASE_URL=postgresql://airflow:airflow@postgres:5432/airflow
```

Generate security keys:
```bash
python -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())"
python -c "import secrets; print('SECRET_KEY=' + secrets.token_hex(32))"
```

## Project Structure

```
global-job-market-platform/
├── dags/
│   ├── ingestion_dag.py
│   ├── processing_dag.py
│   └── refinement_dag.py
├── src/
│   ├── ingestion/
│   │   ├── ingest_api.py        # Adzuna API multi-country
│   │   └── ingest_scraper.py    # RemoteOK scraper
│   ├── processing/
│   │   ├── process_jobs.py      # PySpark: raw → trusted
│   │   └── refine_jobs.py       # PySpark: trusted → refined
│   ├── warehouse/
│   │   └── load_warehouse.py    # Star Schema loader
│   ├── api/
│   │   ├── main.py              # FastAPI endpoints
│   │   └── Dockerfile
│   └── dashboard/
│       ├── app.py               # Streamlit dark mode dashboard
│       └── Dockerfile
├── sql/
│   └── schema.sql               # Star Schema DDL
├── data/
│   ├── raw/                     # Bronze layer (JSON)
│   ├── trusted/                 # Silver layer (Parquet)
│   ├── refined/                 # Gold layer (aggregations)
│   └── quarantine/              # Invalid records
├── Dockerfile                   # Airflow image
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Key Engineering Decisions

**Why PySpark inside Airflow?** The Airflow container includes Java 17 and PySpark, allowing batch processing without a separate Spark cluster. This simplifies the architecture for a single-node setup while demonstrating distributed processing concepts.

**Why Star Schema over a flat table?** The dimensional model enables efficient analytical queries across multiple dimensions (time, company, location) and mirrors what you'd build in a real data warehouse (Snowflake, Redshift, BigQuery).

**Why FastAPI between the database and Streamlit?** The dashboard consumes the REST API instead of querying the database directly. This is the production-grade pattern — the API layer handles connection pooling, query optimization, and can serve multiple consumers simultaneously.

**Why partitioned Parquet?** The trusted layer is partitioned by country, enabling Spark to skip irrelevant partitions during processing (partition pruning) — a key performance optimization in production data lakes.