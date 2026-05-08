# Global Job Market Platform

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apacheairflow)
![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?logo=apachespark)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Streamlit](https://img.shields.io/badge/Streamlit-1.35-FF4B4B?logo=streamlit)

A production-grade data platform that ingests, processes, and exposes global job market data through a REST API and interactive dashboard — orchestrated by Apache Airflow, processed by PySpark, stored in a PostgreSQL Star Schema, and consumed via FastAPI and Streamlit.

---

## The Problem

Understanding the global data engineering job market requires collecting and normalizing data from multiple countries and sources. Raw job data is inconsistent, nested, and unstructured. This platform solves that by building a complete data pipeline: from raw API responses to a queryable REST API backed by a dimensional data warehouse.

---

## Architecture

```
Adzuna API (BR / US / GB / AT)      RemoteOK Scraper
              └──────────────┬──────────────┘
                             ↓
                   Data Lake — raw/ (JSON)
                   [Bronze Layer — partitioned by country]
                             ↓
                  PySpark Batch Processing
                  ├── HTML cleaning
                  ├── Validation & quarantine
                  ├── Seniority extraction (regex)
                  └── Salary normalization (USD)
                             ↓
              ┌──────────────┴──────────────┐
           trusted/ (Parquet)          quarantine/
           [Silver Layer]              [rejected records]
                             ↓
                  PySpark Aggregations
                             ↓
                   refined/ (3 Parquet files)
                   [Gold Layer]
                             ↓
               PostgreSQL — Star Schema
                             ↓
                     FastAPI (REST API)
                             ↓
               Streamlit Dashboard (dark mode)

          Apache Airflow orchestrates every step (3 DAGs)
          Docker Compose runs the full stack with one command
```

---

## Star Schema

```
                   ┌─────────────┐
                   │  dim_date   │
                   ├─────────────┤
                   │ id (PK)     │
                   │ date        │
                   │ year        │
                   │ month       │
                   │ day         │
                   │ weekday     │
                   └──────┬──────┘
                          │
┌─────────────┐  ┌────────┴──────────┐  ┌──────────────┐
│ dim_company │  │ fact_job_postings  │  │ dim_location │
├─────────────┤  ├───────────────────┤  ├──────────────┤
│ id (PK)     ├──│ id (PK)           ├──│ id (PK)      │
│ name        │  │ company_id (FK)   │  │ country      │
│ domain      │  │ location_id (FK)  │  │ city         │
└─────────────┘  │ date_id (FK)      │  └──────────────┘
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

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Python, Requests, BeautifulSoup | Multi-country API + web scraping |
| Processing | Apache Spark 3.x (PySpark) | Batch cleaning, validation, aggregation |
| Orchestration | Apache Airflow 2.8 | DAG scheduling and dependency management |
| Data Warehouse | PostgreSQL 13 — Star Schema | Dimensional model for analytical queries |
| API | FastAPI + SQLAlchemy | REST endpoints with Pydantic validation |
| Dashboard | Streamlit + Plotly | Interactive dark-mode analytics UI |
| Infrastructure | Docker Compose | Single-command full stack deployment |

---

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `ingestion_dag` | 08:00 UTC daily | Adzuna API (4 countries, 200 jobs each) + RemoteOK scraper |
| `processing_dag` | 09:00 UTC daily | PySpark: raw JSON → trusted Parquet + quarantine |
| `refinement_dag` | 10:00 UTC daily | PySpark: trusted → 3 analytical aggregations |

---

## Refined Layer (Gold)

Three pre-aggregated Parquet files ready for consumption without additional computation:

| File | Description |
|---|---|
| `salary_by_country_seniority.parquet` | Avg salary (USD) with stddev, grouped by country and seniority |
| `top_skills_by_country.parquet` | Most demanded skills extracted from job titles and descriptions |
| `jobs_volume_trend.parquet` | Daily posting volume with company count and avg salary per country |

---

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

Interactive API docs (Swagger UI) available at `http://localhost:8000/docs`

---

## How to Run

**Prerequisites:** Docker Desktop, Git, Python 3.10+

```bash
# 1. Clone the repository
git clone https://github.com/matheustrindad/global-job-market-platform.git
cd global-job-market-platform

# 2. Set up environment variables
cp .env.example .env
# Edit .env and add your ADZUNA_APP_ID and ADZUNA_APP_KEY

# 3. Generate security keys
python -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())"
python -c "import secrets; print('SECRET_KEY=' + secrets.token_hex(32))"

# 4. Start the full stack
docker-compose up -d

# 5. Wait ~30 seconds, then access:
#    Airflow UI      → http://localhost:8080  (airflow / airflow)
#    FastAPI Swagger → http://localhost:8000/docs
#    Dashboard       → http://localhost:8501
```

---

## Environment Variables

```bash
ADZUNA_APP_ID=your_app_id        # https://developer.adzuna.com/
ADZUNA_APP_KEY=your_app_key
FERNET_KEY=generated_by_cryptography
SECRET_KEY=generated_by_secrets
DATABASE_URL=postgresql://airflow:airflow@postgres:5432/airflow
```

---

## Project Structure

```
global-job-market-platform/
├── dags/
│   ├── ingestion_dag.py         # Airflow DAG: ingest
│   ├── processing_dag.py        # Airflow DAG: process
│   └── refinement_dag.py        # Airflow DAG: refine
├── src/
│   ├── ingestion/
│   │   ├── ingest_api.py        # Adzuna multi-country ingestion
│   │   └── ingest_scraper.py    # RemoteOK scraper
│   ├── processing/
│   │   ├── process_jobs.py      # PySpark: raw → trusted
│   │   └── refine_jobs.py       # PySpark: trusted → refined
│   ├── warehouse/
│   │   └── load_warehouse.py    # Star Schema ETL loader
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
│   └── quarantine/              # Invalid records with rejection reason
├── Dockerfile                   # Airflow + Java 17 + PySpark image
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Key Engineering Decisions

**Why PySpark inside Airflow?**
The Airflow container includes Java 17 and PySpark, enabling batch processing without a separate Spark cluster. This simplifies the architecture while demonstrating distributed processing patterns — partition pruning, window functions, and columnar storage.

**Why Star Schema over a flat table?**
The dimensional model enables efficient analytical queries across time, company, and location dimensions — the same pattern used in Snowflake, Redshift, and BigQuery. It also keeps the fact table narrow and fast to scan.

**Why FastAPI between the database and Streamlit?**
The dashboard consumes the REST API rather than querying the database directly. This is the production pattern: the API layer handles connection pooling, query optimization, and can serve multiple consumers (dashboard, external clients, other services) simultaneously.

**Why partitioned Parquet?**
The trusted layer is partitioned by country (`country=us/`, `country=gb/`), enabling Spark to skip irrelevant partitions during processing. This is partition pruning — a key performance optimization in production data lakes.

**Why two ingestion sources?**
Combining a structured API (Adzuna) with a scraper (RemoteOK) demonstrates handling heterogeneous data sources with different schemas — a common real-world challenge. The pipeline normalizes both into a unified schema before processing.