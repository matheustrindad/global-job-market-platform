\# Global Job Market Platform



A production-grade data platform that ingests, processes, and exposes global job market data through a REST API and interactive dashboard — built with Apache Airflow, PySpark, PostgreSQL Star Schema, FastAPI, and Streamlit.



\## Architecture



```

Adzuna API (BR/US/GB/AT)          RemoteOK Scraper

&#x20;        └──────────────┬──────────────┘

&#x20;                       ↓

&#x20;             Data Lake — raw/ (JSON)

&#x20;                       ↓

&#x20;            PySpark Batch Processing

&#x20;             ├── Validation \& cleaning

&#x20;             ├── Seniority extraction

&#x20;             └── Salary normalization (USD)

&#x20;                       ↓

&#x20;        ┌──────────────┴──────────────┐

&#x20;     trusted/ (Parquet)         quarantine/

&#x20;                       ↓

&#x20;            PySpark Aggregations

&#x20;                       ↓

&#x20;          refined/ (3 Parquet files)

&#x20;                       ↓

&#x20;       PostgreSQL — Star Schema

&#x20;                       ↓

&#x20;             FastAPI (REST API)

&#x20;                       ↓

&#x20;         Streamlit Dashboard (dark mode)



&#x20;    Apache Airflow orchestrates every step

&#x20;    Docker Compose runs the full stack

```



\## Star Schema



```

&#x20;                   ┌─────────────┐

&#x20;                   │  dim\_date   │

&#x20;                   │─────────────│

&#x20;                   │ id (PK)     │

&#x20;                   │ date        │

&#x20;                   │ year        │

&#x20;                   │ month       │

&#x20;                   │ day         │

&#x20;                   │ weekday     │

&#x20;                   └──────┬──────┘

&#x20;                          │

┌─────────────┐    ┌───────┴───────────┐    ┌──────────────┐

│ dim\_company │    │ fact\_job\_postings │    │ dim\_location │

│─────────────│    │───────────────────│    │──────────────│

│ id (PK)     ├────│ id (PK)           ├────│ id (PK)      │

│ name        │    │ company\_id (FK)   │    │ country      │

│ domain      │    │ location\_id (FK)  │    │ city         │

└─────────────┘    │ date\_id (FK)      │    └──────────────┘

&#x20;                  │ title             │

&#x20;                  │ salary\_min        │

&#x20;                  │ salary\_max        │

&#x20;                  │ salary\_min\_usd    │

&#x20;                  │ salary\_max\_usd    │

&#x20;                  │ seniority         │

&#x20;                  │ is\_remote         │

&#x20;                  │ redirect\_url      │

&#x20;                  │ source            │

&#x20;                  │ ingested\_at       │

&#x20;                  └───────────────────┘

```



\## Tech Stack



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



\## Airflow DAGs



| DAG | Schedule | Description |

|---|---|---|

| `ingestion\_dag` | 08:00 UTC | Adzuna API (4 countries) + RemoteOK scraper |

| `processing\_dag` | 09:00 UTC | PySpark: raw JSON → trusted Parquet + quarantine |

| `refinement\_dag` | 10:00 UTC | PySpark: trusted → 3 analytical aggregations |



\## Refined Layer



Three pre-aggregated Parquet files ready for consumption:



\- `salary\_by\_country\_seniority.parquet` — avg salary (USD) by country and seniority level

\- `top\_skills\_by\_country.parquet` — most demanded skills extracted from job titles

\- `jobs\_volume\_trend.parquet` — daily posting volume with company count and avg salary



\## API Endpoints



```bash

\# Health check

GET /health



\# Job listings with filters

GET /jobs?country=us\&seniority=senior\&salary\_min=100000\&limit=50



\# Average salary by country and seniority

GET /salaries?country=gb



\# Top hiring companies

GET /companies?top\_n=10\&country=us



\# Daily posting volume trend

GET /trends?days=30\&country=br

```



Interactive API docs available at `http://localhost:8000/docs`



\## How to Run



\*\*Prerequisites:\*\* Docker Desktop, Git



```bash

\# 1. Clone the repository

git clone https://github.com/matheustrindad/global-job-market-platform.git

cd global-job-market-platform



\# 2. Set up environment variables

cp .env.example .env

\# Edit .env and add your ADZUNA\_APP\_ID and ADZUNA\_APP\_KEY



\# 3. Start the full stack

docker-compose up -d



\# 4. Wait \~30 seconds, then access:

\#    Airflow UI    → http://localhost:8080  (airflow/airflow)

\#    FastAPI docs  → http://localhost:8000/docs

\#    Dashboard     → http://localhost:8501

```



\## Environment Variables



```bash

ADZUNA\_APP\_ID=your\_app\_id

ADZUNA\_APP\_KEY=your\_app\_key

FERNET\_KEY=generated\_by\_cryptography

SECRET\_KEY=generated\_by\_secrets

DATABASE\_URL=postgresql://airflow:airflow@postgres:5432/airflow

```



Generate security keys:

```bash

python -c "from cryptography.fernet import Fernet; print('FERNET\_KEY=' + Fernet.generate\_key().decode())"

python -c "import secrets; print('SECRET\_KEY=' + secrets.token\_hex(32))"

```



\## Project Structure



```

global-job-market-platform/

├── dags/

│   ├── ingestion\_dag.py

│   ├── processing\_dag.py

│   └── refinement\_dag.py

├── src/

│   ├── ingestion/

│   │   ├── ingest\_api.py        # Adzuna API multi-country

│   │   └── ingest\_scraper.py    # RemoteOK scraper

│   ├── processing/

│   │   ├── process\_jobs.py      # PySpark: raw → trusted

│   │   └── refine\_jobs.py       # PySpark: trusted → refined

│   ├── warehouse/

│   │   └── load\_warehouse.py    # Star Schema loader

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



\## Key Engineering Decisions



\*\*Why PySpark inside Airflow?\*\* The Airflow container includes Java 17 and PySpark, allowing batch processing without a separate Spark cluster. This simplifies the architecture for a single-node setup while demonstrating distributed processing concepts.



\*\*Why Star Schema over a flat table?\*\* The dimensional model enables efficient analytical queries across multiple dimensions (time, company, location) and mirrors what you'd build in a real data warehouse (Snowflake, Redshift, BigQuery).



\*\*Why FastAPI between the database and Streamlit?\*\* The dashboard consumes the REST API instead of querying the database directly. This is the production-grade pattern — the API layer handles connection pooling, query optimization, and can serve multiple consumers simultaneously.



\*\*Why partitioned Parquet?\*\* The trusted layer is partitioned by country, enabling Spark to skip irrelevant partitions during processing (partition pruning) — a key performance optimization in production data lakes.

