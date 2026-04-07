"""
FastAPI REST layer — reads from PostgreSQL Star Schema.
Endpoints:
  GET /jobs            — list postings with filters
  GET /salaries        — avg salary by country + seniority
  GET /companies       — top companies by posting volume
  GET /trends          — daily posting volume (last 30 days)
  GET /health          — liveness probe for Docker health check

Swagger UI auto-generated at http://localhost:8000/docs
"""

import os
from contextlib import asynccontextmanager
from datetime import date, timedelta
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/job_market"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify DB connection on startup
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    yield


app = FastAPI(
    title="Job Market Data Platform API",
    description=(
        "REST API for the Job Market Data Platform — "
        "powered by a Star Schema data warehouse built with PySpark + Airflow."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Response schemas ──────────────────────────────────────────────────────────

class JobOut(BaseModel):
    job_id:       int
    title:        str
    company:      Optional[str]
    city:         Optional[str]
    country:      Optional[str]
    seniority:    Optional[str]
    is_remote:    Optional[bool]
    salary_min:   Optional[float]
    salary_max:   Optional[float]
    posted_date:  Optional[date]

    class Config:
        from_attributes = True


class SalaryRow(BaseModel):
    country:    str
    seniority:  str
    avg_min:    Optional[float]
    avg_max:    Optional[float]
    job_count:  int


class CompanyRow(BaseModel):
    company:   str
    job_count: int


class TrendRow(BaseModel):
    posted_date: date
    job_count:   int


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok"}


@app.get("/jobs", response_model=list[JobOut], tags=["jobs"])
def list_jobs(
    country:    Optional[str]   = Query(None, description="Country code: br, us, gb, at"),
    seniority:  Optional[str]   = Query(None, description="junior | mid | senior"),
    salary_min: Optional[float] = Query(None, description="Minimum salary (local currency)"),
    is_remote:  Optional[bool]  = Query(None),
    limit:      int             = Query(100, le=500),
    offset:     int             = Query(0),
    db: Session = Depends(get_db),
):
    """List job postings with optional filters. Max 500 per request."""
    sql = """
        SELECT
            f.id        AS job_id,
            f.title,
            c.name      AS company,
            l.city,
            l.country,
            f.seniority,
            f.is_remote,
            f.salary_min,
            f.salary_max,
            d.date      AS posted_date
        FROM fact_job_postings f
        LEFT JOIN dim_company  c ON f.company_id  = c.id
        LEFT JOIN dim_location l ON f.location_id = l.id
        LEFT JOIN dim_date     d ON f.date_id      = d.id
        WHERE 1=1
    """
    params: dict = {"limit": limit, "offset": offset}

    if country:
        sql += " AND LOWER(l.country) = :country"
        params["country"] = country.lower()
    if seniority:
        sql += " AND f.seniority = :seniority"
        params["seniority"] = seniority.lower()
    if salary_min is not None:
        sql += " AND f.salary_min >= :salary_min"
        params["salary_min"] = salary_min
    if is_remote is not None:
        sql += " AND f.is_remote = :is_remote"
        params["is_remote"] = is_remote

    sql += " ORDER BY d.date DESC LIMIT :limit OFFSET :offset"

    rows = db.execute(text(sql), params).fetchall()
    return [JobOut(**dict(r._mapping)) for r in rows]


@app.get("/salaries", response_model=list[SalaryRow], tags=["analytics"])
def salary_stats(
    country:   Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Average salary grouped by country and seniority."""
    sql = """
        SELECT
            l.country,
            f.seniority,
            ROUND(AVG(f.salary_min)::numeric, 2) AS avg_min,
            ROUND(AVG(f.salary_max)::numeric, 2) AS avg_max,
            COUNT(*)                              AS job_count
        FROM fact_job_postings f
        LEFT JOIN dim_location l ON f.location_id = l.id
        WHERE f.salary_min IS NOT NULL
    """
    params: dict = {}
    if country:
        sql += " AND LOWER(l.country) = :country"
        params["country"] = country.lower()

    sql += " GROUP BY l.country, f.seniority ORDER BY l.country, f.seniority"

    rows = db.execute(text(sql), params).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No salary data found")
    return [SalaryRow(**dict(r._mapping)) for r in rows]


@app.get("/companies", response_model=list[CompanyRow], tags=["analytics"])
def top_companies(
    country: Optional[str] = Query(None),
    top_n:   int           = Query(20, le=100),
    db: Session = Depends(get_db),
):
    """Top companies by number of open job postings."""
    sql = """
        SELECT
            c.name  AS company,
            COUNT(*) AS job_count
        FROM fact_job_postings f
        LEFT JOIN dim_company  c ON f.company_id  = c.id
        LEFT JOIN dim_location l ON f.location_id = l.id
        WHERE c.name IS NOT NULL
    """
    params: dict = {"top_n": top_n}
    if country:
        sql += " AND LOWER(l.country) = :country"
        params["country"] = country.lower()

    sql += " GROUP BY c.name ORDER BY job_count DESC LIMIT :top_n"

    rows = db.execute(text(sql), params).fetchall()
    return [CompanyRow(**dict(r._mapping)) for r in rows]


@app.get("/trends", response_model=list[TrendRow], tags=["analytics"])
def posting_trends(
    days:    int           = Query(30, le=365, description="Rolling window in days"),
    country: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Daily posting volume for the last N days."""
    since = date.today() - timedelta(days=days)
    sql = """
        SELECT
            d.date  AS posted_date,
            COUNT(*) AS job_count
        FROM fact_job_postings f
        LEFT JOIN dim_date     d ON f.date_id      = d.id
        LEFT JOIN dim_location l ON f.location_id  = l.id
        WHERE d.date >= :since
    """
    params: dict = {"since": since}
    if country:
        sql += " AND LOWER(l.country) = :country"
        params["country"] = country.lower()

    sql += " GROUP BY d.date ORDER BY d.date"

    rows = db.execute(text(sql), params).fetchall()
    return [TrendRow(**dict(r._mapping)) for r in rows]
