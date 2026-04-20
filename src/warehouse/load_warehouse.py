"""
Carrega dados da camada trusted/ (Parquet) no Star Schema PostgreSQL.
Ordem: dim_company → dim_location → dim_date → fact_job_postings
"""

import glob
import logging
import os
from datetime import date

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("load_warehouse")

DB_URL = os.getenv("DATABASE_URL_LOCAL", os.getenv("DATABASE_URL", "postgresql://airflow:airflow@localhost:5433/airflow"))


def get_or_create(conn, table: str, match: dict, insert: dict) -> int:
    """Retorna o id de um registro existente ou insere e retorna o novo id."""
    where = " AND ".join(
        f"{k} = :{k}" if v is not None else f"{k} IS NULL"
        for k, v in match.items()
    )
    params = {k: v for k, v in match.items() if v is not None}
    row = conn.execute(text(f"SELECT id FROM {table} WHERE {where}"), params).fetchone()
    if row:
        return row[0]

    cols = ", ".join(insert.keys())
    vals = ", ".join(f":{k}" for k in insert.keys())
    result = conn.execute(
        text(f"INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING id"),
        insert,
    )
    return result.fetchone()[0]


def load_dim_date(conn, d: date) -> int:
    return get_or_create(
        conn, "dim_date",
        match={"date": d},
        insert={"date": d, "year": d.year, "month": d.month,
                "day": d.day, "weekday": d.weekday()},
    )


def load_dim_company(conn, name: str) -> int:
    if not name or str(name).strip() == "":
        name = "Unknown"
    name = str(name).strip()[:255]
    return get_or_create(conn, "dim_company", match={"name": name}, insert={"name": name})


def load_dim_location(conn, country: str, city: str = None) -> int:
    country = str(country).strip().upper()[:10] if country else "XX"
    city = str(city).strip()[:255] if city and str(city).strip() else None
    return get_or_create(
        conn, "dim_location",
        match={"country": country, "city": city},
        insert={"country": country, "city": city},
    )


def run(trusted_dir: str = "data/trusted", db_url: str = None) -> dict:
    url = db_url or DB_URL
    engine = create_engine(url, pool_pre_ping=True)

    files = glob.glob(os.path.join(trusted_dir, "**/*.parquet"), recursive=True)
    if not files:
        log.warning("Nenhum arquivo Parquet encontrado em %s", trusted_dir)
        return {"loaded": 0, "skipped": 0}

    log.info("Encontrados %d arquivos Parquet", len(files))
    total_loaded = total_skipped = 0

    with engine.begin() as conn:
        for fpath in files:
            try:
                df = pd.read_parquet(fpath)
                log.info("Processando %s (%d linhas)", os.path.basename(fpath), len(df))

                for _, row in df.iterrows():
                    try:
                        company_id  = load_dim_company(conn, row.get("company"))
                        location_id = load_dim_location(conn, row.get("country"), row.get("location"))

                        raw_date = row.get("posted_date")
                        if pd.isna(raw_date) if hasattr(raw_date, '__class__') else not raw_date:
                            raw_date = date.today()
                        if hasattr(raw_date, "date"):
                            raw_date = raw_date.date()
                        date_id = load_dim_date(conn, raw_date)

                        conn.execute(text("""
                            INSERT INTO fact_job_postings
                                (job_id, title, company_id, location_id, date_id,
                                 salary_min, salary_max, salary_min_usd, salary_max_usd,
                                 seniority, is_remote, source, ingested_at)
                            VALUES
                                (:job_id, :title, :company_id, :location_id, :date_id,
                                 :salary_min, :salary_max, :salary_min_usd, :salary_max_usd,
                                 :seniority, :is_remote, :source, :ingested_at)
                        """), {
                            "job_id":        str(row.get("id", ""))[:100],
                            "title":         str(row.get("title", ""))[:500],
                            "company_id":    company_id,
                            "location_id":   location_id,
                            "date_id":       date_id,
                            "salary_min":    float(row["salary_min"])    if pd.notna(row.get("salary_min"))    else None,
                            "salary_max":    float(row["salary_max"])    if pd.notna(row.get("salary_max"))    else None,
                            "salary_min_usd": float(row["salary_min_usd"]) if pd.notna(row.get("salary_min_usd")) else None,
                            "salary_max_usd": float(row["salary_max_usd"]) if pd.notna(row.get("salary_max_usd")) else None,
                            "seniority":     row.get("seniority"),
                            "is_remote":     bool(row.get("is_remote", False)),
                            "source":        str(row.get("_source", "adzuna"))[:50] if row.get("_source") else "adzuna",
                            "ingested_at":   row.get("ingested_at"),
                        })
                        total_loaded += 1

                    except Exception as e:
                        log.warning("Linha ignorada: %s", e)
                        total_skipped += 1

            except Exception as e:
                log.error("Erro ao ler %s: %s", fpath, e)

    log.info("Carga concluída — loaded: %d | skipped: %d", total_loaded, total_skipped)
    return {"loaded": total_loaded, "skipped": total_skipped}


if __name__ == "__main__":
    result = run()
    print(result)