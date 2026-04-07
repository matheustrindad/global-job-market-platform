"""
PySpark batch processor — reads raw/ JSON, cleans and validates,
writes Parquet to trusted/ and invalid records to quarantine/.

Transformations:
  - Normalize column names and types
  - Validate required fields (title, country, date)
  - Salary >= 0, currency normalization
  - Deduplicate by (id, country)
  - Extract seniority label from title
  - Tag is_remote from location text

Run standalone:  python src/processing/process_jobs.py
Run via Airflow: processing_dag.py calls process_jobs.run()
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("process_jobs")

VALID_COUNTRIES = {"br", "us", "gb", "at", "remote"}

SENIORITY_PATTERNS = {
    "senior":    r"(?i)(senior|sr\.?|lead|principal|staff)",
    "junior":    r"(?i)(junior|jr\.?|entry.level|associate|trainee|intern)",
    "mid":       r"(?i)(mid.level|pleno|middle)",
}

# USD is the canonical salary currency for comparison
SALARY_USD_FACTORS = {
    "br": 0.20,   # BRL → USD (approximate)
    "gb": 1.27,   # GBP → USD
    "at": 1.09,   # EUR → USD
    "us": 1.00,
    "remote": 1.00,
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("JobMarketProcessing")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, raw_dir: Path) -> DataFrame:
    """Read all JSON files from raw/ recursively."""
    pattern = str(raw_dir / "**" / "*.json")
    df = spark.read.option("multiLine", True).json(pattern)
    log.info("Raw records loaded: %d", df.count())
    return df


def normalize_columns(df: DataFrame) -> DataFrame:
    """Rename and cast fields to a unified schema."""
    # Different sources use different field names — unify them
    df = df.withColumnRenamed("position", "title") if "position" in df.columns else df

    # Adzuna uses salary_min/salary_max as floats; RemoteOK sometimes sends nulls
    for col in ("salary_min", "salary_max"):
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))
        else:
            df = df.withColumn(col, F.lit(None).cast(DoubleType()))

    # Standardize country code to lowercase
    df = df.withColumn("country", F.lower(F.col("_country")))

    # Parse ingestion timestamp
    df = df.withColumn(
        "ingested_at",
        F.to_timestamp(F.col("_ingested_at"))
    )

    # Parse posted date (Adzuna: ISO string, RemoteOK: Unix timestamp string)
    df = df.withColumn(
        "posted_date",
        F.coalesce(
            F.to_date(F.col("posted_date"), "yyyy-MM-dd"),
            F.to_date(F.from_unixtime(F.col("posted_date").cast("long")), "yyyy-MM-dd"),
        ) if "posted_date" in df.columns else F.current_date()
    )

    # Drop raw metadata columns
    drop_cols = [c for c in ("_country", "_ingested_at", "_source") if c in df.columns]
    df = df.drop(*drop_cols)

    return df


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split into valid and quarantine DataFrames.
    Returns (valid_df, quarantine_df).
    """
    valid_country = F.col("country").isin(list(VALID_COUNTRIES))
    valid_title   = F.col("title").isNotNull() & (F.length(F.trim(F.col("title"))) > 2)
    valid_salary  = (
        F.col("salary_min").isNull() |
        (F.col("salary_min") >= 0)
    ) & (
        F.col("salary_max").isNull() |
        (F.col("salary_max") >= 0)
    )

    is_valid = valid_country & valid_title & valid_salary

    # Tag quarantine reason for auditability
    quarantine_df = (
        df.filter(~is_valid)
        .withColumn("quarantine_reason", F.when(~valid_title, "missing_title")
                                          .when(~valid_country, "invalid_country")
                                          .when(~valid_salary, "negative_salary")
                                          .otherwise("unknown"))
        .withColumn("quarantined_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )

    valid_df = df.filter(is_valid)
    log.info("Valid: %d | Quarantine: %d", valid_df.count(), quarantine_df.count())
    return valid_df, quarantine_df


def enrich(df: DataFrame) -> DataFrame:
    """Add derived columns: seniority, is_remote, salary_usd."""
    # Seniority extraction from title
    seniority_col = F.lit("mid")  # default
    for level, pattern in SENIORITY_PATTERNS.items():
        seniority_col = F.when(F.col("title").rlike(pattern), F.lit(level)).otherwise(seniority_col)

    df = df.withColumn("seniority", seniority_col)

    # Remote flag
    df = df.withColumn(
        "is_remote",
        F.col("country").eqNullSafe("remote") |
        F.lower(F.coalesce(F.col("location"), F.lit(""))).rlike(r"remote|anywhere|worldwide")
    )

    # Salary in USD (for cross-country comparison)
    salary_usd_map = F.create_map(
        *[item for k, v in SALARY_USD_FACTORS.items() for item in (F.lit(k), F.lit(v))]
    )
    factor = F.coalesce(salary_usd_map[F.col("country")], F.lit(1.0))
    df = df.withColumn("salary_min_usd", (F.col("salary_min") * factor).cast(DoubleType()))
    df = df.withColumn("salary_max_usd", (F.col("salary_max") * factor).cast(DoubleType()))

    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep latest record per (id, country) based on ingested_at."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("id", "country").orderBy(F.col("ingested_at").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
    log.info("After deduplication: %d records", df.count())
    return df


def run(raw_dir: str = "data/raw", trusted_dir: str = "data/trusted", quarantine_dir: str = "data/quarantine") -> dict:
    """Entry point — called by Airflow DAG or directly."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df       = read_raw(spark, Path(raw_dir))
    normalized   = normalize_columns(raw_df)
    valid_df, qdf = validate(normalized)
    enriched     = enrich(valid_df)
    deduped      = deduplicate(enriched)

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Write trusted (Parquet, partitioned by country)
    trusted_path = str(Path(trusted_dir) / run_date)
    (
        deduped
        .write
        .mode("overwrite")
        .partitionBy("country")
        .parquet(trusted_path)
    )
    log.info("Trusted layer written → %s", trusted_path)

    # Write quarantine
    if qdf.count() > 0:
        quarantine_path = str(Path(quarantine_dir) / run_date)
        qdf.write.mode("overwrite").parquet(quarantine_path)
        log.info("Quarantine written → %s", quarantine_path)

    spark.stop()
    return {"valid": deduped.count(), "quarantined": qdf.count(), "date": run_date}


if __name__ == "__main__":
    result = run()
    print(result)
