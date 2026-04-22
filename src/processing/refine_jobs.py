"""
Refined layer — lê trusted/ (Parquet) e gera 3 agregações analíticas:
  1. salary_by_country_seniority.parquet
  2. top_skills_by_country.parquet
  3. jobs_volume_trend.parquet
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("refine_jobs")

SKILL_KEYWORDS = [
    "python", "sql", "spark", "airflow", "kafka", "docker", "kubernetes",
    "aws", "azure", "gcp", "dbt", "snowflake", "databricks", "pandas",
    "scala", "java", "terraform", "git", "postgresql", "mongodb",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("JobMarketRefined")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def run(
    trusted_dir: str = "data/trusted",
    refined_dir: str = "data/refined",
) -> dict:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

    # Lê toda a camada trusted
    import glob
    files = glob.glob(str(Path(trusted_dir) / "**" / "*.parquet"), recursive=True)
    if not files:
        log.warning("Nenhum arquivo Parquet em %s", trusted_dir)
        spark.stop()
        return {}

    df = spark.read.parquet(*files)
    log.info("Trusted records loaded: %d", df.count())

    Path(refined_dir).mkdir(parents=True, exist_ok=True)

    # ── 1. Salary by country + seniority ────────────────────────
    salary_agg = (
        df.filter(F.col("salary_min_usd").isNotNull())
        .groupBy("country", "seniority")
        .agg(
            F.round(F.avg("salary_min_usd"), 2).alias("avg_salary_min_usd"),
            F.round(F.avg("salary_max_usd"), 2).alias("avg_salary_max_usd"),
            F.round(F.stddev("salary_min_usd"), 2).alias("stddev_salary"),
            F.count("*").alias("job_count"),
        )
        .orderBy("country", "seniority")
    )
    salary_path = str(Path(refined_dir) / "salary_by_country_seniority.parquet")
    salary_agg.write.mode("overwrite").parquet(salary_path)
    log.info("salary_by_country_seniority → %d rows", salary_agg.count())

    # ── 2. Top skills by country ─────────────────────────────────
    # Extrai skills do título + description via keywords
    skill_cols = []
    for skill in SKILL_KEYWORDS:
        col_name = f"has_{skill}"
        df = df.withColumn(
            col_name,
            F.lower(F.coalesce(F.col("title"), F.lit(""))).rlike(f"\\b{skill}\\b") |
            F.lower(F.coalesce(F.col("description_clean") if "description_clean" in df.columns
                               else F.col("title"), F.lit(""))).rlike(f"\\b{skill}\\b")
        )
        skill_cols.append((skill, col_name))

    skill_rows = []
    for skill, col_name in skill_cols:
        skill_rows.append(
            df.filter(F.col(col_name))
            .groupBy("country")
            .agg(F.count("*").alias("mention_count"))
            .withColumn("skill", F.lit(skill))
        )

    from functools import reduce
    from pyspark.sql import DataFrame as SparkDF

    skills_df = reduce(SparkDF.unionAll, skill_rows)
    skills_agg = (
        skills_df
        .groupBy("country", "skill")
        .agg(F.sum("mention_count").alias("mention_count"))
        .orderBy("country", F.col("mention_count").desc())
    )
    skills_path = str(Path(refined_dir) / "top_skills_by_country.parquet")
    skills_agg.write.mode("overwrite").parquet(skills_path)
    log.info("top_skills_by_country → %d rows", skills_agg.count())

    # ── 3. Jobs volume trend ─────────────────────────────────────
    trend_agg = (
        df.filter(F.col("posted_date").isNotNull())
        .groupBy("posted_date", "country")
        .agg(
            F.count("*").alias("job_count"),
            F.countDistinct("company").alias("company_count"),
            F.round(F.avg("salary_min_usd"), 2).alias("avg_salary_usd"),
        )
        .orderBy("posted_date", "country")
    )
    trend_path = str(Path(refined_dir) / "jobs_volume_trend.parquet")
    trend_agg.write.mode("overwrite").parquet(trend_path)
    log.info("jobs_volume_trend → %d rows", trend_agg.count())

    spark.stop()
    return {
        "salary_rows":  salary_agg.count(),
        "skills_rows":  skills_agg.count(),
        "trend_rows":   trend_agg.count(),
    }


if __name__ == "__main__":
    result = run()
    print(result)