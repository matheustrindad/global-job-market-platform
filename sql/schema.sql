-- ============================================================
-- Star Schema — Job Market Data Platform
-- Dimensões + Tabela Fato com chaves estrangeiras
-- ============================================================

-- Dimensões primeiro (sem dependências)
DROP TABLE IF EXISTS fact_job_postings CASCADE;
DROP TABLE IF EXISTS dim_company     CASCADE;
DROP TABLE IF EXISTS dim_location    CASCADE;
DROP TABLE IF EXISTS dim_date        CASCADE;

-- ── Dimensão: Empresa ────────────────────────────────────────
CREATE TABLE dim_company (
    id      SERIAL PRIMARY KEY,
    name    VARCHAR(255) NOT NULL,
    domain  VARCHAR(255),
    UNIQUE (name)
);

-- ── Dimensão: Localização ────────────────────────────────────
CREATE TABLE dim_location (
    id           SERIAL PRIMARY KEY,
    country      VARCHAR(10)  NOT NULL,
    city         VARCHAR(255),
    UNIQUE (country, city)
);

-- ── Dimensão: Tempo ──────────────────────────────────────────
CREATE TABLE dim_date (
    id       SERIAL PRIMARY KEY,
    date     DATE NOT NULL UNIQUE,
    year     SMALLINT NOT NULL,
    month    SMALLINT NOT NULL,
    day      SMALLINT NOT NULL,
    weekday  SMALLINT NOT NULL  -- 0=Monday … 6=Sunday
);

-- ── Tabela Fato: Vagas ───────────────────────────────────────
CREATE TABLE fact_job_postings (
    id             SERIAL PRIMARY KEY,
    job_id         VARCHAR(100),
    title          VARCHAR(500),
    company_id     INT REFERENCES dim_company(id),
    location_id    INT REFERENCES dim_location(id),
    date_id        INT REFERENCES dim_date(id),
    salary_min     DOUBLE PRECISION,
    salary_max     DOUBLE PRECISION,
    salary_min_usd DOUBLE PRECISION,
    salary_max_usd DOUBLE PRECISION,
    seniority      VARCHAR(50),
    is_remote      BOOLEAN,
    source         VARCHAR(50),
    ingested_at    TIMESTAMP
);

-- Índices para performance nas queries da FastAPI
CREATE INDEX idx_fact_company  ON fact_job_postings(company_id);
CREATE INDEX idx_fact_location ON fact_job_postings(location_id);
CREATE INDEX idx_fact_date     ON fact_job_postings(date_id);
CREATE INDEX idx_fact_seniority ON fact_job_postings(seniority);