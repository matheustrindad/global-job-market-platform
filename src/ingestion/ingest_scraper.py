"""
Web scraper — scrapes RemoteOK public job board (JSON endpoint, no login required).
Saves raw JSON to data/raw/remoteok/<date>.json

RemoteOK exposes a public JSON API at https://remoteok.com/api
which is freely scrapeable (they explicitly allow it in their docs).
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("ingest_scraper")

REMOTEOK_API  = "https://remoteok.com/api"
REMOTEOK_HTML = "https://remoteok.com/remote-data-dev-jobs"  # fallback HTML scrape

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; JobMarketPlatformBot/1.0; "
        "+https://github.com/matheustrindad/job-market-platform)"
    )
}

TARGET_KEYWORDS = {"data", "engineer", "analyst", "pipeline", "spark", "airflow", "python"}


def _normalize_remoteok(job: dict, ts: str) -> dict:
    """Map RemoteOK fields to our internal schema."""
    tags = job.get("tags") or []
    return {
        "id":           str(job.get("id", "")),
        "title":        job.get("position", ""),
        "company":      job.get("company", ""),
        "location":     "Remote",
        "country":      "REMOTE",
        "salary_min":   job.get("salary_min"),
        "salary_max":   job.get("salary_max"),
        "description":  job.get("description", ""),
        "tags":         tags,
        "url":          job.get("url", ""),
        "posted_date":  job.get("date", ""),
        "_ingested_at": ts,
        "_country":     "remote",
        "_source":      "remoteok_api",
    }


def fetch_remoteok_api() -> list[dict]:
    """Fetch jobs from RemoteOK's public JSON endpoint."""
    resp = requests.get(REMOTEOK_API, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    # First element is a legal/meta note, not a job — skip it
    jobs = [j for j in raw if isinstance(j, dict) and "id" in j]
    log.info("RemoteOK API returned %d raw records", len(jobs))
    return jobs


def fetch_remoteok_html_fallback() -> list[dict]:
    """
    BeautifulSoup HTML scraper — fallback if the JSON endpoint fails.
    Demonstrates scraping skills for the portfolio even if the API works.
    """
    resp = requests.get(REMOTEOK_HTML, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    jobs = []
    for row in soup.select("tr.job"):
        try:
            title_el   = row.select_one("h2[itemprop='title']")
            company_el = row.select_one("h3[itemprop='name']")
            tags_els   = row.select("a.tag span")
            link_el    = row.get("data-url", "")

            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            # Only keep data-related jobs
            if not any(kw in title.lower() for kw in TARGET_KEYWORDS):
                continue

            jobs.append({
                "id":       row.get("data-id", ""),
                "title":    title,
                "company":  company_el.get_text(strip=True) if company_el else "",
                "location": "Remote",
                "country":  "REMOTE",
                "tags":     [t.get_text(strip=True) for t in tags_els],
                "url":      f"https://remoteok.com{link_el}",
            })
        except Exception as e:
            log.debug("Skipping malformed row: %s", e)
            continue

    log.info("HTML fallback scraped %d data-related jobs", len(jobs))
    return jobs


def filter_data_jobs(jobs: list[dict]) -> list[dict]:
    """Keep only jobs with data-engineering-related tags or title."""
    filtered = []
    for job in jobs:
        title = (job.get("title") or job.get("position") or "").lower()
        tags  = [t.lower() for t in (job.get("tags") or [])]
        combined = title + " " + " ".join(tags)
        if any(kw in combined for kw in TARGET_KEYWORDS):
            filtered.append(job)
    log.info("After keyword filter: %d / %d jobs retained", len(filtered), len(jobs))
    return filtered


def run(raw_dir: str = "data/raw") -> int:
    """Entry point — called by Airflow DAG or directly."""
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ts       = datetime.now(timezone.utc).isoformat()
    out_dir  = Path(raw_dir) / "remoteok"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Polite crawl delay
    time.sleep(2)

    try:
        raw_jobs = fetch_remoteok_api()
        source   = "api"
    except Exception as e:
        log.warning("API fetch failed (%s), falling back to HTML scraper", e)
        raw_jobs = fetch_remoteok_html_fallback()
        source   = "html"

    filtered   = filter_data_jobs(raw_jobs)
    normalized = [_normalize_remoteok(j, ts) for j in filtered]

    out_file = out_dir / f"{run_date}_{source}.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(normalized, fh, ensure_ascii=False, indent=2)

    log.info("Scraper saved %d jobs → %s", len(normalized), out_file)
    return len(normalized)


if __name__ == "__main__":
    run()
