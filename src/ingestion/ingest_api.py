"""
Adzuna API ingestion — saves raw JSON to data/raw/<country>/<date>.json
Countries: BR, US, GB, AT
"""

import os
import json
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("ingest_api")

APP_ID  = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
BASE_URL = "https://api.adzuna.com/v1/api/jobs"

COUNTRIES = {
    "br": {"what": "data engineer", "results_per_page": 50},
    "us": {"what": "data engineer",  "results_per_page": 50},
    "gb": {"what": "data engineer",  "results_per_page": 50},
    "at": {"what": "data engineer",  "results_per_page": 50},
}

MAX_PAGES = 4  # 4 pages × 50 results = up to 200 jobs per country


def fetch_country(country: str, params: dict, raw_dir: Path, run_date: str) -> int:
    """Fetch all pages for one country and write one JSON file per page."""
    out_dir = raw_dir / country
    out_dir.mkdir(parents=True, exist_ok=True)

    total_saved = 0
    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}/{country}/search/{page}"
        payload = {
            "app_id":           APP_ID,
            "app_key":          APP_KEY,
            "results_per_page": params["results_per_page"],
            "what":             params["what"],
            "content-type":     "application/json",
        }
        try:
            resp = requests.get(url, params=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.HTTPError as e:
            log.warning("HTTP %s for %s page %d — skipping", e.response.status_code, country, page)
            break
        except requests.exceptions.RequestException as e:
            log.error("Request failed for %s page %d: %s", country, page, e)
            break

        jobs = data.get("results", [])
        if not jobs:
            log.info("No more results for %s at page %d", country, page)
            break

        # Enrich each record with ingestion metadata
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        for job in jobs:
            job["_ingested_at"] = ingestion_ts
            job["_country"]     = country
            job["_source"]      = "adzuna_api"

        out_file = out_dir / f"{run_date}_page{page:02d}.json"
        with open(out_file, "w", encoding="utf-8") as fh:
            json.dump(jobs, fh, ensure_ascii=False, indent=2)

        total_saved += len(jobs)
        log.info("Saved %d jobs → %s", len(jobs), out_file)

    return total_saved


def run(raw_dir: str = "data/raw") -> dict:
    """Entry point — called by Airflow DAG or directly."""
    if not APP_ID or not APP_KEY:
        raise EnvironmentError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in .env")

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_path = Path(raw_dir)
    summary  = {}

    for country, params in COUNTRIES.items():
        log.info("Fetching %s …", country.upper())
        count = fetch_country(country, params, raw_path, run_date)
        summary[country] = count
        log.info("Total saved for %s: %d", country.upper(), count)

    log.info("Ingestion complete: %s", summary)
    return summary


if __name__ == "__main__":
    run()
