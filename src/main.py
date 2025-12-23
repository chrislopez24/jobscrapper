import logging
import time
import sys
from pathlib import Path

import yaml

from src.scraper import JobScraper
from src.storage import JobStorage
from src.notifier import JobNotifier
from src.filters import filter_jobs
from src.jooble import JoobleClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def load_config(path: str = "config/config.yaml") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        logger.error(f"Config not found: {path}")
        sys.exit(1)

    with open(config_path) as f:
        return yaml.safe_load(f)


def process_jobs(
    jobs: list[dict],
    config: dict,
    storage: JobStorage,
    notifier: JobNotifier,
    source: str,
    linkedin_429: bool = False,
) -> str:
    """Filter, dedupe, and notify. Returns summary string."""
    if not jobs:
        return f"{source}: 0 scraped"

    scraped = len(jobs)

    # Apply filters
    jobs = filter_jobs(
        jobs,
        title_must_contain=config.get("title_must_contain"),
        location_exclude=config.get("location_exclude"),
        remote_only=config.get("remote_only", False),
        spain_only=config.get("spain_only", False),
    )
    filtered = len(jobs)

    # Dedupe
    new_jobs = storage.filter_new_jobs(jobs)
    new_count = len(new_jobs)

    # Build summary
    summary = f"{source}: {scraped} → {filtered} → {new_count} new"

    if new_jobs:
        storage.mark_jobs_seen(new_jobs)
        if notifier.notify(new_jobs, linkedin_429=linkedin_429):
            summary += " ✓"
        else:
            summary += " (notify failed)"

    if linkedin_429:
        summary += " [429]"

    return summary


def main():
    config = load_config()

    scraper = JobScraper(rate_limits=config.get("rate_limits"))
    storage = JobStorage(db_path=config.get("db_path", "data/jobs.db"))
    notifier = JobNotifier(apprise_urls=config.get("apprise_urls", []))

    jooble_api_key = config.get("jooble_api_key")
    jooble = JoobleClient(jooble_api_key) if jooble_api_key else None

    # Intervals
    jobspy_interval = config.get("interval_hours", 4) * 3600
    jooble_interval = config.get("jooble_interval_hours", 6) * 3600

    searches = config.get("searches", [])
    sites = config.get("sites", ["linkedin", "indeed"])
    results_wanted = config.get("results_per_search", 25)
    hours_old = config.get("hours_old", 24)

    sources = []
    if sites:
        sources.append(f"JobSpy({','.join(sites)}) every {jobspy_interval//3600}h")
    if jooble:
        sources.append(f"Jooble every {jooble_interval//3600}h")

    logger.info(f"Started | {len(searches)} searches | {' | '.join(sources)}")

    last_jobspy_run = 0
    last_jooble_run = 0

    while True:
        now = time.time()

        # JobSpy
        if sites and (now - last_jobspy_run >= jobspy_interval):
            try:
                jobs = scraper.scrape_searches(
                    searches=searches,
                    sites=sites,
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                )
                summary = process_jobs(
                    jobs, config, storage, notifier, "JobSpy",
                    linkedin_429=scraper.linkedin_429,
                )
                logger.info(summary)
                last_jobspy_run = now
            except Exception as e:
                logger.error(f"JobSpy error: {e}")

        # Jooble
        if jooble and (now - last_jooble_run >= jooble_interval):
            try:
                jobs = jooble.search_multiple(
                    searches=searches,
                    results_per_search=results_wanted,
                )
                summary = process_jobs(jobs, config, storage, notifier, "Jooble")
                logger.info(summary)
                last_jooble_run = now
            except Exception as e:
                logger.error(f"Jooble error: {e}")

        # Cleanup old jobs (silent, only logs on error)
        try:
            storage.cleanup_old(days=30)
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

        time.sleep(60)


if __name__ == "__main__":
    main()
