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
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def load_config(path: str = "config/config.yaml") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        logger.error(f"Config file not found: {path}")
        sys.exit(1)

    with open(config_path) as f:
        return yaml.safe_load(f)


def scrape_and_notify(
    jobs: list[dict],
    config: dict,
    storage: JobStorage,
    notifier: JobNotifier,
    source: str,
):
    """Filter, dedupe, and notify for a batch of jobs."""
    if not jobs:
        return

    logger.info(f"{source}: {len(jobs)} jobs scraped")

    # Apply filters
    jobs = filter_jobs(
        jobs,
        title_must_contain=config.get("title_must_contain"),
        location_exclude=config.get("location_exclude"),
        remote_only=config.get("remote_only", False),
    )
    logger.info(f"{source}: {len(jobs)} after filters")

    # Dedupe
    new_jobs = storage.filter_new_jobs(jobs)
    logger.info(f"{source}: {len(new_jobs)} new jobs")

    if new_jobs:
        storage.mark_jobs_seen(new_jobs)
        notifier.notify(new_jobs)


def main():
    config = load_config()

    rate_limits = config.get("rate_limits")
    scraper = JobScraper(rate_limits=rate_limits)

    storage = JobStorage(db_path=config.get("db_path", "data/jobs.db"))

    apprise_urls = config.get("apprise_urls", [])
    notifier = JobNotifier(apprise_urls=apprise_urls)

    # Initialize Jooble if API key is configured
    jooble_api_key = config.get("jooble_api_key")
    jooble = JoobleClient(jooble_api_key) if jooble_api_key else None

    # Intervals (in seconds)
    jobspy_interval = config.get("interval_hours", 4) * 3600
    jooble_interval = config.get("jooble_interval_hours", 6) * 3600

    searches = config.get("searches", [])
    sites = config.get("sites", ["linkedin", "indeed"])
    results_wanted = config.get("results_per_search", 25)
    hours_old = config.get("hours_old", 24)

    logger.info(f"Job Scrapper started")
    logger.info(f"Searches: {len(searches)}")
    logger.info(f"JobSpy sites: {sites} (every {jobspy_interval // 3600}h)")
    if jooble:
        logger.info(f"Jooble: enabled (every {jooble_interval // 3600}h)")

    # Track last run times
    last_jobspy_run = 0
    last_jooble_run = 0

    while True:
        now = time.time()

        # Run JobSpy if interval elapsed
        if sites and (now - last_jobspy_run >= jobspy_interval):
            try:
                logger.info("Running JobSpy (LinkedIn, Indeed)...")
                jobs = scraper.scrape_searches(
                    searches=searches,
                    sites=sites,
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                )
                scrape_and_notify(jobs, config, storage, notifier, "JobSpy")
                last_jobspy_run = now
            except Exception as e:
                logger.error(f"JobSpy error: {e}", exc_info=True)

        # Run Jooble if interval elapsed
        if jooble and (now - last_jooble_run >= jooble_interval):
            try:
                logger.info("Running Jooble...")
                jobs = jooble.search_multiple(
                    searches=searches,
                    results_per_search=results_wanted,
                )
                scrape_and_notify(jobs, config, storage, notifier, "Jooble")
                last_jooble_run = now
            except Exception as e:
                logger.error(f"Jooble error: {e}", exc_info=True)

        # Cleanup old jobs periodically
        storage.cleanup_old(days=30)

        # Sleep for 1 minute between checks
        time.sleep(60)


if __name__ == "__main__":
    main()
