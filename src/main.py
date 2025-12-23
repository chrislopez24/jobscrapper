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


def run_once(
    config: dict,
    scraper: JobScraper,
    storage: JobStorage,
    notifier: JobNotifier,
    jooble: JoobleClient | None = None,
):
    logger.info("Starting job search...")

    searches = config.get("searches", [])
    sites = config.get("sites", ["linkedin", "indeed"])
    results_wanted = config.get("results_per_search", 25)
    hours_old = config.get("hours_old", 24)

    all_jobs = []

    # Scrape from JobSpy (LinkedIn, Indeed, etc.)
    if sites:
        jobspy_jobs = scraper.scrape_searches(
            searches=searches,
            sites=sites,
            results_wanted=results_wanted,
            hours_old=hours_old,
        )
        all_jobs.extend(jobspy_jobs)
        logger.info(f"JobSpy: {len(jobspy_jobs)} jobs")

    # Scrape from Jooble
    if jooble:
        jooble_jobs = jooble.search_multiple(
            searches=searches,
            results_per_search=results_wanted,
        )
        all_jobs.extend(jooble_jobs)
        logger.info(f"Jooble: {len(jooble_jobs)} jobs")

    logger.info(f"Total jobs scraped: {len(all_jobs)}")

    # Apply filters
    all_jobs = filter_jobs(
        all_jobs,
        title_must_contain=config.get("title_must_contain"),
        location_exclude=config.get("location_exclude"),
    )
    logger.info(f"Jobs after filtering: {len(all_jobs)}")

    new_jobs = storage.filter_new_jobs(all_jobs)
    logger.info(f"New jobs found: {len(new_jobs)}")

    if new_jobs:
        storage.mark_jobs_seen(new_jobs)
        notifier.notify(new_jobs)

    storage.cleanup_old(days=30)


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

    interval_hours = config.get("interval_hours", 5)
    interval_seconds = interval_hours * 3600

    logger.info(f"Job Scrapper started. Interval: {interval_hours}h")
    logger.info(f"Searches: {len(config.get('searches', []))}")
    logger.info(f"Sites: {config.get('sites', [])}")
    if jooble:
        logger.info("Jooble: enabled")

    while True:
        try:
            run_once(config, scraper, storage, notifier, jooble)
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)

        logger.info(f"Sleeping for {interval_hours} hours...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
