import logging
import time
import sys
from pathlib import Path

import yaml

from src.scraper import JobScraper
from src.storage import JobStorage
from src.notifier import JobNotifier

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


def run_once(config: dict, scraper: JobScraper, storage: JobStorage, notifier: JobNotifier):
    logger.info("Starting job search...")

    searches = config.get("searches", [])
    sites = config.get("sites", ["linkedin", "indeed"])
    results_wanted = config.get("results_per_search", 25)
    hours_old = config.get("hours_old", 24)

    all_jobs = scraper.scrape_searches(
        searches=searches,
        sites=sites,
        results_wanted=results_wanted,
        hours_old=hours_old,
    )

    logger.info(f"Total jobs scraped: {len(all_jobs)}")

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

    interval_hours = config.get("interval_hours", 5)
    interval_seconds = interval_hours * 3600

    logger.info(f"Job Scrapper started. Interval: {interval_hours}h")
    logger.info(f"Searches: {len(config.get('searches', []))}")
    logger.info(f"Sites: {config.get('sites', [])}")

    while True:
        try:
            run_once(config, scraper, storage, notifier)
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)

        logger.info(f"Sleeping for {interval_hours} hours...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
