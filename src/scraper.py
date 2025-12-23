import time
import logging
from typing import Any

from jobspy import scrape_jobs

logger = logging.getLogger(__name__)

DEFAULT_RATE_LIMITS = {
    "linkedin": {"delay": 2.0, "max_retries": 3, "backoff": 2},
    "indeed": {"delay": 1.0, "max_retries": 3, "backoff": 2},
    "glassdoor": {"delay": 1.5, "max_retries": 3, "backoff": 2},
    "google": {"delay": 1.0, "max_retries": 3, "backoff": 2},
    "zip_recruiter": {"delay": 1.0, "max_retries": 3, "backoff": 2},
}


class JobScraper:
    def __init__(self, rate_limits: dict[str, dict] | None = None):
        self.rate_limits = rate_limits or DEFAULT_RATE_LIMITS
        self.linkedin_429 = False

    def _get_rate_limit(self, site: str) -> dict:
        return self.rate_limits.get(site, {"delay": 1.0, "max_retries": 3, "backoff": 2})

    def scrape_site(
        self,
        site: str,
        search_term: str,
        location: str,
        remote: bool = True,
        results_wanted: int = 25,
        hours_old: int = 24,
        country: str = "spain",
    ) -> list[dict]:
        rate_limit = self._get_rate_limit(site)
        delay = rate_limit["delay"]
        max_retries = rate_limit["max_retries"]
        backoff = rate_limit["backoff"]

        for attempt in range(max_retries):
            try:
                logger.debug(f"{site}: searching '{search_term}'")

                df = scrape_jobs(
                    site_name=[site],
                    search_term=search_term,
                    location=location,
                    is_remote=remote,
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                    country_indeed=country,
                )

                time.sleep(delay)

                if df is None or df.empty:
                    return []

                return df.to_dict("records")

            except Exception as e:
                error_msg = str(e).lower()
                if "429" in error_msg or "rate" in error_msg:
                    if site == "linkedin":
                        self.linkedin_429 = True
                    wait_time = delay * (backoff ** attempt)
                    logger.warning(f"{site}: 429 rate limit, retry {attempt+1}/{max_retries}")
                    time.sleep(wait_time)
                else:
                    logger.debug(f"{site}: error - {e}")
                    if attempt == max_retries - 1:
                        return []
                    time.sleep(delay)

        return []

    def scrape_all(
        self,
        sites: list[str],
        search_term: str,
        location: str,
        remote: bool = True,
        results_wanted: int = 25,
        hours_old: int = 24,
        country: str = "spain",
    ) -> list[dict]:
        all_jobs = []

        for site in sites:
            jobs = self.scrape_site(
                site=site,
                search_term=search_term,
                location=location,
                remote=remote,
                results_wanted=results_wanted,
                hours_old=hours_old,
                country=country,
            )
            all_jobs.extend(jobs)

            if site != sites[-1]:
                rate_limit = self._get_rate_limit(site)
                time.sleep(rate_limit["delay"])

        return all_jobs

    def scrape_searches(
        self,
        searches: list[dict[str, Any]],
        sites: list[str],
        results_wanted: int = 25,
        hours_old: int = 24,
    ) -> list[dict]:
        all_jobs = []
        self.linkedin_429 = False

        for search in searches:
            jobs = self.scrape_all(
                sites=sites,
                search_term=search["term"],
                location=search.get("location", "Spain"),
                remote=search.get("remote", True),
                results_wanted=results_wanted,
                hours_old=hours_old,
                country=search.get("country", "spain"),
            )
            all_jobs.extend(jobs)

        return all_jobs
