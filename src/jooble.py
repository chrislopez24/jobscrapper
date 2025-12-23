import logging
import requests

logger = logging.getLogger(__name__)

JOOBLE_API_URL = "https://jooble.org/api/{api_key}"


class JoobleClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.url = JOOBLE_API_URL.format(api_key=api_key)

    def search(
        self,
        keywords: str,
        location: str = "Spain",
        page: int = 1,
        results_per_page: int = 20,
    ) -> list[dict]:
        """
        Search for jobs on Jooble.

        Args:
            keywords: Search terms (e.g., "Product Designer")
            location: Location to search in (default: Spain)
            page: Page number (default: 1)
            results_per_page: Number of results per page (default: 20)

        Returns:
            List of job dicts with normalized keys
        """
        payload = {
            "keywords": keywords,
            "location": location,
            "page": str(page),
            "ResultOnPage": str(results_per_page),
        }

        try:
            logger.debug(f"Jooble: searching '{keywords}'")
            response = requests.post(
                self.url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            jobs = data.get("jobs", [])
            return [self._normalize_job(job) for job in jobs]

        except requests.exceptions.RequestException as e:
            logger.warning(f"Jooble API error: {e}")
            return []

    def _normalize_job(self, job: dict) -> dict:
        """Normalize Jooble job to match JobSpy format."""
        return {
            "title": job.get("title", ""),
            "company": job.get("company", ""),
            "location": job.get("location", ""),
            "job_url": job.get("link", ""),
            "site": "jooble",
            "description": job.get("snippet", ""),
            "date_posted": job.get("updated", ""),
            # Jooble doesn't always provide structured salary
            "min_amount": None,
            "max_amount": None,
            "currency": None,
        }

    def search_multiple(
        self,
        searches: list[dict],
        results_per_search: int = 20,
    ) -> list[dict]:
        """
        Run multiple searches.

        Args:
            searches: List of search configs with 'term' and optional 'location'
            results_per_search: Max results per search

        Returns:
            Combined list of jobs
        """
        all_jobs = []

        for search in searches:
            term = search.get("term", "")
            location = search.get("location", "Spain")

            jobs = self.search(
                keywords=term,
                location=location,
                results_per_page=results_per_search,
            )
            all_jobs.extend(jobs)

        return all_jobs
