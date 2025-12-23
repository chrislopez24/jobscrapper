import logging
import math

logger = logging.getLogger(__name__)


def _clean_str(value) -> str:
    """Clean value to lowercase string."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).lower().strip()


def filter_jobs(
    jobs: list[dict],
    title_must_contain: list[str] | None = None,
    location_exclude: list[str] | None = None,
) -> list[dict]:
    """
    Filter jobs based on title keywords and location exclusions.

    Args:
        jobs: List of job dicts from scraper
        title_must_contain: Job title must contain at least one of these (case-insensitive)
        location_exclude: Exclude jobs with location containing any of these (case-insensitive)

    Returns:
        Filtered list of jobs
    """
    if not title_must_contain and not location_exclude:
        return jobs

    title_keywords = [kw.lower() for kw in (title_must_contain or [])]
    location_blacklist = [loc.lower() for loc in (location_exclude or [])]

    filtered = []
    excluded_title = 0
    excluded_location = 0

    for job in jobs:
        title = _clean_str(job.get("title"))
        location = _clean_str(job.get("location"))

        # Check title keywords
        if title_keywords:
            if not any(kw in title for kw in title_keywords):
                excluded_title += 1
                logger.debug(f"Excluded by title: {job.get('title')}")
                continue

        # Check location blacklist
        if location_blacklist:
            if any(bl in location for bl in location_blacklist):
                excluded_location += 1
                logger.debug(f"Excluded by location: {job.get('title')} @ {job.get('location')}")
                continue

        filtered.append(job)

    if excluded_title or excluded_location:
        logger.info(f"Filtered out {excluded_title} by title, {excluded_location} by location")

    return filtered
