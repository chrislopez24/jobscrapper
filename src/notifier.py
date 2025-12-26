import logging
import math
import re
import apprise

logger = logging.getLogger(__name__)


def _clean_value(value) -> str | None:
    """Clean pandas NaN and empty values."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    val = str(value).strip()
    if val.lower() in ("nan", "none", ""):
        return None
    return val


def _clean_location(location: str | None) -> str | None:
    """Clean and simplify location strings."""
    if not location:
        return None

    # Remove verbose country names
    location = re.sub(r",?\s*Kingdom of Spain", "", location, flags=re.IGNORECASE)
    location = re.sub(r",?\s*Spain$", "", location, flags=re.IGNORECASE)

    # Clean up whitespace and trailing commas
    location = re.sub(r"\s+", " ", location).strip().strip(",").strip()

    # If empty after cleaning, return Spain as default
    if not location:
        return "Spain"

    return location


def _format_salary(min_amt, max_amt, currency: str) -> str | None:
    """Format salary range compactly."""
    def is_valid(val):
        return val and not (isinstance(val, float) and math.isnan(val))

    if not is_valid(min_amt):
        return None

    # Format in thousands (k) for readability
    def fmt(val):
        if val >= 1000:
            return f"{val/1000:.0f}k"
        return f"{val:.0f}"

    symbol = "€" if currency.upper() in ("EUR", "EURO") else currency

    if is_valid(max_amt):
        return f"{symbol}{fmt(min_amt)}-{fmt(max_amt)}"
    return f"{symbol}{fmt(min_amt)}+"


class JobNotifier:
    def __init__(self, apprise_urls: list[str] | None = None):
        self.apprise = apprise.Apprise()
        apprise_urls = apprise_urls or []
        for url in apprise_urls:
            self.apprise.add(url)
        self.enabled = len(apprise_urls) > 0

    def _format_job_compact(self, job: dict) -> str:
        """Format a single job in one compact line."""
        title = _clean_value(job.get("title")) or "Unknown Position"
        company = _clean_value(job.get("company")) or "?"
        url = _clean_value(job.get("job_url"))
        location = _clean_location(_clean_value(job.get("location")))

        salary = _format_salary(
            job.get("min_amount"),
            job.get("max_amount"),
            _clean_value(job.get("currency")) or "EUR"
        )

        # Remove location from title if duplicated
        if location and location.lower() in title.lower():
            # Try to clean title like "Designer - Madrid, Spain" -> "Designer"
            title = re.sub(rf"\s*[-–]\s*{re.escape(location)}.*$", "", title, flags=re.IGNORECASE)
            title = re.sub(rf",\s*{re.escape(location)}.*$", "", title, flags=re.IGNORECASE)

        # Build compact line: → [Title](url) @ Company · Location · €50k-70k
        parts = []

        if url:
            parts.append(f"[{title}]({url})")
        else:
            parts.append(title)

        parts.append(f"@ {company}")

        if location:
            parts.append(location)

        if salary:
            parts.append(salary)

        return "→ " + " · ".join(parts)

    def _format_message(self, jobs: list[dict]) -> str:
        """Format all jobs grouped by source."""
        if not jobs:
            return ""

        # Group by site
        by_site: dict[str, list[dict]] = {}
        for job in jobs:
            site = (_clean_value(job.get("site")) or "other").upper()
            by_site.setdefault(site, []).append(job)

        lines = []
        for site, site_jobs in by_site.items():
            lines.append(f"**{site}** ({len(site_jobs)})")
            for job in site_jobs:
                lines.append(self._format_job_compact(job))
            lines.append("")  # Blank line between sources

        return "\n".join(lines).strip()

    def _chunk_jobs(self, jobs: list[dict], max_chars: int = 1800) -> list[list[dict]]:
        """Split jobs into chunks that fit within character limit."""
        chunks = []
        current_chunk = []
        current_len = 0

        for job in jobs:
            job_text = self._format_job_compact(job)
            job_len = len(job_text) + 1  # +1 for newline

            # If single job exceeds limit, it goes alone
            if job_len > max_chars:
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = []
                    current_len = 0
                chunks.append([job])
                continue

            # If adding this job exceeds limit, start new chunk
            if current_len + job_len > max_chars:
                chunks.append(current_chunk)
                current_chunk = [job]
                current_len = job_len
            else:
                current_chunk.append(job)
                current_len += job_len

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

    def notify(self, jobs: list[dict], linkedin_429: bool = False) -> bool:
        if not jobs:
            return True

        if not self.enabled:
            logger.warning("Notifications disabled: no Apprise URLs configured")
            return False

        chunks = self._chunk_jobs(jobs)
        total_chunks = len(chunks)
        all_success = True

        for i, chunk in enumerate(chunks, 1):
            message = self._format_message(chunk)

            if total_chunks == 1:
                title = f"{len(jobs)} New Jobs"
            else:
                title = f"New Jobs ({i}/{total_chunks})"

            if linkedin_429 and i == 1:
                title += " ⚠️ LinkedIn 429"

            try:
                result = self.apprise.notify(
                    title=title,
                    body=message,
                    body_format=apprise.NotifyFormat.MARKDOWN,
                )
                if not result:
                    logger.error(f"Notification delivery failed (chunk {i}/{total_chunks})")
                    all_success = False
            except Exception as e:
                logger.error(f"Notification error (chunk {i}/{total_chunks}): {e}")
                all_success = False

        return all_success

    def test(self) -> bool:
        return self.apprise.notify(
            title="Job Scrapper Test",
            body="Notifications working!",
        )
