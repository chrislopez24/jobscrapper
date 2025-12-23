import logging
import math
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


class JobNotifier:
    def __init__(self, apprise_urls: list[str] | None = None):
        self.apprise = apprise.Apprise()
        apprise_urls = apprise_urls or []
        for url in apprise_urls:
            self.apprise.add(url)
        self.enabled = len(apprise_urls) > 0

    def _format_job(self, job: dict) -> str:
        title = _clean_value(job.get("title")) or "Unknown Position"
        company = _clean_value(job.get("company")) or "Unknown Company"
        location = _clean_value(job.get("location"))
        url = _clean_value(job.get("job_url")) or ""
        site = _clean_value(job.get("site")) or ""

        salary_min = job.get("min_amount")
        salary_max = job.get("max_amount")
        currency = _clean_value(job.get("currency")) or "EUR"

        # Build job line
        parts = []

        # Title with link
        if url:
            parts.append(f"**[{title}]({url})**")
        else:
            parts.append(f"**{title}**")

        # Company and location
        info = [company]
        if location:
            info.append(location)
        parts.append(" | ".join(info))

        # Salary if available
        if salary_min and not (isinstance(salary_min, float) and math.isnan(salary_min)):
            if salary_max and not (isinstance(salary_max, float) and math.isnan(salary_max)):
                parts.append(f"{salary_min:,.0f} - {salary_max:,.0f} {currency}")
            else:
                parts.append(f"{salary_min:,.0f}+ {currency}")

        return "\n".join(parts)

    def _format_message(self, jobs: list[dict]) -> str:
        if not jobs:
            return ""

        # Group by site
        by_site: dict[str, list[dict]] = {}
        for job in jobs:
            site = _clean_value(job.get("site")) or "other"
            by_site.setdefault(site, []).append(job)

        lines = []

        for site, site_jobs in by_site.items():
            lines.append(f"**{site.upper()}** ({len(site_jobs)})")
            lines.append("─" * 20)

            for job in site_jobs[:5]:  # Max 5 per site
                lines.append(self._format_job(job))
                lines.append("")

            if len(site_jobs) > 5:
                lines.append(f"_+{len(site_jobs) - 5} more from {site}_")
                lines.append("")

        return "\n".join(lines)

    def notify(self, jobs: list[dict]) -> bool:
        if not jobs:
            logger.info("No jobs to notify")
            return True

        if not self.enabled:
            logger.warning("No Apprise URLs configured, skipping notification")
            return False

        message = self._format_message(jobs)
        title = f"New Jobs Found: {len(jobs)}"

        try:
            result = self.apprise.notify(
                title=title,
                body=message,
                body_format=apprise.NotifyFormat.MARKDOWN,
            )
            if result:
                logger.info(f"Notification sent for {len(jobs)} jobs")
            else:
                logger.error("Failed to send notification")
            return result
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
            return False

    def test(self) -> bool:
        return self.apprise.notify(
            title="Job Scrapper Test",
            body="Notifications are working correctly!",
        )
