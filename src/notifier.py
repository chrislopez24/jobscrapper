import logging
import apprise

logger = logging.getLogger(__name__)


class JobNotifier:
    def __init__(self, apprise_urls: list[str] | None = None):
        self.apprise = apprise.Apprise()
        apprise_urls = apprise_urls or []
        for url in apprise_urls:
            self.apprise.add(url)
        self.enabled = len(apprise_urls) > 0

    def _format_job(self, job: dict) -> str:
        title = job.get("title", "Unknown")
        company = job.get("company", "Unknown")
        location = job.get("location", "")
        url = job.get("job_url", "")
        site = job.get("site", "")

        salary_min = job.get("min_amount")
        salary_max = job.get("max_amount")
        currency = job.get("currency", "EUR")

        salary = ""
        if salary_min and salary_max:
            salary = f"\n   Salary: {salary_min:,.0f} - {salary_max:,.0f} {currency}"
        elif salary_min:
            salary = f"\n   Salary: {salary_min:,.0f}+ {currency}"

        return f"**{title}**\n   {company} | {location}{salary}\n   [{site}]({url})"

    def _format_message(self, jobs: list[dict]) -> str:
        if not jobs:
            return ""

        lines = [f"**{len(jobs)} new job(s) found:**\n"]
        for job in jobs[:10]:  # Limit to 10 per notification
            lines.append(self._format_job(job))
            lines.append("")

        if len(jobs) > 10:
            lines.append(f"_...and {len(jobs) - 10} more_")

        return "\n".join(lines)

    def notify(self, jobs: list[dict]) -> bool:
        if not jobs:
            logger.info("No jobs to notify")
            return True

        if not self.enabled:
            logger.warning("No Apprise URLs configured, skipping notification")
            return False

        message = self._format_message(jobs)
        title = f"Job Alert: {len(jobs)} new position(s)"

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
