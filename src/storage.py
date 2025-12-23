import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime


class JobStorage:
    def __init__(self, db_path: str = "data/jobs.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS seen_jobs (
                    job_id TEXT PRIMARY KEY,
                    title TEXT,
                    company TEXT,
                    url TEXT,
                    site TEXT,
                    seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_seen_at ON seen_jobs(seen_at)
            """)

    @staticmethod
    def generate_job_id(url: str, company: str, title: str) -> str:
        raw = f"{url}|{company}|{title}".lower().strip()
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def is_seen(self, job_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "SELECT 1 FROM seen_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            return result is not None

    def mark_seen(self, job_id: str, title: str, company: str, url: str, site: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO seen_jobs (job_id, title, company, url, site)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, title, company, url, site),
            )

    def filter_new_jobs(self, jobs: list[dict]) -> list[dict]:
        new_jobs = []
        seen_in_batch = set()  # Dedupe within current batch

        for job in jobs:
            job_id = self.generate_job_id(
                job.get("job_url", ""),
                job.get("company", ""),
                job.get("title", ""),
            )
            # Skip if already in this batch or in database
            if job_id in seen_in_batch or self.is_seen(job_id):
                continue

            seen_in_batch.add(job_id)
            job["_job_id"] = job_id
            new_jobs.append(job)

        return new_jobs

    def mark_jobs_seen(self, jobs: list[dict]):
        for job in jobs:
            job_id = job.get("_job_id") or self.generate_job_id(
                job.get("job_url", ""),
                job.get("company", ""),
                job.get("title", ""),
            )
            self.mark_seen(
                job_id,
                job.get("title", ""),
                job.get("company", ""),
                job.get("job_url", ""),
                job.get("site", ""),
            )

    def cleanup_old(self, days: int = 30):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM seen_jobs WHERE seen_at < datetime('now', ?)",
                (f"-{days} days",),
            )
