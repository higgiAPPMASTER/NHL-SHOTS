"""User-triggered NHL runs with short HTTP requests and bounded status memory.

Importing this module starts nothing. Jobs start only when the authenticated
host route calls start(). There is no scheduler and no automatic restart.
"""
import asyncio
import logging
import secrets
import time
import traceback
from datetime import datetime, timezone


LOG = logging.getLogger("nhl_money_shots")


class NHLRunJobs:
    def __init__(self, run, progress):
        self.run = run
        self.progress = progress
        self.jobs = {}
        self.tasks = {}

    def active(self, ds):
        return next((j for j in self.jobs.values()
                     if j["date"] == ds and j["status"] == "RUNNING"), None)

    def start(self, ds, kind):
        job_id = secrets.token_hex(16)
        self.jobs[job_id] = {
            "job_id": job_id, "date": ds, "kind": kind, "status": "RUNNING",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "started_monotonic": time.monotonic(),
        }
        try:
            task = asyncio.create_task(self._execute(job_id))
        except Exception:
            del self.jobs[job_id]
            raise
        self.tasks[job_id] = task
        task.add_done_callback(lambda done: self.tasks.pop(job_id, None))
        finished = [key for key, row in self.jobs.items()
                    if row["status"] != "RUNNING"]
        for key in finished[:-7]:
            del self.jobs[key]
        return self.status(job_id)

    async def _execute(self, job_id):
        job = self.jobs[job_id]
        LOG.info("NHL %s job started for %s", job["kind"], job["date"])
        try:
            result = await self.run(job["date"], job["kind"])
            job.update(status="COMPLETED", result=result)
            LOG.info("NHL %s job completed for %s", job["kind"], job["date"])
        except asyncio.CancelledError:
            job.update(status="INTERRUPTED",
                       error="The server stopped before this run finished. Retry after it is online.")
            LOG.warning("NHL %s job interrupted for %s", job["kind"], job["date"])
            raise
        except Exception as exc:
            job.update(status="FAILED",
                       error=f"NHL run failed ({type(exc).__name__}). See the server logs.")
            # Print stack locations, not exception text that could contain
            # provider credentials or a private request URL.
            LOG.error("NHL %s job failed for %s (%s)\n%s", job["kind"],
                      job["date"], type(exc).__name__,
                      "".join(traceback.format_tb(exc.__traceback__)))
        finally:
            job["finished_at"] = datetime.now(timezone.utc).isoformat()

    def status(self, job_id):
        job = self.jobs.get(job_id)
        if job is None:
            return None
        return {
            **{key: value for key, value in job.items()
               if key != "started_monotonic"},
            "elapsed_seconds": int(time.monotonic() - job["started_monotonic"]),
            "progress": dict(self.progress()) if job["status"] == "RUNNING" else {},
        }