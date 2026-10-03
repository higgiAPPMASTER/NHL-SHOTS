"""User-triggered NHL runs with short HTTP requests and bounded status memory.

Importing this module starts nothing. Jobs start only when the authenticated
host route calls start(). There is no scheduler and no automatic restart.
"""
import asyncio
import logging
import secrets
import threading
import time
import traceback
from datetime import datetime, timezone


LOG = logging.getLogger("nhl_money_shots")


class NHLRunJobs:
    def __init__(self, run, progress):
        self.run = run
        self.progress = progress
        self.jobs = {}
        self.workers = {}
        self.lock = threading.RLock()

    def active(self, ds):
        with self.lock:
            return next((dict(j) for j in self.jobs.values()
                         if j["date"] == ds and j["status"] == "RUNNING"), None)

    def start(self, ds, kind):
        job_id = secrets.token_hex(16)
        with self.lock:
            self.jobs[job_id] = {
                "job_id": job_id, "date": ds, "kind": kind, "status": "RUNNING",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "started_monotonic": time.monotonic(),
            }
            # A task on the ASGI event loop is not isolation: synchronous
            # history processing and persistence would still block HTTP.
            worker = threading.Thread(
                target=self._worker, args=(job_id,),
                name=f"nhl-{kind}-{ds}", daemon=True)
            self.workers[job_id] = worker
            try:
                worker.start()
            except Exception:
                del self.workers[job_id]
                del self.jobs[job_id]
                raise
            finished = [key for key, row in self.jobs.items()
                        if row["status"] != "RUNNING"]
            for key in finished[:-7]:
                del self.jobs[key]
        return self.status(job_id)

    def _worker(self, job_id):
        try:
            # Each run owns its loop and HTTP clients; no application-loop
            # futures or clients are passed into this worker.
            asyncio.run(self._execute(job_id))
        finally:
            with self.lock:
                self.workers.pop(job_id, None)

    def _update(self, job_id, **values):
        with self.lock:
            self.jobs[job_id].update(values)

    async def _execute(self, job_id):
        with self.lock:
            job = dict(self.jobs[job_id])
        LOG.info("NHL %s job started for %s", job["kind"], job["date"])
        try:
            result = await self.run(job["date"], job["kind"])
            self._update(job_id, status="COMPLETED", result=result)
            LOG.info("NHL %s job completed for %s", job["kind"], job["date"])
        except asyncio.CancelledError:
            self._update(job_id, status="INTERRUPTED",
                         error="The server stopped before this run finished. Retry after it is online.")
            LOG.warning("NHL %s job interrupted for %s", job["kind"], job["date"])
            raise
        except Exception as exc:
            self._update(job_id, status="FAILED",
                         error=f"NHL run failed ({type(exc).__name__}). See the server logs.")
            # Print stack locations, not exception text that could contain
            # provider credentials or a private request URL.
            LOG.error("NHL %s job failed for %s (%s)\n%s", job["kind"],
                      job["date"], type(exc).__name__,
                      "".join(traceback.format_tb(exc.__traceback__)))
        finally:
            self._update(job_id, finished_at=datetime.now(timezone.utc).isoformat())

    def status(self, job_id):
        with self.lock:
            saved = self.jobs.get(job_id)
            if saved is None:
                return None
            job = dict(saved)
        return {
            **{key: value for key, value in job.items()
               if key != "started_monotonic"},
            "elapsed_seconds": int(time.monotonic() - job["started_monotonic"]),
            "progress": dict(self.progress()) if job["status"] == "RUNNING" else {},
        }
