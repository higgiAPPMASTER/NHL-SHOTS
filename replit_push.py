# ===================================================================
# replit_push.py — Drop this into the SAME folder as main.py on Render
# ===================================================================
# What it does:
#   After your daily analysis finishes, this pushes the results to
#   your Replit Postgres database so the hub can serve them instantly
#   to 10,000+ users without waking up this Render app.
#
# Setup on Render (each sport app: MLB / NHL / NBA / NFL):
#   1. Add this file (replit_push.py) to the repo (same folder as main.py)
#   2. In Render dashboard → Environment, add TWO env vars:
#        REPLIT_API_URL    = https://YOUR-REPLIT-APP.replit.app
#        INTERNAL_API_TOKEN = (the same long token you saved in Replit)
#   3. In main.py, add the 2-line patch shown at the bottom of this file
# ===================================================================

import os
import logging

# httpx is already in your requirements.txt — no new dependency needed
import httpx

log = logging.getLogger(__name__)


def push_picks_to_replit(sport: str, result: dict, html: str = "") -> None:
    """
    Push the result of run_analysis() to the Replit picks cache.
    Silently logs errors — never crashes the Render app if Replit is down.

    sport:  "mlb" | "nhl" | "nba" | "nfl"
    result: the dict returned by run_analysis() — must have a 'date' key
    html:   (optional) the fully-rendered HTML page for this sport. When
            provided, the Replit hub serves it directly at
            moneypicksarena.com/dashboard/<sport> — users get instant
            picks without ever waking up this Render app.
    """
    base_url = os.environ.get("REPLIT_API_URL", "").rstrip("/")
    token = os.environ.get("INTERNAL_API_TOKEN", "")

    if not base_url or not token:
        log.warning("[replit_push] REPLIT_API_URL or INTERNAL_API_TOKEN not set — skipping")
        return

    date_str = result.get("date") or ""
    if not date_str:
        log.warning("[replit_push] result missing 'date' — skipping push")
        return

    body = {
        "date": date_str,
        "payload": result,  # the entire run_analysis dict
        "pickCount": len(result.get("picks") or []),
        "propCount": len(result.get("all_picks") or []),
    }
    # Only include html when actually provided — otherwise the API leaves the
    # previously-stored snapshot intact (instead of blanking it).
    if html:
        body["html"] = html

    url = f"{base_url}/api/picks/{sport}"
    try:
        with httpx.Client(timeout=10.0) as client:
            r = client.post(
                url,
                json=body,
                headers={"x-internal-token": token, "Content-Type": "application/json"},
            )
            if r.status_code == 200:
                log.info(f"[replit_push] {sport.upper()} pushed ({body['pickCount']} picks)")
            else:
                log.error(f"[replit_push] {sport.upper()} push failed {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"[replit_push] {sport.upper()} push exception: {e}")


# ===================================================================
# PATCH FOR main.py — change SPORT to "mlb" / "nhl" / "nba" / "nfl"
# ===================================================================
#
# At the TOP of main.py, near the other imports:
#
#     from replit_push import push_picks_to_replit
#
# In the /run endpoint, right BEFORE `return result`:
#
#     push_picks_to_replit("nba", result)   # <-- change "nba" per app
#     return result
#
# Also add the same call inside your daily /cron endpoint (the 1 AM
# fetch) right after `result = await run_analysis(...)`, so the cache
# fills up overnight even when no user visits.
# ===================================================================
