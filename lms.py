"""Isolated LMS boards, immutable pregame captures and read-only records.

No work runs at import time. Uses the existing Supabase ledger, with different
app keys, and never changes the host app's picks, records or database schema.
"""
import asyncio
import gzip
import hashlib
import json
import logging
import math
import os
import threading
import zlib
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import Request as UrlRequest, urlopen
from zoneinfo import ZoneInfo

from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
from lms_rules import CATALOGUE

LOG = logging.getLogger("lms")
VERSION = "lms-source-adaptations-1"
ET = ZoneInfo("America/New_York")
LOCK = threading.RLock()
SERVICES = {}


def today():
    return datetime.now(ET).date().isoformat()


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(str(value).replace("+", "").strip())
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def timestamp(value):
    try:
        d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return d.astimezone(timezone.utc) if d.tzinfo else None
    except (ValueError, TypeError):
        return None


def implied(odds):
    o = number(odds)
    if o is None or not o or o < -1000:
        return None
    return (-o / (100 - o)) if o < 0 else 100 / (100 + o)


def clean(value):
    """JSON-safe boundary, preserving valid zeroes and rejecting non-finites."""
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if hasattr(value, "item"):
        return clean(value.item())
    return str(value)


def valid_date(value):
    try:
        return date.fromisoformat(value or today()).isoformat()
    except (TypeError, ValueError):
        raise HTTPException(400, "Use a YYYY-MM-DD date.")


def safe_source(value):
    try:
        p = urlparse(str(value))
        return bool(p.scheme in ("http", "https") and p.hostname and
                    not p.username and not p.password and
                    not any(k in p.query.lower() for k in
                            ("token=", "key=", "secret=", "signature=")))
    except ValueError:
        return False


class Ledger:
    """Strict, paginated REST access. Unavailable storage is never an empty record."""
    def __init__(self, sport):
        self.app = sport + "_lms"
        self.url = (os.getenv("SUPABASE_URL") or "").rstrip("/")
        self.key = os.getenv("SUPABASE_SERVICE_KEY") or ""

    def request(self, method, params, rows=None, ignore=False):
        if not self.url or not self.key:
            raise RuntimeError("LMS durable storage is unavailable: Supabase is not configured.")
        headers = {"apikey": self.key, "Authorization": "Bearer " + self.key,
                   "Content-Type": "application/json", "Accept": "application/json"}
        if rows is not None:
            headers["Prefer"] = (
                "resolution=" + ("ignore-duplicates" if ignore else "merge-duplicates") +
                ",return=representation")
            params = {**params, "on_conflict": "app,date,category,side"}
        req = UrlRequest(self.url + "/rest/v1/mpa_track_ledger?" + urlencode(params),
                         data=json.dumps(clean(rows), allow_nan=False).encode()
                         if rows is not None else None, headers=headers, method=method)
        try:
            with urlopen(req, timeout=15) as response:
                raw = response.read()
                return json.loads(raw) if raw else []
        except Exception:
            # Never expose credentials, signed URLs or response bodies to the UI.
            raise RuntimeError("LMS durable storage request failed; no record was assumed empty.") from None

    def get(self, params=None):
        params = {"app": "eq." + self.app, "select": "*", **(params or {})}
        rows, offset = [], 0
        while True:
            page = self.request("GET", {**params, "order": "date.asc,category.asc",
                                        "offset": offset, "limit": 1000})
            if not isinstance(page, list):
                raise RuntimeError("LMS storage returned an invalid record.")
            rows.extend(page)
            if len(page) < 1000:
                return rows
            offset += 1000

    def save(self, ds, category, detail, *, ignore=False, result=None):
        row = {"app": self.app, "date": ds, "category": category, "side": "ALL",
               "detail": clean(detail), "wins": int(result == "WIN"),
               "losses": int(result == "LOSS"),
               "locked": result in ("WIN", "LOSS", "PUSH", "VOID"),
               "locked_at": datetime.now(timezone.utc).isoformat()}
        return self.request("POST", {}, [row], ignore=ignore)

    def insert_picks(self, ds, picks):
        rows = [{"app": self.app, "date": ds, "category": "play_" + p["id"],
                 "side": "ALL", "detail": clean(p), "wins": 0, "losses": 0,
                 "locked": False, "locked_at": p["captured_at"]} for p in picks]
        # No per-pick network fan-out. Ignore duplicates atomically at the DB.
        for start in range(0, len(rows), 250):
            self.request("POST", {}, rows[start:start + 250], ignore=True)

    def detail(self, ds, category):
        rows = self.get({"date": "eq." + ds, "category": "eq." + category})
        return rows[0].get("detail") if rows else None


def summary(rows):
    counts = {r: 0 for r in ("WIN", "LOSS", "PUSH", "VOID", "PENDING")}
    profit = staked = 0.0
    for row in rows:
        result = row.get("result") or "PENDING"
        counts[result if result in counts else "PENDING"] += 1
        if result in ("WIN", "LOSS", "PUSH") and number(row.get("odds")) not in (None, 0):
            staked += number(row.get("stake")) or 1.0
            profit += number(row.get("profit")) or 0.0
    n = counts["WIN"] + counts["LOSS"]
    return {"wins": counts["WIN"], "losses": counts["LOSS"], "push": counts["PUSH"],
            "void": counts["VOID"], "pending": counts["PENDING"],
             "hit_rate": round(counts["WIN"] / n, 6) if n else None,
            "profit": round(profit, 3), "priced_staked": round(staked, 3),
             "roi": round(profit / staked, 6) if staked else None}


class LMS:
    def __init__(self, sport, namespace):
        self.sport, self.ns = sport, namespace
        self.store = Ledger(sport)
        self.contexts = {}

    def authorised(self, request, *, admin=False):
        tok = (request.query_params.get("token") or
               request.headers.get("Authorization", "").removeprefix("Bearer ").strip())
        adm = request.query_params.get("admin", "")
        check = self.ns.get("_" + self.sport + "_bet_admin_ok") or self.ns.get("_bet_admin_ok")
        is_admin = bool(check and check(tok, adm))
        if not is_admin:
            admin_token = self.ns.get("_is_admin_token")
            is_admin = bool(admin_token and admin_token(tok)) or bool(
                adm and os.getenv("INTERNAL_API_TOKEN") and adm == os.getenv("INTERNAL_API_TOKEN"))
        if admin:
            return is_admin
        if is_admin:
            return True
        if self.sport == "nba":
            get_user = self.ns.get("get_user")
            return bool(get_user and get_user(request))
        verify = self.ns.get("_verify_hub_token")
        return bool(verify and verify(tok))

    def empty(self, ds):
        return {"sport": self.sport.upper(), "date": ds, "picks": [],
                "captured_at": None, "version": VERSION,
                "methods": [{**r, "status": "NOT_RUN", "message":
                             "Run the normal sport pipeline to load a server-generated LMS candidate pool.",
                             "examined": 0, "qualifying": 0, "pending": []}
                            for r in CATALOGUE[self.sport]]}

    def board(self, ds):
        saved = self.store.detail(ds, "__board__")
        board = (saved or {}).get("board") if isinstance(saved, dict) else None
        return board or self.empty(ds)

    def evaluate(self, ds, context, inputs):
        board = self.empty(ds)
        board["captured_at"] = datetime.now(timezone.utc).isoformat()
        board["context_note"] = context.get("note", "")
        now = datetime.now(timezone.utc)
        all_picks = {}
        for report in board["methods"]:
            report["pending"], failures, unavailable = [], [], 0
            if report["kind"] in ("RESEARCH", "LIVE"):
                report.update(status="CONTEXT_ONLY", message=
                    "Documented coverage only: not an automated 80%+ pregame recipe."
                    if report["kind"] == "RESEARCH" else
                    "Live methods require actual entry/hedge prices and live timestamps; not emitted as pregame picks.")
                continue
            markets = report["market"]
            markets = (markets,) if isinstance(markets, str) else tuple(markets)
            pool = [c for c in context.get("candidates", [])
                    if c.get("market") in markets and c.get("side") == report["side"]]
            report["examined"] = len(pool)
            for candidate in pool:
                c = dict(candidate)
                start = timestamp(c.get("game_start"))
                if not start or now >= start:
                    unavailable += 1
                    continue
                facts = {**c.get("facts", {}), **(inputs.get(c["entity_id"], {}).get("values") or {})}
                ln = number(c.get("line"))
                odds = number(c.get("odds"))
                imp = implied(odds)
                if ln is None or imp is None or not c.get("book"):
                    unavailable += 1
                    continue
                if report["exact_line"] is not None and ln != report["exact_line"]:
                    continue
                probability = number(c.get("probability"))
                n = number(c.get("sample_size"))
                # Historical rates must belong to this exact quoted line/side.
                if probability is None or not 0 <= probability <= 1 or not n:
                    unavailable += 1
                    continue
                checks, missing = [], []
                for rule in report["rules"]:
                    value = facts.get(rule["field"])
                    if value is None:
                        missing.append(rule["field"])
                        continue
                    value = (number(value) if rule["type"] == "number" else value)
                    if value is None:
                        missing.append(rule["field"])
                        continue
                    expected = rule["value"]
                    op = rule["op"]
                    passed = ((value == expected) if op == "==" else
                              (value <= expected) if op == "<=" else value >= expected)
                    checks.append({"label": rule["label"], "value": value, "passed": passed})
                if missing:
                    report["pending"].append({"entity_id": c["entity_id"],
                                              "name": c["name"], "missing": missing})
                    continue
                if not all(check["passed"] for check in checks):
                    failures.append(c["entity_id"])
                    continue
                edge = probability - imp
                if report["edge_min"] is not None:
                    edge_failed = (edge <= 0 if report["edge_min"] == 0
                                   else edge < report["edge_min"])
                    if edge_failed:
                        continue
                if report["rate_min"] is not None and probability < report["rate_min"]:
                    continue
                game_date = start.astimezone(ET).date().isoformat()
                event_identity = c.get("event_id") or c.get("quote_event_id") or c["game_start"]
                actor = c.get("player_id") or c["name"].casefold()
                key = "|".join(map(str, (game_date, report["id"], actor, c.get("team"),
                                         event_identity, c["market"], c["side"], ln)))
                identity = hashlib.sha256(key.encode()).hexdigest()[:32]
                pick = {**c, "id": identity, "date": game_date, "method_id": report["id"],
                        "method": report["name"], "line": ln, "odds": odds,
                        "implied_probability": imp, "edge": edge, "checks": checks,
                        "source_url": report["source_url"], "author_claim": report["claim"],
                        "adaptations": report["adaptations"], "version": VERSION,
                        "verified_inputs": inputs.get(c["entity_id"]),
                        "play_kind": report["kind"], "stake": 1.0,
                        "captured_at": board["captured_at"], "result": "PENDING",
                        "tracking_status": "NOT_SAVED"}
                old = all_picks.get(identity)
                if old is None or imp < old["implied_probability"]:
                    all_picks[identity] = pick
            report["qualifying"] = sum(p["method_id"] == report["id"] for p in all_picks.values())
            report["pending"] = list({p["entity_id"] + ":" + ",".join(p["missing"]): p
                                      for p in report["pending"]}.values())
            report.update(status="QUALIFYING" if report["qualifying"] else
                          "INPUTS_REQUIRED" if report["pending"] else "NO_QUALIFIERS",
                          message=f"{report['examined']} quotes examined; {report['qualifying']} qualifying. "
                          f"{len(report['pending'])} entities need verified inputs. "
                          f"{unavailable} quotes unavailable, started, unpriced or without exact-line history.")
            if not pool:
                report["message"] = "No matching genuine quote/history in the loaded candidate pool; no substitute line was created."
        board["picks"] = sorted(all_picks.values(), key=lambda p: (-p["probability"], -p["edge"], p["name"]))
        return board

    def capture(self, ds, context, *, settle=False):
        ds = valid_date(ds)
        if ds < today() or not context.get("official", True):
            return {**self.empty(ds), "storage_warning":
                    "Historical, preseason or unverified official-slate preview excluded from forward LMS records."}
        with LOCK:
            self.contexts[ds] = clean(context)
            while len(self.contexts) > 7:
                del self.contexts[min(self.contexts)]
            days = {ds}
            for c in context.get("candidates", []):
                start = timestamp(c.get("game_start"))
                if start:
                    days.add(start.astimezone(ET).date().isoformat())
            inputs = {}
            for saved in self.store.get({"date": "in.(" + ",".join(sorted(days)) + ")",
                                         "category": "eq.__inputs__"}):
                inputs.update(saved.get("detail") or {})
            board = self.evaluate(ds, context, inputs)
            eligible = [p for p in board["picks"] if
                        datetime.now(timezone.utc) < timestamp(p["game_start"])]
            for game_date in sorted({p["date"] for p in eligible}):
                self.store.insert_picks(game_date, [p for p in eligible if p["date"] == game_date])
            frozen_rows = self.store.get({"date": "in.(" + ",".join(sorted(days)) + ")",
                                         "category": "like.play_%"})
            frozen_map = {row["category"]: row.get("detail") for row in frozen_rows}
            for pick in board["picks"]:
                frozen = frozen_map.get("play_" + pick["id"])
                if not frozen:
                    if pick not in eligible:
                        pick["tracking_status"] = "MISSED_PREGAME_WINDOW"
                        continue
                    raise RuntimeError("LMS capture could not be confirmed in durable storage.")
                pick.update(frozen)
                pick["tracking_status"] = "FROZEN"
            self.store.save(ds, "__board__", {"board": board, "context": clean(context)})
        if settle:
            self.settle()
        return board

    def attach(self, result, local_context=None):
        """Called only by an existing, authorised host pipeline/cache path."""
        if not isinstance(result, dict):
            return
        from lms_data import prepare
        ds = str(result.get("date") or (local_context or {}).get("target_date") or today())[:10]
        try:
            context = prepare(self.sport, result, self.ns, local_context or {})
            result["lms"] = self.capture(ds, context)
        except Exception as exc:
            LOG.warning("LMS %s capture unavailable: %s", self.sport, type(exc).__name__)
            # LMS failure is explicit but never destroys the host's normal board.
            result["lms"] = {**self.empty(ds), "storage_warning": str(exc)}

    def rebuild(self, ds):
        with LOCK:
            saved = self.store.detail(ds, "__board__") or {}
            context = self.contexts.get(ds) or saved.get("context")
        if not context:
            raise HTTPException(409, "Run the normal sport pipeline once to load LMS history and genuine quotes.")
        from lms_quotes import augment
        context = {**context, "candidates": augment(
            self.sport, ds, context.get("candidates") or [], context.get("profiles") or [])}
        return self.capture(ds, context)

    def verify_inputs(self, body):
        ds = valid_date(body.get("date"))
        if ds < today():
            raise HTTPException(409, "Past-date inputs cannot create or rewrite forward picks.")
        entity = str(body.get("entity_id") or "")
        if len(entity) > 180 or not entity:
            raise HTTPException(400, "Select a candidate entity.")
        if not safe_source(body.get("source_url")):
            raise HTTPException(400, "Provide a public source URL without credentials or token parameters.")
        schema = {r["field"]: r for method in CATALOGUE[self.sport] for r in method["rules"]}
        values = body.get("values")
        if not isinstance(values, dict) or not values or any(k not in schema for k in values):
            raise HTTPException(400, "Only the displayed recipe inputs may be verified.")
        for key, value in values.items():
            kind = schema[key]["type"]
            if kind == "boolean" and not isinstance(value, bool):
                raise HTTPException(400, key + " must be a boolean.")
            if kind == "number" and number(value) is None:
                raise HTTPException(400, key + " must be a finite number.")
            if kind == "text" and (not isinstance(value, str) or len(value) > 100):
                raise HTTPException(400, key + " must be short text.")
            if key.endswith("_rank") and number(value) < 1:
                raise HTTPException(400, "Rank must be a positive number.")
        saved = self.store.detail(ds, "__board__") or {}
        context = self.contexts.get(ds) or saved.get("context") or {}
        matches = [c for c in context.get("candidates", []) if c.get("entity_id") == entity]
        if not matches or not any(timestamp(c.get("game_start")) and
                                  datetime.now(timezone.utc) < timestamp(c["game_start"]) for c in matches):
            raise HTTPException(409, "Inputs can only be verified for a loaded, unstarted candidate.")
        with LOCK:
            input_date = timestamp(matches[0]["game_start"]).astimezone(ET).date().isoformat()
            current = self.store.detail(input_date, "__inputs__") or {}
            old = current.get(entity) or {}
            current[entity] = {"values": {**old.get("values", {}), **clean(values)},
                               "source_url": body["source_url"],
                               "verified_at": datetime.now(timezone.utc).isoformat(),
                               "verification": "Owner-entered source verification; not independently audited"}
            self.store.save(input_date, "__inputs__", current)
        return self.rebuild(ds)

    def record(self, ds=None):
        params = {"category": "like.play_%"}
        if ds:
            params["date"] = "eq." + ds
        rows = [r["detail"] for r in self.store.get(params) if isinstance(r.get("detail"), dict)]
        groups = {}
        for row in rows:
            groups.setdefault(row["method_id"], []).append(row)
        return {"sport": self.sport.upper(), "rows": rows, "summary": summary(rows),
                "by_method": [{"method_id": key, "method": group[0]["method"],
                               "play_kind": group[0].get("play_kind"), **summary(group)}
                              for key, group in sorted(groups.items())],
                "stake": 1, "stake_unit": "unit", "isolated": True}

    def settle(self, ds=None):
        from lms_settlement import settle_rows
        params = {"category": "like.play_%", "locked": "eq.false"}
        if ds:
            params["date"] = "eq." + ds
        stored = self.store.get(params)
        rows = [r["detail"] for r in stored if isinstance(r.get("detail"), dict)]
        for row in settle_rows(self.sport, rows):
            self.store.save(row["date"], "play_" + row["id"], row, result=row.get("result"))
        return self.record(ds)

    def install(self, app):
        base = (os.getenv("BASE_PATH") or "").rstrip("/")

        @app.get("/lms", response_class=HTMLResponse)
        async def lms_page(request: Request):
            template = Path(__file__).with_name("lms.html").read_text(encoding="utf-8")
            return HTMLResponse(template.replace("__LMS_SPORT__", self.sport.upper())
                                .replace("__LMS_BASE__", json.dumps(base))
                                .replace("__LMS_ADMIN__", json.dumps(self.authorised(request, admin=True))))

        def allowed(request, admin=False):
            if not self.authorised(request, admin=admin):
                raise HTTPException(403 if admin else 401, "Admin required." if admin else
                                    "Sign in through Money Picks Arena to view LMS.")

        async def action(fn, *args):
            try:
                return await asyncio.to_thread(fn, *args)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(503, str(exc)) from None

        @app.get("/api/lms/board")
        async def board(request: Request, date: str = ""):
            allowed(request)
            return await action(self.board, valid_date(date))

        @app.get("/api/lms/record")
        async def record(request: Request, date: str = ""):
            allowed(request)
            return await action(self.record, valid_date(date) if date else None)

        @app.post("/api/lms/rebuild")
        async def rebuild(request: Request, date: str = ""):
            allowed(request, True)
            return await action(self.rebuild, valid_date(date))

        @app.post("/api/lms/inputs")
        async def inputs(request: Request):
            allowed(request, True)
            return await action(self.verify_inputs, await request.json())

        @app.post("/api/lms/settle")
        async def settle(request: Request):
            allowed(request, True)
            try:
                body = await request.json()
            except Exception:
                body = {}
            ds = valid_date(body["date"]) if body.get("date") else None
            return await action(self.settle, ds)

        # Add only a navigation entry, never redesign/remove existing sections.
        link = ('<script>(function(){function add(){if(document.getElementById("lms-entry"))return;'
                'var a=document.createElement("a");a.id="lms-entry";a.textContent="LMS";'
                'a.style.cssText="position:fixed;right:18px;bottom:22px;z-index:9999;'
                'padding:10px 18px;background:#173b64;color:#fff;border:1px solid #60a5fa;'
                'border-radius:8px;font:bold 14px sans-serif;text-decoration:none";'
                'function href(){var root=document.baseURI;'
                'try{var es=performance.getEntriesByType("resource");'
                'for(var i=es.length-1;i>=0;i--){var p=new URL(es[i].name);'
                'if(/\\/api\\/(results?|progress|cache)(\\/|$)/.test(p.pathname)){'
                'root=p.origin+p.pathname.slice(0,p.pathname.indexOf("/api/"))+"/";break;}}}catch(e){}'
                'var u=new URL(' + json.dumps(base + "/lms") + ',root);'
                'var q=new URLSearchParams(location.search);'
                '["admin","token"].forEach(function(k){if(q.get(k))u.searchParams.set(k,q.get(k));});'
                'if(!u.searchParams.get("token")){try{var t=localStorage.getItem("__mpa_token");'
                'if(t)u.searchParams.set("token",t);}catch(e){}}return u;}'
                'a.href=href().toString();a.addEventListener("click",function(){'
                'var u=href();a.href=u.toString();if(u.origin!==location.origin){a.target="_blank";a.rel="noopener";}});'
                'document.body.appendChild(a);}'
                'if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",add);else add();'
                '})();</script>')
        for key in ("HTML", "_HTML"):
            if isinstance(self.ns.get(key), str):
                body = self.ns[key]
                index = body.rfind("</body>")
                if index >= 0:
                    self.ns[key] = body[:index] + link + body[index:]

        # NBA also serves an inline/dynamic template. Keep the same additive
        # entry there; this middleware never touches API responses or other pages.
        @app.middleware("http")
        async def lms_navigation(request, call_next):
            response = await call_next(request)
            if (request.method != "GET" or request.url.path not in ("/", base + "/") or
                    response.status_code != 200 or
                    "text/html" not in response.headers.get("content-type", "") or
                    response.headers.get("content-encoding", "").lower() not in (
                        "", "identity", "gzip", "deflate")):
                return response
            raw = b"".join([chunk async for chunk in response.body_iterator])
            encoding = response.headers.get("content-encoding", "").lower()
            if encoding == "gzip":
                raw = gzip.decompress(raw)
            elif encoding == "deflate":
                raw = zlib.decompress(raw)
            body = raw.decode("utf-8")
            index = body.rfind("</body>")
            if index >= 0 and "lms-entry" not in body:
                body = body[:index] + link + body[index:]
            rebuilt = HTMLResponse(body, status_code=response.status_code,
                                   background=response.background)
            # Preserve duplicate Set-Cookie/security/CORS headers. The body
            # is now plain UTF-8; old compression, length and ETag cannot apply.
            rebuilt.raw_headers = [
                (k, v) for k, v in response.raw_headers
                if k.lower() not in (b"content-length", b"content-encoding", b"etag")]
            rebuilt.raw_headers.append((b"content-length", str(len(body.encode("utf-8"))).encode()))
            return rebuilt


def install(sport, app, namespace):
    service = LMS(sport, namespace)
    service.install(app)
    SERVICES[sport] = service
    return service


def attach_registered(sport, result):
    service = SERVICES.get(sport)
    if service:
        service.attach(result)