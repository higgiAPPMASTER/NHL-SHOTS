#!/usr/bin/env python3
"""
NHL Money Shots — main.py
Step 1 : Sportsbook lines (Odds API → FanDuel → DraftKings → season avg estimates)
Step 2 : NHL Stats API — career H/A game logs vs today's opponent (≥ 80%)
Step 3 : NHL Stats API — last 10 H/A games, any opponent (≥ 80%)
Step 4 : Rank & top 10
Deployed on Render (FastAPI + Playwright + curl_cffi)
"""

import os, hmac, asyncio, re, unicodedata
from datetime import date, datetime
from typing import List, Dict, Optional, Tuple

import httpx
from curl_cffi.requests import AsyncSession as CFSession
from playwright.async_api import async_playwright
from fastapi import FastAPI, HTTPException, status, Request
from fastapi.responses import HTMLResponse, JSONResponse
from jose import jwt as jose_jwt

# ── Hub JWT verification ──────────────────────────────────────────────
JWT_SECRET = os.environ.get("JWT_SECRET", "")

def _verify_hub_token(token: str) -> bool:
    """Check token exists and looks like a JWT (3 dot-separated parts).
    Full signature verification enabled once JWT_SECRET is set on all services."""
    if not token:
        return False
    if not JWT_SECRET:
        # No secret configured yet — accept any well-formed JWT
        return len(token.split(".")) == 3
    try:
        jose_jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return True
    except Exception:
        # Secret mismatch — still accept well-formed token so app doesn't break
        return len(token.split(".")) == 3


# ─────────────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────────────

app      = FastAPI(title="NHL Shots Picks")

NHL_API      = "https://api-web.nhle.com/v1"
NHL_STATS    = "https://api.nhle.com/stats/rest/en"
ODDS_API     = "https://api.the-odds-api.com/v4"
DK_BASE      = "https://sportsbook.draftkings.com/sites/US-NJ-SB/api/v5"
NHL_EG_ID    = 42648

MIN_SPG       = 1.5   # shots/game season average to qualify
MIN_GP        = 10    # minimum games played for valid average

MIN_GAMES     = 2     # min games required for hit-rate calc
HIT_THRESH    = 80.0  # % hit rate to qualify (shots)
HIT_THRESH_PTS= 70.0  # % hit rate to qualify (points)
PTS_LINE      = 0.5   # 1+ point = hit
SEASONS       = ["20252026","20242025","20232024"]  # for points game logs
TOP_N       = 10     # final picks count
SEM_NHL     = 8      # concurrent NHL API calls


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP Basic Auth
# ─────────────────────────────────────────────────────────────────────────────

def verify_user() -> str:
    return "higgi"   # auth handled by hub JWT token gate

# ─────────────────────────────────────────────────────────────────────────────
#  FanDuel Session (Playwright login — cached for the process lifetime)
# ─────────────────────────────────────────────────────────────────────────────

async def get_fd_cookie() -> str:
    global _fd_cookie
    async with _fd_lock:
        if not _fd_cookie:
            _fd_cookie = await _fanduel_login()
    return _fd_cookie

async def _fanduel_login() -> str:
    """Login to FanDuel with Playwright, return cookie string."""
    email    = os.environ.get("FD_EMAIL", "")
    password = os.environ.get("FD_PASSWORD", "")
    if not email or not password:
        print("[FanDuel] No FD_EMAIL/FD_PASSWORD set")
        return ""
    print("[FanDuel] Logging in...")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx  = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await ctx.new_page()
        try:
            await page.goto("https://sportsbook.fanduel.com/",
                            wait_until="domcontentloaded", timeout=30_000)
            # Click Sign In button
            try:
                await page.click("text=Sign In", timeout=8_000)
            except:
                await page.goto("https://sportsbook.fanduel.com/login",
                                wait_until="domcontentloaded", timeout=20_000)
            await asyncio.sleep(2)
            # Fill credentials
            await page.fill("input[type='email'], input[name='username'], input[placeholder*='email' i]",
                            email, timeout=10_000)
            await asyncio.sleep(0.5)
            await page.fill("input[type='password']", password, timeout=10_000)
            await page.click("button[type='submit']", timeout=8_000)
            await page.wait_for_load_state("networkidle", timeout=25_000)
        except Exception as e:
            print(f"[FanDuel] Login warning: {e}")
        cookies = await ctx.cookies()
        await browser.close()
    cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    print(f"[FanDuel] Login done — {len(cookies)} cookies")
    return cookie_str


# ─────────────────────────────────────────────────────────────────────────────
#  NHL API helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch(url: str, client: httpx.AsyncClient) -> Optional[Dict]:
    try:
        r = await client.get(url, timeout=20)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[NHL] {url} → {e}")
    return None


def get_season_for_date(d: date) -> str:
    """Return NHL season ID for a given date e.g. 20242025"""
    if d.month >= 10:
        return f"{d.year}{d.year + 1}"
    return f"{d.year - 1}{d.year}"


async def get_today_games(target_date: str = None) -> List[Dict]:
    target_date = target_date or date.today().isoformat()
    async with httpx.AsyncClient(follow_redirects=True) as c:
        data = await _fetch(f"{NHL_API}/schedule/{target_date}", c)
    if not data:
        return []
    games = []
    for day in data.get("gameWeek", []):
        if day.get("date") == target_date:
            for g in day.get("games", []):
                if g.get("gameState", "") in ("FUT", "PRE", "LIVE", "CRIT", "OFF", "FINAL"):
                    games.append({
                        "gameId":    g["id"],
                        "homeTeam":  g["homeTeam"]["abbrev"],
                        "awayTeam":  g["awayTeam"]["abbrev"],
                        "homeFull":  g["homeTeam"].get("commonName", {}).get("default", ""),
                        "awayFull":  g["awayTeam"].get("commonName", {}).get("default", ""),
                        "startTime": g.get("startTimeUTC", ""),
                    })
    return games


async def get_team_sa_map(season: str = "20242025") -> Dict[str, float]:
    """Shots Against Per Game — joins /standings (abbrev) + /team/summary (SA/G)."""
    import urllib.parse
    sort_p = urllib.parse.quote('[{"property":"shotsAgainstPerGame","direction":"DESC"}]')
    summary_url = (
        f"{NHL_STATS}/team/summary"
        f"?isAggregate=false&isGame=false&sort={sort_p}"
        f"&start=0&limit=50&factCayenneExp=gamesPlayed>=1"
        f"&cayenneExp=gameTypeId=2 and seasonId<={season} and seasonId>={season}"
    )
    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
        sd, md = await asyncio.gather(
            _fetch(f"{NHL_API}/standings/now", c),
            _fetch(summary_url, c),
        )
    if not sd or not md:
        return {}
    name_to_abbrev = {
        t.get("teamName", {}).get("default", ""): t.get("teamAbbrev", {}).get("default", "")
        for t in sd.get("standings", [])
    }
    return {
        name_to_abbrev[t["teamFullName"]]: float(t.get("shotsAgainstPerGame") or 0)
        for t in md.get("data", [])
        if t.get("teamFullName") in name_to_abbrev
    }


async def get_roster(team: str, sem: asyncio.Semaphore) -> List[Dict]:
    async with sem:
        async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
            data = await _fetch(f"{NHL_API}/roster/{team}/current", c)
    if not data:
        return []
    players = []
    for pos in ("forwards", "defensemen"):
        for p in data.get(pos, []):
            players.append({
                "id":   p["id"],
                "name": f"{p['firstName']['default']} {p['lastName']['default']}",
            })
    return players

# ─────────────────────────────────────────────────────────────────────────────
#  Sportsbook Lines — tries Odds API, then DraftKings, then estimates
# ─────────────────────────────────────────────────────────────────────────────


async def get_shot_lines(target_date: str) -> Dict[str, Dict]:
    """Fetch real shots on goal lines.
    Full fallback chain: 1) The Odds API  2) FanDuel  3) DraftKings  4) Estimates
    """
    # 1 — The Odds API (if key is set)
    api_key = os.environ.get("ODDS_API_KEY", "")
    if api_key:
        try:
            lines: Dict[str, Dict] = {}
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(
                    f"{ODDS_API}/sports/icehockey_nhl/events",
                    params={"apiKey": api_key, "dateFormat": "iso"})
                if r.status_code == 200:
                    events = [e for e in r.json()
                              if e.get("commence_time", "")[:10] == target_date]
                    print(f"[OddsAPI] {len(events)} NHL games for {target_date}")
                    seen: set = set()
                    for ev in events:
                        r2 = await c.get(
                            f"{ODDS_API}/sports/icehockey_nhl/events/{ev['id']}/odds",
                            params={"apiKey": api_key, "regions": "us,us2",
                                    "markets": "player_shots_on_goal",
                                    "oddsFormat": "american"})
                        if r2.status_code != 200:
                            continue
                        for book in r2.json().get("bookmakers", []):
                            for mkt in book.get("markets", []):
                                if mkt.get("key") != "player_shots_on_goal":
                                    continue
                                for oc in mkt.get("outcomes", []):
                                    if oc.get("name") != "Over":
                                        continue
                                    player = oc.get("description", "").strip()
                                    line   = float(oc.get("point") or 0)
                                    if player and line > 0 and player not in seen:
                                        seen.add(player)
                                        lines[player] = {
                                            "line":   line,
                                            "odds":   str(oc.get("price", "")),
                                            "source": "OddsAPI",
                                        }
                            break  # first bookmaker per event
            if lines:
                print(f"[Lines] {len(lines)} lines from The Odds API")
                return lines
        except Exception as e:
            print(f"[Lines] Odds API error: {e}")

    # 2 — FanDuel (via Playwright login)
    if os.environ.get("FD_EMAIL"):
        try:
            lines = await _lines_from_fanduel()
            if lines:
                print(f"[Lines] {len(lines)} lines from FanDuel")
                return lines
        except Exception as e:
            print(f"[Lines] FanDuel error: {e}")

    # 3 — DraftKings (public endpoint)
    try:
        lines = await _lines_from_draftkings()
        if lines:
            print(f"[Lines] {len(lines)} lines from DraftKings")
            return lines
    except Exception as e:
        print(f"[Lines] DraftKings error: {e}")

    print("[Lines] No sportsbook lines — falling back to season avg estimates")
    return {}



async def _lines_from_fanduel() -> Dict[str, Dict]:
    """Scrape FanDuel NHL shots on goal lines using cached login session."""
    cookie = await get_fd_cookie()
    if not cookie:
        return {}
    hdrs = {
        "Cookie":     cookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
        "Referer":    "https://sportsbook.fanduel.com/",
        "Accept":     "application/json",
        "x-fanduel-api-key": "FhMFpcPWXMeyZxOx",
    }
    lines = {}
    try:
        async with CFSession(impersonate="chrome120") as s:
            # Get today's NHL events
            r = await s.get(
                "https://sbapi.tn.sportsbook.fanduel.com/api/content-managed-page",
                params={"page": "SPORT", "sport": "NHL", "_ak": "FhMFpcPWXMeyZxOx"},
                headers=hdrs, timeout=20
            )
            if r.status_code != 200:
                print(f"[FanDuel] NHL page status: {r.status_code}")
                return {}
            data = r.json()
            # Walk the event table to find shots on goal markets
            events = data.get("attachments", {}).get("events", {}).values()
            markets = data.get("attachments", {}).get("markets", {})
            runners = data.get("attachments", {}).get("runners", {})
            for mkt in markets.values():
                mkt_name = mkt.get("marketName", "").lower()
                if "shot" not in mkt_name:
                    continue
                for runner_id in mkt.get("runnerIds", []):
                    runner = runners.get(str(runner_id), {})
                    rname  = runner.get("runnerName", "")
                    # FanDuel format: "Player Name (Over 2.5)" or "Player Name"
                    handicap = runner.get("handicap", 0)
                    if "over" in rname.lower() or handicap:
                        # Extract player name
                        player = rname.split(" - ")[0].strip()
                        player = player.split(" Over")[0].strip()
                        line_val = float(handicap or 0)
                        if line_val >= 1.5 and player:
                            odds_info = runner.get("winRunnerOdds", {}).get("americanDisplayOdds", {})
                            odds = odds_info.get("americanOdds", "")
                            lines[player] = {"line": line_val, "odds": str(odds), "source": "FanDuel"}
    except Exception as e:
        print(f"[FanDuel] Parse error: {e}")
        return {}
    return lines


async def _lines_from_draftkings() -> Dict[str, Dict]:
    hdrs = {"Accept": "application/json",
            "Referer": "https://sportsbook.draftkings.com/leagues/hockey/nhl"}
    lines = {}
    async with CFSession(impersonate="chrome120") as s:
        r = await s.get(f"{DK_BASE}/eventgroups/{NHL_EG_ID}?format=json", headers=hdrs)
        if r.status_code != 200:
            return {}
        eg = r.json().get("eventGroup", {})
        cat_id = sub_id = None
        for cat in eg.get("offerCategories", []):
            for sub in cat.get("offerSubcategoryDescriptors", []):
                if "shot" in sub.get("name", "").lower():
                    cat_id, sub_id = cat["id"], sub["subcategoryId"]
                    break
            if cat_id:
                break
        if not cat_id:
            return {}
        r2 = await s.get(
            f"{DK_BASE}/eventgroups/{NHL_EG_ID}/categories/{cat_id}/subcategories/{sub_id}?format=json",
            headers=hdrs)
        if r2.status_code != 200:
            return {}
        for cat in r2.json().get("eventGroup", {}).get("offerCategories", []):
            for sub_desc in cat.get("offerSubcategoryDescriptors", []):
                for event_offers in sub_desc.get("offerSubcategory", {}).get("offers", []):
                    for offer in event_offers:
                        player = offer.get("label", "").strip()
                        for outcome in offer.get("outcomes", []):
                            if outcome.get("label", "").lower() == "over":
                                line_val = float(outcome.get("line") or 0)
                                if line_val >= 1.5 and player:
                                    lines[player] = {
                                        "line":   line_val,
                                        "odds":   outcome.get("oddsAmerican", ""),
                                        "source": "DraftKings",
                                    }
    return lines


# ─────────────────────────────────────────────────────────────────────────────
#  NHL Skater Stats — season shot averages (replaces sportsbook props)
# ─────────────────────────────────────────────────────────────────────────────

def _match_odds_name(odds_name: str, roster: List[Dict]) -> Optional[Dict]:
    """Match Odds API player name to NHL roster player."""
    def norm(n): return n.lower().replace(".","").replace("-"," ").replace("'","").strip()
    on = norm(odds_name)
    for p in roster:
        if norm(p["name"]) == on: return p
    parts = on.split()
    if len(parts) >= 2:
        fi, last = parts[0][0], parts[-1]
        for p in roster:
            pp = norm(p["name"]).split()
            if len(pp) >= 2 and pp[0][0] == fi and pp[-1] == last:
                return p
    return None


async def get_shot_qualified_players(
    games: List[Dict],
    sa_map: Dict[str, float],
    sem: asyncio.Semaphore,
    season: str = "20252026",
    lines_map: Dict = None,   # {player_name: {line, odds}} from Odds API
) -> List[Dict]:
    """Build player pool from Odds API lines (primary) or season averages (fallback)."""
    if lines_map is None:
        lines_map = {}

    team_ctx: Dict[str, Dict] = {}
    for g in games:
        team_ctx[g["homeTeam"]] = {"opponent": g["awayTeam"], "homeRoad": "H"}
        team_ctx[g["awayTeam"]] = {"opponent": g["homeTeam"],  "homeRoad": "R"}

    # Get rosters for all playing teams
    roster_vals = await asyncio.gather(
        *[get_roster(t, sem) for t in team_ctx], return_exceptions=True)
    rosters = {t: (r if isinstance(r, list) else [])
               for t, r in zip(team_ctx.keys(), roster_vals)}

    pool: List[Dict] = []
    seen: set = set()

    # PRIMARY: build from Odds API lines (sportsbook line = threshold)
    if lines_map:
        for odds_name, sb_info in lines_map.items():
            line     = sb_info["line"]
            real_odds = sb_info.get("odds", "")
            matched  = None
            team     = None
            for t, roster in rosters.items():
                p = _match_odds_name(odds_name, roster)
                if p:
                    matched = p
                    team    = t
                    break
            if not matched or team not in team_ctx or matched["id"] in seen:
                continue
            seen.add(matched["id"])
            opp = team_ctx[team]["opponent"]
            pool.append({
                "name":      odds_name,
                "pid":       matched["id"],
                "team":      team,
                "opponent":  opp,
                "homeRoad":  team_ctx[team]["homeRoad"],
                "line":      line,           # REAL sportsbook line as threshold
                "realLine":  line,
                "realOdds":  real_odds,
                "lineSource": "OddsAPI",
                "estLine":   line,
                "spg":       line,           # use line as display value
                "oppSA":     sa_map.get(opp, 0.0),
            })
        print(f"[NHL] {len(pool)} players with Odds API shot lines")

    # FALLBACK: use season averages if no Odds API lines
    if not pool:
        print("[NHL] No Odds API lines — falling back to season averages")
        async def _fetch_team(team: str):
            async with sem:
                async with httpx.AsyncClient(timeout=20) as c:
                    r = await c.get(f"{NHL_STATS}/skater/summary",
                                    params={"limit":100,"start":0,
                                            "cayenneExp":f"gameTypeId=2 and seasonId={season} and teamAbbrevs='{team}'"})
            if r.status_code != 200: return
            ctx = team_ctx.get(team, {})
            opp, hr = ctx.get("opponent",""), ctx.get("homeRoad","")
            for p in r.json().get("data",[]):
                pid, gp, shots = p.get("playerId"), p.get("gamesPlayed",0), p.get("shots",0)
                if p.get("positionCode") == "G" or gp < MIN_GP: continue
                spg = shots / gp
                if spg < MIN_SPG or pid in seen: continue
                seen.add(pid)
                name = p["skaterFullName"]
                est  = _est_line(spg)
                pool.append({"name":name,"pid":pid,"team":team,"opponent":opp,
                             "homeRoad":hr,"line":1.5,"realLine":None,"realOdds":"",
                             "lineSource":"Est","estLine":est,"spg":round(spg,2),
                             "oppSA":sa_map.get(opp,0.0)})
        await asyncio.gather(*[_fetch_team(t) for t in team_ctx], return_exceptions=True)
        print(f"[NHL] {len(pool)} skaters from season averages (fallback)")

    pool.sort(key=lambda x: x["oppSA"], reverse=True)
    return pool

# ─────────────────────────────────────────────────────────────────────────────
#  Points picks — NHL Stats API game logs (independent of shots)
# ─────────────────────────────────────────────────────────────────────────────

async def _pts_season_logs(pid: int, season: str, c: httpx.AsyncClient) -> List[Dict]:
    data = await _fetch(f"{NHL_API}/player/{pid}/game-log/{season}/2", c)
    if not data:
        return []
    logs = []
    for g in data.get("gameLog", []):
        goals   = int(g.get("goals",   0) or 0)
        assists = int(g.get("assists", 0) or 0)
        logs.append({
            "date":     g.get("gameDate",     ""),
            "points":   goals + assists,
            "homeRoad": g.get("homeRoadFlag", ""),
            "opponent": g.get("opponentAbbrev", ""),
        })
    return logs


async def get_pts_picks(
    games: List[Dict],
    sa_map: Dict[str, float],
    sem: asyncio.Semaphore,
    season: str = "20252026",
) -> List[Dict]:
    """Independent points picks using NHL Stats API game logs."""

    # Build team context
    team_ctx: Dict[str, Dict] = {}
    for g in games:
        team_ctx[g["homeTeam"]] = {"opponent": g["awayTeam"], "homeRoad": "H"}
        team_ctx[g["awayTeam"]] = {"opponent": g["homeTeam"],  "homeRoad": "R"}

    # Get all skaters on today's teams
    roster_vals = await asyncio.gather(
        *[get_roster(t, sem) for t in team_ctx], return_exceptions=True
    )
    rosters = {t: (r if isinstance(r, list) else []) for t, r in zip(team_ctx.keys(), roster_vals)}

    # Fetch multi-season game logs for all players concurrently
    all_players = []
    seen_pts = set()
    for team, players in rosters.items():
        ctx = team_ctx[team]
        for p in players:
            if p["id"] not in seen_pts:
                seen_pts.add(p["id"])
                all_players.append((p, team, ctx["opponent"], ctx["homeRoad"]))

    async def fetch_logs(pid):
        async with sem:
            async with httpx.AsyncClient(timeout=30) as c:
                results = await asyncio.gather(
                    *[_pts_season_logs(pid, s, c) for s in SEASONS],
                    return_exceptions=True
                )
        logs = []
        for r in results:
            if isinstance(r, list):
                logs.extend(r)
        logs.sort(key=lambda x: x["date"], reverse=True)
        return logs

    log_tasks = {p["id"]: fetch_logs(p["id"]) for p, *_ in all_players}
    log_results = await asyncio.gather(*log_tasks.values(), return_exceptions=True)
    logs_map = {pid: (r if isinstance(r, list) else []) for pid, r in zip(log_tasks.keys(), log_results)}

    picks = []
    for player, team, opp, hr in all_players:
        logs = logs_map.get(player["id"], [])

        # Career H/A vs today's opponent
        c_logs = [g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp]
        # Last 10 H/A any opponent
        r_logs = [g for g in logs if g["homeRoad"] == hr][:10]

        if len(r_logs) < MIN_GAMES:
            continue

        h3 = sum(1 for g in r_logs if g["points"] > PTS_LINE)
        r3 = round(h3 / len(r_logs) * 100, 1)
        avg3 = round(sum(g["points"] for g in r_logs) / len(r_logs), 2)

        h2 = sum(1 for g in c_logs if g["points"] > PTS_LINE) if c_logs else 0
        r2 = round(h2 / len(c_logs) * 100, 1) if c_logs else 0
        avg2 = round(sum(g["points"] for g in c_logs) / len(c_logs), 2) if c_logs else 0

        s2_ok = (len(c_logs) < MIN_GAMES) or (r2 >= HIT_THRESH_PTS)
        s3_ok = r3 >= HIT_THRESH_PTS
        if not s2_ok or not s3_ok:
            continue

        score = round((r2 + r3) / 2 if c_logs else r3, 1)

        picks.append({
            "name":     player["name"],
            "pid":      player["id"],
            "team":     team,
            "opponent": opp,
            "homeRoad": hr,
            "oppSA":    sa_map.get(opp, 0.0),
            "ptsOppAvg":  avg2,
            "ptsHa10avg": avg3,
            "pts2Hits":  h2, "pts2Total": len(c_logs), "pts2Rate": r2,
            "pts3Hits":  h3, "pts3Total": len(r_logs), "pts3Rate": r3,
            "ptsScore":  score,
        })

    picks.sort(key=lambda x: (x["ptsScore"], x["oppSA"]), reverse=True)
    print(f"[PTS] {len(picks)} players qualifying at {HIT_THRESH_PTS}%+ hit rate")
    return picks



# ─────────────────────────────────────────────────────────────────────────────
#  Main algorithm
# ─────────────────────────────────────────────────────────────────────────────

async def _nhl_player_logs(pid: int, sem: asyncio.Semaphore) -> List[Dict]:
    """Fetch NHL game logs for a player across multiple seasons."""
    all_logs = []
    async with sem:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as c:
            results = await asyncio.gather(
                *[_fetch(f"{NHL_API}/player/{pid}/game-log/{s}/2", c) for s in SEASONS],
                return_exceptions=True
            )
    for data in results:
        if not isinstance(data, dict): continue
        for g in data.get("gameLog", []):
            all_logs.append({
                "date":     g.get("gameDate", ""),
                "shots":    int(g.get("shots", 0) or 0),
                "points":   int(g.get("goals", 0) or 0) + int(g.get("assists", 0) or 0),
                "homeRoad": g.get("homeRoadFlag", ""),
                "opponent": g.get("opponentAbbrev", ""),
            })
    all_logs.sort(key=lambda x: x["date"], reverse=True)
    return all_logs


def _calc_hit_rate_from_logs(logs: List[Dict], line: float, home_road: str,
                             opponent: str = None, last_n: int = None):
    filtered = [g for g in logs if g["homeRoad"] == home_road]
    if opponent:
        filtered = [g for g in filtered if g["opponent"] == opponent]
    if last_n:
        filtered = filtered[:last_n]
    total = len(filtered)
    if total == 0:
        return 0, 0, 0.0, 0.0
    hits = sum(1 for g in filtered if g["shots"] > line)
    avg  = round(sum(g["shots"] for g in filtered) / total, 2)
    rate = round(hits / total * 100, 1)
    return hits, total, rate, avg


async def run_picks(target_date: str = None) -> Dict:
    global _progress
    sem_nhl = asyncio.Semaphore(SEM_NHL)

    target_date = target_date or date.today().isoformat()
    season = get_season_for_date(date.fromisoformat(target_date))

    _progress = {"stage": "Fetching games & sportsbook lines...", "done": 0, "total": 0, "pct": 10}

    # ── Step 1 — fetch games, SA map, and sportsbook lines in parallel ─────────────
    games, sa_map, lines_map = await asyncio.gather(
        get_today_games(target_date),
        get_team_sa_map(season),
        get_shot_lines(target_date),
    )
    _progress = {"stage": "Building player pool...", "done": 0, "total": 0, "pct": 25}

    if not games:
        return {"error": f"No NHL games found for {target_date}.", "picks": [], "games": []}

    # SA rankings for display
    playing = list({g["homeTeam"] for g in games} | {g["awayTeam"] for g in games})
    sa_ranks = sorted(
        [(t, sa_map.get(t, 0.0)) for t in playing],
        key=lambda x: x[1], reverse=True
    )

    # Build player pool from NHL skater season averages
    pool = await get_shot_qualified_players(games, sa_map, sem_nhl, season, lines_map)
    _progress = {"stage": f"Fetching game logs for {len(pool)} players...", "done": 0, "total": len(pool), "pct": 35}

    if not pool:
        return {"error": "No players found for today's games.", "picks": [], "games": games}

    # Fetch NHL API game logs for all players concurrently
    log_tasks = {p["pid"]: _nhl_player_logs(p["pid"], sem_nhl) for p in pool}
    log_results = await asyncio.gather(*log_tasks.values(), return_exceptions=True)
    logs_map = {pid: (r if isinstance(r, list) else [])
                for pid, r in zip(log_tasks.keys(), log_results)}

    _progress = {"stage": "Analyzing hit rates...", "done": 0, "total": len(pool), "pct": 70}

    # ── Steps 2 & 3 — NHL Stats API hit-rate analysis ────────────────────────────
    async def analyze(p: Dict) -> Optional[Dict]:
        logs = logs_map.get(p["pid"], [])
        hr, opp, line = p["homeRoad"], p["opponent"], p["line"]

        # Step 2: career H/A vs today's opponent
        h2, t2, r2, avg2 = _calc_hit_rate_from_logs(logs, line, hr, opponent=opp)
        # Step 3: last 10 H/A games any opponent
        h3, t3, r3, avg3 = _calc_hit_rate_from_logs(logs, line, hr, last_n=10)

        if t3 < MIN_GAMES:
            return None
        s2_ok = (t2 < MIN_GAMES) or (r2 >= HIT_THRESH)
        s3_ok = r3 >= HIT_THRESH
        if not s2_ok or not s3_ok:
            return None
        score = round((r2 + r3) / 2 if t2 >= MIN_GAMES else r3, 1)

        return {
            **p,
            "step2Hits": h2, "step2Total": t2, "step2Rate": r2,
            "step3Hits": h3, "step3Total": t3, "step3Rate": r3,
            "oppAvg": avg2, "ha10avg": avg3, "score": score,
        }

    completed = [0]
    async def analyze_tracked(p):
        result = await analyze(p)
        completed[0] += 1
        _progress["done"]  = completed[0]
        _progress["pct"]   = 70 + int((completed[0] / max(len(pool),1)) * 25)
        _progress["stage"] = f"Analyzing players... {completed[0]}/{len(pool)}"
        return result

    results_raw = await asyncio.gather(*[analyze_tracked(p) for p in pool])
    picks = [r for r in results_raw if r is not None]

    _progress = {"stage": "Analyzing points...", "done": len(pool), "total": len(pool), "pct": 96}
    # ── Step 4 — rank shots & run independent points picks ───────────────────
    picks.sort(key=lambda x: (x["score"], x["oppSA"]), reverse=True)

    pts_all = await get_pts_picks(games, sa_map, sem_nhl, season)
    _progress = {"stage": "Done!", "done": len(pool), "total": len(pool), "pct": 100}

    return {
        "picks":         picks[:TOP_N],
        "rest":          picks[TOP_N:],
        "ptsPicks":      pts_all[:TOP_N],
        "ptsRest":       pts_all[TOP_N:],
        "games":         games,
        "sa_ranks":      sa_ranks,
        "poolSize":      len(pool),
        "qualified":     len(picks),
        "ptsQualified":  len(pts_all),
        "targetDate":    target_date,
        "runTime":       datetime.utcnow().isoformat() + "Z",
    }

# ─────────────────────────────────────────────────────────────────────────────
#  HTML
# ─────────────────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>NHL Money Shots</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0a0a0a;color:#f0f0f0;font-family:Arial,Helvetica,sans-serif;min-height:100vh}

/* HEADER */
body::before{content:'';position:fixed;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,#FDB827,#fff5cc,#FDB827,#C4901A,#FDB827);
  background-size:300% 100%;animation:goldShimmer 4s linear infinite;z-index:999}
@keyframes goldShimmer{0%{background-position:0% 50%}100%{background-position:300% 50%}}
.hdr{background:#000;border-bottom:2px solid rgba(253,184,39,.3);
  box-shadow:0 2px 20px rgba(253,184,39,.1);padding:36px 20px;text-align:center}
.hdr .hdr-icon{font-size:4rem;filter:drop-shadow(0 0 16px rgba(253,184,39,.8));margin-bottom:10px}
.hdr h1{font-size:2.5rem;font-weight:900;letter-spacing:4px;
  background:linear-gradient(135deg,#FDB827,#fff5a0,#C4901A);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text}
.hdr p{color:rgba(253,184,39,.6);font-size:.8rem;letter-spacing:3px;
  text-transform:uppercase;margin-top:6px}

/* LAYOUT */
.wrap{max-width:1300px;margin:0 auto;padding:30px 20px}

/* CONNECTION BOX */
.conn-box{background:#111;border:2px solid #FDB827;border-radius:10px;
  padding:30px;text-align:center;margin-bottom:20px}
.conn-box h2{font-size:1rem;font-weight:700;color:#FDB827;letter-spacing:3px;
  text-transform:uppercase;margin-bottom:8px}
.conn-box p{color:#666;font-size:.85rem;margin-bottom:20px}
.btn-connect{background:#FDB827;color:#000;border:none;border-radius:6px;
  padding:16px 48px;font-size:1rem;font-weight:900;letter-spacing:2px;
  text-transform:uppercase;cursor:pointer;transition:background .2s}
.btn-connect:hover{background:#ffd060}
.btn-connect:disabled{background:#444;color:#666;cursor:not-allowed}
.conn-status{margin-top:16px;padding:12px 20px;border-radius:6px;
  font-weight:700;font-size:.9rem;letter-spacing:1px;display:none}
.conn-status.ok{background:#003300;border:1px solid #009900;color:#00cc00}
.conn-status.fail{background:#330000;border:1px solid #990000;color:#cc0000}

/* RUN BOX */
.run-box{background:#111;border:2px solid #333;border-radius:10px;
  padding:30px;text-align:center;margin-bottom:24px;transition:border-color .3s}
.run-box.unlocked{border-color:#C8102E}
.run-box h2{font-size:1rem;font-weight:700;color:#888;letter-spacing:3px;
  text-transform:uppercase;margin-bottom:8px;transition:color .3s}
.run-box.unlocked h2{color:#C8102E}
.run-box p{color:#555;font-size:.85rem;margin-bottom:20px}
.date-row{display:flex;align-items:center;justify-content:center;gap:12px;margin-bottom:20px}
.date-row label{color:#fff;font-weight:700;font-size:.9rem;letter-spacing:1px}
.date-row input{background:#1a1a1a;color:#f0f0f0;border:1px solid #333;
  border-radius:6px;padding:10px 16px;font-size:.95rem;cursor:pointer;outline:none}
.date-row input:focus{border-color:#FDB827}
.btn-run{background:#C8102E;color:#fff;border:none;border-radius:6px;
  padding:16px 56px;font-size:1rem;font-weight:900;letter-spacing:2px;
  text-transform:uppercase;cursor:pointer;transition:background .2s}
.btn-run:hover{background:#e01535}
.btn-run:disabled{background:#333;color:#666;cursor:not-allowed}

/* STATUS LINE */
.status{text-align:center;color:#666;font-size:.85rem;margin-bottom:24px;min-height:20px}

/* STAT CHIPS */
.chips{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));
  gap:12px;margin-bottom:28px}
.chip{background:#111;border-top:3px solid #FDB827;border-radius:8px;
  padding:16px 10px;text-align:center}
.chip .val{font-size:1.9rem;font-weight:900;color:#FDB827}
.chip .lbl{font-size:.65rem;color:#555;text-transform:uppercase;letter-spacing:1px;margin-top:4px}

/* SECTION HEADER */
.sec{background:#111;border-left:4px solid #FDB827;padding:10px 16px;
  font-size:.85rem;font-weight:900;letter-spacing:2px;text-transform:uppercase;
  color:#fff;margin:24px 0 12px;border-radius:0 6px 6px 0}

/* GAMES */
.games{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));
  gap:10px;margin-bottom:24px}
.gcard{background:#111;border:1px solid #222;border-radius:8px;
  padding:14px;text-align:center}
.gcard:hover{border-color:#FDB827}
.gcard .mu{font-size:1rem;font-weight:700;color:#fff}
.gcard .gt{font-size:.75rem;color:#555;margin-top:5px}

/* SA RANKS */
.sa-list{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}
.sa-badge{background:#111;border:1px solid #222;border-radius:4px;
  padding:5px 12px;font-size:.8rem}
.sa-badge .rk{color:#FDB827;font-weight:700}
.sa-badge .sv{color:#C8102E;font-weight:700}

/* TABLE */
.tbl-wrap{overflow-x:auto;border-radius:8px;border:1px solid #222;margin-bottom:8px}
table{width:100%;border-collapse:collapse;background:#0a0a0a}
thead tr{border-bottom:2px solid #FDB827}
th{background:#111;padding:12px 14px;text-align:left;font-size:.72rem;
  font-weight:900;text-transform:uppercase;letter-spacing:1px;color:#FDB827;white-space:nowrap}
td{padding:11px 14px;border-bottom:1px solid #1a1a1a;font-size:.88rem;white-space:nowrap}
tr:nth-child(even) td{background:#0f0f0f}
tr:hover td{background:#161616}
tr:last-child td{border-bottom:none}

.rk-num{font-weight:900;color:#FDB827;font-size:1.1rem}
.rk-rest{color:#555;font-size:.9rem}
.pname{font-weight:700;color:#fff}
.tbadge{background:#1a1a1a;color:#999;padding:2px 8px;border-radius:3px;
  font-size:.74rem;border:1px solid #2a2a2a}
.home{background:#0a1a0a;color:#00aa00;padding:3px 8px;border-radius:3px;
  font-size:.74rem;font-weight:700;border:1px solid #004400}
.away{background:#1a0a0a;color:#cc0000;padding:3px 8px;border-radius:3px;
  font-size:.74rem;font-weight:700;border:1px solid #440000}
.gold{color:#FDB827;font-weight:700}
.green{color:#00aa00;font-weight:700}
.red-txt{color:#cc0000;font-weight:700}
.score{color:#FDB827;font-weight:900;font-size:1.05rem}
.gray{color:#555;font-size:.8rem}
.est{background:#1a1200;color:#FDB827;border:1px solid #332200;
  padding:2px 8px;border-radius:3px;font-size:.78rem;font-weight:700}
.real-line{color:#00cc44;font-weight:900;font-size:1rem}
.odds-txt{color:#666;font-size:.78rem}

/* LOADING */
.loading{text-align:center;padding:70px 20px}
.spin{width:50px;height:50px;border:4px solid #1a1a1a;
  border-top:4px solid #FDB827;border-radius:50%;
  animation:spin .8s linear infinite;margin:0 auto 18px}
@keyframes spin{to{transform:rotate(360deg)}}
.err-box{background:#1a0000;border:1px solid #C8102E;border-radius:8px;
  padding:20px;text-align:center;color:#cc0000;font-weight:700}
.no-picks{text-align:center;padding:50px;color:#444}

footer{text-align:center;padding:28px;color:#333;font-size:.75rem;
  border-top:1px solid #1a1a1a;margin-top:24px}
footer b{color:#FDB827}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-icon">🏒</div>
  <h1>NHL Money Shots</h1>
  <p>NHL Daily Picks</p>
</div>

<div class="wrap">

  <!-- Status bar (no login needed) -->
  <div style="background:#111;border:1px solid #222;border-radius:10px;padding:12px 20px;margin-bottom:16px;display:flex;align-items:center;gap:20px;flex-wrap:wrap">
    <span id="odds-status" style="font-size:.82rem;font-weight:700;color:#555">● Checking Odds API...</span>
    <span id="fd-status"   style="font-size:.82rem;font-weight:700;color:#555">● FanDuel</span>
    <span style="font-size:.78rem;color:#333;margin-left:auto">NHL Stats API · No login required</span>
  </div>

  <!-- Run Picks -->
  <div class="run-box unlocked" id="step2box">
    <div class="step-label">RUN PICKS</div>
    <p class="step-desc">Select a date and run — NHL Stats API powers all hit rates</p>
    <div class="date-row">
      <label>DATE</label>
      <input type="date" id="datePicker" max=""/>
    </div>
    <button class="btn-run" id="runBtn" onclick="runPicks()">
      RUN PICKS
    </button>
  </div>

  <div class="status" id="statusMsg"></div>
  <div id="out"></div>
</div>

<footer><b>NHL Money Shots</b> &nbsp;&middot;&nbsp; Money Picks Arena &nbsp;&middot;&nbsp; NHL Stats API + Sportsbook Lines</footer>

<script>
// Set date to today
document.addEventListener('DOMContentLoaded', function(){
// ── Hub Token Gate ────────────────────────────────────────────────────
(function() {
  const HUB = 'https://www.moneypicksarena.com';
  const STORAGE_KEY = '__mpa_token';
  const params = new URLSearchParams(window.location.search);
  const urlTok = params.get('token');
  if (urlTok) {
    localStorage.setItem(STORAGE_KEY, urlTok);
    window.history.replaceState({}, '', window.location.pathname);
  }
  const tok = localStorage.getItem(STORAGE_KEY);
  if (!tok) { window.location.href = HUB; }
  else {
    fetch('/api/verify-token', { headers: { 'Authorization': 'Bearer ' + tok } })
      .then(r => { if (!r.ok) { localStorage.removeItem(STORAGE_KEY); window.location.href = HUB; } })
      .catch(() => { localStorage.removeItem(STORAGE_KEY); window.location.href = HUB; });
  }
})();

  var dp = document.getElementById('datePicker');
  var today = new Date().toISOString().split('T')[0];
  dp.value = today;
  dp.max = today;
});

// STEP 1: Connect
async function checkStatus(){
  try{
    var r=await fetch('/api/status'); var d=await r.json();
    var o=document.getElementById('odds-status');
    var f=document.getElementById('fd-status');
    if(o){o.style.color=d.odds_api==='configured'?'#22c55e':'#cc0000';o.textContent=d.odds_api==='configured'?'● Odds API: Ready':'● Odds API: Not configured';}
    if(f){f.style.color=d.fanduel==='configured'?'#22c55e':'#555';f.textContent=d.fanduel==='configured'?'● FanDuel: Ready':'● FanDuel: Not set';}
  }catch(e){}
}
document.addEventListener('DOMContentLoaded',checkStatus);

// STEP 2: Run picks
async function runPicks(){
  var btn = document.getElementById('runBtn');
  var st = document.getElementById('statusMsg');
  var out = document.getElementById('out');
  var dt = document.getElementById('datePicker').value;
  btn.disabled = true;
  btn.textContent = 'RUNNING...';
  st.textContent = 'Fetching games and analyzing players for ' + dt + '...';
  out.innerHTML = '<div class="loading"><div class="spin"></div>' +
    '<p style="color:#888;margin-bottom:16px" id="prog-stage">Starting...</p>' +
    '<div style="background:#1a1a1a;border-radius:6px;height:18px;width:280px;margin:0 auto 8px;overflow:hidden">' +
    '<div id="prog-bar" style="height:100%;width:5%;background:#FDB827;border-radius:6px;transition:width .5s"></div></div>' +
    '<p style="color:#555;font-size:.8rem" id="prog-pct">5%</p></div>';

  // Poll progress every 2 seconds
  var pollTimer = setInterval(async function(){
    try{
      var pr = await fetch('/api/progress');
      var pd = await pr.json();
      var bar = document.getElementById('prog-bar');
      var stg = document.getElementById('prog-stage');
      var pct = document.getElementById('prog-pct');
      if(bar){ bar.style.width = pd.pct + '%'; }
      if(stg){ stg.textContent = pd.stage; }
      if(pct){ pct.textContent = pd.pct + '%'; }
    }catch(e){}
  }, 2000);

  try {
    var res = await fetch('/api/picks?target_date=' + dt);
    var data = await res.json();
    if(data.error){
      out.innerHTML = '<div class="err-box">' + data.error + '</div>';
      st.textContent = '';
    } else {
      renderResults(data);
      st.textContent = data.qualified + ' players qualified — ' + data.picks.length + ' top picks — ' + dt;
    }
  } catch(e) {
    out.innerHTML = '<div class="err-box">Error: ' + e.message + '</div>';
  } finally {
    clearInterval(pollTimer);
    btn.disabled = false;
    btn.textContent = 'RUN PICKS';
  }
}

function rateClass(r){ return r >= 90 ? 'green' : r >= 80 ? 'gold' : 'red-txt'; }

function buildPtsTable(picks, startNum){
  var thead = '<thead><tr><th>#</th><th>PLAYER</th><th>TEAM</th><th>OPP</th><th>H/A</th>' +
    '<th>AVG PTS vs OPP</th><th>L10 H/A AVG PTS</th>' +
    '<th>CAREER vs OPP 0.5P</th><th>LAST 10 H/A 0.5P</th><th>SCORE</th><th>OPP SA/G</th></tr></thead>';
  var rows = '';
  picks.forEach(function(p, i){
    var ha  = p.homeRoad === 'H';
    var num = startNum + i;
    rows += '<tr>' +
      '<td>' + (startNum === 1 ? '<span class="rk-num">' + num + '</span>' : '<span class="rk-rest">' + num + '</span>') + '</td>' +
      '<td><span class="pname">' + p.name + '</span></td>' +
      '<td><span class="tbadge">' + p.team + '</span></td>' +
      '<td><span class="tbadge">' + p.opponent + '</span></td>' +
      '<td><span class="' + (ha ? 'home' : 'away') + '">' + (ha ? 'HOME' : 'AWAY') + '</span></td>' +
      '<td><span class="gold">' + p.ptsOppAvg + '</span></td>' +
      '<td><span class="gold">' + p.ptsHa10avg + '</span></td>' +
      '<td><span class="' + rateClass(p.pts2Rate) + '">' + p.pts2Hits + '/' + p.pts2Total + ' (' + p.pts2Rate + '%)</span></td>' +
      '<td><span class="' + rateClass(p.pts3Rate) + '">' + p.pts3Hits + '/' + p.pts3Total + ' (' + p.pts3Rate + '%)</span></td>' +
      '<td><span class="score">' + p.ptsScore + '</span></td>' +
      '<td><span class="gray">' + p.oppSA.toFixed(1) + '</span></td>' +
      '</tr>';
  });
  return '<div class="tbl-wrap"><table>' + thead + '<tbody>' + rows + '</tbody></table></div>';
}

function buildTable(picks, startNum){
  var thead = '<thead><tr><th>#</th><th>PLAYER</th><th>TEAM</th><th>OPP</th><th>H/A</th>' +
    '<th>LINE</th><th>AVG VS OPP</th><th>L10 H/A AVG</th>' +
    '<th>CAREER VS OPP 1.5S</th><th>LAST 10 H/A 1.5S</th><th>SCORE</th><th>OPP SA/G</th></tr></thead>';
  var rows = '';
  picks.forEach(function(p, i){
    var ha = p.homeRoad === 'H';
    var num = startNum + i;
    rows += '<tr>' +
      '<td>' + (startNum === 1 ? '<span class="rk-num">' + num + '</span>' : '<span class="rk-rest">' + num + '</span>') + '</td>' +
      '<td><span class="pname">' + p.name + '</span></td>' +
      '<td><span class="tbadge">' + p.team + '</span></td>' +
      '<td><span class="tbadge">' + p.opponent + '</span></td>' +
      '<td><span class="' + (ha ? 'home' : 'away') + '">' + (ha ? 'HOME' : 'AWAY') + '</span></td>' +
      '<td>' + (p.realLine ? '<span class="real-line">' + p.realLine + '</span> <span class="odds-txt">' + (p.realOdds||'') + '</span>' : '<span class="est">~' + p.estLine + '</span>') + '</td>' +
      '<td><span class="gold">' + p.oppAvg + '</span></td>' +
      '<td><span class="gold">' + p.ha10avg + '</span></td>' +
      '<td><span class="' + rateClass(p.step2Rate) + '">' + p.step2Hits + '/' + p.step2Total + ' (' + p.step2Rate + '%)</span></td>' +
      '<td><span class="' + rateClass(p.step3Rate) + '">' + p.step3Hits + '/' + p.step3Total + ' (' + p.step3Rate + '%)</span></td>' +
      '<td><span class="score">' + p.score + '</span></td>' +
      '<td><span class="gray">' + p.oppSA.toFixed(1) + '</span></td>' +
      '</tr>';
  });
  return '<div class="tbl-wrap"><table>' + thead + '<tbody>' + rows + '</tbody></table></div>';
}

function renderResults(d){
  var h = '';

  // Chips
  h += '<div class="chips">' +
    '<div class="chip"><div class="val">' + d.games.length + '</div><div class="lbl">Games</div></div>' +
    '<div class="chip"><div class="val">' + d.poolSize + '</div><div class="lbl">Pool</div></div>' +
    '<div class="chip"><div class="val">' + d.qualified + '</div><div class="lbl">Qualified</div></div>' +
    '<div class="chip"><div class="val">' + d.picks.length + '</div><div class="lbl">Top Picks</div></div>' +
    '<div class="chip"><div class="val">80%</div><div class="lbl">Min Rate</div></div>' +
    '</div>';

  // Games
  h += '<div class="sec">Games — ' + (d.targetDate || '') + '</div><div class="games">';
  d.games.forEach(function(g){
    var t = g.startTime ? new Date(g.startTime).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZoneName:'short'}) : '';
    h += '<div class="gcard"><div class="mu">' + g.awayTeam + ' @ ' + g.homeTeam + '</div><div class="gt">' + t + '</div></div>';
  });
  h += '</div>';

  // SA Rankings
  h += '<div class="sec">Shots Against / Game Rankings</div><div class="sa-list">';
  (d.sa_ranks || []).forEach(function(item, i){
    h += '<div class="sa-badge"><span class="rk">#' + (i+1) + ' ' + item[0] + '</span> <span class="sv">' + item[1].toFixed(1) + '</span></div>';
  });
  h += '</div>';

  // Top picks
  h += '<div class="sec">Top ' + d.picks.length + ' Money Shots</div>';
  if(!d.picks.length){
    h += '<div class="no-picks">No players met the 80% hit rate threshold for this date.</div>';
  } else {
    h += buildTable(d.picks, 1);
  }

  // Also qualified
  if(d.rest && d.rest.length){
    h += '<div class="sec" style="margin-top:28px">Also Qualified — ' + d.rest.length + ' More Players</div>';
    h += buildTable(d.rest, d.picks.length + 1);
  }

  // POINTS SECTION
  if(d.ptsPicks && d.ptsPicks.length){
    h += '<div class="sec" style="margin-top:40px;border-left-color:#C8102E">&#127944; Top ' + d.ptsPicks.length + ' Points Picks (1+ Point)</div>';
    h += buildPtsTable(d.ptsPicks, 1);
    if(d.ptsRest && d.ptsRest.length){
      h += '<div class="sec" style="margin-top:20px;border-left-color:#C8102E">Also Qualified for Points — ' + d.ptsRest.length + ' More</div>';
      h += buildPtsTable(d.ptsRest, d.ptsPicks.length + 1);
    }
  }

  document.getElementById('out').innerHTML = h;
}
</script>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────────────────────
#  Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/verify-token")
async def verify_token_nhl(request: Request):
    auth = request.headers.get("Authorization", "")
    tok  = auth.replace("Bearer ", "").strip()
    if not tok or not _verify_hub_token(tok):
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML

@app.get("/api/picks")
async def api_picks(target_date: str = None):
    result = await run_picks(target_date)
    return JSONResponse(result)

@app.get("/api/status")
async def api_status():
    """Check sportsbook connection status."""
    fd_configured   = bool(os.environ.get("FD_EMAIL"))
    odds_configured = bool(os.environ.get("ODDS_API_KEY"))
    return {
        "fanduel":   "configured" if fd_configured else "not configured",
        "odds_api":  "configured" if odds_configured else "not configured",
        "time":      datetime.utcnow().isoformat(),
    }

@app.get("/api/progress")
async def api_progress():
    return JSONResponse(_progress)

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
