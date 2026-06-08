#!/usr/bin/env python3
"""
NHL Money Shots - main.py
Step 1 : Sportsbook lines (Odds API → season avg estimates)
Step 2 : NHL Stats API — career H/A game logs vs today’s opponent (≥ 80%)
Step 3 : NHL Stats API — last 10 H/A games, any opponent (≥ 80%)
Step 4 : Rank & top 10
Deployed on Render (FastAPI + httpx)
"""

import os, hmac, asyncio, re, unicodedata, time, json
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, status, Request
from fastapi.responses import HTMLResponse, JSONResponse
from jose import jwt as jose_jwt

# ── Hub JWT verification ──────────────────────────────────────────────
JWT_SECRET = os.environ.get("JWT_SECRET", "")

def _verify_hub_token(token: str) -> bool:
    if not token or len(token.split(".")) != 3:
        return False
    if not JWT_SECRET:
        return False
    try:
        jose_jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return True
    except Exception:
        return False

_ADMIN_EMAILS = {e.strip().lower() for e in os.environ.get("ADMIN_EMAIL", "higgi117711@gmail.com").split(",") if e.strip()}

def _token_email(token: str) -> str:
    if not token or len(token.split(".")) != 3 or not JWT_SECRET:
        return ""
    try:
        payload = jose_jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return str(payload.get("sub", "")).strip().lower()
    except Exception:
        return ""

def _is_admin_token(token: str) -> bool:
    return bool(_ADMIN_EMAILS) and _token_email(token) in _ADMIN_EMAILS


# ─────────────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────────────

app      = FastAPI(title="NHL Shots Picks")

NHL_API      = "https://api-web.nhle.com/v1"
NHL_STATS    = "https://api.nhle.com/stats/rest/en"
ODDS_API     = "https://api.the-odds-api.com/v4"

MIN_SPG       = 1.5   # shots/game season average to qualify
MIN_GP        = 10    # minimum games played for valid average

MIN_GAMES     = 2     # min games required for hit-rate calc
RECENT_DAYS   = 14    # player must have a game within this many days to count as "playing today"
HIT_THRESH         = 70.0  # % hit rate to qualify (shots always vs 1.5 base line)
HIT_THRESH_PTS     = 65.0  # % hit rate to qualify (points)
PTS_LINE      = 0.5   # 1+ point = hit
AST_LINE      = 0.5   # 1+ assist = hit
SAVES_LINE    = 24.5  # baseline goalie saves line when no book line posted
HIT_THRESH_AST     = 60.0  # % hit rate to qualify (assists)
HIT_THRESH_GOALS   = 50.0  # % hit rate to qualify (goals scored)
HIT_THRESH_SAVES   = 55.0  # % hit rate to qualify (goalie saves)
UNDER_THRESH       = 60.0  # under-rate % to qualify as a fade candidate (under cards/track)
UNDER_MIN_VO       = 2     # min H/A games vs THIS opponent for a vs-opp under
UNDER_MIN_ANY      = 3     # min H/A games vs anyone for an any-opp under
SEASONS       = ["20252026","20242025","20232024","20222023","20212022"]  # for points game logs
TOP_N       = 10     # final picks count
SEM_NHL     = 8      # concurrent NHL API calls


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP Basic Auth
# ─────────────────────────────────────────────────────────────────────────────

def verify_user() -> str:
    return "higgi"   # auth handled by hub JWT token gate

# ── File-based Picks Cache ────────────────────────────────────────────────────
import pathlib
_CACHE_DIR = pathlib.Path("/tmp/mpa_cache")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_CACHE_TTL = 6 * 3600  # 6 hours

def _cache_path(app: str, date_key: str) -> pathlib.Path:
    return _CACHE_DIR / f"{app}_{date_key}.json"

def _cache_get(app: str, date_key: str):
    p = _cache_path(app, date_key)
    try:
        if p.exists() and (time.time() - p.stat().st_mtime) < _CACHE_TTL:
            data = json.loads(p.read_text(encoding="utf-8"))
            print(f"[Cache] FILE HIT {app}/{date_key}")
            return data
    except Exception as e:
        print(f"[Cache] Read error: {e}")
    return None

def _cache_set(app: str, date_key: str, result: dict):
    try:
        _cache_path(app, date_key).write_text(
            json.dumps(result, ensure_ascii=False), encoding="utf-8")
        print(f"[Cache] FILE SET {app}/{date_key}")
    except Exception as e:
        print(f"[Cache] Write error: {e}")

def _cache_clear(app: str = None):
    for p in _CACHE_DIR.glob("*.json"):
        if app is None or p.name.startswith(app + "_"):
            p.unlink(missing_ok=True)


# Odds-layer cache: stores the raw Odds API lines per date so re-runs (cron,
# forced re-rank, runs after the result cache expires) reuse the odds already
# pulled instead of hitting the Odds API again. Shorter TTL than the result
# cache so lines still refresh over the day. Cleared by _cache_clear (same
# "<app>_" prefix), so a true fresh run still re-pulls.
_ODDS_TTL = 3 * 3600  # 3 hours

def _odds_cache_get(app: str, date_key: str):
    p = _CACHE_DIR / f"{app}_odds_{date_key}.json"
    try:
        if p.exists() and (time.time() - p.stat().st_mtime) < _ODDS_TTL:
            print(f"[OddsCache] HIT {app}/{date_key}")
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[OddsCache] read error: {e}")
    return None

def _odds_cache_set(app: str, date_key: str, data):
    try:
        (_CACHE_DIR / f"{app}_odds_{date_key}.json").write_text(
            json.dumps(data, ensure_ascii=False), encoding="utf-8")
        print(f"[OddsCache] SET {app}/{date_key}")
    except Exception as e:
        print(f"[OddsCache] write error: {e}")



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


async def get_team_sa_map(season: str = "20252026") -> Dict[str, float]:
    """Shots Against Per Game - joins /standings (abbrev) + /team/summary (SA/G)."""
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


def _played_recently(logs: List[Dict], ref_date: str, days: int = RECENT_DAYS) -> bool:
    """True if the player has at least one game within `days` of ref_date.
    NHL doesn't post confirmed lineups pre-game, so this is our proxy for
    "actually in today's playing group" — it drops healthy scratches, AHL
    call-ups who got sent down, and long-term injured depth players."""
    if not logs:
        return False
    try:
        cutoff = date.fromisoformat(ref_date) - timedelta(days=days)
    except Exception:
        return True  # unparseable ref date -> don't over-filter
    for g in logs:
        d = (g.get("date") or "")[:10]
        if not d:
            continue
        try:
            if date.fromisoformat(d) >= cutoff:
                return True
        except Exception:
            continue
    return False


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


async def get_goalies(team: str, sem: asyncio.Semaphore) -> List[Dict]:
    """Goalies for a team — separate pool from skaters (saves market)."""
    async with sem:
        async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
            data = await _fetch(f"{NHL_API}/roster/{team}/current", c)
    if not data:
        return []
    goalies = []
    for p in data.get("goalies", []):
        goalies.append({
            "id":   p["id"],
            "name": f"{p['firstName']['default']} {p['lastName']['default']}",
        })
    return goalies

# ─────────────────────────────────────────────────────────────────────────────
#  Sportsbook Lines - tries Odds API, then DraftKings, then estimates
# ─────────────────────────────────────────────────────────────────────────────


def _book_tag(real_line, ha10avg, vs_line_rate):
    """Tag a pick relative to sportsbook line.
       SUGGESTED if avg clearly beats line + recent hit rate is strong.
       FADE if avg is clearly under line + recent hit rate is weak."""
    if real_line is None or ha10avg is None:
        return ""
    edge = ha10avg - real_line
    if edge >= 0.3 and vs_line_rate >= 60:
        return "SUGGESTED"
    if edge <= -0.3 and vs_line_rate <= 40:
        return "FADE"
    return ""


def _proj_count(l10_avg, l10_n, opp_avg, opp_n, opp_sa, league_sa, days_rest=None):
    """Opponent-adjusted projected stat count.
    Blend recent H/A form (l10_avg over l10_n games) with career-vs-opponent
    history (opp_avg over opp_n games), sample-weighted so a thin vs-opp sample
    barely moves the projection (no hard minimum). Then scale by how the
    opponent's shots-allowed/game compares to league average (clamped 0.85-1.15),
    with a light back-to-back penalty. Returns (proj, opp_factor, rest_factor)."""
    l10_n = max(int(l10_n or 0), 0)
    opp_n = max(min(int(opp_n or 0), 10), 0)   # cap vs-opp weight at the L10 anchor
    if l10_n + opp_n == 0:
        base = float(l10_avg or 0.0)
    else:
        base = ((l10_avg or 0.0) * l10_n + (opp_avg or 0.0) * opp_n) / (l10_n + opp_n)
    if opp_sa and league_sa:
        opp_factor = max(0.85, min(1.15, opp_sa / league_sa))
    else:
        opp_factor = 1.0
    rest_factor = 0.97 if (days_rest is not None and days_rest <= 1) else 1.0
    return round(base * opp_factor * rest_factor, 2), round(opp_factor, 3), rest_factor


def _days_rest(logs, ref_date):
    """Days since the player's most recent game (from their own logs). None if unknown."""
    try:
        ref = date.fromisoformat(ref_date)
        ds = [(g.get("date") or "")[:10] for g in logs if g.get("date")]
        ds = [date.fromisoformat(d) for d in ds if d]
        ds = [d for d in ds if d < ref]   # ignore games on/after the run date
        if not ds:
            return None
        return (ref - max(ds)).days
    except Exception:
        return None


def _under_fields(logs, stat_key, uline, hr, opp):
    """Build under-candidate fields for ANY market from a player's own game logs.

    A fade qualifies on EITHER last-10 H/A vs THIS opponent OR last-10 H/A vs
    anyone clearing UNDER_THRESH, so genuine unders surface even when the player
    fails the OVER gate. underRate/Hits/Total are set to the qualifying basis
    (vs-opp preferred) so the card + ladder render the relevant sample.
    """
    vo = [g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10]
    an = [g for g in logs if g["homeRoad"] == hr][:10]
    vo_h = sum(1 for g in vo if g[stat_key] < uline); vo_t = len(vo)
    an_h = sum(1 for g in an if g[stat_key] < uline); an_t = len(an)
    vo_r = round(vo_h / vo_t * 100, 1) if vo_t else 0.0
    an_r = round(an_h / an_t * 100, 1) if an_t else 0.0
    vo_ok = vo_t >= UNDER_MIN_VO and vo_r >= UNDER_THRESH
    an_ok = an_t >= UNDER_MIN_ANY and an_r >= UNDER_THRESH
    if vo_ok:
        basis, uh, ut, ur = "vs opp", vo_h, vo_t, vo_r
    elif an_ok:
        basis, uh, ut, ur = "L10 H/A", an_h, an_t, an_r
    else:
        basis, uh, ut, ur = "", an_h, an_t, an_r
    return {
        "underOk": bool(vo_ok or an_ok), "underBasis": basis,
        "underHits": uh, "underTotal": ut, "underRate": ur, "underLine": uline,
        "underHitsVo": vo_h, "underTotVo": vo_t, "underRateVo": vo_r,
        "underHitsAny": an_h, "underTotAny": an_t, "underRateAny": an_r,
    }


def _parse_toi(s: str) -> int:
    """'MM:SS' → seconds. Returns 0 on failure."""
    try:
        parts = str(s or "0:00").split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return 0


def _hot_streak(logs: List[Dict], stat_key: str, line: float, hr: str, n: int = 5) -> Tuple[int, int]:
    """Count hits over `line` in last `n` H/A games. Returns (hits, total)."""
    games = [g for g in logs if g.get("homeRoad") == hr][:n]
    return sum(1 for g in games if g.get(stat_key, 0) > line), len(games)


async def get_opp_goalie_svpct(season: str) -> Dict[str, float]:
    """Returns team_abbrev → primary goalie season SV% (goalie with most GP on team).
    Used to display opposing goalie quality on each skater card."""
    import urllib.parse
    sort_p = urllib.parse.quote('[{"property":"gamesPlayed","direction":"DESC"}]')
    url = (f"{NHL_STATS}/goalie/summary"
           f"?isAggregate=false&isGame=false&sort={sort_p}"
           f"&start=0&limit=200&factCayenneExp=gamesPlayed>=3"
           f"&cayenneExp=gameTypeId=2 and seasonId<={season} and seasonId>={season}")
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
            data = await _fetch(url, c)
        if not data:
            return {}
        team_best: Dict[str, Dict] = {}
        for g in data.get("data", []):
            team = (g.get("teamAbbrevs") or "").strip()
            sv = float(g.get("savePct") or 0)
            gp = int(g.get("gamesPlayed") or 0)
            name = g.get("goalieFullName") or g.get("skaterFullName", "")
            if not team or "," in team:
                continue
            if team not in team_best or gp > team_best[team]["gp"]:
                team_best[team] = {"sv": sv, "gp": gp, "name": name}
        result = {team: round(v["sv"], 3) for team, v in team_best.items()}
        print(f"[Goalies] SV% map: {len(result)} teams")
        return result
    except Exception as e:
        print(f"[Goalies] SV% fetch error: {e}")
        return {}


async def get_shot_lines(target_date: str) -> Dict[str, Dict]:
    """Fetch real shots on goal lines from The Odds API.
    Tries icehockey_nhl first, then icehockey_nhl_championship (playoffs).
    Falls back to empty dict (algorithm still runs using 1.5 baseline).
    """
    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        print("[Lines] ODDS_API_KEY not set — using 1.5 baseline estimates")
        return {}, {}, {}, {}, {}

    _oc = _odds_cache_get("nhl", target_date)
    if _oc is not None:
        return (_oc.get("lines", {}), _oc.get("pts", {}),
                _oc.get("ast", {}), _oc.get("sv", {}), _oc.get("goals", {}))

    tomorrow = (date.fromisoformat(target_date) + timedelta(days=1)).isoformat()
    SPORT_KEYS = ["icehockey_nhl", "icehockey_nhl_championship"]

    try:
        lines: Dict[str, Dict] = {}
        pts_lines: Dict[str, Dict] = {}
        ast_lines: Dict[str, Dict] = {}
        sv_lines: Dict[str, Dict] = {}
        goal_lines: Dict[str, Dict] = {}
        async with httpx.AsyncClient(timeout=20) as c:
            for sport_key in SPORT_KEYS:
                r = await c.get(
                    f"{ODDS_API}/sports/{sport_key}/events",
                    params={"apiKey": api_key, "dateFormat": "iso"})
                if r.status_code != 200:
                    continue
                events = [e for e in r.json()
                          if e.get("commence_time", "")[:10] in (target_date, tomorrow)]
                print(f"[OddsAPI] {sport_key}: {len(events)} games for {target_date}")
                if not events:
                    continue

                for ev in events:
                    r2 = await c.get(
                        f"{ODDS_API}/sports/{sport_key}/events/{ev['id']}/odds",
                        params={"apiKey": api_key, "regions": "us",
                                "markets": ("player_shots_on_goal,player_points,"
                                            "player_assists,player_total_saves,player_goal_scorer"),
                                "oddsFormat": "american"})
                    if r2.status_code != 200:
                        continue
                    # Map each market key to its destination dict. We take the first
                    # bookmaker that posts a given market (Over AND Under come together
                    # in one market response, so the Under side is free).
                    targets = {
                        "player_shots_on_goal": lines,
                        "player_points":        pts_lines,
                        "player_assists":       ast_lines,
                        "player_total_saves":   sv_lines,
                        "player_goal_scorer":   goal_lines,
                    }
                    got = {k: False for k in targets}
                    for book in r2.json().get("bookmakers", []):
                        for mkt in book.get("markets", []):
                            mkey = mkt.get("key")
                            if mkey not in targets or got[mkey]:
                                continue
                            target = targets[mkey]
                            for oc in mkt.get("outcomes", []):
                                nm = oc.get("name")
                                if nm not in ("Over", "Under"):
                                    continue
                                player = oc.get("description", "").strip()
                                line   = float(oc.get("point") or 0)
                                if not player or line <= 0:
                                    continue
                                rec = target.setdefault(player, {
                                    "line": line, "odds": "",
                                    "under_odds": "", "source": "OddsAPI"})
                                if nm == "Over" and not rec["odds"]:
                                    rec["odds"] = str(oc.get("price", ""))
                                elif nm == "Under" and not rec["under_odds"]:
                                    rec["under_odds"] = str(oc.get("price", ""))
                            got[mkey] = True
                        if all(got.values()):
                            break

                if lines or pts_lines or ast_lines or sv_lines or goal_lines:
                    break  # found lines — no need to try next sport key

        print(f"[Lines] {len(lines)} shot | {len(pts_lines)} point | "
              f"{len(ast_lines)} assist | {len(sv_lines)} saves | {len(goal_lines)} goals lines from The Odds API")
        if lines or pts_lines or ast_lines or sv_lines or goal_lines:
            _odds_cache_set("nhl", target_date, {
                "lines": lines, "pts": pts_lines,
                "ast": ast_lines, "sv": sv_lines, "goals": goal_lines})
        return lines, pts_lines, ast_lines, sv_lines, goal_lines
    except Exception as e:
        print(f"[Lines] Odds API error: {e}")
        return {}, {}, {}, {}, {}



async def _lines_from_fanduel() -> Dict[str, Dict]:
    """DEPRECATED — removed. Odds API is the only source."""
    return {}


async def _lines_from_draftkings() -> Dict[str, Dict]:  # kept for reference — not called
    """DEPRECATED — DraftKings scraper removed. Odds API is the only source."""
    return {}


# ─────────────────────────────────────────────────────────────────────────────
#  NHL Skater Stats - season shot averages (replaces sportsbook props)
# ─────────────────────────────────────────────────────────────────────────────

def _match_odds_name(odds_name: str, roster: List[Dict]) -> Optional[Dict]:
    """Match Odds API player name to NHL roster player - handles accents & initials."""
    def norm(n):
        # Strip accents: Slafkovský → slafkovsky
        nfd = unicodedata.normalize("NFD", n)
        ascii_ = nfd.encode("ascii", "ignore").decode("ascii")
        return ascii_.lower().replace(".","").replace("-"," ").replace("'","").strip()
    on = norm(odds_name)
    # 1. Exact match
    for p in roster:
        if norm(p["name"]) == on: return p
    # 2. First initial + last name
    parts = on.split()
    if len(parts) >= 2:
        fi, last = parts[0][0], parts[-1]
        for p in roster:
            pp = norm(p["name"]).split()
            if len(pp) >= 2 and pp[0][0] == fi and pp[-1] == last:
                return p
    # 3. Last name only (for single-name odds entries)
    if parts:
        last = parts[-1]
        matches = [p for p in roster if norm(p["name"]).split()[-1] == last]
        if len(matches) == 1: return matches[0]
    return None


async def get_shot_qualified_players(
    games: List[Dict],
    sa_map: Dict[str, float],
    sem: asyncio.Semaphore,
    season: str = "20252026",
    lines_map: Dict = None,
) -> List[Dict]:
    """Build pool from ALL roster players using 1.5 as the algorithm line.
    Real sportsbook lines (from lines_map) are attached for display only."""
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

    # Always use ALL roster players - line=1.5 is the algorithm base
    for team, players in rosters.items():
        ctx = team_ctx.get(team, {})
        opp = ctx.get("opponent", "")
        hr  = ctx.get("homeRoad", "")
        for p in players:
            if p["id"] in seen:
                continue
            seen.add(p["id"])
            # Look up real sportsbook line for display only
            real_line, real_odds, line_source = None, "", "Est"
            real_under_odds = ""
            for odds_name, sb_info in lines_map.items():
                if _match_odds_name(odds_name, [p]):
                    real_line       = sb_info["line"]
                    real_odds       = sb_info.get("odds", "")
                    real_under_odds = sb_info.get("under_odds", "")
                    line_source     = sb_info.get("source", "OddsAPI")
                    break
            pool.append({
                "name":       p["name"],
                "pid":        p["id"],
                "team":       team,
                "opponent":   opp,
                "homeRoad":   hr,
                "line":       1.5,        # ALWAYS 1.5 for the algorithm
                "realLine":   real_line,  # Sportsbook line - display only
                "realOdds":   real_odds,
                "realUnderOdds": real_under_odds,
                "lineSource": line_source,
                "estLine":    1.5,
                "spg":        0,
                "oppSA":      sa_map.get(opp, 0.0),
            })

    print(f"[NHL] {len(pool)} roster players in pool | {len(lines_map)} real lines for display")
    pool.sort(key=lambda x: x["oppSA"], reverse=True)
    return pool

# ─────────────────────────────────────────────────────────────────────────────
#  Points picks - NHL Stats API game logs (independent of shots)
# ─────────────────────────────────────────────────────────────────────────────

async def _pts_season_logs(pid: int, season: str, c: httpx.AsyncClient) -> List[Dict]:
    """Fetch both regular season (2) and playoff (3) game logs."""
    logs = []
    for gtype in (2, 3):  # regular season + playoffs
        data = await _fetch(f"{NHL_API}/player/{pid}/game-log/{season}/{gtype}", c)
        if not data:
            continue
        for g in data.get("gameLog", []):
            goals   = int(g.get("goals",   0) or 0)
            assists = int(g.get("assists", 0) or 0)
            logs.append({
                "date":       g.get("gameDate",     ""),
                "points":     goals + assists,
                "goals":      goals,
                "assists":    assists,
                "toi_sec":    _parse_toi(g.get("toi", "0:00")),
                "pp_toi_sec": _parse_toi(g.get("powerPlayToi", "0:00")),
                "homeRoad":   g.get("homeRoadFlag", ""),
                "opponent":   g.get("opponentAbbrev", ""),
            })
    return logs


async def _goalie_season_logs(pid: int, season: str, c: httpx.AsyncClient) -> List[Dict]:
    """Goalie game logs — saves = shotsAgainst - goalsAgainst."""
    logs = []
    for gtype in (2, 3):  # regular season + playoffs
        data = await _fetch(f"{NHL_API}/player/{pid}/game-log/{season}/{gtype}", c)
        if not data:
            continue
        for g in data.get("gameLog", []):
            sa = int(g.get("shotsAgainst", 0) or 0)
            ga = int(g.get("goalsAgainst", 0) or 0)
            saves = sa - ga
            if saves < 0:
                saves = 0
            logs.append({
                "date":     g.get("gameDate",     ""),
                "saves":    saves,
                "homeRoad": g.get("homeRoadFlag", ""),
                "opponent": g.get("opponentAbbrev", ""),
            })
    return logs


async def get_pts_picks(
    games: List[Dict],
    sa_map: Dict[str, float],
    sem: asyncio.Semaphore,
    season: str = "20252026",
    pts_lines_map: Dict[str, Dict] = None,
    ast_lines_map: Dict[str, Dict] = None,
    target_date: str = None,
    goal_lines_map: Dict[str, Dict] = None,
    goalie_map: Dict[str, float] = None,
):
    """Independent points + assists + goals picks using NHL Stats API game logs.
    Returns (points_picks, assist_picks, points_unders, assist_unders, goal_picks, goal_unders)."""

    pts_lines_map = pts_lines_map or {}
    ast_lines_map = ast_lines_map or {}
    goal_lines_map = goal_lines_map or {}
    goalie_map = goalie_map or {}

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

    pts_picks, ast_picks, goal_picks = [], [], []
    pts_unders, ast_unders, goal_unders = [], [], []
    for player, team, opp, hr in all_players:
        logs = logs_map.get(player["id"], [])
        # Only players actually in today's rotation (drops scratches/AHL/injured depth)
        if target_date and not _played_recently(logs, target_date):
            continue

        # Career H/A vs today's opponent — cap at last 10 for consistency w/ shots
        c_logs = [g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10]
        # Last 10 H/A any opponent
        r_logs = [g for g in logs if g["homeRoad"] == hr][:10]

        if len(r_logs) < MIN_GAMES:
            continue

        def build_pick(stat_key, base_line, thresh, lines_map, mkt_label):
            """Normalized pick for one market (points or assists). Returns None
            if the player doesn't clear the threshold."""
            h3 = sum(1 for g in r_logs if g[stat_key] > base_line)
            r3 = round(h3 / len(r_logs) * 100, 1)
            avg3 = round(sum(g[stat_key] for g in r_logs) / len(r_logs), 2)
            h2 = sum(1 for g in c_logs if g[stat_key] > base_line) if c_logs else 0
            r2 = round(h2 / len(c_logs) * 100, 1) if c_logs else 0
            avg2 = round(sum(g[stat_key] for g in c_logs) / len(c_logs), 2) if c_logs else 0
            # Qualify on career H/A vs opp if we have it, else last-10 H/A
            qualifies = (r2 >= thresh) if len(c_logs) >= MIN_GAMES else (r3 >= thresh)
            over_ok = bool(qualifies)
            score = round((r2 + r3) / 2 if c_logs else r3, 1)
            real_line, real_odds, under_odds = None, "", ""
            for odds_name, sb_info in (lines_map or {}).items():
                if _match_odds_name(odds_name, [{"name": player["name"]}]):
                    real_line = sb_info.get("line")
                    real_odds = sb_info.get("odds", "")
                    under_odds = sb_info.get("under_odds", "")
                    break
            vsl_hits, vsl_total, vsl_rate = 0, 0, 0.0
            gap, tag = None, ""
            if real_line is not None and r_logs:
                vsl_hits = sum(1 for g in r_logs if g[stat_key] > real_line)
                vsl_total = len(r_logs)
                vsl_rate = round(vsl_hits / vsl_total * 100, 1) if vsl_total else 0.0
                gap = round(avg3 - real_line, 2)
                tag = _book_tag(real_line, avg3, vsl_rate)
            # Under track — vs-opp OR any-opp H/A (so genuine fades surface)
            uline = real_line if real_line is not None else base_line
            uf = _under_fields(logs, stat_key, uline, hr, opp)
            if not over_ok and not uf["underOk"]:
                return None
            # Game log for the per-card dropdown (vs opp if available, else L10 H/A)
            g_src = ([g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10]
                     or [g for g in logs if g["homeRoad"] == hr][:10])
            glog = [{"d": g["date"], "v": g[stat_key]} for g in g_src]
            # Signal factors
            toi_avg_sec = round(sum(g.get("toi_sec", 0) for g in r_logs) / len(r_logs)) if r_logs else 0
            pp_toi_avg_sec = round(sum(g.get("pp_toi_sec", 0) for g in r_logs) / len(r_logs)) if r_logs else 0
            hot_hits_p, hot_total_p = _hot_streak(logs, stat_key, base_line, hr, 5)
            rest_days_p = _days_rest(logs, target_date)
            opp_sv = goalie_map.get(opp)
            return {
                "name": player["name"], "pid": player["id"], "team": team,
                "opponent": opp, "homeRoad": hr, "oppSA": sa_map.get(opp, 0.0),
                "realLine": real_line, "realOdds": real_odds, "realUnderOdds": under_odds,
                "mkt": mkt_label,
                "dispLine": (real_line if real_line is not None else base_line),
                "avg": avg3, "avgA": avg2,
                "rateA": r2, "hitsA": h2, "totA": len(c_logs),
                "rateB": r3, "hitsB": h3, "totB": len(r_logs),
                "dispScore": score,
                "vsLineHits": vsl_hits, "vsLineTotal": vsl_total, "vsLineRate": vsl_rate,
                "gap": gap, "tag": tag,
                **uf, "overOk": over_ok,
                "glog": glog,
                "restDays": rest_days_p, "hotHits": hot_hits_p, "hotTotal": hot_total_p,
                "toiAvgSec": toi_avg_sec, "ppToiAvgSec": pp_toi_avg_sec,
                "oppGoalieSv": opp_sv,
            }

        pp = build_pick("points", PTS_LINE, HIT_THRESH_PTS, pts_lines_map, "Points (1+)")
        if pp:
            # Keep legacy point keys so the existing table + parlay code still works
            pp.update({
                "ptsOppAvg": pp["avgA"], "ptsHa10avg": pp["avg"],
                "pts2Hits": pp["hitsA"], "pts2Total": pp["totA"], "pts2Rate": pp["rateA"],
                "pts3Hits": pp["hitsB"], "pts3Total": pp["totB"], "pts3Rate": pp["rateB"],
                "ptsScore": pp["dispScore"],
            })
            if pp["overOk"]: pts_picks.append(pp)
            if pp["underOk"]: pts_unders.append(pp)

        ap = build_pick("assists", AST_LINE, HIT_THRESH_AST, ast_lines_map, "Assists (1+)")
        if ap:
            if ap["overOk"]: ast_picks.append(ap)
            if ap["underOk"]: ast_unders.append(ap)

        gp = build_pick("goals", 0.5, HIT_THRESH_GOALS, goal_lines_map, "Goals (1+)")
        if gp:
            if gp["overOk"]: goal_picks.append(gp)
            if gp["underOk"]: goal_unders.append(gp)

    pts_picks.sort(key=lambda x: (x["ptsScore"], x["oppSA"]), reverse=True)
    ast_picks.sort(key=lambda x: (x["dispScore"], x["oppSA"]), reverse=True)
    goal_picks.sort(key=lambda x: (x["dispScore"], x["oppSA"]), reverse=True)
    pts_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    ast_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    goal_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    print(f"[PTS] {len(pts_picks)} points | {len(ast_picks)} assists | {len(goal_picks)} goals | "
          f"{len(pts_unders)} pts unders | {len(ast_unders)} ast unders | {len(goal_unders)} goal unders")
    return pts_picks, ast_picks, pts_unders, ast_unders, goal_picks, goal_unders


async def get_saves_picks(
    games: List[Dict],
    sa_map: Dict[str, float],
    sem: asyncio.Semaphore,
    season: str = "20252026",
    sv_lines_map: Dict[str, Dict] = None,
    target_date: str = None,
) -> List[Dict]:
    """Goalie saves picks using NHL Stats API goalie game logs."""
    sv_lines_map = sv_lines_map or {}

    team_ctx: Dict[str, Dict] = {}
    for g in games:
        team_ctx[g["homeTeam"]] = {"opponent": g["awayTeam"], "homeRoad": "H"}
        team_ctx[g["awayTeam"]] = {"opponent": g["homeTeam"],  "homeRoad": "R"}

    roster_vals = await asyncio.gather(
        *[get_goalies(t, sem) for t in team_ctx], return_exceptions=True)
    rosters = {t: (r if isinstance(r, list) else [])
               for t, r in zip(team_ctx.keys(), roster_vals)}

    all_goalies = []
    seen = set()
    for team, players in rosters.items():
        ctx = team_ctx[team]
        for p in players:
            if p["id"] not in seen:
                seen.add(p["id"])
                all_goalies.append((p, team, ctx["opponent"], ctx["homeRoad"]))

    async def fetch_logs(pid):
        async with sem:
            async with httpx.AsyncClient(timeout=30) as c:
                results = await asyncio.gather(
                    *[_goalie_season_logs(pid, s, c) for s in SEASONS],
                    return_exceptions=True)
        logs = []
        for r in results:
            if isinstance(r, list):
                logs.extend(r)
        logs.sort(key=lambda x: x["date"], reverse=True)
        return logs

    log_tasks = {p["id"]: fetch_logs(p["id"]) for p, *_ in all_goalies}
    log_results = await asyncio.gather(*log_tasks.values(), return_exceptions=True)
    logs_map = {pid: (r if isinstance(r, list) else [])
                for pid, r in zip(log_tasks.keys(), log_results)}

    picks = []
    unders = []
    for goalie, team, opp, hr in all_goalies:
        logs = logs_map.get(goalie["id"], [])
        # Only goalies actively playing (drops third-string/AHL/injured goalies)
        if target_date and not _played_recently(logs, target_date):
            continue
        c_logs = [g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10]
        r_logs = [g for g in logs if g["homeRoad"] == hr][:10]
        if len(r_logs) < MIN_GAMES:
            continue

        # Real book line (player_total_saves) — fuzzy name match
        real_line, real_odds, under_odds = None, "", ""
        for odds_name, sb_info in sv_lines_map.items():
            if _match_odds_name(odds_name, [{"name": goalie["name"]}]):
                real_line  = sb_info.get("line")
                real_odds  = sb_info.get("odds", "")
                under_odds = sb_info.get("under_odds", "")
                break
        base_line = real_line if real_line is not None else SAVES_LINE

        h3 = sum(1 for g in r_logs if g["saves"] > base_line)
        r3 = round(h3 / len(r_logs) * 100, 1)
        avg3 = round(sum(g["saves"] for g in r_logs) / len(r_logs), 2)
        h2 = sum(1 for g in c_logs if g["saves"] > base_line) if c_logs else 0
        r2 = round(h2 / len(c_logs) * 100, 1) if c_logs else 0
        avg2 = round(sum(g["saves"] for g in c_logs) / len(c_logs), 2) if c_logs else 0

        qualifies = (r2 >= HIT_THRESH_SAVES) if len(c_logs) >= MIN_GAMES else (r3 >= HIT_THRESH_SAVES)
        over_ok = bool(qualifies)
        uf = _under_fields(logs, "saves", base_line, hr, opp)
        if not over_ok and not uf["underOk"]:
            continue
        score = round((r2 + r3) / 2 if c_logs else r3, 1)

        gap, tag = None, ""
        if real_line is not None:
            gap = round(avg3 - real_line, 2)
            tag = _book_tag(real_line, avg3, r3)

        g_src = c_logs or r_logs
        glog = [{"d": g["date"], "v": g["saves"]} for g in g_src]
        rest_days_sv = _days_rest(logs, target_date)
        hot_hits_sv, hot_total_sv = _hot_streak(logs, "saves", base_line, hr, 5)

        rec = {
            "name": goalie["name"], "pid": goalie["id"], "team": team,
            "opponent": opp, "homeRoad": hr, "oppSA": sa_map.get(opp, 0.0),
            "realLine": real_line, "realOdds": real_odds, "realUnderOdds": under_odds,
            "mkt": "Goalie Saves", "dispLine": base_line,
            "avg": avg3, "avgA": avg2,
            "rateA": r2, "hitsA": h2, "totA": len(c_logs),
            "rateB": r3, "hitsB": h3, "totB": len(r_logs),
            "dispScore": score,
            "vsLineHits": (h3 if real_line is not None else 0),
            "vsLineTotal": (len(r_logs) if real_line is not None else 0),
            "vsLineRate": (r3 if real_line is not None else 0.0),
            "gap": gap, "tag": tag,
            **uf, "overOk": over_ok,
            "glog": glog,
            "restDays": rest_days_sv, "hotHits": hot_hits_sv, "hotTotal": hot_total_sv,
            "toiAvgSec": 0, "ppToiAvgSec": 0, "oppGoalieSv": None,
        }
        if over_ok: picks.append(rec)
        if uf["underOk"]: unders.append(rec)

    picks.sort(key=lambda x: (x["dispScore"], x["avg"]), reverse=True)
    unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    print(f"[SAVES] {len(picks)} goalies over | {len(unders)} unders")
    return picks, unders



# ─────────────────────────────────────────────────────────────────────────────
#  Main algorithm
# ─────────────────────────────────────────────────────────────────────────────

async def _nhl_player_logs(pid: int, sem: asyncio.Semaphore) -> List[Dict]:
    """Fetch NHL game logs for a player across multiple seasons."""
    all_logs = []
    async with sem:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as c:
            results = await asyncio.gather(
                *[_fetch(f"{NHL_API}/player/{pid}/game-log/{s}/{gt}", c) for s in SEASONS for gt in (2, 3)],
                return_exceptions=True
            )
    for data in results:
        if not isinstance(data, dict): continue
        for g in data.get("gameLog", []):
            all_logs.append({
                "date":       g.get("gameDate", ""),
                "shots":      int(g.get("shots", 0) or 0),
                "goals":      int(g.get("goals", 0) or 0),
                "assists":    int(g.get("assists", 0) or 0),
                "points":     int(g.get("goals", 0) or 0) + int(g.get("assists", 0) or 0),
                "toi_sec":    _parse_toi(g.get("toi", "0:00")),
                "pp_toi_sec": _parse_toi(g.get("powerPlayToi", "0:00")),
                "homeRoad":   g.get("homeRoadFlag", ""),
                "opponent":   g.get("opponentAbbrev", ""),
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

    # ── Step 1 - games first; bail out on an off-day before any other fetch ────────
    games = await get_today_games(target_date)
    if not games:
        return {"no_games": True,
                "message": f"No NHL games scheduled for {target_date}.",
                "picks": [], "games": []}

    # Games exist — now fetch SA map, lines, and goalie SV% map in parallel.
    sa_map, _lines_tuple, goalie_map = await asyncio.gather(
        get_team_sa_map(season),
        get_shot_lines(target_date),
        get_opp_goalie_svpct(season),
    )
    lines_map, pts_lines_map, ast_lines_map, sv_lines_map, goal_lines_map = _lines_tuple
    _progress = {"stage": "Building player pool...", "done": 0, "total": 0, "pct": 25}

    # SA rankings for display
    playing = list({g["homeTeam"] for g in games} | {g["awayTeam"] for g in games})
    sa_ranks = sorted(
        [(t, sa_map.get(t, 0.0)) for t in playing],
        key=lambda x: x[1], reverse=True
    )

    # League-average shots-against/game — baseline for opponent-strength scaling
    _sa_vals = [v for v in sa_map.values() if v and v > 0]
    league_sa = round(sum(_sa_vals) / len(_sa_vals), 2) if _sa_vals else 0.0

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

    # ── Steps 2 & 3 - NHL Stats API hit-rate analysis ────────────────────────────
    async def analyze(p: Dict) -> Optional[Dict]:
        logs = logs_map.get(p["pid"], [])
        # Only players actually in today's rotation (drops scratches/AHL/injured depth)
        if not _played_recently(logs, target_date):
            return None
        hr, opp, line = p["homeRoad"], p["opponent"], p["line"]

        # Step 2: career H/A vs today's opponent
        h2, t2, r2, avg2 = _calc_hit_rate_from_logs(logs, line, hr, opponent=opp, last_n=10)
        # Step 3: last 10 H/A games any opponent
        h3, t3, r3, avg3 = _calc_hit_rate_from_logs(logs, line, hr, last_n=10)

        if t3 < MIN_GAMES:
            return None
        # Use lower threshold when no real sportsbook line (season avg fallback)
        s2_ok = (t2 < MIN_GAMES) or (r2 >= HIT_THRESH)
        s3_ok = r3 >= HIT_THRESH
        over_ok = bool(s2_ok and s3_ok)
        score = round((r2 + r3) / 2 if t2 >= MIN_GAMES else r3, 1)

        # NEW: hit rate vs real sportsbook line (last 10 H/A) + gap + tag
        real_line = p.get("realLine")
        vsl_hits, vsl_total, vsl_rate = 0, 0, 0.0
        gap = None
        tag = ""
        if real_line is not None:
            vsl_hits, vsl_total, vsl_rate, _ = _calc_hit_rate_from_logs(
                logs, real_line, hr, last_n=10)
            gap = round(avg3 - real_line, 2)
            tag = _book_tag(real_line, avg3, vsl_rate)

        # Under track (vs-opp OR any-opp H/A) + game log for the per-card dropdown
        uline = real_line if real_line is not None else line
        uf = _under_fields(logs, "shots", uline, hr, opp)
        if not over_ok and not uf["underOk"]:
            return None
        _ha = [g for g in logs if g["homeRoad"] == hr][:10]
        _gsrc = ([g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10] or _ha)
        glog = [{"d": g["date"], "v": g["shots"]} for g in _gsrc]

        # Opponent-adjusted projected shot count + edge vs the line
        rest_days = _days_rest(logs, target_date)
        proj, opp_factor, rest_factor = _proj_count(
            avg3, t3, avg2, t2, p.get("oppSA", 0.0), league_sa, rest_days)
        proj_line = real_line if real_line is not None else line
        proj_edge = round(proj - proj_line, 2)
        proj_pick = "OVER" if proj_edge > 0 else ("UNDER" if proj_edge < 0 else "")

        # Signal factors
        toi_avg_sec = round(sum(g.get("toi_sec", 0) for g in _ha) / len(_ha)) if _ha else 0
        pp_toi_avg_sec = round(sum(g.get("pp_toi_sec", 0) for g in _ha) / len(_ha)) if _ha else 0
        hot_hits, hot_total = _hot_streak(logs, "shots", line, hr, 5)
        opp_sv = goalie_map.get(opp)

        return {
            **p,
            "step2Hits": h2, "step2Total": t2, "step2Rate": r2,
            "step3Hits": h3, "step3Total": t3, "step3Rate": r3,
            "oppAvg": avg2, "ha10avg": avg3, "score": score,
            "vsLineHits": vsl_hits, "vsLineTotal": vsl_total, "vsLineRate": vsl_rate,
            "gap": gap, "tag": tag,
            "mkt": "Shots on Goal",
            "dispLine": (real_line if real_line is not None else line),
            "avg": avg3, "avgA": avg2,
            "rateA": r2, "hitsA": h2, "totA": t2,
            "rateB": r3, "hitsB": h3, "totB": t3,
            "dispScore": score,
            "realUnderOdds": p.get("realUnderOdds", ""),
            **uf, "overOk": over_ok,
            "proj": proj, "projEdge": proj_edge, "projPick": proj_pick,
            "oppFactor": opp_factor, "restFactor": rest_factor, "leagueSA": league_sa,
            "glog": glog,
            "restDays": rest_days, "hotHits": hot_hits, "hotTotal": hot_total,
            "toiAvgSec": toi_avg_sec, "ppToiAvgSec": pp_toi_avg_sec,
            "oppGoalieSv": opp_sv,
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
    picks = [r for r in results_raw if r and r.get("overOk")]
    shot_unders = [r for r in results_raw if r and r.get("underOk")]
    shot_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)

    _progress = {"stage": "Analyzing points...", "done": len(pool), "total": len(pool), "pct": 96}
    # ── Step 4 - rank shots & run independent points picks ───────────────────
    picks.sort(key=lambda x: (x.get("projEdge", -999), x["score"], x["oppSA"]), reverse=True)

    pts_all, ast_all, pts_unders, ast_unders, goal_all, goal_unders_all = await get_pts_picks(
        games, sa_map, sem_nhl, season, pts_lines_map, ast_lines_map, target_date, goal_lines_map,
        goalie_map=goalie_map)
    _progress = {"stage": "Analyzing goalie saves...", "done": len(pool), "total": len(pool), "pct": 98}
    saves_all, saves_unders = await get_saves_picks(games, sa_map, sem_nhl, season, sv_lines_map, target_date)
    _progress = {"stage": "Done!", "done": len(pool), "total": len(pool), "pct": 100}

    _result = {
        "picks":         picks[:TOP_N],
        "rest":          picks[TOP_N:TOP_N*2],
        "ptsPicks":      pts_all[:TOP_N],
        "ptsRest":       pts_all[TOP_N:TOP_N*2],
        "astPicks":      ast_all[:TOP_N],
        "astRest":       ast_all[TOP_N:TOP_N*2],
        "goalPicks":     goal_all[:TOP_N],
        "goalRest":      goal_all[TOP_N:TOP_N*2],
        "savesPicks":    saves_all[:TOP_N],
        "savesRest":     saves_all[TOP_N:TOP_N*2],
        "shotUnders":    shot_unders[:TOP_N],
        "shotUndersRest": shot_unders[TOP_N:TOP_N*2],
        "ptsUnders":     pts_unders[:TOP_N],
        "ptsUndersRest": pts_unders[TOP_N:TOP_N*2],
        "astUnders":     ast_unders[:TOP_N],
        "astUndersRest": ast_unders[TOP_N:TOP_N*2],
        "goalUnders":    goal_unders_all[:TOP_N],
        "goalUndersRest": goal_unders_all[TOP_N:TOP_N*2],
        "savesUnders":   saves_unders[:TOP_N],
        "savesUndersRest": saves_unders[TOP_N:TOP_N*2],
        "games":         games,
        "sa_ranks":      sa_ranks,
        "poolSize":      len(pool),
        "qualified":     len(picks),
        "ptsQualified":  len(pts_all),
        "astQualified":  len(ast_all),
        "savesQualified": len(saves_all),
        "season":        season,
        "targetDate":    target_date,
        "runTime":       datetime.utcnow().isoformat() + "Z",
        "date":          target_date,
    }
    try:
        from replit_push import push_picks_to_replit
        # Bake the picks into the page HTML so the Replit hub can serve an
        # instant, no-cold-start snapshot at moneypicksarena.com/dashboard/nhl.
        import json as _json
        _inject = (
            '<script>window.__INITIAL_PICKS__ = '
            + _json.dumps(_result).replace('</', '<\\/')
            + ';</script></head>'
        )
        _snapshot_html = HTML.replace('</head>', _inject, 1)
        push_picks_to_replit("nhl", _result, html=_snapshot_html)
    except Exception as _e:
        print(f"[replit_push] nhl push failed: {_e}")
    return _result

# ─────────────────────────────────────────────────────────────────────────────
#  HTML
# ─────────────────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>NHL Money Shots - Money Picks Arena</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Source+Sans+Pro:wght@300;400;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
/* responsive: phones & tablets (mobile fit) */
html,body{max-width:100%;overflow-x:hidden}
img{max-width:100%;height:auto}
@media (max-width:1200px){table{display:block;width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;white-space:nowrap}}
@media (max-width:560px){table{font-size:12px}table th,table td{padding:6px 8px}}
body{background:#0f0f0f;color:#fff;font-family:'Source Sans Pro',sans-serif;min-height:100vh}
.bg-glow{position:fixed;inset:0;background:radial-gradient(ellipse at 50% 20%,rgba(245,158,11,.05),transparent 65%);pointer-events:none;z-index:0}
nav{position:fixed;top:0;width:100%;background:rgba(10,10,10,.95);backdrop-filter:blur(12px);border-bottom:1px solid #1c1c1c;z-index:100;padding:0 32px;height:80px;display:flex;align-items:center;justify-content:space-between}
.logo{font-family:'Playfair Display',serif;font-size:28px;font-weight:900;color:#f59e0b;letter-spacing:.02em;line-height:1}
.logo span{color:#fff}
.nav-right{display:flex;align-items:center;gap:14px}
.nav-sport{background:#15803d;color:#fff;font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;padding:3px 10px;border-radius:4px}
.nav-app{font-size:13px;font-weight:600;color:#9ca3af;letter-spacing:.05em}
.page{position:relative;z-index:1;max-width:1300px;margin:0 auto;padding:104px 24px 40px}
.app-hdr{text-align:center;margin-bottom:36px}
.app-hdr h1{font-family:'Playfair Display',serif;font-size:2.6rem;font-weight:900;color:#fff;margin-bottom:6px}
.app-hdr h1 span{color:#f59e0b}
.app-hdr p{font-size:.85rem;color:#6b7280;letter-spacing:.15em;text-transform:uppercase}
.card{background:#161616;border:1px solid #262626;border-radius:20px;padding:24px;margin-bottom:16px}
.status-bar{display:flex;align-items:center;gap:16px;flex-wrap:wrap;padding:14px 20px;background:#161616;border:1px solid #262626;border-radius:14px;margin-bottom:20px}
.sdot{display:inline-flex;align-items:center;gap:6px;font-size:.82rem;font-weight:600;color:#6b7280}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.dot-green{background:#4ade80;animation:pulse 2s infinite}
.dot-amber{background:#f59e0b}
.dot-red{background:#ef4444}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.date-row{display:flex;align-items:center;justify-content:center;gap:12px;margin-bottom:20px}
.date-row label{color:#9ca3af;font-weight:600;font-size:.85rem;letter-spacing:.08em;text-transform:uppercase}
.date-row input{background:#0a0a0a;color:#fff;border:1px solid #2a2a2a;border-radius:10px;padding:10px 16px;font-size:.95rem;font-family:'Source Sans Pro',sans-serif;cursor:pointer;outline:none;transition:border .2s}
.date-row input:focus{border-color:#f59e0b}
.btn-run{background:#f59e0b;color:#000;border:none;border-radius:8px;padding:14px 52px;font-size:.95rem;font-weight:700;font-family:'Source Sans Pro',sans-serif;cursor:pointer;transition:all .2s}
.btn-run:hover{background:#fbbf24;transform:translateY(-1px);box-shadow:0 4px 20px rgba(245,158,11,.35)}
.btn-run:disabled{background:#2a2a2a;color:#4b5563;cursor:not-allowed;transform:none;box-shadow:none}
.status-msg{text-align:center;color:#6b7280;font-size:.85rem;margin-bottom:24px;min-height:20px}
.chips{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:12px;margin-bottom:28px}
.chip{background:#161616;border:1px solid #262626;border-top:3px solid #f59e0b;border-radius:14px;padding:16px 10px;text-align:center}
.chip .val{font-size:1.8rem;font-weight:900;color:#f59e0b;font-family:'Playfair Display',serif}
.chip .lbl{font-size:.65rem;color:#6b7280;text-transform:uppercase;letter-spacing:.1em;margin-top:4px;font-weight:600}
.sec{display:flex;align-items:center;gap:10px;font-size:.78rem;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.15em;margin:28px 0 12px}
.sec::after{content:'';flex:1;height:1px;background:rgba(245,158,11,.15)}
.games{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:10px;margin-bottom:24px}
.gcard{background:#161616;border:1px solid #262626;border-radius:14px;padding:14px;text-align:center;transition:border-color .2s}
.gcard:hover{border-color:#f59e0b}
.gcard .mu{font-size:1rem;font-weight:700;color:#fff}
.gcard .gt{font-size:.75rem;color:#6b7280;margin-top:5px}
.sa-list{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}
.sa-badge{background:#161616;border:1px solid #262626;border-radius:6px;padding:5px 12px;font-size:.8rem}
.sa-badge .rk{color:#f59e0b;font-weight:700}
.sa-badge .sv{color:#C8102E;font-weight:700}
.tbl-wrap{overflow-x:auto;border-radius:14px;border:1px solid #262626;margin-bottom:8px}
table{width:100%;border-collapse:collapse;background:#161616;table-layout:auto}
thead tr{border-bottom:1px solid rgba(245,158,11,.2)}
th{background:#1a1a1a;padding:8px 6px;text-align:center;font-size:.62rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:#f59e0b;white-space:nowrap;font-family:'Source Sans Pro',sans-serif;line-height:1.15}
td{padding:7px 6px;border-bottom:1px solid #1c1c1c;font-size:.78rem;white-space:nowrap;text-align:center}
.pname{font-size:.82rem}
tr:nth-child(even) td{background:#141414}
tr:hover td{background:#1c1c1c}
tr:last-child td{border-bottom:none}
.rk-num{font-weight:900;color:#f59e0b;font-size:1.1rem;font-family:'Playfair Display',serif}
.rk-rest{color:#4b5563;font-size:.9rem}
.pname{font-weight:700;color:#fff}
.tbadge{background:#1a1a1a;color:#9ca3af;padding:2px 8px;border-radius:4px;font-size:.74rem;border:1px solid #2a2a2a}
.home{background:rgba(74,222,128,.08);color:#4ade80;padding:3px 8px;border-radius:4px;font-size:.74rem;font-weight:700;border:1px solid rgba(74,222,128,.2)}
.away{background:rgba(239,68,68,.08);color:#f87171;padding:3px 8px;border-radius:4px;font-size:.74rem;font-weight:700;border:1px solid rgba(239,68,68,.2)}
.gold{color:#f59e0b;font-weight:700}
.green{color:#4ade80;font-weight:700}
.red-txt{color:#f87171;font-weight:700}
.score{color:#f59e0b;font-weight:900;font-size:1.05rem;font-family:'Playfair Display',serif}
.gray{color:#6b7280;font-size:.8rem}
.est{background:rgba(245,158,11,.08);color:#f59e0b;border:1px solid rgba(245,158,11,.2);padding:2px 8px;border-radius:4px;font-size:.78rem;font-weight:700}
.real-line{color:#4ade80;font-weight:900;font-size:1rem}
.odds-txt{color:#6b7280;font-size:.78rem}.tag-sug{background:#065f46;color:#d1fae5;padding:2px 6px;border-radius:4px;font-size:.72rem;font-weight:700}.tag-fade{background:#7f1d1d;color:#fecaca;padding:2px 6px;border-radius:4px;font-size:.72rem;font-weight:700}.gap-pos{color:#10b981;font-weight:600}.gap-neg{color:#ef4444;font-weight:600}.gap-zero{color:#6b7280}
.loading{text-align:center;padding:70px 20px}
.spin{width:48px;height:48px;border:3px solid rgba(245,158,11,.15);border-top:3px solid #f59e0b;border-radius:50%;animation:spin .8s linear infinite;margin:0 auto 18px}
@keyframes spin{to{transform:rotate(360deg)}}
.err-box{background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.2);border-radius:12px;padding:20px;text-align:center;color:#f87171;font-weight:700}
.no-picks{text-align:center;padding:50px;color:#4b5563}
.more-btn{width:100%;margin-top:6px;padding:11px 16px;background:#0f172a;border:1px solid #334155;border-radius:12px;font-size:.82rem;font-weight:700;cursor:pointer;letter-spacing:.04em;text-align:center}
.more-btn:hover{background:#1e293b}
details>summary{cursor:pointer;list-style:none;user-select:none}
details>summary::-webkit-details-marker{display:none}
footer{text-align:center;padding:32px 24px;color:#4b5563;font-size:.78rem;border-top:1px solid #1c1c1c;margin-top:24px;font-family:'Source Sans Pro',sans-serif}
.ft-logo{font-family:'Playfair Display',serif;color:#f59e0b;font-weight:700;font-size:.95rem;margin-bottom:6px}
.admin-only{display:none !important}
body.is-admin .admin-only{display:inline-block !important}
#parlayCard{display:none}
body.is-admin #parlayCard{display:block}
/* ===== NBA-style trading cards ===== */
.picks-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;margin-bottom:10px}
.nhl-toolbar{display:flex;justify-content:flex-end;margin:0 0 14px}
#nhlSearch{background:#111;color:#fff;border:1px solid #2a2a2a;border-radius:8px;padding:8px 14px;font-size:.9rem;outline:none;width:240px;max-width:60vw}
.pick-card{position:relative;background:linear-gradient(160deg,#1a1a1a,#121212);border:1px solid #2a2a2a;border-radius:18px;padding:18px 16px 14px;overflow:hidden;transition:border-color .2s,transform .2s}
.pick-card:hover{border-color:#f59e0b;transform:translateY(-2px)}
.pick-card.acc-pts{border-top:3px solid #60a5fa}
.pick-card.acc-shots{border-top:3px solid #f59e0b}
.pick-card.acc-ast{border-top:3px solid #a78bfa}
.pick-card.acc-sv{border-top:3px solid #34d399}
.pick-card.acc-goals{border-top:3px solid #34d399}
.sig-row{display:flex;flex-wrap:wrap;gap:4px;margin:6px 0 4px}
.sig-badge{font-size:.65rem;font-weight:700;padding:2px 7px;border-radius:999px;letter-spacing:.03em;white-space:nowrap}
.sig-b2b{background:rgba(248,113,113,.15);color:#f87171;border:1px solid rgba(248,113,113,.3)}
.sig-fresh{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.25)}
.sig-hot{background:rgba(251,146,60,.15);color:#fb923c;border:1px solid rgba(251,146,60,.3)}
.sig-cold{background:rgba(96,165,250,.12);color:#60a5fa;border:1px solid rgba(96,165,250,.25)}
.sig-toi{background:rgba(167,139,250,.1);color:#a78bfa;border:1px solid rgba(167,139,250,.25)}
.sig-pp{background:rgba(245,158,11,.1);color:#f59e0b;border:1px solid rgba(245,158,11,.25)}
.sig-sv-good{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.25)}
.sig-sv-avg{background:rgba(107,114,128,.15);color:#9ca3af;border:1px solid rgba(107,114,128,.25)}
.sig-sv-tough{background:rgba(248,113,113,.15);color:#f87171;border:1px solid rgba(248,113,113,.3)}
.pc-rank{position:absolute;top:10px;right:14px;font-family:'Playfair Display',serif;font-weight:900;font-size:1.6rem;color:rgba(245,158,11,.35)}
.pc-top{display:flex;align-items:center;gap:12px;margin-bottom:10px}
.hs-wrap{position:relative;width:58px;height:58px;border-radius:50%;flex:0 0 auto;background:#222;border:2px solid #333;overflow:visible;display:flex;align-items:center;justify-content:center}
.hs-img{width:100%;height:100%;object-fit:cover;position:absolute;inset:0;z-index:2;border-radius:50%}
.hs-ini{font-family:'Playfair Display',serif;font-weight:800;font-size:1.2rem;color:#9ca3af;z-index:1}
.pc-logo{width:22px;height:22px;position:absolute;bottom:-3px;right:-3px;z-index:3;background:#0f0f0f;border-radius:50%;padding:1px}
.pc-id{flex:1;min-width:0}
.pc-name{font-weight:800;color:#fff;font-size:1.02rem;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pc-meta{font-size:.74rem;color:#9ca3af;margin-top:4px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.pc-mkt{display:inline-block;font-size:.6rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#6b7280;margin-top:4px}
.pc-tagrow{min-height:1px;margin-bottom:8px}
.pc-line-row{display:flex;align-items:center;justify-content:space-between;background:#0e0e0e;border:1px solid #242424;border-radius:10px;padding:8px 12px;margin-bottom:10px}
.pc-line-row .ln{font-weight:900;color:#4ade80;font-size:1.05rem}
.pc-line-row .od{color:#6b7280;font-size:.76rem}
.pc-line-row .est{background:rgba(245,158,11,.08);color:#f59e0b;border:1px solid rgba(245,158,11,.2);padding:2px 8px;border-radius:5px;font-size:.82rem;font-weight:700}
.pc-proj{display:flex;align-items:center;gap:8px;background:rgba(245,158,11,.07);border:1px solid rgba(245,158,11,.25);border-radius:10px;padding:7px 12px;margin-bottom:10px}
.pc-proj .pp-lab{font-size:.62rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#9ca3af}
.pc-proj .pp-num{font-family:'Playfair Display',serif;font-weight:900;color:#f59e0b;font-size:1.25rem;margin-left:auto}
.pc-proj .pp-edge{font-size:.8rem;font-weight:800}
.pos{color:#4ade80}
.neg{color:#f87171}
.lad-why{font-size:.72rem;color:#9ca3af;padding:6px 4px;line-height:1.4}
.pc-stats{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px}
.pc-stat{background:#141414;border:1px solid #222;border-radius:9px;padding:8px;text-align:center}
.pc-stat .k{font-size:.56rem;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;font-weight:700}
.pc-stat .v{font-weight:800;font-size:.92rem;margin-top:3px}
.pc-foot{display:flex;align-items:center;justify-content:space-between;gap:8px}
.pc-score{font-family:'Playfair Display',serif;font-weight:900;color:#f59e0b;font-size:1.15rem}
.pc-tap{background:none;border:1px solid #333;color:#9ca3af;border-radius:8px;padding:6px 10px;font-size:.7rem;font-weight:700;cursor:pointer;transition:all .2s}
.pc-tap:hover{border-color:#f59e0b;color:#f59e0b}
.uplays{background:#141414;border:1px solid #242424;border-radius:14px;padding:4px 4px;margin-bottom:10px}
.uprow{display:flex;align-items:center;justify-content:space-between;padding:9px 12px;border-bottom:1px solid #1c1c1c;cursor:pointer}
.uprow:last-child{border-bottom:none}
.uprow:hover{background:#1a1a1a}
.uprow .nm{font-weight:700;color:#fff;font-size:.82rem}
.uprow .mt{color:#6b7280;font-size:.72rem;margin-top:2px}
.special-wrap{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:10px}
@media(max-width:680px){.special-wrap{grid-template-columns:1fr}}
.sp-col{background:#141414;border:1px solid #242424;border-radius:14px;padding:14px}
.sp-col h4{font-size:.72rem;font-weight:800;color:#f59e0b;text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px}
.sp-row{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 6px;border-bottom:1px solid #1c1c1c;cursor:pointer}
.sp-row:last-child{border-bottom:none}
.sp-row:hover{background:#1a1a1a}
.sp-row .nm{font-weight:700;color:#fff;font-size:.82rem}
.sp-row .mt{color:#6b7280;font-size:.72rem;margin-top:2px}
.lad-ov{position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:200;display:flex;align-items:center;justify-content:center;padding:18px}
.lad-modal{background:#161616;border:1px solid #2a2a2a;border-radius:18px;max-width:460px;width:100%;max-height:86vh;overflow-y:auto;padding:22px}
.lad-modal h3{font-family:'Playfair Display',serif;color:#fff;font-size:1.25rem;margin-bottom:2px}
.lad-sub{color:#9ca3af;font-size:.8rem;margin-bottom:14px}
.lad-close{float:right;background:none;border:1px solid #333;color:#9ca3af;border-radius:8px;padding:4px 10px;cursor:pointer;font-weight:700}
.lad-glog{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0 14px}
.glchip{background:#0e0e0e;border:1px solid #242424;border-radius:8px;padding:6px 8px;text-align:center;min-width:44px}
.glchip .d{font-size:.56rem;color:#6b7280}
.glchip .v{font-weight:800;font-size:.95rem;margin-top:2px;color:#e5e7eb}
.glchip.hit{border-color:rgba(74,222,128,.35)}
.glchip.hit .v{color:#4ade80}
.glchip.miss .v{color:#f87171}
.lad-stat{display:flex;justify-content:space-between;align-items:center;padding:8px 4px;border-bottom:1px solid #1c1c1c;font-size:.85rem}
.lad-stat:last-child{border-bottom:none}
.lad-stat .k{color:#9ca3af}
.lad-stat .v{font-weight:700}
</style>
</head>
<body>
<div class="bg-glow"></div>

<nav>
  <div class="logo">Money <span>Picks</span> Arena</div>
  <div style="display:flex;gap:8px;align-items:center"><button class="admin-only" onclick="openNhlMyBets()" style="background:#0e7490;color:#fff;border:none;border-radius:10px;padding:9px 16px;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">&#128176; My Bets</button></div>
</nav>

<style>
.nhl-bets-tbl{width:100%;border-collapse:collapse;font-size:.82rem}
.nhl-bets-tbl th{padding:7px 10px;text-align:left;font-size:.72rem;color:#94a3b8;font-weight:700;text-transform:uppercase;letter-spacing:.07em;border-bottom:1px solid #1e293b;white-space:nowrap}
.nhl-bets-tbl td{padding:8px 10px;border-bottom:1px solid #0f172a;vertical-align:middle;color:#e2e8f0}
.nhl-bets-tbl tr:last-child td{border-bottom:none}
.nhl-bets-tbl tr:hover td{background:rgba(255,255,255,.02)}
</style>
<div id="nhl-mybets-card" style="display:none;max-width:960px;margin:0 auto 24px;padding:0 16px">
  <div class="card" style="padding:20px 22px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.4rem;font-weight:700;color:#fff">&#128176; My Bets</h2>
      <button onclick="document.getElementById(&#39;nhl-mybets-card&#39;).style.display=&#39;none&#39;" style="background:#1e293b;border:none;color:#94a3b8;border-radius:8px;padding:8px 11px;font-size:.9rem;cursor:pointer">&#215;</button>
    </div>
    <div id="nhl-mybets-body"><p style="color:#94a3b8;font-size:.85rem">Loading&#8230;</p></div>
  </div>
</div>

<div class="page">
  <div class="app-hdr">
    <h1>NHL <span>Money Shots</span></h1>
    <p>Shots &nbsp;·&nbsp; Points &nbsp;·&nbsp; Assists &nbsp;·&nbsp; Goalie Saves</p>
  </div>

  <div class="card" style="text-align:center">
    <h2 style="font-family:'Playfair Display',serif;font-size:1.5rem;font-weight:700;color:#fff;margin-bottom:6px">Run Today\'s Picks</h2>
    <p style="color:#6b7280;font-size:.88rem;margin-bottom:22px">Select a date - NHL Stats API powers all hit rates</p>
    <div class="date-row">
      <label>Date</label>
      <input type="date" id="datePicker"/>
    </div>
    <button class="btn-run" id="getBtn" onclick="getPicks()">🎯 Get Picks</button>
    <button class="btn-run admin-only" id="runBtn" onclick="runPicks()" style="margin-left:10px">Run Picks</button>
  </div>

  <div class="card" id="parlayCard" style="text-align:center;max-width:600px;margin:20px auto 0">
    <h2 style="font-family:'Playfair Display',serif;font-size:1.3rem;font-weight:700;color:#fff;margin-bottom:6px">🎰 Auto Parlay Builder <span style="font-size:.7rem;color:#777;font-family:sans-serif">admin only</span></h2>
    <p style="font-size:.74rem;color:#888;margin-bottom:14px">Best available legs from today&#39;s board — priced odds combined</p>
    <div style="display:flex;gap:10px;justify-content:center;align-items:center;flex-wrap:wrap">
      <label style="color:#9ca3af;font-size:.85rem;font-weight:600">Legs
        <select id="parlayLegs" style="background:#1a1a1a;color:#fff;border:1px solid #333;border-radius:8px;padding:8px 12px;font-size:.9rem;font-weight:700;margin-left:6px">
          <option>2</option><option selected>3</option><option>4</option><option>5</option><option>6</option><option>7</option><option>8</option><option>9</option><option>10</option>
        </select>
      </label>
      <button class="btn-run" onclick="buildParlay()">Build Best Parlay</button>
      <button class="btn-run" onclick="generateParlay()" style="background:#1f2937">🎲 Generate New</button>
    </div>
    <div id="parlayResult" style="margin-top:16px;text-align:left"></div>
  </div>

  <div class="status-msg" id="statusMsg"></div>
  <div id="out"></div>
</div>

<footer>
  <div class="ft-logo">Money Picks Arena</div>
  <div>NHL Money Shots &nbsp;·&nbsp; NHL Stats API + Sportsbook Lines</div>
  <div style="margin-top:8px;font-size:.7rem">For entertainment and informational purposes only. We do not accept bets or guarantee results. Please gamble responsibly. Must be 18+.</div>
</footer>

<script>
// Set date to today
document.addEventListener('DOMContentLoaded', function(){


  var dp = document.getElementById('datePicker');
  var today = new Date().toISOString().split('T')[0];
  dp.value = today;

  // Snapshot mode: hub serves this page with picks baked in as
  // window.__INITIAL_PICKS__ — skip the /api/picks fetch and render
  // straight from the snapshot.
  if (window.__INITIAL_PICKS__) {
    try {
      var data = window.__INITIAL_PICKS__;
      if (dp && data.date) dp.value = data.date;
      renderResults(data);
      var st = document.getElementById('statusMsg');
      if (st && data.picks) {
        st.textContent = (data.qualified || 0) + ' players qualified -- ' +
                         data.picks.length + ' top picks -- ' + (data.date || '');
      }
    } catch (e) { console.error('snapshot render failed', e); }
  }

});

// STEP 1: Connect
async function checkStatus(){
  try{
    var r=await fetch('/api/status'); var d=await r.json();
    var o=document.getElementById('odds-status');
    var f=document.getElementById('fd-status');
    if(o){var od=o.querySelector('.dot');if(od)od.className='dot '+(d.odds_api==='configured'?'dot-green':'dot-red');o.lastChild.textContent=d.odds_api==='configured'?' Odds API: Ready':' Odds API: Not configured';}
    if(f){var fd=f.querySelector('.dot');if(fd)fd.className='dot '+(d.fanduel==='configured'?'dot-green':'dot-amber');f.lastChild.textContent=d.fanduel==='configured'?' FanDuel: Ready':' FanDuel: Not set';}
  }catch(e){}
}
document.addEventListener('DOMContentLoaded',checkStatus);

(function(){
  var KEY='__mpa_token';
  var p=new URLSearchParams(window.location.search);
  var t=p.get('token');
  if(t){localStorage.setItem(KEY,t);window.history.replaceState({},'',window.location.pathname);}
  if(!localStorage.getItem(KEY)){window.location.href='https://moneypicksarena.com';}
})();
function _applyAdmin(){if(window.IS_ADMIN){document.body&&document.body.classList.add('is-admin');}else{var _wt=localStorage.getItem('__mpa_token')||'';if(_wt){fetch('/api/whoami?token='+encodeURIComponent(_wt)).then(function(r){return r.json();}).then(function(d){if(d&&d.is_admin){window.IS_ADMIN=true;document.body&&document.body.classList.add('is-admin');}}).catch(function(){});}}}
if(document.readyState==='loading'){document.addEventListener('DOMContentLoaded',_applyAdmin);}else{_applyAdmin();}

// ===== Admin Auto Parlay Builder (NHL) =====
function _amToDec(a){var s=String(a==null?'':a).replace('+','').trim();var n=parseFloat(s);if(!n||isNaN(n))return null;return n>0?1+n/100:1+100/Math.abs(n);}
function _decToAm(d){if(!d||d<=1)return null;return d>=2?'+'+Math.round((d-1)*100):'-'+Math.round(100/(d-1));}
function _fmtOdds(o){if(o==null||o==='')return null;var s=String(o).trim();if(!s||s==='0')return null;return (s.charAt(0)==='-'||s.charAt(0)==='+')?s:'+'+s;}
function _floorOk(odds){if(odds==null||odds==='')return true;var a=parseFloat(odds);if(isNaN(a)||a===0)return true;return a>=-500;}
function _legScore(c){return (c.hasOdds?1:0)*1e9+(c.rate||0)*1e4+(c.dec?Math.min(c.dec,11)*100:0);}
function _nhlLeg(p){
  var market=p.mkt||((p.pts2Hits!=null||p.ptsHa10avg!=null)?'Points (1+)':'Shots on Goal');
  var line=(p.realLine!=null?p.realLine:(p.dispLine!=null?p.dispLine:1.5));
  var rate=(p.vsLineRate||p.rateB||p.rateA||p.step3Rate||p.pts3Rate||0);
  var odds=p.realOdds||'';var dec=_amToDec(odds);
  return {player:p.name,team:p.team||'',opp:p.opponent||'',market:market,dir:'OVER',line:line,rate:Math.round(rate||0),odds:odds,dec:dec,hasOdds:!!dec};
}
function _parlayPool(){
  var plays=window.__NHL_PLAYS__||[];var byP={};
  plays.forEach(function(p){
    if(!p||!p.name)return;
    var c=_nhlLeg(p);
    if(!_floorOk(c.odds))return;
    var cur=byP[c.player];
    if(!cur||_legScore(c)>_legScore(cur))byP[c.player]=c;
  });
  return Object.keys(byP).map(function(k){return byP[k];}).sort(function(a,b){return _legScore(b)-_legScore(a);});
}
function _shuffle(a){for(var i=a.length-1;i>0;i--){var j=Math.floor(Math.random()*(i+1));var t=a[i];a[i]=a[j];a[j]=t;}return a;}
function closeParlay(){var o=document.getElementById('parlayResult');if(o)o.innerHTML='';}
function buildParlay(){_renderParlay(false);}
function generateParlay(){_renderParlay(true);}
function _renderParlay(randomize){
  var sel=document.getElementById('parlayLegs');
  var n=parseInt(sel?sel.value:'3',10)||3;
  var out=document.getElementById('parlayResult');
  if(!out)return;
  var cands=_parlayPool();
  if(!cands.length){out.innerHTML='<div style="color:#888;padding:10px">Run today&#39;s picks first, then build a parlay.</div>';return;}
  if(cands.length<n){out.innerHTML='<div style="color:#f87171;padding:10px">Only '+cands.length+' qualifying play'+(cands.length!==1?'s':'')+' on the board. Pick a smaller parlay.</div>';return;}
  function _pick(ordered,avoid){var used={},picked=[],i,c;for(i=0;i<ordered.length&&picked.length<n;i++){c=ordered[i];if(used[c.player])continue;if(avoid&&avoid[c.player])continue;used[c.player]=1;picked.push(c);}for(i=0;i<ordered.length&&picked.length<n;i++){c=ordered[i];if(used[c.player])continue;used[c.player]=1;picked.push(c);}return picked;}
  var legs;
  if(randomize){var avoid=null;if(window._lastParlay&&window._lastParlay.length){avoid={};window._lastParlay.forEach(function(pl){avoid[pl]=1;});}legs=_pick(_shuffle(cands.slice()),avoid).sort(function(a,b){return _legScore(b)-_legScore(a);});}
  else{legs=_pick(cands.slice(),null);}
  window._lastParlay=legs.map(function(l){return l.player;});
  var dec=1,priced=0,missing=0;
  legs.forEach(function(l){if(l.dec){dec*=l.dec;priced++;}else{missing++;}});
  var am=priced?_decToAm(dec):null;var payout=priced?(100*dec):null;
  var dirColor=function(d){return d==='OVER'?'#4ade80':d==='UNDER'?'#f87171':'#9ca3af';};
  var rows=legs.map(function(l,i){var fo=_fmtOdds(l.odds);return '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #1a1a1a">'
    +'<div style="min-width:0">'
    +'<div style="font-weight:800;color:#fff;font-size:.85rem">'+(i+1)+'. '+l.player+' <span style="color:#777;font-size:.7rem">'+l.team+(l.opp?(' vs '+l.opp):'')+'</span></div>'
    +'<div style="color:#999;font-size:.72rem;margin-top:2px">'+l.market+(l.line!=null?(' · line '+l.line):'')+(l.rate?(' · '+l.rate+'% hit'):'')+'</div>'
    +'</div>'
    +'<div style="text-align:right;white-space:nowrap">'
    +'<div style="color:'+dirColor(l.dir)+';font-weight:900;font-size:.8rem">'+l.dir+'</div>'
    +'<div style="color:#f59e0b;font-size:.72rem;font-weight:800">'+(fo||'odds N/A')+'</div>'
    +'</div></div>';}).join('');
  var header='<div style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;border-bottom:1px solid #262626;background:#121212">'
    +'<span style="font-weight:800;color:#ccc;font-size:.74rem">'+(randomize?'RANDOM MIX':'TOP PLAYS')+'</span>'
    +'<span onclick="closeParlay()" title="Close" style="cursor:pointer;color:#888;font-weight:900;font-size:1.15rem;line-height:1;padding:0 6px">×</span></div>';
  var summary='<div style="display:flex;justify-content:space-between;align-items:center;padding:12px;background:linear-gradient(135deg,rgba(245,158,11,.12),rgba(245,158,11,.02));border-top:1px solid #262626">'
    +'<div style="font-weight:900;color:#f59e0b">'+n+'-LEG PARLAY</div>'
    +'<div style="text-align:right">'+(am?('<div style="font-weight:900;color:#4ade80;font-size:1.05rem">'+am+'</div><div style="color:#999;font-size:.7rem">$100 → $'+payout.toFixed(2)+(missing?(' · '+priced+'/'+n+' legs priced'):'')+'</div>'):('<div style="color:#888;font-size:.78rem">No book odds available for these legs</div>'))+'</div>'
    +'</div>';
  out.innerHTML='<div style="background:#0e0e0e;border:1px solid #262626;border-radius:12px;overflow:hidden">'+header+rows+summary+'</div>';
}

// Get Picks: load saved picks for the chosen date (read-only, never runs the pipeline).
async function getPicks(){
  var btn=document.getElementById('getBtn');
  var st=document.getElementById('statusMsg');
  var out=document.getElementById('out');
  var dt=document.getElementById('datePicker').value;
  var orig=btn.textContent;
  btn.disabled=true; btn.textContent='Loading...';
  if(st) st.textContent='Loading saved picks for '+dt+'...';
  try{
    var _nhlTok=localStorage.getItem('__mpa_token')||'';
    var res=await fetch('/api/cached?target_date='+dt+'&token='+encodeURIComponent(_nhlTok));
    if(res.status===404){ if(st) st.textContent=''; if(out) out.innerHTML=''; alert("Today's picks aren't ready yet -- check back a little later."); return; }
    if(!res.ok){ throw new Error('Could not load picks.'); }
    var data=await res.json();
    renderResults(data);
    if(st && data.picks){ st.textContent=(data.qualified||0)+' players qualified -- '+data.picks.length+' top picks -- '+(data.date||''); }
  }catch(e){ if(st) st.textContent=''; alert(e.message||'Could not load picks. Please try again.'); }
  finally{ btn.disabled=false; btn.textContent=orig; }
}

// STEP 2: Run picks
async function runPicks(){
  var btn = document.getElementById('runBtn');
  var st = document.getElementById('statusMsg');
  var out = document.getElementById('out');
  var dt = document.getElementById('datePicker').value;
  btn.disabled = true;
  btn.textContent = 'Running...';
  st.textContent = 'Fetching games and analyzing players for ' + dt + '...';
  out.innerHTML = '<div class="loading"><div class="spin"></div>' +
    '<p style="color:#9ca3af;margin-bottom:16px" id="prog-stage">Starting...</p>' +
    '<div style="background:rgba(245,158,11,.1);border-radius:6px;height:8px;width:280px;margin:0 auto 8px;overflow:hidden">' +
    '<div id="prog-bar" style="height:100%;width:5%;background:#f59e0b;border-radius:6px;transition:width .5s"></div></div>' +
    '<p style="color:#6b7280;font-size:.8rem" id="prog-pct">5%</p></div>';

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
    var _nhlTok=localStorage.getItem('__mpa_token')||'';
    var res = await fetch('/api/picks?target_date=' + dt + '&token=' + encodeURIComponent(_nhlTok));
    var data = await res.json();
    if(data.no_games){
      out.innerHTML = '<div style="text-align:center;padding:40px 20px;color:#9ca3af"><h2 style="color:#f59e0b;margin-bottom:8px">No Games Today</h2><p>' + (data.message || ('No NHL games scheduled for ' + dt + '.')) + ' Check back on game day.</p></div>';
      st.textContent = '';
    } else if(data.error){
      out.innerHTML = '<div class="err-box">' + data.error + '</div>';
      st.textContent = '';
    } else {
      renderResults(data);
      if(st) st.textContent = data.qualified + ' players qualified -- ' + data.picks.length + ' top picks -- ' + dt;
    }
  } catch(e) {
    out.innerHTML = '<div class="err-box">Error: ' + e.message + '</div>';
  } finally {
    clearInterval(pollTimer);
    btn.disabled = false;
    btn.textContent = 'Run Picks';
  }
}

function rateClass(r){ return r >= 90 ? 'green' : r >= 80 ? 'gold' : 'red-txt'; }

// ===== NBA-style cards (NHL) =====
window.__NHLLAD__ = window.__NHLLAD__ || {};
function _initials(name){
  var parts=String(name||'').trim().split(/\s+/);
  if(!parts.length||!parts[0]) return '?';
  if(parts.length===1) return parts[0].slice(0,2).toUpperCase();
  return (parts[0][0]+parts[parts.length-1][0]).toUpperCase();
}
function _accFor(mkt){
  if(mkt==='Points (1+)') return 'acc-pts';
  if(mkt==='Assists (1+)') return 'acc-ast';
  if(mkt==='Goalie Saves') return 'acc-sv';
  if(mkt==='Goals (1+)') return 'acc-goals';
  return 'acc-shots';
}
function _fmtToi(sec){
  if(!sec||sec<60) return '';
  var m=Math.floor(sec/60),s=sec%60;
  return m+':'+(s<10?'0':'')+s;
}
function _sigBadges(p){
  var out='';
  var rd=p.restDays;
  if(rd!=null){
    if(rd<=1) out+='<span class="sig-badge sig-b2b">B2B / No Rest</span>';
    else if(rd>=3) out+='<span class="sig-badge sig-fresh">'+rd+'d Rest</span>';
  }
  var hh=p.hotHits, ht=p.hotTotal;
  if(ht>=3){
    if(hh>=4) out+='<span class="sig-badge sig-hot">&#128293; '+hh+'/'+ht+' Hot</span>';
    else if(hh<=1) out+='<span class="sig-badge sig-cold">&#10052; '+hh+'/'+ht+' Cold</span>';
  }
  var toi=_fmtToi(p.toiAvgSec);
  if(toi) out+='<span class="sig-badge sig-toi">'+toi+' TOI</span>';
  if(p.ppToiAvgSec>60){var pp=_fmtToi(p.ppToiAvgSec);out+='<span class="sig-badge sig-pp">'+pp+' PP</span>';}
  var sv=p.oppGoalieSv;
  if(sv!=null&&sv>0){
    var svStr=sv.toFixed(3);
    var cls=sv<0.895?'sig-sv-good':sv>0.915?'sig-sv-tough':'sig-sv-avg';
    out+='<span class="sig-badge '+cls+'">Opp G .'+Math.round(sv*1000)+'</span>';
  }
  return out?'<div class="sig-row">'+out+'</div>':'';
}
function _ladKey(p){ return 'nlad_'+p.pid+'_'+String(p.mkt||'').replace(/[^a-z]/gi,''); }
function _rateHtml(rate,hits,tot){
  if(!tot) return '<span class="gray">—</span>';
  return '<span class="'+rateClass(rate)+'">'+hits+'/'+tot+' ('+rate+'%)</span>';
}
function nhlCard(p,i){
  var season=(window.__NHL_SEASON__||'20252026');
  var key=_ladKey(p); window.__NHLLAD__[key]=p;
  var ha=p.homeRoad==='H';
  var head='https://assets.nhle.com/mugs/nhl/'+season+'/'+p.team+'/'+p.pid+'.png';
  var logo='https://assets.nhle.com/logos/nhl/svg/'+p.team+'_light.svg';
  var lineHtml=(p.realLine!=null)
    ? `<span class="ln">${p.dispLine}</span> <span class="od">${p.realOdds||''}</span>`
    : `<span class="est">~${p.dispLine}</span>`;
  var lastStat=(p.realLine!=null&&p.vsLineTotal)
    ? `<div class="pc-stat"><div class="k">vs Book L10</div><div class="v ${rateClass(p.vsLineRate)}">${p.vsLineHits}/${p.vsLineTotal} (${p.vsLineRate}%)</div></div>`
    : `<div class="pc-stat"><div class="k">Under L10</div><div class="v ${rateClass(p.underRate)}">${p.underHits}/${p.underTotal} (${p.underRate}%)</div></div>`;
  return `
   <div class="pick-card ${_accFor(p.mkt)}">
     <div class="pc-rank">${i}</div>
     <div class="pc-top">
       <div class="hs-wrap"><span class="hs-ini">${_initials(p.name)}</span>
         <img class="hs-img" src="${head}" onerror="this.style.display='none'"/>
         <img class="pc-logo" src="${logo}" onerror="this.style.display='none'"/>
       </div>
       <div class="pc-id">
         <div class="pc-name">${p.name}</div>
         <div class="pc-meta">${p.team} vs ${p.opponent} <span class="${ha?'home':'away'}">${ha?'HOME':'AWAY'}</span></div>
         <div class="pc-mkt">${p.mkt||''}</div>
       </div>
     </div>
     <div class="pc-tagrow">${fmtTag(p.tag)}</div>
     ${_sigBadges(p)}
     <div class="pc-line-row"><span>${lineHtml}</span><span class="od">Line</span></div>
     ${p.proj!=null?`<div class="pc-proj"><span class="pp-lab">Projected</span><span class="pp-num">${p.proj}</span><span class="pp-edge ${p.projEdge>=0?'pos':'neg'}">${p.projEdge>=0?'+':''}${p.projEdge}</span></div>`:''}
     <div class="pc-stats">
       <div class="pc-stat"><div class="k">Career vs ${p.opponent}</div><div class="v">${_rateHtml(p.rateA,p.hitsA,p.totA)}</div></div>
       <div class="pc-stat"><div class="k">L10 ${ha?'Home':'Away'}</div><div class="v">${_rateHtml(p.rateB,p.hitsB,p.totB)}</div></div>
       <div class="pc-stat"><div class="k">Avg</div><div class="v gold">${p.avg}</div></div>
       ${lastStat}
     </div>
     <div class="pc-foot"><span class="pc-score">${p.dispScore}</span>
       <span style="display:flex;gap:6px">${_nhlBetBtn(p)}<button class="pc-tap" onclick="openNhlLadder('${key}')">📊 Game Log</button></span></div>
   </div>`;
}
function nhlCardGrid(picks){
  if(!picks||!picks.length) return '<div class="no-picks">No qualifying picks for this market.</div>';
  return '<div class="picks-grid">'+picks.map(function(p,i){return nhlCard(p,i+1);}).join('')+'</div>';
}
function nhlRestBlock(rest, label, color){
  if(!rest || !rest.length) return '';
  var c = color || '#4ade80';
  return '<details style="margin-top:8px"><summary class="more-btn" style="color:'+c+';border-color:'+c+'33">&#9655; '+rest.length+' more '+label+'</summary>'
    + '<div class="picks-grid" style="margin-top:12px">'
    + rest.map(function(p,i){return nhlCard(p, 10+i+1);}).join('')
    + '</div></details>';
}
function underClass(r){ return r>=75?'green':r>=65?'gold':'red-txt'; }
function nhlUnderCard(p,i){
  var season=(window.__NHL_SEASON__||'20252026');
  var key=_ladKey(p); window.__NHLLAD__[key]=p;
  var ha=p.homeRoad==='H';
  var head='https://assets.nhle.com/mugs/nhl/'+season+'/'+p.team+'/'+p.pid+'.png';
  var logo='https://assets.nhle.com/logos/nhl/svg/'+p.team+'_light.svg';
  var lineHtml=(p.realLine!=null)
    ? `<span class="ln">U ${p.dispLine}</span> <span class="od">${p.realUnderOdds||''}</span>`
    : `<span class="est">U ~${p.dispLine}</span>`;
  var voHtml=p.underTotVo?`<span class="${underClass(p.underRateVo)}">${p.underHitsVo}/${p.underTotVo} (${p.underRateVo}%)</span>`:'<span class="gray">—</span>';
  var anHtml=p.underTotAny?`<span class="${underClass(p.underRateAny)}">${p.underHitsAny}/${p.underTotAny} (${p.underRateAny}%)</span>`:'<span class="gray">—</span>';
  return `
   <div class="pick-card under-card ${_accFor(p.mkt)}">
     <div class="pc-rank">${i}</div>
     <div class="pc-top">
       <div class="hs-wrap"><span class="hs-ini">${_initials(p.name)}</span>
         <img class="hs-img" src="${head}" onerror="this.style.display='none'"/>
         <img class="pc-logo" src="${logo}" onerror="this.style.display='none'"/>
       </div>
       <div class="pc-id">
         <div class="pc-name">${p.name}</div>
         <div class="pc-meta">${p.team} vs ${p.opponent} <span class="${ha?'home':'away'}">${ha?'HOME':'AWAY'}</span></div>
         <div class="pc-mkt">${p.mkt||''} · UNDER</div>
       </div>
     </div>
     ${_sigBadges(p)}
     <div class="pc-line-row"><span>${lineHtml}</span><span class="od">Under Line</span></div>
     <div class="pc-stats">
       <div class="pc-stat"><div class="k">Under vs ${p.opponent}</div><div class="v">${voHtml}</div></div>
       <div class="pc-stat"><div class="k">Under L10 ${ha?'Home':'Away'}</div><div class="v">${anHtml}</div></div>
       <div class="pc-stat"><div class="k">Avg</div><div class="v gold">${p.avg}</div></div>
       <div class="pc-stat"><div class="k">Basis</div><div class="v">${p.underBasis||'—'}</div></div>
     </div>
     <div class="pc-foot"><span class="pc-score ${underClass(p.underRate)}">${p.underHits}/${p.underTotal} (${p.underRate}%)</span>
       <span style="display:flex;gap:6px">${_nhlBetBtn(p,'UNDER')}<button class="pc-tap" onclick="openNhlLadder('${key}')">📊 Game Log</button></span></div>
   </div>`;
}
function nhlUnderGrid(picks){
  if(!picks||!picks.length) return '';
  return '<div class="picks-grid">'+picks.map(function(p,i){return nhlUnderCard(p,i+1);}).join('')+'</div>';
}
function nhlUnderRestBlock(rest, label, color){
  if(!rest || !rest.length) return '';
  var c = color || '#f87171';
  return '<details style="margin-top:8px"><summary class="more-btn" style="color:'+c+';border-color:'+c+'33">&#9655; '+rest.length+' more '+label+'</summary>'
    + '<div class="picks-grid" style="margin-top:12px">'
    + rest.map(function(p,i){return nhlUnderCard(p, 10+i+1);}).join('')
    + '</div></details>';
}
function _spRow(p){
  var key=_ladKey(p); window.__NHLLAD__[key]=p;
  var best=Math.max(p.rateA||0,p.rateB||0);
  return `<div class="sp-row" onclick="openNhlLadder('${key}')"><div><div class="nm">${p.name}</div><div class="mt">${p.team} vs ${p.opponent} · ${p.dispLine}</div></div><div class="${rateClass(best)}" style="font-weight:800">${best}%</div></div>`;
}
function _spCol(title,picks){
  var rows=(picks||[]).slice(0,8).map(_spRow).join('')||'<div class="mt" style="color:#6b7280;padding:6px">None</div>';
  return `<div class="sp-col"><h4>${title}</h4>${rows}</div>`;
}
function _underBox(picks){
  // Only surface genuine fade candidates: a player must go UNDER his line in at
  // least UNDER_THRESH% of his last-10 H/A games. Without this gate the list
  // included clear OVER plays (e.g. a 9/10 over) ranked at the bottom.
  var UNDER_THRESH=60;
  var u=(picks||[]).filter(function(p){return p.underTotal>=1 && p.underLine!=null && p.underRate>=UNDER_THRESH;})
      .sort(function(a,b){return b.underRate-a.underRate;});
  if(!u.length) return '';
  var rows=u.map(function(p){
    var key=_ladKey(p); window.__NHLLAD__[key]=p;
    return `<div class="uprow" onclick="openNhlLadder('${key}')"><div><div class="nm">${p.name}</div><div class="mt">${p.team} vs ${p.opponent} · under ${p.underLine}</div></div><div class="${rateClass(p.underRate)}" style="font-weight:800">${p.underHits}/${p.underTotal} (${p.underRate}%)</div></div>`;
  }).join('');
  return '<div class="uplays">'+rows+'</div>';
}
function openNhlLadder(key){
  var p=window.__NHLLAD__[key]; if(!p) return;
  var line=p.dispLine;
  var chips=(p.glog||[]).map(function(g){
    var hit=g.v>line; var cls=hit?'hit':'miss';
    var d=String(g.d||'').slice(5);
    return `<div class="glchip ${cls}"><div class="d">${d}</div><div class="v">${g.v}</div></div>`;
  }).join('');
  if(!chips) chips='<span class="gray">No game log available.</span>';
  var vslRow=(p.realLine!=null&&p.vsLineTotal)
    ? `<div class="lad-stat"><span class="k">Hits vs Book Line (${p.realLine}) L10</span><span class="v ${rateClass(p.vsLineRate)}">${p.vsLineHits}/${p.vsLineTotal} (${p.vsLineRate}%)</span></div>`
    : '';
  var html=`
    <div class="lad-modal" onclick="event.stopPropagation()">
      <button class="lad-close" onclick="closeNhlLadder()">✕</button>
      <h3>${p.name}</h3>
      <div class="lad-sub">${p.mkt} · ${p.team} vs ${p.opponent} · Line ${p.dispLine}</div>
      <div style="font-size:.7rem;color:#6b7280;text-transform:uppercase;letter-spacing:.08em;font-weight:700;margin-bottom:4px">Recent Games (green = over line)</div>
      <div class="lad-glog">${chips}</div>
      <div class="lad-stat"><span class="k">Career vs ${p.opponent}</span><span class="v">${_rateHtml(p.rateA,p.hitsA,p.totA)}</span></div>
      <div class="lad-stat"><span class="k">L10 ${p.homeRoad==='H'?'Home':'Away'}</span><span class="v">${_rateHtml(p.rateB,p.hitsB,p.totB)}</span></div>
      ${vslRow}
      <div class="lad-stat"><span class="k">Under Line L10</span><span class="v ${rateClass(p.underRate)}">${p.underHits}/${p.underTotal} (${p.underRate}%)</span></div>
      <div class="lad-stat"><span class="k">Average</span><span class="v gold">${p.avg}</span></div>
      ${p.proj!=null?`<div class="lad-stat"><span class="k">Projected (opp-adjusted)</span><span class="v gold">${p.proj} <span class="${p.projEdge>=0?'pos':'neg'}">(${p.projEdge>=0?'+':''}${p.projEdge} vs ${p.dispLine})</span></span></div>
      <div class="lad-why">${_projWhy(p)}</div>`:''}
      <div class="lad-stat"><span class="k">Score</span><span class="v" style="color:#f59e0b">${p.dispScore}</span></div>
    </div>`;
  var ov=document.createElement('div');
  ov.className='lad-ov'; ov.id='nhlLadOv'; ov.onclick=closeNhlLadder;
  ov.innerHTML=html;
  document.body.appendChild(ov);
}
function closeNhlLadder(){var o=document.getElementById('nhlLadOv');if(o)o.remove();}
function _projWhy(p){
  var bits=['L10 '+(p.homeRoad==='H'?'home':'away')+' avg '+p.avg];
  if(p.totA) bits.push('vs '+p.opponent+' avg '+p.avgA+' ('+p.totA+'g)');
  if(p.oppSA) bits.push(p.opponent+' allows '+p.oppSA+' SA/g (×'+(p.oppFactor||1)+')');
  if(p.restFactor && p.restFactor<1) bits.push('back-to-back ×'+p.restFactor);
  return 'Why: '+bits.join(' · ');
}

function fmtTag(t){
  if(t==='SUGGESTED') return '<span class="tag-sug">⭐ PICK</span>';
  if(t==='FADE')      return '<span class="tag-fade">⚠ FADE</span>';
  return '';
}
function fmtGap(g){
  if(g===null||g===undefined) return '<span class="gap-zero">—</span>';
  var cls = g>0?'gap-pos':(g<0?'gap-neg':'gap-zero');
  var sign = g>0?'+':'';
  return '<span class="'+cls+'">'+sign+g+'</span>';
}
function fmtVsLine(p){
  if(!p.realLine) return '<span class="gray">—</span>';
  return '<span class="'+rateClass(p.vsLineRate)+'">'+p.vsLineHits+'/'+p.vsLineTotal+' ('+p.vsLineRate+'%)</span>';
}

function buildPtsTable(picks, startNum){
  var thead = '<thead><tr><th>#</th><th>PLAYER</th><th>TEAM</th><th>OPP</th><th>H/A</th>' +
    '<th>BOOK</th><th>AVG vs OPP (L10)</th><th>AVG L10 H/A</th><th>HITS BOOK L10</th>' +
    '<th>GAP vs BOOK</th><th>HITS 1+ Career vs OPP</th><th>HITS 1+ L10 H/A</th><th>SCORE</th><th>TAG</th></tr></thead>';
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
      '<td>' + (p.realLine ? '<span class="real-line">' + p.realLine + '</span> <span class="odds-txt">' + (p.realOdds||'') + '</span>' : '<span class="est">~0.5</span>') + '</td>' +
      '<td><span class="gold">' + p.ptsOppAvg + '</span></td>' +
      '<td><span class="gold">' + p.ptsHa10avg + '</span></td>' +
      '<td>' + fmtVsLine(p) + '</td>' +
      '<td>' + fmtGap(p.gap) + '</td>' +
      '<td><span class="' + rateClass(p.pts2Rate) + '">' + p.pts2Hits + '/' + p.pts2Total + ' (' + p.pts2Rate + '%)</span></td>' +
      '<td><span class="' + rateClass(p.pts3Rate) + '">' + p.pts3Hits + '/' + p.pts3Total + ' (' + p.pts3Rate + '%)</span></td>' +
      '<td><span class="score">' + p.ptsScore + '</span></td>' +
      '<td>' + fmtTag(p.tag) + '</td>' +
      '</tr>';
  });
  return '<div class="tbl-wrap"><table>' + thead + '<tbody>' + rows + '</tbody></table></div>';
}

function buildTable(picks, startNum){
  var thead = '<thead><tr><th>#</th><th>PLAYER</th><th>TEAM</th><th>OPP</th><th>H/A</th>' +
    '<th>BOOK</th><th>AVG vs OPP (L10)</th><th>AVG L10 H/A</th><th>HITS BOOK L10</th>' +
    '<th>GAP vs BOOK</th><th>HITS 2+ Career vs OPP</th><th>HITS 2+ L10 H/A</th><th>SCORE</th><th>TAG</th></tr></thead>';
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
      '<td>' + fmtVsLine(p) + '</td>' +
      '<td>' + fmtGap(p.gap) + '</td>' +
      '<td><span class="' + rateClass(p.step2Rate) + '">' + p.step2Hits + '/' + p.step2Total + ' (' + p.step2Rate + '%)</span></td>' +
      '<td><span class="' + rateClass(p.step3Rate) + '">' + p.step3Hits + '/' + p.step3Total + ' (' + p.step3Rate + '%)</span></td>' +
      '<td><span class="score">' + p.score + '</span></td>' +
      '<td>' + fmtTag(p.tag) + '</td>' +
      '</tr>';
  });
  return '<div class="tbl-wrap"><table>' + thead + '<tbody>' + rows + '</tbody></table></div>';
}

function buildNormTable(picks, startNum){
  var thead = '<thead><tr><th>#</th><th>PLAYER</th><th>TEAM</th><th>OPP</th><th>H/A</th>' +
    '<th>BOOK</th><th>AVG vs OPP</th><th>AVG L10 H/A</th><th>HITS BOOK L10</th>' +
    '<th>GAP vs BOOK</th><th>Career vs OPP</th><th>L10 H/A</th><th>SCORE</th><th>TAG</th></tr></thead>';
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
      '<td>' + (p.realLine!=null ? '<span class="real-line">' + p.dispLine + '</span> <span class="odds-txt">' + (p.realOdds||'') + '</span>' : '<span class="est">~' + p.dispLine + '</span>') + '</td>' +
      '<td><span class="gold">' + p.avgA + '</span></td>' +
      '<td><span class="gold">' + p.avg + '</span></td>' +
      '<td>' + fmtVsLine(p) + '</td>' +
      '<td>' + fmtGap(p.gap) + '</td>' +
      '<td>' + _rateHtml(p.rateA,p.hitsA,p.totA) + '</td>' +
      '<td>' + _rateHtml(p.rateB,p.hitsB,p.totB) + '</td>' +
      '<td><span class="score">' + p.dispScore + '</span></td>' +
      '<td>' + fmtTag(p.tag) + '</td>' +
      '</tr>';
  });
  return '<div class="tbl-wrap"><table>' + thead + '<tbody>' + rows + '</tbody></table></div>';
}

function renderResults(d){
  window.__NHL_RAW__ = d;
  window.__NHL_SEASON__ = d.season || '20252026';
  window.__NHL_DATE__ = d.date || '';
  document.getElementById('out').innerHTML = '<div class="nhl-toolbar"><input id="nhlSearch" type="text" placeholder="Search player…" oninput="_nhlPaint(this.value)"/></div><div id="nhlBody"></div>';
  _nhlPaint('');
}
// Re-paints the NHL body filtered by player name. The search box lives outside
// #nhlBody so it keeps focus across keystrokes. `d` is aliased to a shallow copy
// whose pick lists are name-filtered, leaving the original render code untouched.
function _nhlPaint(q){
  var raw=window.__NHL_RAW__; if(!raw) return;
  q=(q||'').toLowerCase().trim();
  function _f(a){return q?(a||[]).filter(function(p){return (p.name||'').toLowerCase().indexOf(q)>=0;}):(a||[]);}
  var d={}; for(var _k in raw){ d[_k]=raw[_k]; }
  d.picks=_f(raw.picks); d.ptsPicks=_f(raw.ptsPicks); d.astPicks=_f(raw.astPicks); d.goalPicks=_f(raw.goalPicks); d.savesPicks=_f(raw.savesPicks);
  d.rest=_f(raw.rest); d.ptsRest=_f(raw.ptsRest); d.astRest=_f(raw.astRest); d.goalRest=_f(raw.goalRest); d.savesRest=_f(raw.savesRest);
  d.shotUnders=_f(raw.shotUnders); d.ptsUnders=_f(raw.ptsUnders); d.astUnders=_f(raw.astUnders); d.goalUnders=_f(raw.goalUnders); d.savesUnders=_f(raw.savesUnders);
  d.shotUndersRest=_f(raw.shotUndersRest); d.ptsUndersRest=_f(raw.ptsUndersRest); d.astUndersRest=_f(raw.astUndersRest); d.goalUndersRest=_f(raw.goalUndersRest); d.savesUndersRest=_f(raw.savesUndersRest);
  var h = '';

  // Chips
  h += '<div class="chips">' +
    '<div class="chip"><div class="val">' + d.games.length + '</div><div class="lbl">Games</div></div>' +
    '<div class="chip"><div class="val">' + ((d.picks||[]).length) + '</div><div class="lbl">Shots</div></div>' +
    '<div class="chip"><div class="val">' + ((d.ptsPicks||[]).length) + '</div><div class="lbl">Points</div></div>' +
    '<div class="chip"><div class="val">' + ((d.astPicks||[]).length) + '</div><div class="lbl">Assists</div></div>' +
    '<div class="chip"><div class="val">' + ((d.goalPicks||[]).length) + '</div><div class="lbl">Goals</div></div>' +
    '<div class="chip"><div class="val">' + ((d.savesPicks||[]).length) + '</div><div class="lbl">Saves</div></div>' +
    '</div>';

  // Games
  h += '<div class="sec">- Games -- ' + (d.targetDate || '') + '</div><div class="games">';
  d.games.forEach(function(g){
    var t = g.startTime ? new Date(g.startTime).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZoneName:'short'}) : '';
    h += '<div class="gcard"><div class="mu">' + g.awayTeam + ' @ ' + g.homeTeam + '</div><div class="gt">' + t + '</div></div>';
  });
  h += '</div>';

  // SA Rankings
  h += '<div class="sec">- Shots Against / Game Rankings</div><div class="sa-list">';
  (d.sa_ranks || []).forEach(function(item, i){
    h += '<div class="sa-badge"><span class="rk">#' + (i+1) + ' ' + item[0] + '</span> <span class="sv">' + item[1].toFixed(1) + '</span></div>';
  });
  h += '</div>';

  // SHOTS cards (OVER)
  h += '<div class="sec">🏒 Top ' + ((d.picks||[]).length) + ' Shots on Goal — OVER</div>';
  h += nhlCardGrid(d.picks);
  h += nhlRestBlock(d.rest, 'shots', '#4ade80');

  // Shots UNDER cards
  if((d.shotUnders||[]).length){
    h += '<div class="sec">⬇ Top ' + d.shotUnders.length + ' Shots on Goal — UNDER</div>';
    h += nhlUnderGrid(d.shotUnders);
    h += nhlUnderRestBlock(d.shotUndersRest, 'shots under', '#f87171');
  }

  // POINTS cards
  if((d.ptsPicks||[]).length){
    h += '<div class="sec">🎯 Top ' + d.ptsPicks.length + ' Points (1+)</div>';
    h += nhlCardGrid(d.ptsPicks);
    h += nhlRestBlock(d.ptsRest, 'points', '#a78bfa');
  }
  if((d.ptsUnders||[]).length){
    h += '<div class="sec">⬇ Top ' + d.ptsUnders.length + ' Points (1+) — UNDER</div>';
    h += nhlUnderGrid(d.ptsUnders);
    h += nhlUnderRestBlock(d.ptsUndersRest, 'points under', '#f87171');
  }
  // ASSISTS cards
  if((d.astPicks||[]).length){
    h += '<div class="sec">🅰️ Top ' + d.astPicks.length + ' Assists (1+)</div>';
    h += nhlCardGrid(d.astPicks);
    h += nhlRestBlock(d.astRest, 'assists', '#f59e0b');
  }
  if((d.astUnders||[]).length){
    h += '<div class="sec">⬇ Top ' + d.astUnders.length + ' Assists (1+) — UNDER</div>';
    h += nhlUnderGrid(d.astUnders);
    h += nhlUnderRestBlock(d.astUndersRest, 'assists under', '#f87171');
  }
  // GOALS cards
  if((d.goalPicks||[]).length){
    h += '<div class="sec">⚽ Top ' + d.goalPicks.length + ' Goals (1+) — OVER</div>';
    h += nhlCardGrid(d.goalPicks);
    h += nhlRestBlock(d.goalRest, 'goals', '#34d399');
  }
  if((d.goalUnders||[]).length){
    h += '<div class="sec">⬇ Top ' + d.goalUnders.length + ' Goals (1+) — UNDER</div>';
    h += nhlUnderGrid(d.goalUnders);
    h += nhlUnderRestBlock(d.goalUndersRest, 'goals under', '#f87171');
  }
  // SAVES cards
  if((d.savesPicks||[]).length){
    h += '<div class="sec">🧤 Top ' + d.savesPicks.length + ' Goalie Saves</div>';
    h += nhlCardGrid(d.savesPicks);
    h += nhlRestBlock(d.savesRest, 'saves', '#60a5fa');
  }
  if((d.savesUnders||[]).length){
    h += '<div class="sec">⬇ Top ' + d.savesUnders.length + ' Goalie Saves — UNDER</div>';
    h += nhlUnderGrid(d.savesUnders);
    h += nhlUnderRestBlock(d.savesUndersRest, 'saves under', '#f87171');
  }

  // SPECIAL — best plays, NBA-style 2-col boxes
  h += '<div class="sec">⭐ Special — Best Plays</div>';
  h += '<div class="special-wrap">' + _spCol('Shot Plays', d.picks) + _spCol('Point Plays', d.ptsPicks||[]) + '</div>';
  if(((d.astPicks||[]).length)||((d.goalPicks||[]).length)){
    h += '<div class="special-wrap">' + _spCol('Assist Plays', d.astPicks||[]) + _spCol('Goal Plays', d.goalPicks||[]) + '</div>';
  }
  if((d.savesPicks||[]).length){
    h += '<div class="special-wrap">' + _spCol('Save Plays', d.savesPicks||[]) + '<div class="sp-box"></div>' + '</div>';
  }

  // All Plays by Game - collapsible (shots + points detail tables)
  var allPlays = (d.picks||[]).concat(d.rest||[])
    .concat(d.ptsPicks||[]).concat(d.ptsRest||[])
    .concat(d.astPicks||[]).concat(d.astRest||[])
    .concat(d.goalPicks||[]).concat(d.goalRest||[])
    .concat(d.savesPicks||[]).concat(d.savesRest||[]);
  window.__NHL_PLAYS__=allPlays;
  if(allPlays.length && d.games && d.games.length){
    h += '<div class="sec" style="margin-top:32px">All Plays by Game</div>';
    d.games.forEach(function(g, gi){
      var gameName = g.awayTeam + ' @ ' + g.homeTeam;
      var gamePlays = allPlays.filter(function(p){
        return p.team===g.homeTeam || p.team===g.awayTeam ||
               p.opponent===g.homeTeam || p.opponent===g.awayTeam;
      });
      if(!gamePlays.length) return;
      var shots = gamePlays.filter(function(p){return p.mkt==='Shots on Goal';});
      var pts   = gamePlays.filter(function(p){return p.mkt==='Points (1+)';});
      var ast   = gamePlays.filter(function(p){return p.mkt==='Assists (1+)';});
      var goals = gamePlays.filter(function(p){return p.mkt==='Goals (1+)';});
      var sv    = gamePlays.filter(function(p){return p.mkt==='Goalie Saves';});
      h += '<div style="margin-bottom:10px">';
      h += '<div onclick="nhlToggle('+gi+')" style="background:#161616;border:1px solid #262626;border-radius:12px;padding:12px 18px;cursor:pointer;display:flex;align-items:center;justify-content:space-between">';
      h += '<span style="font-weight:700;color:#fff;font-size:.92rem">' + gameName + '</span>';
      h += '<div style="display:flex;align-items:center;gap:10px">';
      h += '<span style="background:rgba(245,158,11,.1);color:#f59e0b;padding:3px 12px;border-radius:999px;font-size:.75rem;font-weight:700">';
      h += shots.length + ' shots | ' + pts.length + ' pts | ' + ast.length + ' ast | ' + goals.length + ' goals | ' + sv.length + ' sv</span>';
      h += '<button id="nhltoggle_btn_'+gi+'" onclick="event.stopPropagation();nhlToggle('+gi+')" style="background:none;border:1px solid #374151;color:#9ca3af;border-radius:6px;padding:3px 12px;font-size:.72rem;cursor:pointer">Expand</button>';
      h += '</div></div>';
      h += '<div id="nhltoggle_'+gi+'" style="display:none;margin-top:6px">';
      if(shots.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Shots on Goal</div>';
        h += buildTable(shots, 1);
      }
      if(pts.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Points</div>';
        h += buildPtsTable(pts, 1);
      }
      if(ast.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Assists</div>';
        h += buildNormTable(ast, 1);
      }
      if(goals.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#34d399;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Goals</div>';
        h += buildNormTable(goals, 1);
      }
      if(sv.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Goalie Saves</div>';
        h += buildNormTable(sv, 1);
      }
      h += '</div></div>';
    });
  }

  document.getElementById('nhlBody').innerHTML = h;
  // Parlay pool always reflects the full (unfiltered) slate regardless of search.
  window.__NHL_PLAYS__ = (raw.picks||[]).concat(raw.rest||[]).concat(raw.ptsPicks||[]).concat(raw.ptsRest||[]).concat(raw.astPicks||[]).concat(raw.astRest||[]).concat(raw.goalPicks||[]).concat(raw.goalRest||[]).concat(raw.savesPicks||[]).concat(raw.savesRest||[]);
}

function nhlToggle(n){
  var el=document.getElementById('nhltoggle_'+n);
  var btn=document.getElementById('nhltoggle_btn_'+n);
  if(!el) return;
  var hidden=el.style.display==='none';
  el.style.display=hidden?'block':'none';
  if(btn) btn.textContent=hidden?'Collapse':'Expand';
}
// ── My Bets ──────────────────────────────────────────────────────────────────
function _nhlEsc(s){return(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function _nhlMoney(v){var n=Number(v)||0;return(n>=0?'$':'\u2212$')+Math.abs(n).toFixed(2);}
function _nhlBetAuthQS(){
  var tok=localStorage.getItem('__mpa_token')||'';
  var adm=new URLSearchParams(location.search).get('admin')||'';
  return '?token='+encodeURIComponent(tok)+(adm?('&admin='+encodeURIComponent(adm)):'');
}
function _nhlBetToast(msg){
  var t=document.createElement('div');t.textContent=msg;
  t.style.cssText='position:fixed;bottom:80px;left:50%;transform:translateX(-50%);background:#0e7490;color:#fff;padding:10px 20px;border-radius:10px;font-weight:700;font-size:.85rem;z-index:99999;white-space:nowrap;pointer-events:none;box-shadow:0 4px 20px rgba(0,0,0,.5)';
  document.body.appendChild(t);
  setTimeout(function(){t.style.opacity='0';t.style.transition='opacity .4s';setTimeout(function(){t.remove();},400);},2200);
}
function _nhlBetMkt(m){
  m=(m||'');
  if(m.indexOf('Shot')>=0) return ['SHOTS','Shots on Goal'];
  if(m.indexOf('Point')>=0) return ['POINTS','Points'];
  if(m.indexOf('Assist')>=0) return ['ASSISTS','Assists'];
  if(m.indexOf('Save')>=0) return ['SAVES','Goalie Saves'];
  return ['',''];
}
var _nhlBetN=0;
window.__NHL_BET_SRC__=window.__NHL_BET_SRC__||{};
function _nhlBetBtn(p,forceSide){
  if(p.realLine==null) return '';
  var mk=_nhlBetMkt(p.mkt); if(!mk[0]) return '';
  var side=forceSide||(p.pick==='UNDER'?'UNDER':'OVER');
  var odds=side==='OVER'?(p.realOdds!=null?p.realOdds:p.realUnderOdds):(p.realUnderOdds!=null?p.realUnderOdds:p.realOdds);
  var k='nh'+(++_nhlBetN);
  window.__NHL_BET_SRC__[k]={
    name:p.name,pid:(p.pid!=null?String(p.pid):''),team:(p.team||''),opp:(p.opponent||''),
    category:mk[1],side:side,stat_key:mk[0],stat_label:mk[1],
    line:p.realLine,odds:(odds!=null?odds:null),date:(window.__NHL_DATE__||'')
  };
  return '<button data-betkey="'+k+'" class="admin-only" onclick="event.stopPropagation();_nhlBetForm(this.dataset.betkey)" style="background:#0e7490;color:#fff;border:none;border-radius:8px;padding:6px 10px;font-size:.7rem;font-weight:800;cursor:pointer">Track Bet</button>';
}
function _nhlBetForm(key){
  var src=(window.__NHL_BET_SRC__||{})[key]; if(!src) return;
  window.__NHL_BET_CUR__=src;
  var ov=document.getElementById('nhl-bet-modal');
  if(!ov){
    ov=document.createElement('div'); ov.id='nhl-bet-modal';
    ov.style.cssText='position:fixed;inset:0;background:rgba(2,6,23,.82);z-index:10000;display:flex;align-items:center;justify-content:center;padding:16px';
    ov.onclick=function(e){if(e.target===ov)ov.style.display='none';};
    document.body.appendChild(ov);
  }
  var pickTxt=src.side+' '+src.line+' '+(src.stat_label||'');
  ov.innerHTML=`<div style="background:#0f172a;border:1px solid #0e7490;border-radius:16px;max-width:360px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,.6)">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;padding:16px 18px;border-bottom:1px solid #1e293b">
      <div>
        <div style="font-weight:800;color:#fff;font-size:1.02rem">${_nhlEsc(src.name)}</div>
        <div style="color:#67e8f9;font-size:.82rem;font-weight:800;margin-top:2px">${_nhlEsc(pickTxt)}</div>
        <div style="color:#94a3b8;font-size:.72rem;margin-top:2px">${_nhlEsc(src.category||'')}${src.opp?' &middot; vs '+_nhlEsc(src.opp):''}${src.date?' &middot; '+src.date:''}</div>
      </div>
      <button onclick="document.getElementById('nhl-bet-modal').style.display='none'" style="background:#1e293b;border:none;color:#cbd5e1;width:30px;height:30px;border-radius:8px;cursor:pointer;font-size:1rem">&#215;</button>
    </div>
    <div style="padding:16px 18px;display:grid;gap:12px">
      <label style="font-size:.72rem;color:#94a3b8;font-weight:600">Odds (American)<input id="nhl-bet-odds" type="number" value="${src.odds!=null?src.odds:''}" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fbbf24;font-family:monospace;font-weight:700;font-size:.95rem"></label>
      <label style="font-size:.72rem;color:#94a3b8;font-weight:600">Bet size ($)<input id="nhl-bet-stake" type="number" min="0" step="0.01" placeholder="e.g. 50" style="display:block;width:100%;margin-top:5px;background:#0b1120;border:1px solid #334155;border-radius:8px;padding:9px 11px;color:#fff;font-weight:700;font-size:.95rem"></label>
      <div id="nhl-bet-payout" style="font-size:.78rem;color:#64748b;min-height:1em"></div>
      <div id="nhl-bet-msg" style="font-size:.76rem;color:#f87171;min-height:1em"></div>
      <button id="nhl-bet-save" onclick="_nhlSaveBet()" style="background:#0e7490;color:#fff;border:none;border-radius:9px;padding:11px;font-weight:800;cursor:pointer;font-size:.92rem">Log Bet</button>
    </div>
  </div>`;
  ov.style.display='flex';
  var so=document.getElementById('nhl-bet-odds'),ss=document.getElementById('nhl-bet-stake');
  function _calc(){
    var o=parseFloat(so.value),s=parseFloat(ss.value);
    var pay=document.getElementById('nhl-bet-payout');
    if(!isFinite(o)||!isFinite(s)||s<=0){pay.textContent='';return;}
    var win=o>0?s*(o/100):s*(100/Math.abs(o));
    pay.innerHTML='To win <strong style="color:#4ade80">$'+win.toFixed(2)+'</strong> &middot; total payout <strong style="color:#cbd5e1">$'+(s+win).toFixed(2)+'</strong>';
  }
  so.oninput=_calc;ss.oninput=_calc;_calc();
  setTimeout(function(){ss.focus();},50);
}
async function _nhlSaveBet(){
  var src=window.__NHL_BET_CUR__;if(!src) return;
  var o=parseFloat(document.getElementById('nhl-bet-odds').value);
  var s=parseFloat(document.getElementById('nhl-bet-stake').value);
  var msg=document.getElementById('nhl-bet-msg');
  if(!isFinite(o)){msg.textContent='Enter the odds.';return;}
  if(!isFinite(s)||s<=0){msg.textContent='Enter a bet size greater than 0.';return;}
  var btn=document.getElementById('nhl-bet-save');btn.disabled=true;btn.textContent='Saving\u2026';
  try{
    var body=Object.assign({},src,{odds:Math.round(o),stake:s,placed_at:new Date().toISOString()});
    var res=await fetch('/api/bets'+_nhlBetAuthQS(),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!res.ok){throw new Error(await res.text());}
    document.getElementById('nhl-bet-modal').style.display='none';
    _nhlBetToast('\u2705 Bet logged');
    var mb=document.getElementById('nhl-mybets-card');
    if(mb&&mb.style.display!=='none') openNhlMyBets(false);
  }catch(e){msg.textContent=(e.message||'Save failed');btn.disabled=false;btn.textContent='Log Bet';}
}
async function openNhlMyBets(scroll){
  var card=document.getElementById('nhl-mybets-card');if(!card) return;
  card.style.display='block';
  if(scroll!==false) card.scrollIntoView({behavior:'smooth',block:'start'});
  document.getElementById('nhl-mybets-body').innerHTML='<p style="color:#94a3b8;font-size:.85rem">Loading\u2026</p>';
  try{
    var res=await fetch('/api/bets'+_nhlBetAuthQS());
    if(!res.ok){
      var t=await res.text();
      if(res.status===403) t='Session expired \u2014 reopen from hub';
      throw new Error(t);
    }
    window.__NHL_MYBETS__=await res.json();
    renderNhlMyBets(window.__NHL_MYBETS__);
  }catch(e){
    document.getElementById('nhl-mybets-body').innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading bets')+'</p>';
  }
}
function _nhlBetOddsDisp(o){return o!=null?((o>0?'+':'')+o):'\u2014';}
function _nhlResColor(r){return r==='WIN'?'#4ade80':(r==='LOSS'?'#f87171':(r==='PUSH'?'#facc15':'#94a3b8'));}
function _nhlStatBox(lbl,val,clr){
  return '<div style="background:#111;border-radius:10px;padding:10px 14px;min-width:92px">'
    +'<div style="font-size:.64rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em">'+lbl+'</div>'
    +'<div style="font-size:1.12rem;font-weight:800;color:'+(clr||'#e2e8f0')+'">'+val+'</div></div>';
}
function renderNhlMyBets(d){
  var s=d.summary||{};var bets=d.bets||[];
  var roiTxt=s.roi!=null?((s.roi>0?'+':'')+s.roi+'%'):'\u2014';
  var roiClr=s.roi==null?'#94a3b8':(s.roi>0?'#4ade80':(s.roi<0?'#f87171':'#facc15'));
  var netClr=(s.profit||0)>0?'#4ade80':((s.profit||0)<0?'#f87171':'#cbd5e1');
  var recTxt=(s.wins||0)+'-'+(s.losses||0)+(s.push?('-'+s.push+'P'):'');
  var head='<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:18px">'
    +_nhlStatBox('Record',recTxt,'#e2e8f0')
    +_nhlStatBox('Pending',(s.pending||0),'#94a3b8')
    +_nhlStatBox('Staked',_nhlMoney(s.staked||0),'#cbd5e1')
    +_nhlStatBox('Net',_nhlMoney(s.profit||0),netClr)
    +_nhlStatBox('Returned',_nhlMoney(s.returned||0),'#cbd5e1')
    +_nhlStatBox('ROI',roiTxt,roiClr)
    +'<div style="margin-left:auto"><button onclick="downloadNhlMyBetsCSV()" style="background:#0e7490;color:#fff;border:none;border-radius:8px;padding:8px 12px;font-size:.78rem;font-weight:700;cursor:pointer">&#11015; CSV</button></div>'
    +'</div>';
  var bc=(s.by_category||[]).map(function(c){
    var croi=c.roi!=null?((c.roi>0?'+':'')+c.roi+'%'):'\u2014';
    var cclr=c.roi==null?'#94a3b8':(c.roi>0?'#4ade80':(c.roi<0?'#f87171':'#facc15'));
    return '<tr><td style="font-weight:600">'+_nhlEsc(c.category)+'</td>'
      +'<td style="font-family:monospace">'+c.wins+'-'+c.losses+(c.push?('-'+c.push+'P'):'')+'</td>'
      +'<td style="font-family:monospace;color:#94a3b8">'+(c.pending||0)+'</td>'
      +'<td style="font-family:monospace">'+_nhlMoney(c.staked)+'</td>'
      +'<td style="font-family:monospace;color:'+((c.profit||0)>=0?'#4ade80':'#f87171')+'">'+_nhlMoney(c.profit)+'</td>'
      +'<td style="font-family:monospace;font-weight:700;color:'+cclr+'">'+croi+'</td></tr>';
  }).join('');
  var bcHtml=bc?'<div style="overflow-x:auto;margin-bottom:18px"><table class="nhl-bets-tbl"><thead><tr><th>Category</th><th>W-L</th><th>Pend</th><th>Staked</th><th>Net</th><th>ROI</th></tr></thead><tbody>'+bc+'</tbody></table></div>':'';
  var rows=bets.map(function(b){
    var res=b.result||'pending';
    var delBtn='<button data-delid="'+b.id+'" onclick="_nhlDeleteBet(this.dataset.delid)" title="Remove" style="background:none;border:none;color:#64748b;cursor:pointer;font-size:1rem">&#10006;</button>';
    var pk=b.side+' '+b.line+' '+(b.stat_label||'');
    var actTxt=b.actual!=null?(' <span style="color:#64748b;font-weight:400;font-size:.72rem">('+b.actual+')</span>'):'';
    return '<tr>'
      +'<td style="white-space:nowrap;color:#94a3b8;font-family:monospace;font-size:.76rem">'+(b.date||'')+'</td>'
      +'<td style="font-weight:600">'+_nhlEsc(b.name||'')+'<div style="font-size:.68rem;color:#64748b">'+_nhlEsc(b.category||'')+'</div></td>'
      +'<td style="font-size:.82rem">'+_nhlEsc(pk)+'</td>'
      +'<td style="font-family:monospace">'+_nhlBetOddsDisp(b.odds)+'</td>'
      +'<td style="font-family:monospace">'+_nhlMoney(b.stake)+'</td>'
      +'<td style="font-weight:800;color:'+_nhlResColor(res)+'">'+(res==='pending'?'pending':res)+actTxt+'</td>'
      +'<td style="font-family:monospace;font-weight:700;color:'+((b.profit||0)>=0?'#4ade80':'#f87171')+'">'+(b.profit!=null?_nhlMoney(b.profit):'\u2014')+'</td>'
      +'<td>'+delBtn+'</td></tr>';
  }).join('');
  var rowsHtml=bets.length
    ?'<div style="overflow-x:auto"><table class="nhl-bets-tbl"><thead><tr><th>Date</th><th>Player</th><th>Pick</th><th>Odds</th><th>Stake</th><th>Result</th><th>Profit</th><th></th></tr></thead><tbody>'+rows+'</tbody></table></div>'
    :'<p style="color:#94a3b8;padding:16px">No bets logged yet. Click <strong style="color:#67e8f9">Track Bet</strong> on any pick card to start.</p>';
  document.getElementById('nhl-mybets-body').innerHTML=head+bcHtml+rowsHtml;
}
async function _nhlDeleteBet(id){
  if(!confirm('Remove this bet from your log?')) return;
  try{
    var res=await fetch('/api/bets/'+encodeURIComponent(id)+_nhlBetAuthQS(),{method:'DELETE'});
    if(!res.ok) throw new Error(await res.text());
    openNhlMyBets(false);
  }catch(e){alert(e.message||'Delete failed');}
}
function downloadNhlMyBetsCSV(){
  var d=window.__NHL_MYBETS__;if(!d){alert('Open My Bets first.');return;}
  var rows=[['Date','Player','Team','Category','Side','Pick','Odds','Stake','Result','Actual','Profit']];
  (d.bets||[]).forEach(function(b){
    rows.push([b.date||'',b.name||'',b.team||'',b.category||'',b.side||'',
      b.side+' '+b.line+' '+(b.stat_label||''),
      b.odds!=null?b.odds:'',b.stake!=null?b.stake:'',
      b.result||'',b.actual!=null?b.actual:'',b.profit!=null?b.profit:'']);
  });
  function _c(v){var sv=String(v==null?'':v);if(/[,"\\n]/.test(sv))sv='"'+sv.replace(/"/g,'""')+'"';return sv;}
  var csv=rows.map(function(r){return r.map(_c).join(',');}).join('\\r\\n');
  var blob=new Blob(['\ufeff'+csv],{type:'text/csv;charset=utf-8;'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a');a.href=url;a.download='nhl-my-bets.csv';
  document.body.appendChild(a);a.click();document.body.removeChild(a);URL.revokeObjectURL(url);
}
</script>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────────────────────
#  My Bets (bet tracking) — admin-only, mirrors NBA/MLB
# ─────────────────────────────────────────────────────────────────────────────
import threading as _bt_th, uuid as _bt_uuid

_NHL_BET_LOG_PATH = str(_CACHE_DIR / "_nhl_bet_log.json")
_NHL_BET_LOCK = _bt_th.Lock()
_NHL_BET_STAT_KEYS = ("SHOTS", "POINTS", "ASSISTS", "SAVES")
_NHL_STAT_LABEL = {"SHOTS": "Shots on Goal", "POINTS": "Points",
                   "ASSISTS": "Assists", "SAVES": "Goalie Saves"}
_NHL_CAT_ORDER = ["Shots on Goal", "Points", "Assists", "Goalie Saves"]
_NHL_BOX_CACHE: dict = {}   # (pid, season) → (games_dict, timestamp, permanent)
_NHL_BOX_LOCK = _bt_th.Lock()


def _nhl_load_bets() -> dict:
    try:
        with open(_NHL_BET_LOG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _nhl_save_bets(data: dict):
    try:
        tmp = _NHL_BET_LOG_PATH + f".{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _NHL_BET_LOG_PATH)
    except Exception as e:
        print(f"[nhl_bet_log] save failed: {e}")


def _nhl_bet_admin_ok(tok: str, admin: str) -> bool:
    return _is_admin_token(tok) or (
        bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__"))


def _nhl_bet_user_key(tok: str, admin: str) -> str:
    em = _token_email(tok) if tok else ""
    return em if em else "__admin__"


def _nhl_american_profit(odds, stake, result) -> float:
    try:
        stake = float(stake)
    except Exception:
        return 0.0
    if result == "WIN":
        try:
            o = float(odds)
        except Exception:
            return 0.0
        return stake * (o / 100.0) if o > 0 else stake * (100.0 / abs(o))
    if result == "LOSS":
        return -stake
    return 0.0


def _nhl_seasons_for(date_str: str):
    try:
        y, m, _d = (int(x) for x in date_str.split("-"))
    except Exception:
        return []
    start = y if m >= 8 else y - 1
    return [f"{start}{start + 1}"]


def _nhl_extract_stat(g: dict, stat_key: str):
    try:
        if stat_key == "SHOTS":
            v = g.get("shots")
            if v is None:
                v = g.get("sog")
            return float(v) if v is not None else None
        if stat_key == "ASSISTS":
            return float(g["assists"]) if g.get("assists") is not None else None
        if stat_key == "POINTS":
            if g.get("points") is not None:
                return float(g["points"])
            gl, a = g.get("goals"), g.get("assists")
            if gl is not None and a is not None:
                return float(gl) + float(a)
            return None
        if stat_key == "SAVES":
            if g.get("saves") is not None:
                return float(g["saves"])
            sa, ga = g.get("shotsAgainst"), g.get("goalsAgainst")
            if sa is not None and ga is not None:
                return float(sa) - float(ga)
            return None
    except Exception:
        return None
    return None


def _nhl_season_is_final(season: str) -> bool:
    try:
        end_y = int(season[4:8])
        return date.today() > date(end_y, 6, 30)
    except Exception:
        return False


def _nhl_player_games_raw(pid, season) -> tuple:
    """Return (games_dict, complete) with cache. complete=True when all fetches succeeded."""
    key = (str(pid), season)
    with _NHL_BOX_LOCK:
        entry = _NHL_BOX_CACHE.get(key)
    if entry:
        games, ts, permanent = entry
        if permanent or (time.time() - ts < 120):
            return games, True
    out = {}
    ok = True
    for gt in (2, 3):
        try:
            r = httpx.get(f"{NHL_API}/player/{pid}/game-log/{season}/{gt}",
                          timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                ok = False
                continue
            for g in r.json().get("gameLog", []):
                gd = g.get("gameDate")
                if gd:
                    out[gd] = g
        except Exception as e:
            print(f"[nhl_box] fetch {pid}/{season}/{gt}: {e}")
            ok = False
    permanent = ok and _nhl_season_is_final(season)
    with _NHL_BOX_LOCK:
        _NHL_BOX_CACHE[key] = (out, time.time(), permanent)
    return out, ok


def _nhl_player_games(pid, season) -> dict:
    """Return {gameDate: gamelog_entry} for a player+season (regular + playoff merged)."""
    games, _ = _nhl_player_games_raw(pid, season)
    return games


def _nhl_settle_cached(bet: dict, games: dict) -> bool:
    if bet.get("result") in ("WIN", "LOSS", "PUSH"):
        return False
    g = games.get(bet.get("date"))
    if not g:
        return False
    actual = _nhl_extract_stat(g, bet.get("stat_key"))
    if actual is None:
        return False
    try:
        line = float(bet.get("line"))
    except Exception:
        return False
    side = bet.get("side", "OVER")
    if actual == line:
        res = "PUSH"
    elif side == "OVER":
        res = "WIN" if actual > line else "LOSS"
    else:
        res = "WIN" if actual < line else "LOSS"
    bet["result"] = res
    bet["actual"] = actual
    bet["profit"] = round(_nhl_american_profit(bet.get("odds"), bet.get("stake"), res), 2)
    bet["settled_at"] = date.today().isoformat()
    return True


def _nhl_settle_batch(bets: list) -> tuple:
    """Return (changed, complete). complete=True when all fetches succeeded."""
    today = date.today().isoformat()
    need = {}
    for b in bets:
        if b.get("result") in ("WIN", "LOSS", "PUSH"):
            continue
        bdate, pid = b.get("date"), b.get("pid")
        if not bdate or not pid or bdate >= today:
            continue
        for s in _nhl_seasons_for(bdate):
            need.setdefault((str(pid), s), None)
    if not need:
        return False, True
    cache = {}
    all_ok = True
    for (pid, s) in need:
        try:
            games, ok = _nhl_player_games_raw(pid, s)
            cache[(pid, s)] = games
            if not ok:
                all_ok = False
        except Exception as e:
            print(f"[nhl_bet_log] settle fetch failed {pid}/{s}: {e}")
            cache[(pid, s)] = {}
            all_ok = False
    changed = False
    for b in bets:
        if b.get("result") in ("WIN", "LOSS", "PUSH"):
            continue
        bdate, pid = b.get("date"), str(b.get("pid") or "")
        if not bdate or not pid or bdate >= today:
            continue
        merged = {}
        for s in _nhl_seasons_for(bdate):
            merged.update(cache.get((pid, s), {}))
        if _nhl_settle_cached(b, merged):
            changed = True
    return changed, all_ok


def _nhl_settle_bet(bet: dict) -> bool:
    bdate, pid = bet.get("date"), bet.get("pid")
    if not bdate or not pid or bdate >= date.today().isoformat():
        return False
    merged = {}
    for s in _nhl_seasons_for(bdate):
        try:
            merged.update(_nhl_player_games(pid, s))
        except Exception:
            pass
    return _nhl_settle_cached(bet, merged)


def _nhl_summarize_bets(bets: list) -> dict:
    cats = {}
    tot_staked = tot_profit = 0.0
    w = l = pu = pend = 0
    for b in bets:
        res = b.get("result", "pending")
        try:
            stake = float(b.get("stake") or 0)
        except Exception:
            stake = 0.0
        c = cats.setdefault(b.get("category", "?"),
                            {"wins": 0, "losses": 0, "push": 0, "pending": 0,
                             "staked": 0.0, "profit": 0.0})
        if res == "WIN":
            w += 1; c["wins"] += 1
        elif res == "LOSS":
            l += 1; c["losses"] += 1
        elif res == "PUSH":
            pu += 1; c["push"] += 1
        else:
            pend += 1; c["pending"] += 1
        if res in ("WIN", "LOSS", "PUSH"):
            prof = float(b.get("profit") or 0)
            tot_staked += stake; c["staked"] += stake
            tot_profit += prof; c["profit"] += prof
    roi = (tot_profit / tot_staked * 100.0) if tot_staked > 0 else None
    ordered = _NHL_CAT_ORDER + [k for k in cats if k not in _NHL_CAT_ORDER]
    by_cat = []
    for cat in ordered:
        c = cats.get(cat)
        if not c:
            continue
        st, pr = c["staked"], c["profit"]
        by_cat.append({"category": cat, "wins": c["wins"], "losses": c["losses"],
                       "push": c["push"], "pending": c["pending"],
                       "staked": round(st, 2), "profit": round(pr, 2),
                       "roi": round(pr / st * 100, 1) if st > 0 else None})
    return {"wins": w, "losses": l, "push": pu, "pending": pend,
            "staked": round(tot_staked, 2), "profit": round(tot_profit, 2),
            "returned": round(tot_staked + tot_profit, 2),
            "roi": round(roi, 1) if roi is not None else None,
            "by_category": by_cat}


@app.get("/api/bets")
async def nhl_get_bets(request: Request, token: str = "", admin: str = "", settle: bool = True):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _nhl_bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    key = _nhl_bet_user_key(tok, admin)
    # load + release lock before settling (network calls must not hold the lock)
    with _NHL_BET_LOCK:
        data = _nhl_load_bets()
        bets = list(data.get(key, []))
    if settle:
        loop = asyncio.get_running_loop()
        changed, _ = await loop.run_in_executor(None, _nhl_settle_batch, bets)
        if changed:
            # merge terminal-only: apply WIN/LOSS/PUSH onto still-pending on-disk bets
            with _NHL_BET_LOCK:
                data2 = _nhl_load_bets()
                disk = {b["id"]: b for b in data2.get(key, [])}
                for b in bets:
                    if b.get("result") in ("WIN", "LOSS", "PUSH"):
                        d = disk.get(b["id"])
                        if d and d.get("result") not in ("WIN", "LOSS", "PUSH"):
                            d.update({"result": b["result"], "actual": b.get("actual"),
                                      "profit": b.get("profit"), "settled_at": b.get("settled_at")})
                data2[key] = list(disk.values())
                _nhl_save_bets(data2)
    bets.sort(key=lambda b: (b.get("date", ""), b.get("placed_at", "")), reverse=True)
    return {"bets": bets, "summary": _nhl_summarize_bets(bets)}


@app.post("/api/bets")
async def nhl_add_bet(request: Request, token: str = "", admin: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _nhl_bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    body = await request.json()
    try:
        stake = round(float(body.get("stake")), 2)
        odds = int(round(float(body.get("odds"))))
        line = float(body.get("line"))
    except Exception:
        raise HTTPException(status_code=400, detail="stake, odds and line must be numbers")
    if stake <= 0:
        raise HTTPException(status_code=400, detail="Bet size must be greater than 0")
    name = (body.get("name") or "").strip()
    stat_key = (body.get("stat_key") or "").strip().upper()
    side = (body.get("side") or "OVER").strip().upper()
    if not name or stat_key not in _NHL_BET_STAT_KEYS or side not in ("OVER", "UNDER"):
        raise HTTPException(status_code=400, detail="Invalid bet")
    bdate = (body.get("date") or date.today().isoformat()).strip()
    bet = {"id": _bt_uuid.uuid4().hex[:12], "date": bdate,
           "name": name, "pid": str(body.get("pid") or ""),
           "team": (body.get("team") or "").strip(),
           "opp": (body.get("opp") or "").strip(),
           "category": (body.get("category") or _NHL_STAT_LABEL.get(stat_key, "?")).strip(),
           "side": side, "stat_key": stat_key,
           "stat_label": (body.get("stat_label") or _NHL_STAT_LABEL.get(stat_key, "")).strip(),
           "line": line, "odds": odds, "stake": stake,
           "placed_at": (body.get("placed_at") or date.today().isoformat()),
           "result": "pending", "actual": None, "profit": None, "settled_at": None}
    try:
        _nhl_settle_bet(bet)
    except Exception:
        pass
    with _NHL_BET_LOCK:
        data = _nhl_load_bets()
        key = _nhl_bet_user_key(tok, admin)
        data.setdefault(key, []).append(bet)
        _nhl_save_bets(data)
    return {"ok": True, "bet": bet}


@app.delete("/api/bets/{bet_id}")
async def nhl_delete_bet(bet_id: str, request: Request, token: str = "", admin: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _nhl_bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    with _NHL_BET_LOCK:
        data = _nhl_load_bets()
        key = _nhl_bet_user_key(tok, admin)
        bets = data.get(key, [])
        new_bets = [b for b in bets if b.get("id") != bet_id]
        if len(new_bets) != len(bets):
            data[key] = new_bets
            _nhl_save_bets(data)
    return {"ok": True}


@app.get("/api/bets/summary")
async def nhl_bets_summary(request: Request, token: str = "", admin: str = "", settle: bool = True):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _nhl_bet_admin_ok(tok, admin):
        raise HTTPException(status_code=403, detail="Admin only")
    key = _nhl_bet_user_key(tok, admin)
    with _NHL_BET_LOCK:
        data = _nhl_load_bets()
        bets = list(data.get(key, []))
    if settle:
        loop = asyncio.get_running_loop()
        changed, _ = await loop.run_in_executor(None, _nhl_settle_batch, bets)
        if changed:
            with _NHL_BET_LOCK:
                data2 = _nhl_load_bets()
                disk = {b["id"]: b for b in data2.get(key, [])}
                for b in bets:
                    if b.get("result") in ("WIN", "LOSS", "PUSH"):
                        d = disk.get(b["id"])
                        if d and d.get("result") not in ("WIN", "LOSS", "PUSH"):
                            d.update({"result": b["result"], "actual": b.get("actual"),
                                      "profit": b.get("profit"), "settled_at": b.get("settled_at")})
                data2[key] = list(disk.values())
                _nhl_save_bets(data2)
    return {"sport": "NHL", "summary": _nhl_summarize_bets(bets)}


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

@app.get("/api/whoami")
async def whoami(request: Request, token: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    return {"is_admin": _is_admin_token(tok)}

@app.get("/", response_class=HTMLResponse)
async def index(admin: str = "", token: str = ""):
    is_admin = (bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__")) or _is_admin_token(token)
    js_flag = "true" if is_admin else "false"
    return HTMLResponse(HTML.replace("</head>", f"<script>window.IS_ADMIN = {js_flag};</script></head>", 1))

@app.get("/api/picks")
async def api_picks(request: Request, target_date: str = None, token: str = ""):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _verify_hub_token(tok):
        raise HTTPException(status_code=401, detail="Subscription required — please log in via moneypicksarena.com")
    key = target_date or date.today().isoformat()
    cached = _cache_get("nhl", key)
    if cached:
        return JSONResponse(cached)
    result = await run_picks(target_date)
    if "error" not in result:
        _cache_set("nhl", key, result)
    return JSONResponse(result)


_CRON_BUSY_NHL = False

@app.api_route("/api/cron-run", methods=["GET", "POST"])
async def cron_run_nhl(request: Request, date_str: str = ""):
    # Cron-friendly trigger: authed by the static INTERNAL_API_TOKEN secret sent
    # as a header (kept out of the URL so it isn't logged). No expiring hub login
    # needed. Runs the pipeline + caches it so members can pull the picks, and
    # wakes the free-tier app on Render. An in-flight guard blocks overlapping runs.
    global _CRON_BUSY_NHL
    import hmac
    secret = os.environ.get("INTERNAL_API_TOKEN", "")
    tok = request.headers.get("X-Internal-Token", "") or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not secret or not hmac.compare_digest(tok or "", secret):
        raise HTTPException(status_code=401, detail="Invalid cron token")
    ds = date_str or date.today().isoformat()
    if _CRON_BUSY_NHL:
        return {"ran": False, "cached": bool(_cache_get("nhl", ds)), "date": ds, "reason": "already running"}
    _CRON_BUSY_NHL = True
    try:
        result = await run_picks(ds)
        if isinstance(result, dict) and "error" not in result:
            _cache_set("nhl", ds, result)
    finally:
        _CRON_BUSY_NHL = False
    return {"ran": True, "cached": bool(_cache_get("nhl", ds)), "date": ds}


@app.get("/api/cached")
async def api_cached(request: Request, target_date: str = None, token: str = ""):
    # Read-only: serve picks already saved on file. Never runs the pipeline, so any
    # logged-in member can pull the latest saved picks without triggering a fresh run.
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _verify_hub_token(tok):
        raise HTTPException(status_code=401, detail="Subscription required — please log in via moneypicksarena.com")
    key = target_date or date.today().isoformat()
    cached = _cache_get("nhl", key)
    if cached:
        return JSONResponse(cached)
    raise HTTPException(status_code=404, detail="No saved picks for this date.")


@app.get("/api/warm")
async def api_warm():
    """Pre-compute today's picks - called by cron-job.org at 10 AM."""
    today = date.today().isoformat()
    cached = _cache_get("nhl", today)
    if cached:
        return JSONResponse({"ok": True, "source": "cache", "date": today,
                             "picks": len(cached.get("picks", []))})
    result = await run_picks(today)
    if "error" not in result:
        _cache_set("nhl", today, result)
    return JSONResponse({"ok": "error" not in result, "source": "computed",
                         "date": today, "picks": len(result.get("picks", []))})

@app.post("/api/clear-cache")
async def api_clear_cache():
    _cache_clear("nhl")
    return {"ok": True, "msg": "NHL cache cleared"}

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
