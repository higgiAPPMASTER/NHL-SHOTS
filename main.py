#!/usr/bin/env python3
"""
NHL Money Shots - main.py
Step 1 : Sportsbook lines (Odds API → season avg estimates)
Step 2 : NHL Stats API — career H/A game logs vs today’s opponent (≥ 80%)
Step 3 : NHL Stats API — last 10 H/A games, any opponent (≥ 80%)
Step 4 : Rank & top 10
Deployed on Render (FastAPI + httpx)
"""

import os, hmac, asyncio, re, unicodedata, time, json, logging
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
logger   = logging.getLogger("nhl_money_shots")

NHL_API      = "https://api-web.nhle.com/v1"
NHL_STATS    = "https://api.nhle.com/stats/rest/en"
ODDS_API     = "https://api.the-odds-api.com/v4"

MIN_SPG       = 1.5   # retained historical threshold; no longer used as a book-line fallback
MIN_GP        = 10    # minimum games played for valid average

MIN_GAMES     = 2     # min games required for hit-rate calc
RECENT_DAYS   = 14    # player must have a game within this many days to count as "playing today"
HIT_THRESH         = 70.0  # % hit rate to qualify against the posted sportsbook line
HIT_THRESH_PTS     = 65.0  # % hit rate to qualify (points and power-play points)
PTS_LINE      = 0.5   # legacy default only; published picks require a book line
AST_LINE      = 0.5   # legacy default only; published picks require a book line
SAVES_LINE    = 24.5  # legacy default only; published picks require a book line
HIT_THRESH_AST     = 60.0  # % hit rate to qualify (assists)
HIT_THRESH_GOALS   = 50.0  # % hit rate to qualify (goals scored)
HIT_THRESH_SAVES   = 55.0  # % hit rate to qualify (goalie saves)
UNDER_THRESH       = 60.0  # under-rate % to qualify as a fade candidate (under cards/track)
UNDER_MIN_VO       = 2     # min H/A games vs THIS opponent for a vs-opp under
UNDER_MIN_ANY      = 3     # min H/A games vs anyone for an any-opp under
SEASONS       = ["20252026","20242025","20232024","20222023","20212022"]  # for points game logs
TOP_N       = 10     # final picks count
SEM_NHL     = 14     # concurrent NHL API calls

# This endpoint is polled before the first on-demand run completes.  Keep a
# stable idle value available so a fresh deploy never returns a 500 here.
_progress = {"stage": "Ready", "done": 0, "total": 0, "pct": 0}


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
                        "gameState": g.get("gameState", ""),
                    })
    await _attach_confirmed_game_lineups(games)
    return games


def _boxscore_lineup(boxscore: Dict, game: Dict) -> Dict:
    """Return the dressed skaters and goalies from an NHL game boxscore."""
    by_team = boxscore.get("playerByGameStats") or {}
    lineup = {"skaters": {}, "goalies": {}}
    for box_side, team_key in (("homeTeam", "homeTeam"), ("awayTeam", "awayTeam")):
        team = game.get(team_key, "")
        stats = by_team.get(box_side) or {}
        skaters, goalies = [], []
        for group in ("forwards", "defense"):
            for player in stats.get(group, []) or []:
                pid = player.get("playerId")
                if pid is None:
                    continue
                skaters.append({
                    "id": int(pid),
                    "name": (player.get("name") or {}).get("default", ""),
                })
        for player in stats.get("goalies", []) or []:
            pid = player.get("playerId")
            if pid is None:
                continue
            goalies.append({
                "id": int(pid),
                "name": (player.get("name") or {}).get("default", ""),
            })
        if team:
            lineup["skaters"][team] = skaters
            lineup["goalies"][team] = goalies
    return lineup


async def _attach_confirmed_game_lineups(games: List[Dict]) -> None:
    """Attach official dressed-player lists when NHL has published them.

    The schedule endpoint itself does not provide lineups.  The gamecenter
    boxscore does after a game has begun/completed, which makes it the correct
    source for live games and historical replay.  Future/pre-game games remain
    explicitly unavailable here and are later restricted to book-listed players.
    """
    if not games:
        return
    async with httpx.AsyncClient(follow_redirects=True) as c:
        rows = await asyncio.gather(
            *[_fetch(f"{NHL_API}/gamecenter/{g['gameId']}/boxscore", c) for g in games],
            return_exceptions=True,
        )
    for game, boxscore in zip(games, rows):
        if not isinstance(boxscore, dict):
            game["lineupSource"] = "UNAVAILABLE"
            continue
        lineup = _boxscore_lineup(boxscore, game)
        count = sum(len(players) for players in lineup["skaters"].values())
        count += sum(len(players) for players in lineup["goalies"].values())
        if count:
            game["lineup"] = lineup
            game["lineupSource"] = "CONFIRMED"
            print(f"[Lineup] {game['awayTeam']} @ {game['homeTeam']}: "
                  f"{count} confirmed dressed players")
        else:
            game["lineupSource"] = "UNAVAILABLE"


def _lineup_filtered_rosters(
    games: List[Dict],
    rosters: Dict[str, List[Dict]],
    line_maps: List[Dict],
    player_group: str,
) -> Dict[str, List[Dict]]:
    """Keep only players established for the game-day lineup.

    Official boxscore participants win whenever they exist (live/final games
    and historical replay).  Before puck drop the NHL feed has no projected
    lineup field, so a player must appear in a listed player-prop market.  A
    missing lineup signal never broadens back to the full team roster.
    """
    game_by_team = {}
    for game in games:
        game_by_team[game.get("homeTeam", "")] = game
        game_by_team[game.get("awayTeam", "")] = game

    filtered: Dict[str, List[Dict]] = {}
    locked_states = {"LIVE", "CRIT", "OFF", "FINAL"}
    for team, roster in rosters.items():
        game = game_by_team.get(team, {})
        official = ((game.get("lineup") or {}).get(player_group) or {}).get(team) or []
        if official:
            # Preserve the full roster name when possible, but retain historical
            # participants who have since been traded or sent down.
            current_by_id = {int(p["id"]): p for p in roster if p.get("id") is not None}
            eligible = [
                current_by_id.get(int(p["id"]), p)
                for p in official if p.get("id") is not None
            ]
            filtered[team] = eligible
            game.setdefault("lineupByTeam", {})[team] = "CONFIRMED"
            print(f"[Lineup] {team}: {len(eligible)} confirmed {player_group}")
            continue

        if game.get("gameState") in locked_states:
            # A live/final game without an official boxscore must not fall back
            # to the current club roster or bookmaker names.
            filtered[team] = []
            game.setdefault("lineupByTeam", {})[team] = "UNAVAILABLE"
            print(f"[Lineup] {team}: official {player_group} unavailable; withholding picks")
            continue

        eligible_ids = set()
        for lines in line_maps:
            for odds_name in (lines or {}):
                player = _match_odds_name(odds_name, roster)
                if player and player.get("id") is not None:
                    eligible_ids.add(int(player["id"]))
        filtered[team] = [p for p in roster if int(p.get("id", -1)) in eligible_ids]
        status = "BOOK_LISTED" if filtered[team] else "UNAVAILABLE"
        game.setdefault("lineupByTeam", {})[team] = status
        print(f"[Lineup] {team}: {len(filtered[team])} {player_group} "
              f"from {status.lower().replace('_', ' ')} signal")
    return filtered


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


# ─────────────────────────────────────────────────────────────────────────────
#  NHL Game Predictor — team-level win / total model
# ─────────────────────────────────────────────────────────────────────────────

_NHL_TEAM_FULL: Dict[str, str] = {
    "ANA": "Anaheim Ducks",        "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres",       "CGY": "Calgary Flames",
    "CAR": "Carolina Hurricanes",  "CHI": "Chicago Blackhawks",
    "COL": "Colorado Avalanche",   "CBJ": "Columbus Blue Jackets",
    "DAL": "Dallas Stars",         "DET": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",      "FLA": "Florida Panthers",
    "LAK": "Los Angeles Kings",    "MIN": "Minnesota Wild",
    "MTL": "Montreal Canadiens",   "NSH": "Nashville Predators",
    "NJD": "New Jersey Devils",    "NYI": "New York Islanders",
    "NYR": "New York Rangers",     "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",  "PIT": "Pittsburgh Penguins",
    "SJS": "San Jose Sharks",      "SEA": "Seattle Kraken",
    "STL": "St. Louis Blues",      "TBL": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",  "UTA": "Utah Hockey Club",
    "VAN": "Vancouver Canucks",    "VGK": "Vegas Golden Knights",
    "WSH": "Washington Capitals",  "WPG": "Winnipeg Jets",
}


def _nhl_match_team_name(full_name: str) -> Optional[str]:
    """Map an Odds API or stats-API full team name to NHL abbrev via word overlap."""
    words = set(full_name.lower().split())
    best, best_score = None, 0
    for abbr, fname in _NHL_TEAM_FULL.items():
        score = len(words & set(fname.lower().split()))
        if score > best_score:
            best, best_score = abbr, score
    return best if best_score >= 1 else None


def _odds_event_matches_slate(event: Dict, games: List[Dict]) -> bool:
    """True only when an Odds API event is one of the selected date's games."""
    if not games:
        return False
    away = _nhl_match_team_name(event.get("away_team", ""))
    home = _nhl_match_team_name(event.get("home_team", ""))
    return any(
        game.get("awayTeam") == away and game.get("homeTeam") == home
        for game in games
    )


async def _nhl_gp_fetch_all(target_date: str, season: str) -> dict:
    """Fetch standings, team summary, PP/PK, B2B schedule, and Odds API game lines."""
    import urllib.parse as _up
    yesterday = (date.fromisoformat(target_date) - timedelta(days=1)).isoformat()
    api_key   = os.environ.get("ODDS_API_KEY", "")

    sort_gf = _up.quote('[{"property":"goalsForPerGame","direction":"DESC"}]')
    sort_pp = _up.quote('[{"property":"powerPlayPct","direction":"DESC"}]')
    sort_pk = _up.quote('[{"property":"penaltyKillPct","direction":"DESC"}]')
    _cay    = f"gameTypeId=2 and seasonId<={season} and seasonId>={season}"

    summary_url = (f"{NHL_STATS}/team/summary?isAggregate=false&isGame=false"
                   f"&sort={sort_gf}&start=0&limit=50&factCayenneExp=gamesPlayed>=1"
                   f"&cayenneExp={_cay}")
    pp_url = (f"{NHL_STATS}/team/powerplay?isAggregate=false&isGame=false"
              f"&sort={sort_pp}&start=0&limit=50&factCayenneExp=gamesPlayed>=1"
              f"&cayenneExp={_cay}")
    pk_url = (f"{NHL_STATS}/team/penaltykill?isAggregate=false&isGame=false"
              f"&sort={sort_pk}&start=0&limit=50&factCayenneExp=gamesPlayed>=1"
              f"&cayenneExp={_cay}")

    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
        fetched = await asyncio.gather(
            _fetch(f"{NHL_API}/standings/now", c),
            _fetch(summary_url, c),
            _fetch(pp_url, c),
            _fetch(pk_url, c),
            _fetch(f"{NHL_API}/schedule/{yesterday}", c),
            return_exceptions=True,
        )
    def _safe(x): return x if isinstance(x, dict) else {}
    sd, md, pd, pkd, yd = (_safe(x) for x in fetched)

    # Teams that played yesterday → B2B today
    b2b_teams: set = set()
    for day in yd.get("gameWeek", []):
        if day.get("date") == yesterday:
            for g in day.get("games", []):
                b2b_teams.add(g.get("homeTeam", {}).get("abbrev", ""))
                b2b_teams.add(g.get("awayTeam", {}).get("abbrev", ""))
    b2b_teams.discard("")

    # Per-team dict from standings
    name_to_abbrev: Dict[str, str] = {}
    team_data: Dict[str, dict] = {}
    for t in sd.get("standings", []):
        abbr = t.get("teamAbbrev", {}).get("default", "")
        name = t.get("teamName",  {}).get("default", "")
        if not abbr:
            continue
        name_to_abbrev[name] = abbr
        gp = t.get("gamesPlayed", 1) or 1
        team_data[abbr] = {
            "abbr": abbr, "name": name, "gp": gp,
            "pts":     t.get("points", 0),
            "pctg":    round(t.get("pointPctg", 0) * 100, 1),
            "homeW":   t.get("homeWins",     0), "homeL":  t.get("homeLosses",  0),
            "homeOTL": t.get("homeOtLosses", 0),
            "roadW":   t.get("roadWins",     0), "roadL":  t.get("roadLosses",  0),
            "roadOTL": t.get("roadOtLosses", 0),
            "l10W":    t.get("l10Wins",      0), "l10L":   t.get("l10Losses",   0),
            "l10OTL":  t.get("l10OtLosses",  0),
            "streak":  t.get("streakCode",   ""),
            "gfPG":    round(t.get("goalFor",     0) / gp, 2),
            "gaPG":    round(t.get("goalAgainst", 0) / gp, 2),
            "ppPct": 0.0, "pkPct": 0.0, "sfPG": 0.0, "saPG": 0.0,
            "b2b": abbr in b2b_teams,
        }

    def _abbr_for(full: str) -> str:
        return name_to_abbrev.get(full, "") or _nhl_match_team_name(full) or ""

    for t in md.get("data", []):
        abbr = _abbr_for(t.get("teamFullName", ""))
        if abbr in team_data:
            team_data[abbr]["gfPG"] = round(float(t.get("goalsForPerGame",     0) or 0), 2)
            team_data[abbr]["gaPG"] = round(float(t.get("goalsAgainstPerGame", 0) or 0), 2)
            team_data[abbr]["sfPG"] = round(float(t.get("shotsForPerGame",     0) or 0), 1)
            team_data[abbr]["saPG"] = round(float(t.get("shotsAgainstPerGame", 0) or 0), 1)
    for t in pd.get("data", []):
        abbr = _abbr_for(t.get("teamFullName", ""))
        if abbr in team_data:
            team_data[abbr]["ppPct"] = round(float(t.get("powerPlayPct",   0) or 0), 1)
    for t in pkd.get("data", []):
        abbr = _abbr_for(t.get("teamFullName", ""))
        if abbr in team_data:
            team_data[abbr]["pkPct"] = round(float(t.get("penaltyKillPct", 0) or 0), 1)

    # Odds API — h2h moneylines + totals
    game_lines: dict = {}
    if api_key:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                evs_r = await c.get(
                    f"{ODDS_API}/sports/icehockey_nhl/events",
                    params={"apiKey": api_key, "dateFormat": "iso"})
                if evs_r.status_code == 200:
                    today_evs = [e for e in evs_r.json()
                                 if e.get("commence_time", "")[:10] == target_date]
                    if today_evs:
                        odd_tasks = [
                            c.get(f"{ODDS_API}/sports/icehockey_nhl/events/{ev['id']}/odds",
                                  params={"apiKey": api_key, "regions": "us,us2,eu,ca",
                                          "markets": "h2h,totals", "oddsFormat": "american"})
                            for ev in today_evs
                        ]
                        odd_rs = await asyncio.gather(*odd_tasks, return_exceptions=True)
                        for ev, r in zip(today_evs, odd_rs):
                            if isinstance(r, Exception) or r.status_code != 200:
                                continue
                            h_abbr = _nhl_match_team_name(ev.get("home_team", ""))
                            a_abbr = _nhl_match_team_name(ev.get("away_team", ""))
                            if not h_abbr or not a_abbr:
                                continue
                            entry: dict = {}
                            for book in r.json().get("bookmakers", []):
                                for mkt in book.get("markets", []):
                                    mk = mkt.get("key")
                                    for oc in mkt.get("outcomes", []):
                                        pr  = oc.get("price", 0)
                                        pt  = oc.get("point")
                                        nm  = oc.get("name", "")
                                        na  = _nhl_match_team_name(nm)
                                        if mk == "h2h":
                                            if na == h_abbr and "home_ml" not in entry:
                                                entry["home_ml"] = pr
                                                entry["ml_book"] = book.get("key", "")
                                            elif na == a_abbr and "away_ml" not in entry:
                                                entry["away_ml"] = pr
                                        elif mk == "totals":
                                            side = nm.upper()
                                            if side == "OVER" and "total" not in entry:
                                                entry["total"]     = pt
                                                entry["over_odds"] = pr
                                                entry["tot_book"]  = book.get("key", "")
                                            elif side == "UNDER" and "under_odds" not in entry:
                                                entry["under_odds"] = pr
                            if entry:
                                game_lines[(a_abbr, h_abbr)] = entry
        except Exception as _gp_oe:
            print(f"[GP] Odds API game lines error: {_gp_oe}")

    return {"team_data": team_data, "game_lines": game_lines}


def _nhl_gp_predict(games: list, gp_data: dict) -> list:
    """Pythagorean + H/A history + L10 form + B2B model for each today's game."""
    team_data  = gp_data.get("team_data", {})
    game_lines = gp_data.get("game_lines", {})
    if not team_data:
        return []

    all_gf = [td["gfPG"] for td in team_data.values() if td["gfPG"] > 0]
    all_ga = [td["gaPG"] for td in team_data.values() if td["gaPG"] > 0]
    lg_avg = sum(all_gf) / len(all_gf) if all_gf else 3.0
    lg_ga  = sum(all_ga) / len(all_ga) if all_ga else 3.0

    preds = []
    for g in games:
        home, away = g.get("homeTeam", ""), g.get("awayTeam", "")
        ht, at = team_data.get(home), team_data.get(away)
        if not ht or not at:
            continue

        # Normalised offense/defense strength factors
        h_off = ht["gfPG"] / lg_avg if lg_avg else 1.0
        h_def = lg_ga  / ht["gaPG"] if ht["gaPG"] else 1.0
        a_off = at["gfPG"] / lg_avg if lg_avg else 1.0
        a_def = lg_ga  / at["gaPG"] if at["gaPG"] else 1.0

        # Projected goals (opponent-adjusted) with 5% home-ice advantage
        proj_h = round(lg_avg * h_off * a_def * 1.05, 2)
        proj_a = round(lg_avg * a_off * h_def,         2)

        # B2B penalty (~5%)
        if ht.get("b2b"): proj_h = round(proj_h * 0.95, 2)
        if at.get("b2b"): proj_a = round(proj_a * 0.95, 2)

        # L10 form nudge (multiplier 0.95–1.05)
        l10_h = (ht["l10W"] + 0.5 * ht["l10OTL"]) / 10
        l10_a = (at["l10W"] + 0.5 * at["l10OTL"]) / 10
        proj_h = round(proj_h * (0.95 + 0.10 * l10_h), 2)
        proj_a = round(proj_a * (0.95 + 0.10 * l10_a), 2)

        proj_total = round(proj_h + proj_a, 1)

        # Pythagorean expectation
        pyth = (proj_h ** 2 / (proj_h ** 2 + proj_a ** 2)
                if (proj_h + proj_a) > 0 else 0.5)

        # Historical H/A win-rates (requires ≥5 H/A games for credibility)
        hg = ht["homeW"] + ht["homeL"] + ht["homeOTL"]
        ag = at["roadW"] + at["roadL"] + at["roadOTL"]
        h_ha = (ht["homeW"] + 0.5 * ht["homeOTL"]) / hg if hg >= 5 else 0.5
        a_rd = (at["roadW"] + 0.5 * at["roadOTL"]) / ag if ag >= 5 else 0.5
        ha_blend = (h_ha + (1 - a_rd)) / 2

        # Blend 70% model + 30% H/A history; clamp [0.25, 0.75]
        win_prob = max(0.25, min(0.75, 0.70 * pyth + 0.30 * ha_blend))

        # B2B win-probability adjustment (~4 pts)
        if ht.get("b2b"): win_prob = max(0.25, win_prob - 0.04)
        if at.get("b2b"): win_prob = min(0.75, win_prob + 0.04)
        win_prob = round(win_prob, 3)

        pick_team = home if win_prob >= 0.5 else away
        pick_prob = round((win_prob if win_prob >= 0.5 else 1 - win_prob) * 100, 1)

        gl = game_lines.get((away, home), {})
        book_total  = gl.get("total")
        over_odds   = gl.get("over_odds")
        under_odds  = gl.get("under_odds")
        home_ml     = gl.get("home_ml")
        away_ml     = gl.get("away_ml")

        ou_rec = None
        if book_total is not None:
            diff = proj_total - book_total
            ou_rec = "OVER" if diff > 0.25 else "UNDER" if diff < -0.25 else "PUSH"

        ml_impl_h = None
        if home_ml is not None:
            ml_impl_h = (round(100 / (home_ml + 100), 3) if home_ml > 0
                         else round(-home_ml / (-home_ml + 100), 3))

        preds.append({
            "gameId": g.get("gameId"),
            "homeTeam": home, "awayTeam": away,
            "homeFull": g.get("homeFull", ""), "awayFull": g.get("awayFull", ""),
            "startTime": g.get("startTime", ""),
            "projHome": proj_h, "projAway": proj_a, "projTotal": proj_total,
            "winProbHome": win_prob, "pickTeam": pick_team, "pickProb": pick_prob,
            "hGfPG": ht["gfPG"], "hGaPG": ht["gaPG"],
            "hSfPG": ht["sfPG"], "hSaPG": ht["saPG"],
            "hPpPct": ht["ppPct"], "hPkPct": ht["pkPct"],
            "hHomeRec": f"{ht['homeW']}-{ht['homeL']}-{ht['homeOTL']}",
            "hL10":  f"{ht['l10W']}-{ht['l10L']}-{ht['l10OTL']}",
            "hStreak": ht["streak"], "hPts": ht["pts"], "hPctg": ht["pctg"],
            "hB2b": ht["b2b"],
            "aGfPG": at["gfPG"], "aGaPG": at["gaPG"],
            "aSfPG": at["sfPG"], "aSaPG": at["saPG"],
            "aPpPct": at["ppPct"], "aPkPct": at["pkPct"],
            "aRoadRec": f"{at['roadW']}-{at['roadL']}-{at['roadOTL']}",
            "aL10":  f"{at['l10W']}-{at['l10L']}-{at['l10OTL']}",
            "aStreak": at["streak"], "aPts": at["pts"], "aPctg": at["pctg"],
            "aB2b": at["b2b"],
            "bookTotal": book_total, "overOdds": over_odds, "underOdds": under_odds,
            "homeMl": home_ml, "awayMl": away_ml,
            "ouRec": ou_rec, "mlImpliedH": ml_impl_h,
            "totBook": gl.get("tot_book", ""), "mlBook": gl.get("ml_book", ""),
        })

    return preds


def _nhl_pre_game_logs(logs: List[Dict], target_date: str) -> List[Dict]:
    """Return only logs available before a target game's date.

    Historical replay uses the same player-form calculations as the live
    pipeline, but must not let the target game's result (or later games) leak
    into that pre-game decision.
    """
    if not target_date:
        return list(logs or [])
    cutoff = str(target_date)[:10]
    return [
        g for g in (logs or [])
        if str(g.get("date") or "")[:10] < cutoff
    ]


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
            # A historical replay cannot use the target game's appearance, or
            # a later appearance, to decide whether the player was recently
            # active before puck drop.
            if cutoff <= date.fromisoformat(d) < date.fromisoformat(ref_date):
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


async def _get_historical_shot_lines(c: httpx.AsyncClient, api_key: str,
                                     target_date: str,
                                     games: List[Dict]) -> Dict[str, Dict]:
    """Fetch archived pre-game SOG lines for a completed NHL slate.

    Historical player props are only exposed through the Odds API's event
    endpoint (not its all-games historical odds endpoint).  The 20:00 UTC
    snapshot is a pre-game afternoon snapshot for the normal NHL evening
    slate, and the API returns the closest archived snapshot at or before it.
    """
    snapshot = f"{target_date}T20:00:00Z"
    tomorrow = (date.fromisoformat(target_date) + timedelta(days=1)).isoformat()
    event_url = f"{ODDS_API}/historical/sports/icehockey_nhl/events"
    event_params = {
        "apiKey": api_key, "date": snapshot, "dateFormat": "iso",
        "commenceTimeFrom": f"{target_date}T00:00:00Z",
        "commenceTimeTo": f"{tomorrow}T05:59:59Z",
    }
    try:
        r = await c.get(event_url, params=event_params)
        if r.status_code != 200:
            print(f"[HistoricalLines] event lookup {r.status_code} for {target_date}")
            return {}
        payload = r.json()
        events = payload.get("data", payload if isinstance(payload, list) else [])
        events = [
            ev for ev in events
            if ev.get("commence_time", "")[:10] in (target_date, tomorrow)
            and _odds_event_matches_slate(ev, games)
        ]
        if not events:
            print(f"[HistoricalLines] no archived NHL events for {target_date}")
            return {}
        print(f"[HistoricalLines] {len(events)} events at "
              f"{payload.get('timestamp', snapshot)} for {target_date}")

        sem = asyncio.Semaphore(6)

        async def _event_shot_lines(ev: Dict) -> Dict[str, Dict]:
            async with sem:
                r2 = await c.get(
                    f"{event_url}/{ev['id']}/odds",
                    params={
                        "apiKey": api_key, "date": snapshot,
                        "regions": "us,ca", "markets": "player_shots_on_goal",
                        "oddsFormat": "american",
                    },
                )
            if r2.status_code != 200:
                print(f"[HistoricalLines] event odds {r2.status_code} for "
                      f"{ev.get('away_team')} @ {ev.get('home_team')}")
                return {}
            odds_payload = r2.json()
            game = odds_payload.get("data", odds_payload)
            found: Dict[str, Dict] = {}
            for book in game.get("bookmakers", []):
                for market in book.get("markets", []):
                    if market.get("key") != "player_shots_on_goal":
                        continue
                    for outcome in market.get("outcomes", []):
                        side = outcome.get("name")
                        if side not in ("Over", "Under"):
                            continue
                        player = outcome.get("description", "").strip()
                        try:
                            line = float(outcome.get("point"))
                        except (TypeError, ValueError):
                            continue
                        if not player or line <= 0:
                            continue
                        rec = found.setdefault(player, {
                            "line": line, "odds": "", "under_odds": "",
                            "source": "Historical Odds API",
                        })
                        if side == "Over" and not rec["odds"]:
                            rec["odds"] = str(outcome.get("price", ""))
                        elif side == "Under" and not rec["under_odds"]:
                            rec["under_odds"] = str(outcome.get("price", ""))
            return found

        per_event = await asyncio.gather(*[_event_shot_lines(ev) for ev in events])
        lines: Dict[str, Dict] = {}
        for event_lines in per_event:
            for player, rec in event_lines.items():
                lines.setdefault(player, rec)
        print(f"[HistoricalLines] {len(lines)} archived shot lines for {target_date}")
        return lines
    except Exception as exc:
        print(f"[HistoricalLines] fetch error for {target_date}: {exc}")
        return {}


async def get_shot_lines(target_date: str, games: List[Dict] = None) -> Dict[str, Dict]:
    """Fetch real shots on goal lines from The Odds API.
    Tries icehockey_nhl first, then icehockey_nhl_championship (playoffs).
    Returns empty maps when books have not posted lines; it never invents a
    sportsbook price or claims a model threshold is a book line.
    """
    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        print("[Lines] ODDS_API_KEY not set — no sportsbook props can be published")
        return {}, {}, {}, {}, {}

    games = games or []
    # A separate cache namespace prevents older date+tomorrow mixed slates from
    # being reused after lineup eligibility became date/game specific.
    _oc = _odds_cache_get("nhl_lineup_v2", target_date)
    if _oc is not None:
        cached_lines = _oc.get("lines", {})
        # A past simulation could previously cache an empty live-endpoint
        # response. Do not let that stale miss prevent the new historical
        # player-prop lookup from running.
        if not (date.fromisoformat(target_date) < date.today() and not cached_lines):
            return (cached_lines, _oc.get("pts", {}),
                    _oc.get("ast", {}), _oc.get("sv", {}), _oc.get("goals", {}))
        print(f"[HistoricalLines] refreshing empty cached line set for {target_date}")

    tomorrow = (date.fromisoformat(target_date) + timedelta(days=1)).isoformat()
    SPORT_KEYS = ["icehockey_nhl", "icehockey_nhl_championship"]

    try:
        lines: Dict[str, Dict] = {}
        pts_lines: Dict[str, Dict] = {}
        ast_lines: Dict[str, Dict] = {}
        sv_lines: Dict[str, Dict] = {}
        goal_lines: Dict[str, Dict] = {}
        async with httpx.AsyncClient(timeout=20) as c:
            if date.fromisoformat(target_date) < date.today():
                lines = await _get_historical_shot_lines(c, api_key, target_date, games)
                if lines:
                    _odds_cache_set("nhl_lineup_v2", target_date, {
                        "lines": lines, "pts": {}, "ast": {}, "sv": {}, "goals": {}})
                    return lines, {}, {}, {}, {}
                print(f"[HistoricalLines] no archived shot lines available; "
                      f"simulation fallback may be used for {target_date}")
            for sport_key in SPORT_KEYS:
                r = await c.get(
                    f"{ODDS_API}/sports/{sport_key}/events",
                    params={"apiKey": api_key, "dateFormat": "iso"})
                if r.status_code != 200:
                    continue
                events = [
                    e for e in r.json()
                    if e.get("commence_time", "")[:10] in (target_date, tomorrow)
                    and _odds_event_matches_slate(e, games)
                ]
                print(f"[OddsAPI] {sport_key}: {len(events)} games for {target_date}")
                if not events:
                    continue

                # Fetch all events' odds concurrently (main markets + goal scorer
                # in parallel per event, then all events gathered at once).
                async def _fetch_ev_odds(ev):
                    # Main O/U markets
                    r2 = await c.get(
                        f"{ODDS_API}/sports/{sport_key}/events/{ev['id']}/odds",
                        params={"apiKey": api_key, "regions": "us,us2,eu,ca",
                                "markets": ("player_shots_on_goal,player_points,"
                                            "player_assists,player_total_saves"),
                                "oddsFormat": "american"})
                    if r2.status_code == 200:
                        targets = {
                            "player_shots_on_goal": lines,
                            "player_points":        pts_lines,
                            "player_assists":       ast_lines,
                            "player_total_saves":   sv_lines,
                        }
                        for book in r2.json().get("bookmakers", []):
                            for mkt in book.get("markets", []):
                                mkey = mkt.get("key")
                                if mkey not in targets:
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
                    # Anytime goal scorer — isolated call so a bad market never wipes main
                    try:
                        rg = await c.get(
                            f"{ODDS_API}/sports/{sport_key}/events/{ev['id']}/odds",
                            params={"apiKey": api_key, "regions": "us,us2,eu,ca",
                                    "markets": "player_goal_scorer_anytime",
                                    "oddsFormat": "american"})
                        if rg.status_code == 200:
                            for book in rg.json().get("bookmakers", []):
                                for mkt in book.get("markets", []):
                                    if mkt.get("key") != "player_goal_scorer_anytime":
                                        continue
                                    for oc in mkt.get("outcomes", []):
                                        if oc.get("name") != "Yes":
                                            continue
                                        player = oc.get("description", "").strip()
                                        if not player:
                                            continue
                                        rec = goal_lines.setdefault(player, {
                                            "line": 0.5, "odds": "",
                                            "under_odds": "", "source": "OddsAPI"})
                                        if not rec["odds"]:
                                            rec["odds"] = str(oc.get("price", ""))
                    except Exception as _ge:
                        print(f"[Lines] goal-scorer fetch skipped: {_ge}")

                await asyncio.gather(*[_fetch_ev_odds(ev) for ev in events])

                if lines or pts_lines or ast_lines or sv_lines or goal_lines:
                    break  # found lines — no need to try next sport key

        print(f"[Lines] {len(lines)} shot | {len(pts_lines)} point | "
              f"{len(ast_lines)} assist | {len(sv_lines)} saves | {len(goal_lines)} goals lines from The Odds API")
        if lines or pts_lines or ast_lines or sv_lines or goal_lines:
            _odds_cache_set("nhl_lineup_v2", target_date, {
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
    lineup_maps: List[Dict] = None,
) -> List[Dict]:
    """Build the game-day skater pool and attach posted book lines."""
    if lines_map is None:
        lines_map = {}
    lineup_maps = lineup_maps or [lines_map]

    team_ctx: Dict[str, Dict] = {}
    for g in games:
        team_ctx[g["homeTeam"]] = {"opponent": g["awayTeam"], "homeRoad": "H"}
        team_ctx[g["awayTeam"]] = {"opponent": g["homeTeam"],  "homeRoad": "R"}

    # Get rosters for all playing teams
    roster_vals = await asyncio.gather(
        *[get_roster(t, sem) for t in team_ctx], return_exceptions=True)
    rosters = {t: (r if isinstance(r, list) else [])
               for t, r in zip(team_ctx.keys(), roster_vals)}
    rosters = _lineup_filtered_rosters(games, rosters, lineup_maps, "skaters")

    pool: List[Dict] = []
    seen: set = set()

    # Keep the complete active-player pool for shared game-log work, but leave
    # players without a book line out of the published Shots market.
    for team, players in rosters.items():
        ctx = team_ctx.get(team, {})
        opp = ctx.get("opponent", "")
        hr  = ctx.get("homeRoad", "")
        for p in players:
            if p["id"] in seen:
                continue
            seen.add(p["id"])
            # Look up the actual sportsbook line.  There is deliberately no
            # 1.5 estimate: the analyzer skips a player without this line.
            real_line, real_odds, line_source = None, "", "No book line"
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
                "line":       real_line,
                "realLine":   real_line,
                "realOdds":   real_odds,
                "realUnderOdds": real_under_odds,
                "lineSource": line_source,
                "estLine":    None,
                "spg":        0,
                "oppSA":      sa_map.get(opp, 0.0),
            })

    print(f"[NHL] {len(pool)} lineup-eligible skaters in pool | "
          f"{len(lines_map)} posted shot lines")
    pool.sort(key=lambda x: x["oppSA"], reverse=True)
    return pool, rosters

# ─────────────────────────────────────────────────────────────────────────────
#  Points picks - NHL Stats API game logs (independent of shots)
# ─────────────────────────────────────────────────────────────────────────────

async def _pts_season_logs(pid: int, season: str, c: httpx.AsyncClient) -> List[Dict]:
    """Fetch regular season (2) and playoff (3) game logs concurrently."""
    datas = await asyncio.gather(
        _fetch(f"{NHL_API}/player/{pid}/game-log/{season}/2", c),
        _fetch(f"{NHL_API}/player/{pid}/game-log/{season}/3", c),
        return_exceptions=True,
    )
    logs = []
    for data in datas:
        if not data or isinstance(data, Exception):
            continue
        for g in data.get("gameLog", []):
            goals   = int(g.get("goals",   0) or 0)
            assists = int(g.get("assists", 0) or 0)
            logs.append({
                "date":       g.get("gameDate",     ""),
                "points":     goals + assists,
                "powerPlayPoints": int(g.get("powerPlayPoints", 0) or 0),
                "goals":      goals,
                "assists":    assists,
                "toi_sec":    _parse_toi(g.get("toi", "0:00")),
                "pp_toi_sec": _parse_toi(g.get("powerPlayToi", "0:00")),
                "homeRoad":   g.get("homeRoadFlag", ""),
                "opponent":   g.get("opponentAbbrev", ""),
            })
    return logs


async def _goalie_season_logs(pid: int, season: str, c: httpx.AsyncClient) -> List[Dict]:
    """Goalie game logs — saves = shotsAgainst - goalsAgainst (parallel gtype fetch)."""
    datas = await asyncio.gather(
        _fetch(f"{NHL_API}/player/{pid}/game-log/{season}/2", c),
        _fetch(f"{NHL_API}/player/{pid}/game-log/{season}/3", c),
        return_exceptions=True,
    )
    logs = []
    for data in datas:
        if not data or isinstance(data, Exception):
            continue
        for g in data.get("gameLog", []):
            sa    = int(g.get("shotsAgainst", 0) or 0)
            ga    = int(g.get("goalsAgainst", 0) or 0)
            saves = max(0, sa - ga)
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
    shared_logs: Dict[int, List[Dict]] = None,
    shared_rosters: Dict[str, List[Dict]] = None,
):
    """Independent points + power-play points + assists + goals picks using NHL Stats API game logs.
    Returns (points_picks, assist_picks, points_unders, assist_unders, goal_picks,
    goal_unders, power_play_points_picks, power_play_points_unders).
    shared_logs: pre-fetched {pid: logs} from the shots phase — skips redundant API calls.
    shared_rosters: pre-fetched {team: [players]} from the shots phase — skips roster re-fetch."""

    pts_lines_map = pts_lines_map or {}
    ast_lines_map = ast_lines_map or {}
    goal_lines_map = goal_lines_map or {}
    goalie_map = goalie_map or {}

    # Build team context
    team_ctx: Dict[str, Dict] = {}
    for g in games:
        team_ctx[g["homeTeam"]] = {"opponent": g["awayTeam"], "homeRoad": "H"}
        team_ctx[g["awayTeam"]] = {"opponent": g["homeTeam"],  "homeRoad": "R"}

    # Reuse rosters from shots phase if available — avoids one get_roster call per team
    if shared_rosters is not None:
        rosters = shared_rosters
    else:
        roster_vals = await asyncio.gather(
            *[get_roster(t, sem) for t in team_ctx], return_exceptions=True
        )
        rosters = {t: (r if isinstance(r, list) else []) for t, r in zip(team_ctx.keys(), roster_vals)}

    # Fetch multi-season game logs for all players concurrently
    all_players = []
    seen_pts = set()
    for team, players in rosters.items():
        if team not in team_ctx:
            continue
        ctx = team_ctx[team]
        for p in players:
            if p["id"] not in seen_pts:
                seen_pts.add(p["id"])
                all_players.append((p, team, ctx["opponent"], ctx["homeRoad"]))

    async def fetch_logs(pid):
        # Reuse shots-phase logs — they already contain goals/assists/points/toi/homeRoad/opponent
        if shared_logs is not None and pid in shared_logs:
            return shared_logs[pid]
        # Player not in shots pool (rare) — fetch fresh
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

    pts_picks, ast_picks, goal_picks, pp_picks = [], [], [], []
    pts_unders, ast_unders, goal_unders, pp_unders = [], [], [], []
    for player, team, opp, hr in all_players:
        full_logs = logs_map.get(player["id"], [])
        # Only players actually in today's rotation (drops scratches/AHL/injured depth)
        if target_date and not _played_recently(full_logs, target_date):
            continue
        logs = _nhl_pre_game_logs(full_logs, target_date)

        # Career H/A vs today's opponent — cap at last 10 for consistency w/ shots
        c_logs = [g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10]
        # Last 10 H/A any opponent
        r_logs = [g for g in logs if g["homeRoad"] == hr][:10]

        if len(r_logs) < MIN_GAMES:
            continue

        def build_pick(stat_key, base_line, thresh, lines_map, mkt_label,
                       model_only=False, allow_model_fallback=False):
            """Normalized pick for one market. Model-only markets use base_line
            and remain visibly unpriced instead of pretending a book posted it."""
            analysis_line, real_line, real_odds, under_odds, line_source = None, None, "", "", "No book line"
            if model_only:
                analysis_line = base_line
                line_source = "Model"
            else:
                for odds_name, sb_info in (lines_map or {}).items():
                    if _match_odds_name(odds_name, [{"name": player["name"]}]):
                        analysis_line = sb_info.get("line")
                        real_odds = sb_info.get("odds", "")
                        under_odds = sb_info.get("under_odds", "")
                        line_source = sb_info.get("source", "OddsAPI")
                        break
            if analysis_line is None:
                if not allow_model_fallback:
                    return None
                analysis_line = base_line
                line_source = "No book line"
            posted_line = line_source not in ("Simulation", "Model", "No book line")
            real_line = analysis_line if posted_line or line_source == "Simulation" else None
            line = analysis_line
            h3 = sum(1 for g in r_logs if g[stat_key] > line)
            r3 = round(h3 / len(r_logs) * 100, 1)
            avg3 = round(sum(g[stat_key] for g in r_logs) / len(r_logs), 2)
            h2 = sum(1 for g in c_logs if g[stat_key] > line) if c_logs else 0
            r2 = round(h2 / len(c_logs) * 100, 1) if c_logs else 0
            avg2 = round(sum(g[stat_key] for g in c_logs) / len(c_logs), 2) if c_logs else 0
            # Qualify on career H/A vs opp if we have it, else last-10 H/A.
            qualifies = (r2 >= thresh) if len(c_logs) >= MIN_GAMES else (r3 >= thresh)
            over_ok = bool(qualifies)
            score = round((r2 + r3) / 2 if c_logs else r3, 1)
            vsl_hits, vsl_total, vsl_rate = h3, len(r_logs), r3
            gap = round(avg3 - line, 2)
            tag = _book_tag(line, avg3, vsl_rate)
            # Under track — vs-opp OR any-opp H/A (so genuine fades surface)
            uf = _under_fields(logs, stat_key, line, hr, opp)
            if not over_ok and not uf["underOk"]:
                return None
            # Game log for the per-card dropdown (vs opp if available, else L10 H/A)
            g_src = ([g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10]
                     or [g for g in logs if g["homeRoad"] == hr][:10])
            glog = [{"d": g["date"], "v": g[stat_key]} for g in g_src]
            # Signal factors
            toi_avg_sec = round(sum(g.get("toi_sec", 0) for g in r_logs) / len(r_logs)) if r_logs else 0
            pp_toi_avg_sec = round(sum(g.get("pp_toi_sec", 0) for g in r_logs) / len(r_logs)) if r_logs else 0
            hot_hits_p, hot_total_p = _hot_streak(logs, stat_key, line, hr, 5)
            rest_days_p = _days_rest(logs, target_date)
            opp_sv = goalie_map.get(opp)
            sim_actual, sim_void_reason = _nhl_sim_actual_from_logs(
                full_logs, target_date, hr, opp, stat_key)
            return {
                "name": player["name"], "pid": player["id"], "team": team,
                "opponent": opp, "homeRoad": hr, "oppSA": sa_map.get(opp, 0.0),
                "realLine": real_line, "realOdds": real_odds, "realUnderOdds": under_odds,
                "lineSource": line_source,
                "mkt": mkt_label,
                "dispLine": line,
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
                "simActual": sim_actual, "simVoidReason": sim_void_reason,
            }

        pp = build_pick(
            "points", PTS_LINE, HIT_THRESH_PTS, pts_lines_map, "Points (1+)",
            allow_model_fallback=True)
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

        ppp = build_pick(
            "powerPlayPoints", PTS_LINE, HIT_THRESH_PTS, {},
            "Power Play Points (1+)", model_only=True)
        if ppp:
            if ppp["overOk"]: pp_picks.append(ppp)
            if ppp["underOk"]: pp_unders.append(ppp)

    pts_picks.sort(key=lambda x: (x["ptsScore"], x["oppSA"]), reverse=True)
    ast_picks.sort(key=lambda x: (x["dispScore"], x["oppSA"]), reverse=True)
    goal_picks.sort(key=lambda x: (x["dispScore"], x["oppSA"]), reverse=True)
    pp_picks.sort(key=lambda x: (x["dispScore"], x["oppSA"]), reverse=True)
    pts_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    ast_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    goal_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    pp_unders.sort(key=lambda x: (x["underRate"], x["underTotal"]), reverse=True)
    print(f"[PTS] {len(pts_picks)} points | {len(ast_picks)} assists | {len(goal_picks)} goals | "
          f"{len(pp_picks)} power-play points | {len(pts_unders)} pts unders | "
          f"{len(ast_unders)} ast unders | {len(goal_unders)} goal unders | "
          f"{len(pp_unders)} power-play point unders")
    return (pts_picks, ast_picks, pts_unders, ast_unders, goal_picks, goal_unders,
            pp_picks, pp_unders)


async def get_saves_picks(
    games: List[Dict],
    sa_map: Dict[str, float],
    sem: asyncio.Semaphore,
    season: str = "20252026",
    sv_lines_map: Dict[str, Dict] = None,
    target_date: str = None,
    simulate: bool = False,
    allow_fallback: bool = False,
    lineup_maps: List[Dict] = None,
) -> List[Dict]:
    """Goalie saves picks using only game-day eligible goalies."""
    sv_lines_map = sv_lines_map or {}
    lineup_maps = lineup_maps or [sv_lines_map]

    team_ctx: Dict[str, Dict] = {}
    for g in games:
        team_ctx[g["homeTeam"]] = {"opponent": g["awayTeam"], "homeRoad": "H"}
        team_ctx[g["awayTeam"]] = {"opponent": g["homeTeam"],  "homeRoad": "R"}

    roster_vals = await asyncio.gather(
        *[get_goalies(t, sem) for t in team_ctx], return_exceptions=True)
    rosters = {t: (r if isinstance(r, list) else [])
               for t, r in zip(team_ctx.keys(), roster_vals)}
    rosters = _lineup_filtered_rosters(games, rosters, lineup_maps, "goalies")

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
        full_logs = logs_map.get(goalie["id"], [])
        # Only goalies actively playing (drops third-string/AHL/injured goalies)
        if target_date and not _played_recently(full_logs, target_date):
            continue
        logs = _nhl_pre_game_logs(full_logs, target_date)
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
        line_source = "OddsAPI"
        if real_line is None:
            if not (simulate or allow_fallback):
                continue
            # Simulation-only fallback.  This branch is never used by the
            # live/cron pipeline unless no book line exists; the caller marks
            # that unpriced run separately and live cards show the estimate.
            real_line, line_source = 24.5, "Simulation"
            if allow_fallback and not simulate:
                line_source = "No book line"
        base_line = real_line
        book_line = real_line if line_source != "No book line" else None

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
        gap = round(avg3 - real_line, 2)
        tag = _book_tag(real_line, avg3, r3)

        g_src = c_logs or r_logs
        glog = [{"d": g["date"], "v": g["saves"]} for g in g_src]
        rest_days_sv = _days_rest(logs, target_date)
        hot_hits_sv, hot_total_sv = _hot_streak(logs, "saves", base_line, hr, 5)
        sim_actual_sv, sim_void_reason_sv = _nhl_sim_actual_from_logs(
            full_logs, target_date, hr, opp, "saves")

        rec = {
            "name": goalie["name"], "pid": goalie["id"], "team": team,
            "opponent": opp, "homeRoad": hr, "oppSA": sa_map.get(opp, 0.0),
            "realLine": book_line, "realOdds": real_odds, "realUnderOdds": under_odds,
            "lineSource": line_source,
            "mkt": "Goalie Saves", "dispLine": base_line,
            "avg": avg3, "avgA": avg2,
            "rateA": r2, "hitsA": h2, "totA": len(c_logs),
            "rateB": r3, "hitsB": h3, "totB": len(r_logs),
            "dispScore": score,
            "vsLineHits": h3, "vsLineTotal": len(r_logs), "vsLineRate": r3,
            "gap": gap, "tag": tag,
            **uf, "overOk": over_ok,
            "glog": glog,
            "restDays": rest_days_sv, "hotHits": hot_hits_sv, "hotTotal": hot_total_sv,
            "toiAvgSec": 0, "ppToiAvgSec": 0, "oppGoalieSv": None,
            "simActual": sim_actual_sv, "simVoidReason": sim_void_reason_sv,
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
                "powerPlayPoints": int(g.get("powerPlayPoints", 0) or 0),
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


def _nhl_sim_actual_from_logs(logs: list, target_date: str, home_road: str,
                              opponent: str, stat_key: str) -> tuple:
    """Return a replay stat and an explicit reason when the player is void."""
    target_logs = [g for g in logs if g.get("date") == target_date]
    for game in target_logs:
        if game.get("homeRoad") == home_road and game.get("opponent") == opponent:
            actual = game.get(stat_key)
            if actual is not None:
                return actual, ""
            return None, "The NHL game log did not contain this stat."
    if target_logs:
        return None, (
            f"Player appeared in a different {target_date} matchup than this "
            "replay's expected team/opponent."
        )
    return None, f"No recorded NHL appearance on {target_date}."


def _nhl_sim_prop_summary(result: dict, historical: bool = False) -> dict:
    """Grade displayed player-prop calls without writing to the record."""
    market_lists = [
        ("picks", "OVER", "Shots on Goal"), ("rest", "OVER", "Shots on Goal"),
        ("shotUnders", "UNDER", "Shots on Goal"),
        ("shotUndersRest", "UNDER", "Shots on Goal"),
        ("ptsPicks", "OVER", "Points"), ("ptsRest", "OVER", "Points"),
        ("ptsUnders", "UNDER", "Points"),
        ("ptsUndersRest", "UNDER", "Points"),
        ("ppPicks", "OVER", "Power Play Points"), ("ppRest", "OVER", "Power Play Points"),
        ("ppUnders", "UNDER", "Power Play Points"),
        ("ppUndersRest", "UNDER", "Power Play Points"),
        ("astPicks", "OVER", "Assists"), ("astRest", "OVER", "Assists"),
        ("astUnders", "UNDER", "Assists"),
        ("astUndersRest", "UNDER", "Assists"),
        ("goalPicks", "OVER", "Goals"), ("goalRest", "OVER", "Goals"),
        ("goalUnders", "UNDER", "Goals"),
        ("goalUndersRest", "UNDER", "Goals"),
        ("savesPicks", "OVER", "Goalie Saves"),
        ("savesRest", "OVER", "Goalie Saves"),
        ("savesUnders", "UNDER", "Goalie Saves"),
        ("savesUndersRest", "UNDER", "Goalie Saves"),
    ]
    totals = {"wins": 0, "losses": 0, "pushes": 0, "voids": 0, "pending": 0}
    by_market = {}
    for result_key, side, market in market_lists:
        bucket = by_market.setdefault(
            market, {"wins": 0, "losses": 0, "pushes": 0, "voids": 0, "pending": 0})
        for pick in result.get(result_key) or []:
            actual = pick.get("simActual")
            line = pick.get("realLine")
            if line is None:
                line = pick.get("dispLine")
            outcome = None
            try:
                if actual is not None and line is not None:
                    actual, line = float(actual), float(line)
                    if actual == line:
                        outcome = "pushes"
                    elif (side == "OVER" and actual > line) or (
                            side == "UNDER" and actual < line):
                        outcome = "wins"
                    else:
                        outcome = "losses"
            except (TypeError, ValueError):
                outcome = None
            # On a completed historical slate, a current-roster player without
            # a matching appearance did not participate in this replay. Void
            # that call rather than misrepresenting it as still pending.
            key = outcome or ("voids" if historical else "pending")
            totals[key] += 1
            bucket[key] += 1

    def finish(counts):
        decided = counts["wins"] + counts["losses"]
        return {
            **counts, "decided": decided,
            "percentage": round(counts["wins"] / decided * 100, 1)
            if decided else None,
        }

    return {
        **finish(totals),
        "by_market": [
            {"market": market, **finish(counts)}
            for market, counts in by_market.items()
        ],
    }


def _nhl_simulation_stats(target_date: str, game_predictions: list,
                          result: dict) -> dict:
    """Build display-only historical results for a simulation date."""
    stats = {
        "date": target_date,
        "team": {"wins": 0, "losses": 0, "pushes": 0, "pending": 0,
                 "decided": 0, "percentage": None},
        "player_props": _nhl_sim_prop_summary(
            result, historical=target_date < date.today().isoformat()),
        "note": (
            "Retrospective replay only. Results are not saved to the official "
            "pre-game record."
        ),
    }
    try:
        graded = _nhl_grade_gp_date(target_date, game_predictions or [])
        summary = _nhl_gp_summary(graded.get("detail") or [])
        stats["team"] = {
            "wins": summary["mlWins"], "losses": summary["mlLosses"],
            "pushes": summary["mlPushes"],
            "pending": sum(
                1 for row in graded.get("detail") or []
                if not row.get("mlResult")
            ),
            "decided": summary["mlWins"] + summary["mlLosses"],
            "percentage": summary["mlRate"],
        }
    except Exception as exc:
        print(f"[nhl_sim] historical Game Predictor grading failed {target_date}: {exc}")
        stats["team"]["pending"] = len(game_predictions or [])
        stats["team"]["error"] = "Completed game results were unavailable."
    return stats


def _nhl_historical_replay_payload(result: dict) -> dict:
    """Shape a non-persistent simulation as a Track Record day for viewing."""
    from collections import Counter

    detail = []
    void_reasons = Counter()
    for result_key, category, stat_key, side, is_overflow in _NHL_TRK_LISTS:
        rank_start = _NHL_TRK_TOP + 1 if is_overflow else 1
        for rank, pick in enumerate(result.get(result_key) or [], rank_start):
            line = pick.get("realLine")
            if line is None:
                line = pick.get("line")
            if line is None:
                line = pick.get("dispLine")
            odds = pick.get("realOdds") if side == "OVER" else pick.get("realUnderOdds")
            actual = pick.get("simActual")
            void_reason = ""
            outcome = None
            try:
                if actual is None or line is None:
                    outcome = "VOID"
                    void_reason = pick.get("simVoidReason") or (
                        "No matching historical NHL appearance was available."
                    )
                else:
                    actual, line = float(actual), float(line)
                    if actual == line:
                        outcome = "PUSH"
                    elif (side == "OVER" and actual > line) or (
                            side == "UNDER" and actual < line):
                        outcome = "WIN"
                    else:
                        outcome = "LOSS"
            except (TypeError, ValueError):
                outcome = "VOID"
                void_reason = "The historical stat or line could not be read."
            base_row = {
                "name": pick.get("name", ""), "team": pick.get("team", ""),
                "category": category, "stat_key": stat_key, "side": side,
                "line": line, "odds": odds, "rank": rank,
                "is_overflow": bool(is_overflow), "line_source": pick.get("lineSource", ""),
                "result": outcome, "actual": actual, "void_reason": void_reason,
                "profit": round(_nhl_american_profit(odds, _NHL_TRK_STAKE, outcome), 2)
                if outcome in ("WIN", "LOSS", "PUSH") and odds not in (None, "", "0")
                else None,
            }
            if outcome == "VOID":
                void_reasons[void_reason] += 1
            detail.append(base_row)
            # Historical replays must mirror the official grader: a qualifying
            # 80-100% pick is counted once in its native market and once in the
            # Locks category, while retaining its main/overflow source pool.
            try:
                lock_score = float(
                    pick.get("dispScore") or pick.get("ptsScore")
                    or pick.get("score") or 0
                )
            except (TypeError, ValueError):
                lock_score = 0.0
            if lock_score >= 80:
                detail.append({**base_row, "category": "80-100% Locks"})
                if outcome == "VOID":
                    void_reasons[void_reason] += 1

    gp_detail = _nhl_grade_gp_date(
        result.get("date") or result.get("targetDate") or "",
        result.get("game_predictions") or [],
    ).get("detail") or []
    gp = _nhl_gp_summary(gp_detail) if gp_detail else None
    main_detail = [row for row in detail if not row.get("is_overflow")]
    overflow_detail = [row for row in detail if row.get("is_overflow")]
    wins = sum(1 for row in main_detail if row["result"] == "WIN")
    losses = sum(1 for row in main_detail if row["result"] == "LOSS")
    pushes = sum(1 for row in main_detail if row["result"] == "PUSH")
    voids = sum(1 for row in main_detail if row["result"] == "VOID")
    overflow_wins = sum(1 for row in overflow_detail if row["result"] == "WIN")
    overflow_losses = sum(1 for row in overflow_detail if row["result"] == "LOSS")
    overflow_pushes = sum(1 for row in overflow_detail if row["result"] == "PUSH")
    overflow_voids = sum(1 for row in overflow_detail if row["result"] == "VOID")
    return {
        "date": result.get("date") or result.get("targetDate"),
        "detail": main_detail, "overflow_detail": overflow_detail, "gp": gp,
        "is_historical_replay": True,
        "note": (
            "Historical replay only — it is not an official pre-game snapshot "
            "and is excluded from the permanent Track Record."
        ),
        "summary": {
            "wins": wins, "losses": losses, "pushes": pushes, "voids": voids,
            "decided": wins + losses,
            "percentage": round(wins / (wins + losses) * 100, 1)
            if wins + losses else None,
            "void_reasons": [
                {"reason": reason, "count": count}
                for reason, count in void_reasons.most_common()
            ],
        },
        "overflow_summary": {
            "wins": overflow_wins, "losses": overflow_losses,
            "pushes": overflow_pushes, "voids": overflow_voids,
            "decided": overflow_wins + overflow_losses,
            "percentage": round(overflow_wins / (overflow_wins + overflow_losses) * 100, 1)
            if overflow_wins + overflow_losses else None,
        },
    }


async def run_picks(target_date: str = None, simulate: bool = False) -> Dict:
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
        get_shot_lines(target_date, games),
        get_opp_goalie_svpct(season),
    )
    lines_map, pts_lines_map, ast_lines_map, sv_lines_map, goal_lines_map = _lines_tuple
    unpriced_mode = not any((lines_map, pts_lines_map, ast_lines_map, sv_lines_map, goal_lines_map))
    if simulate or unpriced_mode:
        # Run Simulation always uses fallback lines.  A normal early-morning
        # run also continues with them when books have not posted any player
        # props yet; those live cards remain visibly unpriced and are not
        # presented as sportsbook lines.
        lines_map = lines_map or {}
        pts_lines_map = pts_lines_map or {}
        ast_lines_map = ast_lines_map or {}
        sv_lines_map = sv_lines_map or {}
        goal_lines_map = goal_lines_map or {}
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
    pool, skater_rosters = await get_shot_qualified_players(
        games, sa_map, sem_nhl, season, lines_map,
        lineup_maps=[lines_map, pts_lines_map, ast_lines_map, goal_lines_map],
    )
    archived_shot_line_count = sum(
        1 for player in pool
        if player.get("lineSource") == "Historical Odds API"
    )
    if simulate or unpriced_mode:
        fallback_source = "Simulation" if simulate else "No book line"
        for player in pool:
            if player.get("realLine") is None:
                player.update({
                    "line": 1.5, "realLine": 1.5 if simulate else None, "realOdds": "",
                    "realUnderOdds": "", "lineSource": fallback_source,
                    "estLine": 1.5,
                })
        def _fill_sim_map(existing, default_line):
            for player in (skater_rosters or {}).values():
                for roster_player in player:
                    existing.setdefault(roster_player.get("name", ""), {
                        "line": default_line, "odds": "", "under_odds": "",
                        "source": fallback_source,
                    })
        _fill_sim_map(lines_map, 1.5)
        # Points (1+) model/replay cards must use the 0.5 threshold.  Using
        # 1.5 here made every fallback point OVER require two points while the
        # card still said "Points (1+)", so the OVER board could vanish even
        # though the UNDER board was populated.
        _fill_sim_map(pts_lines_map, PTS_LINE)
        _fill_sim_map(ast_lines_map, 0.5)
        _fill_sim_map(goal_lines_map, 0.5)
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
        full_logs = logs_map.get(p["pid"], [])
        # Only players actually in today's rotation (drops scratches/AHL/injured depth)
        if not _played_recently(full_logs, target_date):
            return None
        logs = _nhl_pre_game_logs(full_logs, target_date)
        book_line = p.get("realLine")
        analysis_line = book_line if book_line is not None else p.get("line")
        if analysis_line is None:
            return None
        hr, opp, line = p["homeRoad"], p["opponent"], analysis_line

        # Step 2: career H/A vs today's opponent
        h2, t2, r2, avg2 = _calc_hit_rate_from_logs(logs, line, hr, opponent=opp, last_n=10)
        # Step 3: last 10 H/A games any opponent
        h3, t3, r3, avg3 = _calc_hit_rate_from_logs(logs, line, hr, last_n=10)

        if t3 < MIN_GAMES:
            return None
        s2_ok = (t2 < MIN_GAMES) or (r2 >= HIT_THRESH)
        s3_ok = r3 >= HIT_THRESH
        over_ok = bool(s2_ok and s3_ok)
        score = round((r2 + r3) / 2 if t2 >= MIN_GAMES else r3, 1)

        # NEW: hit rate vs real sportsbook line (last 10 H/A) + gap + tag
        vsl_hits, vsl_total, vsl_rate = 0, 0, 0.0
        gap = None
        tag = ""
        if book_line is not None:
            vsl_hits, vsl_total, vsl_rate, _ = _calc_hit_rate_from_logs(
                logs, book_line, hr, last_n=10)
            gap = round(avg3 - book_line, 2)
            tag = _book_tag(book_line, avg3, vsl_rate)

        # Under track (vs-opp OR any-opp H/A) + game log for the per-card dropdown
        uf = _under_fields(logs, "shots", line, hr, opp)
        if not over_ok and not uf["underOk"]:
            return None
        _ha = [g for g in logs if g["homeRoad"] == hr][:10]
        _gsrc = ([g for g in logs if g["homeRoad"] == hr and g["opponent"] == opp][:10] or _ha)
        glog = [{"d": g["date"], "v": g["shots"]} for g in _gsrc]

        # Opponent-adjusted projected shot count + edge vs the line
        rest_days = _days_rest(logs, target_date)
        proj, opp_factor, rest_factor = _proj_count(
            avg3, t3, avg2, t2, p.get("oppSA", 0.0), league_sa, rest_days)
        proj_edge = round(proj - line, 2)
        proj_pick = "OVER" if proj_edge > 0 else ("UNDER" if proj_edge < 0 else "")
        sim_actual, sim_void_reason = _nhl_sim_actual_from_logs(
            full_logs, target_date, hr, opp, "shots")

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
            "dispLine": line,
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
            "simActual": sim_actual, "simVoidReason": sim_void_reason,
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

    _progress = {"stage": "Analyzing points...", "done": len(pool), "total": len(pool), "pct": 96}
    (pts_all, ast_all, pts_unders, ast_unders, goal_all, goal_unders_all,
     pp_all, pp_unders) = await get_pts_picks(
        games, sa_map, sem_nhl, season, pts_lines_map, ast_lines_map, target_date, goal_lines_map,
        goalie_map=goalie_map, shared_logs=logs_map, shared_rosters=skater_rosters)
    _progress = {"stage": "Analyzing goalie saves...", "done": len(pool), "total": len(pool), "pct": 98}
    saves_all, saves_unders = await get_saves_picks(
        games, sa_map, sem_nhl, season, sv_lines_map, target_date,
        simulate=simulate, allow_fallback=unpriced_mode and not simulate,
        lineup_maps=[sv_lines_map],
    )
    # ── Game Predictor ─────────────────────────────────────────────────────
    _progress = {"stage": "Building Game Predictor...", "done": len(pool), "total": len(pool), "pct": 98}
    try:
        _gp_data   = await _nhl_gp_fetch_all(target_date, season)
        game_preds = _nhl_gp_predict(games, _gp_data)
    except Exception as _gp_err:
        print(f"[GP] game predictor error: {_gp_err}")
        game_preds = []

    # Full lookup-only profile pool.  This keeps the visible boards capped at
    # Top 10 + 10 overflow while allowing the player lookup to find qualified
    # records that rank below the rendered board cut.
    player_profiles = []
    profile_seen = set()
    for profile_group in (
        results_raw,
        pts_all, pts_unders,
        pp_all, pp_unders,
        ast_all, ast_unders,
        goal_all, goal_unders_all,
        saves_all, saves_unders,
    ):
        for profile in profile_group:
            if not profile:
                continue
            profile_key = (profile.get("pid"), profile.get("mkt"))
            if profile_key in profile_seen:
                continue
            profile_seen.add(profile_key)
            player_profiles.append(profile)

    _result = {
        "picks":         picks[:TOP_N],
        "rest":          picks[TOP_N:TOP_N*2],
        "ptsPicks":      pts_all[:TOP_N],
        "ptsRest":       pts_all[TOP_N:TOP_N*2],
        "ppPicks":       pp_all[:TOP_N],
        "ppRest":        pp_all[TOP_N:TOP_N*2],
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
        "ppUnders":      pp_unders[:TOP_N],
        "ppUndersRest":  pp_unders[TOP_N:TOP_N*2],
        "astUnders":     ast_unders[:TOP_N],
        "astUndersRest": ast_unders[TOP_N:TOP_N*2],
        "goalUnders":    goal_unders_all[:TOP_N],
        "goalUndersRest": goal_unders_all[TOP_N:TOP_N*2],
        "savesUnders":   saves_unders[:TOP_N],
        "savesUndersRest": saves_unders[TOP_N:TOP_N*2],
        "playerProfiles": player_profiles,
        "games":         games,
        "sa_ranks":      sa_ranks,
        "poolSize":      len(pool),
        "qualified":     len(picks),
        "ptsQualified":  len(pts_all),
        "ppQualified":   len(pp_all),
        "astQualified":  len(ast_all),
        "savesQualified": len(saves_all),
        "season":        season,
        "targetDate":    target_date,
        "runTime":          datetime.utcnow().isoformat() + "Z",
        "date":             target_date,
        "game_predictions": game_preds,
        "simulation": bool(simulate),
        "archivedShotLineCount": archived_shot_line_count,
        "unpriced": bool(unpriced_mode and not simulate),
    }
    if simulate:
        _result["simulationStats"] = _nhl_simulation_stats(
            target_date, game_preds, _result)
        _result["historicalTrackRecord"] = _nhl_historical_replay_payload(_result)
        if archived_shot_line_count:
            _result["simulationStats"]["lineNote"] = (
                f"{archived_shot_line_count} archived player SOG lines were used "
                "for this replay. A simulation estimate remains only where an "
                "archived line was unavailable."
            )
        _result["simulationNotice"] = (
            "Point-in-time simulation: player-form data uses only games before "
            f"{target_date}; archived player SOG lines are used where available, "
            "while missing player lines use a model estimate. Historical results "
            "below are display only and are not cached or tracked."
        )
        _progress = {"stage": "Done!", "done": len(pool), "total": len(pool), "pct": 100}
        return _result
    # Persist the pre-game player and GP snapshots before building the hub
    # shell, so a read-only hub snapshot can also show the current slate as
    # pending rather than waiting for the first grading pass.
    _nhl_save_gp_snapshot(target_date, _result)
    _nhl_save_picks_snapshot(target_date, _result)
    try:
        from replit_push import push_picks_to_replit
        # Bake the picks into the page HTML so the Replit hub can serve an
        # instant, no-cold-start snapshot at moneypicksarena.com/dashboard/nhl.
        import json as _json
        try:
            _track_record = _nhl_track_record_payload()
        except Exception:
            # A picks snapshot is still useful if historical data is
            # temporarily unavailable.  The exception is retained in Render
            # logs rather than turning the entire member page into a 500.
            logger.exception("NHL snapshot Track Record payload failed")
            _track_record = None
        try:
            _gp_record = _nhl_gp_record_payload()
        except Exception:
            logger.exception("NHL snapshot GP Record payload failed")
            _gp_record = None
        _inject = (
            '<script>window.__INITIAL_PICKS__ = '
            + _json.dumps(_result).replace('</', '<\\/')
            + ';window.__INITIAL_TRACK_RECORD__ = '
            + _json.dumps(_track_record).replace('</', '<\\/')
            + ';window.__INITIAL_GP_RECORD__ = '
            + _json.dumps(_gp_record).replace('</', '<\\/')
            + ';</script></head>'
        )
        _snapshot_html = HTML.replace('</head>', _inject, 1)
        push_picks_to_replit("nhl", _result, html=_snapshot_html)
    except Exception as _e:
        print(f"[replit_push] nhl push failed: {_e}")
    _progress = {"stage": "Done!", "done": len(pool), "total": len(pool), "pct": 100}
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
.sec{display:flex;align-items:center;gap:10px;font-size:1.12rem;font-weight:900;color:#fbbf24;text-transform:uppercase;letter-spacing:.12em;margin:30px 0 14px;padding:9px 13px;border-left:4px solid #f59e0b;border-radius:8px;background:linear-gradient(90deg,rgba(245,158,11,.14),rgba(245,158,11,.02) 72%,transparent);text-shadow:0 0 12px rgba(251,191,36,.28)}
.sec::after{content:'';flex:1;height:2px;background:linear-gradient(90deg,rgba(245,158,11,.55),rgba(245,158,11,.08),transparent)}
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
/* ── NHL Game Predictor ──────────────────────────────────────────────────── */
.gp-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;margin-bottom:24px}
@media(max-width:680px){.gp-grid{grid-template-columns:1fr}}
.gp-card{background:#161616;border:1px solid #262626;border-radius:18px;padding:18px;overflow:hidden}
.gp-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.gp-mu{font-size:1.05rem;font-weight:900;color:#fff;letter-spacing:.02em}
.gp-time{font-size:.7rem;color:#6b7280}
.gp-pick{border-radius:10px;padding:10px 14px;margin-bottom:12px;display:flex;align-items:center;gap:10px}
.gp-pick-team{font-weight:900;font-size:.95rem}
.gp-pick-prob{font-size:.75rem;color:#cbd5e1;margin-top:2px}
.gp-bar-wrap{margin-bottom:10px}
.gp-bar-labels{display:flex;justify-content:space-between;margin-bottom:4px;font-size:.7rem;font-weight:700}
.gp-bar-outer{background:#1a1a1a;border-radius:4px;height:9px;overflow:hidden;display:flex}
.gp-bar-home{background:linear-gradient(90deg,#f59e0b,#fbbf24)}
.gp-bar-away{background:linear-gradient(90deg,#60a5fa,#3b82f6)}
.gp-totals{display:flex;gap:8px;margin-bottom:12px}
.gp-tbox{flex:1;background:#0e0e0e;border:1px solid #1e1e1e;border-radius:9px;padding:7px 8px;text-align:center}
.gp-tbox .gk{font-size:.56rem;color:#6b7280;text-transform:uppercase;letter-spacing:.07em;font-weight:700}
.gp-tbox .gv{font-weight:900;font-size:1rem;margin-top:3px;font-family:'Playfair Display',serif;color:#f59e0b}
.gp-tbox.ou-over{border-color:rgba(74,222,128,.3)}.gp-tbox.ou-over .gv{color:#4ade80}
.gp-tbox.ou-under{border-color:rgba(248,113,113,.3)}.gp-tbox.ou-under .gv{color:#f87171}
.gp-tbox.ou-push{border-color:rgba(107,114,128,.3)}.gp-tbox.ou-push .gv{color:#9ca3af}
.gp-tbox.ou-book .gv{color:#9ca3af}
.gp-teams{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}
.gp-team{background:#111;border:1px solid #1e1e1e;border-radius:10px;padding:9px}
.gp-thdr{display:flex;align-items:center;gap:5px;margin-bottom:6px}
.gp-tlogo{width:20px;height:20px;object-fit:contain}
.gp-tabbr{font-weight:900;font-size:.85rem;color:#fff}
.gp-tha{font-size:.58rem;font-weight:700;padding:1px 5px;border-radius:3px}
.gp-tha.h-ha{background:rgba(74,222,128,.1);color:#4ade80;border:1px solid rgba(74,222,128,.2)}
.gp-tha.a-ha{background:rgba(96,165,250,.1);color:#60a5fa;border:1px solid rgba(96,165,250,.2)}
.gp-sr{display:flex;justify-content:space-between;font-size:.7rem;padding:2px 0;border-bottom:1px solid #1a1a1a}
.gp-sr:last-child{border-bottom:none}
.gp-sr .sk{color:#6b7280}.gp-sr .sv{font-weight:700;color:#e5e7eb}
.gp-badges{display:flex;flex-wrap:wrap;gap:3px;margin-top:5px}
.gp-b2b{background:rgba(248,113,113,.12);color:#f87171;border:1px solid rgba(248,113,113,.25);border-radius:4px;font-size:.6rem;font-weight:700;padding:1px 5px}
.gp-stk{border-radius:4px;font-size:.6rem;font-weight:700;padding:1px 5px}
.gp-stk.win-stk{background:rgba(74,222,128,.1);color:#4ade80;border:1px solid rgba(74,222,128,.2)}
.gp-stk.loss-stk{background:rgba(248,113,113,.1);color:#f87171;border:1px solid rgba(248,113,113,.2)}
.gp-ml-row{font-size:.68rem;color:#6b7280;margin-top:6px;display:flex;justify-content:space-between}
footer{text-align:center;padding:32px 24px;color:#4b5563;font-size:.78rem;border-top:1px solid #1c1c1c;margin-top:24px;font-family:'Source Sans Pro',sans-serif}
.ft-logo{font-family:'Playfair Display',serif;color:#f59e0b;font-weight:700;font-size:.95rem;margin-bottom:6px}
.admin-only{display:none !important}
body.is-admin .admin-only{display:inline-block !important}
#parlayCard{display:none}
body.is-admin #parlayCard{display:block}
/* ===== NBA-style trading cards ===== */
.picks-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;margin-bottom:10px}
.nhl-toolbar{display:flex;justify-content:flex-end;margin:0 0 14px}
.nhl-lookup{width:min(100%,520px);background:linear-gradient(135deg,rgba(245,158,11,.1),rgba(17,17,17,.9));border:1px solid rgba(245,158,11,.3);border-radius:12px;padding:10px 12px}
.nhl-lookup-label{color:#f59e0b;font-size:.65rem;font-weight:900;letter-spacing:.1em;text-transform:uppercase;margin-bottom:6px}
.nhl-lookup-row{display:flex;gap:8px;align-items:center}
#nhlSearch{background:#111;color:#fff;border:1px solid #2a2a2a;border-radius:8px;padding:8px 14px;font-size:.9rem;outline:none;width:100%;min-width:0}
#nhlSearch:focus{border-color:#f59e0b;box-shadow:0 0 0 2px rgba(245,158,11,.12)}
.nhl-lookup-btn{background:#f59e0b;color:#111;border:0;border-radius:8px;padding:9px 13px;font-size:.78rem;font-weight:900;cursor:pointer;white-space:nowrap}
.nhl-lookup-btn:hover{background:#fbbf24}
.nhl-lookup-hint{color:#9ca3af;font-size:.68rem;margin-top:6px;min-height:1em}
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
.pc-mkt{display:inline-block;font-size:.82rem;font-weight:900;letter-spacing:.1em;text-transform:uppercase;color:#fbbf24;margin-top:5px;padding:3px 8px;border:1px solid rgba(251,191,36,.42);border-radius:6px;background:rgba(251,191,36,.1);text-shadow:0 0 9px rgba(251,191,36,.25)}
.pick-card.acc-pts .pc-mkt{color:#93c5fd;border-color:rgba(96,165,250,.5);background:rgba(96,165,250,.12);text-shadow:0 0 9px rgba(96,165,250,.3)}
.pick-card.acc-shots .pc-mkt{color:#fbbf24;border-color:rgba(245,158,11,.5);background:rgba(245,158,11,.12)}
.pick-card.acc-ast .pc-mkt{color:#c4b5fd;border-color:rgba(167,139,250,.5);background:rgba(167,139,250,.12);text-shadow:0 0 9px rgba(167,139,250,.3)}
.pick-card.acc-sv .pc-mkt{color:#6ee7b7;border-color:rgba(52,211,153,.5);background:rgba(52,211,153,.12);text-shadow:0 0 9px rgba(52,211,153,.3)}
.pick-card.acc-goals .pc-mkt{color:#6ee7b7;border-color:rgba(52,211,153,.5);background:rgba(52,211,153,.12);text-shadow:0 0 9px rgba(52,211,153,.3)}
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
.lad-modal{background:#161616;border:1px solid #2a2a2a;border-radius:18px;max-width:700px;width:100%;max-height:88vh;overflow-y:auto;padding:22px}
.lad-modal h3{font-family:'Playfair Display',serif;color:#fff;font-size:1.25rem;margin-bottom:2px}
.lad-sub{color:#9ca3af;font-size:.8rem;margin-bottom:14px}
.lad-close{float:right;background:none;border:1px solid #333;color:#9ca3af;border-radius:8px;padding:4px 10px;cursor:pointer;font-weight:700}
.lad-glog{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0 14px}
.glchip{background:#0e0e0e;border:1px solid #242424;border-radius:8px;padding:7px 9px;text-align:center;min-width:76px}
.glchip .d{font-size:.68rem;color:#9ca3af;white-space:nowrap}
.glchip .v{font-weight:800;font-size:.95rem;margin-top:2px;color:#e5e7eb}
.glchip.hit{border-color:rgba(74,222,128,.35)}
.glchip.hit .v{color:#4ade80}
.glchip.miss .v{color:#f87171}
.lad-stat{display:flex;justify-content:space-between;align-items:center;padding:8px 4px;border-bottom:1px solid #1c1c1c;font-size:.85rem}
.lad-stat:last-child{border-bottom:none}
.lad-stat .k{color:#9ca3af}
.lad-stat .v{font-weight:700}
.lad-profile{display:flex;align-items:center;gap:10px;margin:6px 0 16px;padding-bottom:12px;border-bottom:1px solid #262626}
.lad-profile-team{font-size:.72rem;color:#9ca3af;margin-top:3px}
.lad-market-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.lad-market{background:#111;border:1px solid #262626;border-radius:12px;padding:12px}
.lad-market h4{color:#fbbf24;font-size:.78rem;font-weight:900;letter-spacing:.04em;margin:0 0 4px}
.lad-market-meta{color:#6b7280;font-size:.66rem;margin-bottom:8px}
.lad-market-stats{display:grid;grid-template-columns:1fr 1fr;gap:6px}
.lad-market-stat{background:#181818;border-radius:8px;padding:7px 6px;text-align:center;min-width:0}
.lad-market-stat .k{color:#6b7280;font-size:.56rem;text-transform:uppercase;letter-spacing:.03em}
.lad-market-stat .v{font-size:.76rem;font-weight:800;margin-top:3px;white-space:nowrap}
.lad-market-games{display:flex;flex-wrap:wrap;gap:4px;margin-top:8px}
.lad-market-games .glchip{min-width:76px;padding:6px 7px}
.lad-unavailable{color:#6b7280;font-size:.72rem;line-height:1.45;padding:8px 0 2px}
@media(max-width:620px){.lad-market-grid{grid-template-columns:1fr}.nhl-lookup-row{align-items:stretch}.nhl-lookup-btn{padding-inline:10px}}
</style>
</head>
<body>
<div class="bg-glow"></div>

<nav>
  <div class="logo">Money <span>Picks</span> Arena</div>
  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;justify-content:flex-end"><button onclick="openNhlGPRecord()" style="background:#0e7490;color:#fff;border:none;border-radius:10px;padding:9px 16px;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">&#128302; GP Record</button><button onclick="document.getElementById('nhl-track-section').scrollIntoView({behavior:'smooth',block:'start'})" style="background:#065f46;color:#fff;border:none;border-radius:10px;padding:9px 16px;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">&#128202; Track Record</button><button onclick="openNhlOverflowRecord()" style="background:#b45309;color:#fff;border:none;border-radius:10px;padding:9px 16px;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">&#11088; NHL Overflow</button><button onclick="openNhlSpecialRecord()" style="background:#854d0e;color:#fff;border:none;border-radius:10px;padding:9px 16px;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">&#11088; Special Record</button><button class="admin-only" onclick="openNhlMyBets()" style="background:#0e7490;color:#fff;border:none;border-radius:10px;padding:9px 16px;font-weight:800;font-size:.82rem;cursor:pointer;white-space:nowrap">&#128176; My Bets</button></div>
</nav>

<style>
.nhl-bets-tbl{width:100%;border-collapse:collapse;font-size:.82rem}
.nhl-bets-tbl th{padding:7px 10px;text-align:left;font-size:.72rem;color:#94a3b8;font-weight:700;text-transform:uppercase;letter-spacing:.07em;border-bottom:1px solid #1e293b;white-space:nowrap}
.nhl-bets-tbl td{padding:8px 10px;border-bottom:1px solid #0f172a;vertical-align:middle;color:#e2e8f0}
.nhl-bets-tbl tr:last-child td{border-bottom:none}
.nhl-bets-tbl tr:hover td{background:rgba(255,255,255,.02)}
/* NHL Track Record */
.nhl-trk-tbl{width:100%;border-collapse:collapse;font-size:.92rem}
.nhl-trk-tbl th{padding:12px 14px;text-align:left;color:#8be9ff;font-size:.74rem;font-weight:800;text-transform:uppercase;letter-spacing:.1em;background:#09111f;white-space:nowrap;border-bottom:1px solid rgba(103,232,249,.25)}
.nhl-trk-tbl td{padding:11px 14px;border-bottom:1px solid rgba(30,41,59,.8);white-space:nowrap;vertical-align:middle}
.nhl-trk-tbl tr:last-child td{border-bottom:none}
.nhl-trk-tbl tr:hover td{background:rgba(255,255,255,.02)}
.nhl-trk-bar-wrap{width:100px;background:#1e293b;border-radius:4px;height:9px;overflow:hidden;display:inline-block;vertical-align:middle}
.nhl-trk-bar{height:100%;border-radius:4px}
.nhl-trk-group{margin:0 0 18px;border:1px solid #263449;border-left:4px solid var(--trk-accent,#22d3ee);border-radius:16px;overflow:hidden;background:linear-gradient(145deg,rgba(15,23,42,.96),rgba(10,15,26,.98));box-shadow:0 8px 22px rgba(0,0,0,.16)}
.nhl-trk-group-head{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;padding:15px 18px;background:linear-gradient(90deg,color-mix(in srgb,var(--trk-accent,#22d3ee) 14%,transparent),transparent);border-bottom:1px solid rgba(148,163,184,.14)}
.nhl-trk-group-title{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.nhl-trk-group-kicker{font-size:.68rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.14em;font-weight:800}
.nhl-trk-group-name{font-size:1.08rem;color:#fff;font-weight:900;letter-spacing:.01em}
.nhl-trk-group-side{font-size:.72rem;font-weight:900;letter-spacing:.1em;padding:4px 9px;border-radius:999px;color:var(--trk-accent,#67e8f9);border:1px solid color-mix(in srgb,var(--trk-accent,#67e8f9) 55%,transparent);background:color-mix(in srgb,var(--trk-accent,#67e8f9) 12%,transparent)}
.nhl-trk-group-summary{display:flex;align-items:center;gap:12px;flex-wrap:wrap;color:#cbd5e1;font-size:.82rem;font-weight:700}
.nhl-trk-group-rate{font-size:1.08rem;font-family:monospace;font-weight:900;color:var(--trk-accent,#67e8f9)}
.nhl-trk-group-pl{font-family:monospace;font-weight:900}
.nhl-trk-table-scroll{overflow-x:auto}
.nhl-trk-note{color:#94a3b8!important;font-size:.78rem!important;white-space:normal!important;min-width:150px}
.nhl-trk-result{display:inline-block;min-width:66px;text-align:center;padding:4px 8px;border-radius:999px;font-size:.72rem;letter-spacing:.05em;font-weight:900}
.nhl-trk-result.win{color:#86efac;background:rgba(34,197,94,.14);border:1px solid rgba(74,222,128,.32)}
.nhl-trk-result.loss{color:#fca5a5;background:rgba(239,68,68,.14);border:1px solid rgba(248,113,113,.32)}
.nhl-trk-result.push{color:#fde68a;background:rgba(234,179,8,.14);border:1px solid rgba(250,204,21,.32)}
.nhl-trk-result.void,.nhl-trk-result.pending{color:#cbd5e1;background:rgba(100,116,139,.14);border:1px solid rgba(148,163,184,.24)}
@media(max-width:680px){.nhl-trk-group-head{padding:13px 14px}.nhl-trk-group-name{font-size:1rem}.nhl-trk-tbl{font-size:.86rem}.nhl-trk-tbl th{font-size:.68rem;padding:10px}.nhl-trk-tbl td{padding:10px}}
.nhl-jump-chip{cursor:pointer;transition:transform .15s ease,border-color .15s ease,background .15s ease}
.nhl-jump-chip:hover{transform:translateY(-2px);border-color:#f59e0b;background:#1d1d1d}
.nhl-game-jump{cursor:pointer;transition:border-color .15s ease,transform .15s ease}
.nhl-game-jump:hover{border-color:#f59e0b!important;transform:translateY(-1px)}
.nhl-scroll-anchor{scroll-margin-top:18px;height:1px}
.nhl-game-row{scroll-margin-top:18px}
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
    <p>Shots &nbsp;·&nbsp; Points &nbsp;·&nbsp; Power Play Points &nbsp;·&nbsp; Assists &nbsp;·&nbsp; Goalie Saves</p>
  </div>

  <div class="card" style="text-align:center">
    <h2 style="font-family:'Playfair Display',serif;font-size:1.5rem;font-weight:700;color:#fff;margin-bottom:6px">Get NHL Picks</h2>
    <p style="color:#6b7280;font-size:.88rem;margin-bottom:22px">Choose today for the saved board, or any completed date for a historical replay and its Track Record</p>
    <div class="date-row">
      <label>Date</label>
      <input type="date" id="datePicker"/>
    </div>
    <button class="btn-run" id="getBtn" onclick="getPicks()">🎯 Get Picks</button>
  </div>

  <div class="card" id="parlayCard" style="text-align:center;max-width:600px;margin:20px auto 0">
    <h2 style="font-family:'Playfair Display',serif;font-size:1.3rem;font-weight:700;color:#fff;margin-bottom:6px">🎰 Auto Parlay Builder <span style="font-size:.7rem;color:#777;font-family:sans-serif">admin only</span></h2>
    <p style="font-size:.74rem;color:#888;margin-bottom:14px">Best available legs from today&#39;s selected categories — priced odds combined</p>
    <div style="display:flex;gap:10px;justify-content:center;align-items:center;flex-wrap:wrap">
      <label style="color:#9ca3af;font-size:.85rem;font-weight:600">Legs
        <select id="parlayLegs" style="background:#1a1a1a;color:#fff;border:1px solid #333;border-radius:8px;padding:8px 12px;font-size:.9rem;font-weight:700;margin-left:6px">
          <option>2</option><option selected>3</option><option>4</option><option>5</option><option>6</option><option>7</option><option>8</option><option>9</option><option>10</option>
        </select>
      </label>
      <div style="position:relative;display:inline-block">
        <button class="btn-run" id="nhl-parlay-cats-btn" onclick="toggleNhlCatMenu(event)" style="background:#1f2937">&#9776; Categories (10/10) &#9662;</button>
        <div id="nhl-parlay-cats-menu" style="display:none;position:absolute;z-index:60;top:calc(100% + 6px);left:0;background:#0e0e0e;border:1px solid #2a2a2a;border-radius:10px;padding:10px 12px;min-width:220px;box-shadow:0 12px 34px rgba(0,0,0,.55);text-align:left">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px">
            <span style="font-size:.63rem;color:#888;font-weight:800;letter-spacing:.06em">PARLAY CATEGORIES</span>
            <span style="font-size:.63rem"><a onclick="_nhlParlayCatSetAll(true)" style="color:#63cab7;cursor:pointer;font-weight:800">All</a> <span style="color:#444">·</span> <a onclick="_nhlParlayCatSetAll(false)" style="color:#ff8a65;cursor:pointer;font-weight:800">None</a></span>
          </div>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="SHOTS_O" checked onchange="_nhlParlayCatChanged()"> Shots on Goal — OVER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="SHOTS_U" checked onchange="_nhlParlayCatChanged()"> Shots on Goal — UNDER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="POINTS_O" checked onchange="_nhlParlayCatChanged()"> Points (1+) — OVER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="POINTS_U" checked onchange="_nhlParlayCatChanged()"> Points (1+) — UNDER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="ASSISTS_O" checked onchange="_nhlParlayCatChanged()"> Assists (1+) — OVER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="ASSISTS_U" checked onchange="_nhlParlayCatChanged()"> Assists (1+) — UNDER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="GOALS_O" checked onchange="_nhlParlayCatChanged()"> Goals (1+) — OVER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="GOALS_U" checked onchange="_nhlParlayCatChanged()"> Goals (1+) — UNDER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="SAVES_O" checked onchange="_nhlParlayCatChanged()"> Goalie Saves — OVER</label>
          <label style="display:block;color:#d1d5db;font-size:.76rem;padding:5px 2px;cursor:pointer"><input type="checkbox" class="nhl-parlay-cat-cb" value="SAVES_U" checked onchange="_nhlParlayCatChanged()"> Goalie Saves — UNDER</label>
        </div>
      </div>
      <button class="btn-run" onclick="buildParlay()">Build Best Parlay</button>
      <button class="btn-run" onclick="generateParlay()" style="background:#7c3aed;box-shadow:0 4px 14px rgba(124,58,237,.28)">🎲 Generate New</button>
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
window.NHL_PARLAY_CATS = {SHOTS_O:true,SHOTS_U:true,POINTS_O:true,POINTS_U:true,ASSISTS_O:true,ASSISTS_U:true,GOALS_O:true,GOALS_U:true,SAVES_O:true,SAVES_U:true};
function _nhlParlayCatCount(){var n=0,t=0;for(var k in window.NHL_PARLAY_CATS){t++;if(window.NHL_PARLAY_CATS[k])n++;}return n+'/'+t;}
function _paintNhlParlayCatBtn(){var b=document.getElementById('nhl-parlay-cats-btn');if(b)b.innerHTML='&#9776; Categories ('+_nhlParlayCatCount()+') &#9662;';}
function toggleNhlCatMenu(e){if(e)e.stopPropagation();var m=document.getElementById('nhl-parlay-cats-menu');if(m)m.style.display=m.style.display==='block'?'none':'block';}
function _syncNhlParlayCats(){var cbs=document.querySelectorAll('.nhl-parlay-cat-cb');for(var i=0;i<cbs.length;i++)window.NHL_PARLAY_CATS[cbs[i].value]=cbs[i].checked;}
function _nhlParlayCatChanged(){_syncNhlParlayCats();_paintNhlParlayCatBtn();if((document.getElementById('parlayResult').innerHTML||'').trim())buildParlay();}
function _nhlParlayCatSetAll(v){var cbs=document.querySelectorAll('.nhl-parlay-cat-cb');for(var i=0;i<cbs.length;i++)cbs[i].checked=v;_nhlParlayCatChanged();}
document.addEventListener('DOMContentLoaded',function(){_syncNhlParlayCats();_paintNhlParlayCatBtn();});
function _nhlLeg(p){
  var market=p.mkt||((p.pts2Hits!=null||p.ptsHa10avg!=null)?'Points (1+)':'Shots on Goal');
  var line=p.realLine;
  if(line==null) return null;
  var dir=p._parlaySide==='UNDER'?'UNDER':'OVER';
  var rate=dir==='UNDER'
    ?(p.underRate||p.underRateAny||p.underRateVo||0)
    :(p.vsLineRate||p.rateB||p.rateA||p.step3Rate||p.pts3Rate||0);
  var odds=dir==='UNDER'?(p.realUnderOdds||''):(p.realOdds||'');var dec=_amToDec(odds);
  return {player:p.name,playerKey:(p.pid!=null?String(p.pid):String(p.name||'')),team:p.team||'',opp:p.opponent||'',market:market,dir:dir,line:line,rate:Math.round(rate||0),odds:odds,dec:dec,hasOdds:!!dec};
}
function _nhlParlayCatKey(c){
  var base={'Shots on Goal':'SHOTS','Points (1+)':'POINTS','Assists (1+)':'ASSISTS','Goals (1+)':'GOALS','Goalie Saves':'SAVES'}[c.market]||'SHOTS';
  return base+(c.dir==='UNDER'?'_U':'_O');
}
function _parlayPool(){
  var plays=window.__NHL_PLAYS__||[];var byP={};
  plays.forEach(function(p){
    if(!p||!p.name)return;
    var c=_nhlLeg(p);
    if(!c)return;
    if(window.NHL_PARLAY_CATS&&window.NHL_PARLAY_CATS[_nhlParlayCatKey(c)]===false)return;
    if(!_floorOk(c.odds))return;
    var cur=byP[c.playerKey];
    if(!cur||_legScore(c)>_legScore(cur))byP[c.playerKey]=c;
  });
  return Object.keys(byP).map(function(k){return byP[k];}).sort(function(a,b){return _legScore(b)-_legScore(a);});
}
function _shuffle(a){for(var i=a.length-1;i>0;i--){var j=Math.floor(Math.random()*(i+1));var t=a[i];a[i]=a[j];a[j]=t;}return a;}
function closeParlay(){var o=document.getElementById('parlayResult');if(o)o.innerHTML='';}
function buildParlay(){_renderParlay(false);}
function makeAnotherParlay(){_renderParlay(true);}
// Keep the previous name available for any already-open page state.
function generateParlay(){makeAnotherParlay();}
function _renderParlay(randomize){
  var sel=document.getElementById('parlayLegs');
  var n=parseInt(sel?sel.value:'3',10)||3;
  var out=document.getElementById('parlayResult');
  if(!out)return;
  _syncNhlParlayCats();
  var anyCat=false;for(var cat in window.NHL_PARLAY_CATS){if(window.NHL_PARLAY_CATS[cat]){anyCat=true;break;}}
  if(!anyCat){out.innerHTML='<div style="color:#f87171;padding:10px">Pick at least one category from the Categories menu.</div>';return;}
  var cands=_parlayPool();
  if(!cands.length){out.innerHTML='<div style="color:#888;padding:10px">Run today&#39;s picks first, then build a parlay.</div>';return;}
  var avoid={};
  if(randomize&&window._lastParlay&&window._lastParlay.length){
    window._lastParlay.forEach(function(playerKey){avoid[playerKey]=1;});
  }
  var fresh=randomize?cands.filter(function(c){return !avoid[c.playerKey];}):cands;
  if(fresh.length<n){
    if(randomize){
      out.innerHTML='<div style="color:#f59e0b;padding:10px">Only '+fresh.length+' new player'+(fresh.length!==1?'s':'')+' remain after the last parlay. Reduce the leg count or build the best parlay again.</div>';
    }else{
      out.innerHTML='<div style="color:#f87171;padding:10px">Only '+cands.length+' qualifying play'+(cands.length!==1?'s':'')+' on the board. Pick a smaller parlay.</div>';
    }
    return;
  }
  function _pick(ordered){var used={},picked=[],i,c;for(i=0;i<ordered.length&&picked.length<n;i++){c=ordered[i];if(used[c.playerKey])continue;used[c.playerKey]=1;picked.push(c);}return picked;}
  var legs;
  if(randomize){legs=_pick(_shuffle(fresh.slice())).sort(function(a,b){return _legScore(b)-_legScore(a);});}
  else{legs=_pick(cands.slice());}
  window._lastParlay=legs.map(function(l){return l.playerKey;});
  window.__NHL_CURRENT_PARLAY__=legs;
  _paintNhlParlay(legs,n,randomize);
}
function _paintNhlParlay(legs,n,randomize){
  var out=document.getElementById('parlayResult');
  if(!out)return;
  var dec=1,priced=0,missing=0;
  legs.forEach(function(l){if(l.dec){dec*=l.dec;priced++;}else{missing++;}});
  var am=priced?_decToAm(dec):null;var payout=priced?(100*dec):null;
  var dirColor=function(d){return d==='OVER'?'#4ade80':d==='UNDER'?'#f87171':'#9ca3af';};
  var rows=legs.map(function(l,i){var fo=_fmtOdds(l.odds);return '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #1a1a1a">'
    +'<div style="display:flex;align-items:center;gap:8px;min-width:0">'
    +'<div style="min-width:0;flex:1">'
    +'<div style="font-weight:800;color:#fff;font-size:.85rem">'+(i+1)+'. '+l.player+' <span style="color:#777;font-size:.7rem">'+l.team+(l.opp?(' vs '+l.opp):'')+'</span></div>'
    +'<div style="color:#999;font-size:.72rem;margin-top:2px">'+l.market+(l.line!=null?(' · line '+l.line):'')+(l.rate?(' · '+l.rate+'% hit'):'')+'</div>'
    +'</div>'
    +'<button type="button" onclick="replaceNhlParlayLeg('+i+')" title="Generate a new player prop" aria-label="Generate a new player prop" style="flex:0 0 auto;background:#7c3aed;color:#fff;border:0;border-radius:6px;width:25px;height:25px;padding:0;cursor:pointer;font-size:.95rem;font-weight:900;line-height:25px">↻</button>'
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
function replaceNhlParlayLeg(index){
  var legs=window.__NHL_CURRENT_PARLAY__||[];
  var current=legs[index];
  if(!current)return;
  var cands=_parlayPool(),used={};
  legs.forEach(function(l,i){if(i!==index)used[l.playerKey]=1;});
  var options=cands.filter(function(c){return !used[c.playerKey]&&c.playerKey!==current.playerKey;});
  var sameProp=options.filter(function(c){return c.market===current.market&&c.dir===current.dir;});
  if(sameProp.length)options=sameProp;
  if(!options.length){
    var out=document.getElementById('parlayResult');
    if(out)out.insertAdjacentHTML('afterbegin','<div style="color:#f59e0b;padding:10px;font-size:.78rem">No unused player prop is available for this leg.</div>');
    return;
  }
  var replacement=_shuffle(options.slice())[0];
  var next=legs.slice();next[index]=replacement;
  window.__NHL_CURRENT_PARLAY__=next;
  window._lastParlay=next.map(function(l){return l.playerKey;});
  var sel=document.getElementById('parlayLegs');
  _paintNhlParlay(next,parseInt(sel?sel.value:'3',10)||3,true);
}

// Get Picks loads today's saved board, or builds a view-only replay for any past date.
async function getPicks(){
  var btn=document.getElementById('getBtn');
  var st=document.getElementById('statusMsg');
  var out=document.getElementById('out');
  var dt=document.getElementById('datePicker').value;
  var orig=btn.textContent;
  var isHistorical=dt&&dt<new Date().toISOString().slice(0,10);
  btn.disabled=true; btn.textContent='Loading...';
  if(st) st.textContent=isHistorical?'Building historical picks and Track Record for '+dt+'...':'Loading saved picks for '+dt+'...';
  var pollTimer=null;
  if(isHistorical&&out){
    out.innerHTML='<div class="loading"><div class="spin"></div>'
      +'<p style="color:#9ca3af;margin-bottom:16px" id="prog-stage">Starting historical replay...</p>'
      +'<div style="background:rgba(245,158,11,.1);border-radius:6px;height:8px;width:280px;margin:0 auto 8px;overflow:hidden">'
      +'<div id="prog-bar" style="height:100%;width:5%;background:#f59e0b;border-radius:6px;transition:width .5s"></div></div>'
      +'<p style="color:#6b7280;font-size:.8rem" id="prog-pct">5%</p></div>';
    pollTimer=setInterval(async function(){
      try{
        var pr=await fetch('/api/progress'),pd=await pr.json();
        var bar=document.getElementById('prog-bar'),stage=document.getElementById('prog-stage'),pct=document.getElementById('prog-pct');
        if(bar)bar.style.width=pd.pct+'%';
        if(stage)stage.textContent=pd.stage;
        if(pct)pct.textContent=pd.pct+'%';
      }catch(e){}
    },2000);
  }
  try{
    var _nhlTok=localStorage.getItem('__mpa_token')||'';
    var url=isHistorical
      ?'/api/picks?target_date='+encodeURIComponent(dt)+'&simulate=true&token='+encodeURIComponent(_nhlTok)
      :'/api/cached?target_date='+encodeURIComponent(dt)+'&token='+encodeURIComponent(_nhlTok);
    var res=await fetch(url);
    if(res.status===404){ if(st) st.textContent=''; if(out) out.innerHTML=''; alert("Today's picks aren't ready yet -- check back a little later."); return; }
    if(!res.ok){ throw new Error('Could not load picks.'); }
    var data=await res.json();
    if(data.no_games){
      if(out)out.innerHTML='<div style="text-align:center;padding:40px 20px;color:#9ca3af"><h2 style="color:#f59e0b;margin-bottom:8px">No NHL Games</h2><p>'+(data.message||('No NHL games scheduled for '+dt+'.'))+'</p></div>';
      if(st)st.textContent='';
      return;
    }
    if(data.error) throw new Error(data.error);
    renderResults(data);
    if(isHistorical&&data.historicalTrackRecord){
      _nhlTrkReplay=data.historicalTrackRecord;
      var trkDate=document.getElementById('nhlTrkDate');
      var ovfDate=document.getElementById('nhlOvfDate');
      if(trkDate)trkDate.value=dt;
      if(ovfDate)ovfDate.value=dt;
      _nhlTrkDayName();
      _nhlOvfDayName();
      if(!_nhlTrkData)_nhlTrkData={dates:[],stake:20};
      renderNhlTrackDay();
      renderNhlOverflowDay();
    }
    if(st && data.picks){
      st.textContent=isHistorical
        ?'HISTORICAL REPLAY — pick board and Track Record updated for '+dt+'; not added to the official record'
        :(data.qualified||0)+' players qualified -- '+data.picks.length+' top picks -- '+(data.date||'');
    }
  }catch(e){ if(st) st.textContent=''; alert(e.message||'Could not load picks. Please try again.'); }
  finally{if(pollTimer)clearInterval(pollTimer);btn.disabled=false;btn.textContent=orig;}
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
  if(mkt==='Power Play Points (1+)') return 'acc-pts';
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
function _nhlLineSourceBadge(p){
  if(p.lineSource==='Historical Odds API'){
    return '<span style="margin-left:5px;padding:2px 5px;border-radius:4px;background:rgba(59,130,246,.18);color:#93c5fd;font-size:.55rem;font-weight:900;letter-spacing:.05em">ARCHIVED</span>';
  }
  if(p.lineSource==='Simulation'||p.lineSource==='Model'){
    return '<span style="margin-left:5px;color:#94a3b8;font-size:.6rem;font-weight:800">MODEL</span>';
  }
  return '';
}
function nhlCard(p,i){
  var season=(window.__NHL_SEASON__||'20252026');
  var key=_ladKey(p); window.__NHLLAD__[key]=p;
  var ha=p.homeRoad==='H';
  var head='https://assets.nhle.com/mugs/nhl/'+season+'/'+p.team+'/'+p.pid+'.png';
  var logo='https://assets.nhle.com/logos/nhl/svg/'+p.team+'_light.svg';
  var lineHtml=(p.realLine!=null)
    ? `<span class="ln">${p.dispLine}</span> <span class="od">${p.realOdds||''}</span>${_nhlLineSourceBadge(p)}`
    : `<span class="est">~${p.dispLine}</span>`;
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
        <div class="pc-stat"><div class="k">L10 Avg</div><div class="v gold">${p.avg}</div></div>
        <div class="pc-stat"><div class="k">Avg vs ${p.opponent}</div><div class="v gold">${p.totA?p.avgA:'—'}</div></div>
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
    ? `<span class="ln">U ${p.dispLine}</span> <span class="od">${p.realUnderOdds||''}</span>${_nhlLineSourceBadge(p)}`
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
function _nhlRecordMatches(p,target){
  if(!p||!target)return false;
  if(p.pid!=null&&target.pid!=null&&String(p.pid)===String(target.pid))return true;
  return String(p.name||'').toLowerCase()===String(target.name||'').toLowerCase()
    && (!target.team||!p.team||String(p.team)===String(target.team))
    && (!target.opponent||!p.opponent||String(p.opponent)===String(target.opponent));
}
function _nhlRecordsForPlayer(target){
  var raw=window.__NHL_RAW__||{}, keys=[
    'playerProfiles',
    'picks','rest','ptsPicks','ptsRest','ppPicks','ppRest','astPicks','astRest',
    'goalPicks','goalRest','savesPicks','savesRest','shotUnders','shotUndersRest',
    'ptsUnders','ptsUndersRest','ppUnders','ppUndersRest','astUnders','astUndersRest',
    'goalUnders','goalUndersRest','savesUnders','savesUndersRest'
  ], byMarket={};
  keys.forEach(function(k){
    (raw[k]||[]).forEach(function(p){
      var m=p&&p.mkt;
      if(!m||!_nhlRecordMatches(p,target))return;
      if(!byMarket[m]||((byMarket[m].realLine==null)&&(p.realLine!=null)))byMarket[m]=p;
    });
  });
  return byMarket;
}
function _nhlMarketStat(label,value){
  return '<div class="lad-market-stat"><div class="k">'+label+'</div><div class="v">'+value+'</div></div>';
}
function _nhlGameDateLabel(raw){
  var s=String(raw||'').slice(0,10),m=s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if(!m)return String(raw||'');
  var months=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var month=parseInt(m[2],10),day=parseInt(m[3],10);
  return months[month-1]+' '+day+', '+m[1];
}
function _nhlMarketCard(label,record,opponent){
  if(!record){
    var missing=label==='Goalie Saves'?'No goalie-saves record available for this player.':'No qualifying '+label.toLowerCase()+' record available in this board snapshot.';
    return '<article class="lad-market"><h4>'+label+'</h4><div class="lad-unavailable">'+missing+'</div></article>';
  }
  var line=record.realLine!=null?'Book line '+record.realLine:'Model line '+record.dispLine;
  var vs=record.totA?_rateHtml(record.rateA,record.hitsA,record.totA):'<span class="gray">No history</span>';
  var recent=record.totB?_rateHtml(record.rateB,record.hitsB,record.totB):'<span class="gray">No history</span>';
  var games=(record.glog||[]).map(function(g){
    var hit=record.dispLine!=null&&Number(g.v)>Number(record.dispLine);
    return '<div class="glchip '+(hit?'hit':'miss')+'"><div class="d">'+_nhlSafe(_nhlGameDateLabel(g.d))+'</div><div class="v">'+_nhlSafe(g.v)+'</div></div>';
  }).join('');
  if(!games)games='<span class="gray">No recent game values available.</span>';
  var under=(record.underTotal?record.underHits+'/'+record.underTotal+' ('+record.underRate+'%)':'—');
  return '<article class="lad-market"><h4>'+label+'</h4><div class="lad-market-meta">'+_nhlSafe(line)+' · '+(record.totA||0)+' vs '+_nhlSafe(opponent)+' game'+(record.totA===1?'':'s')+'</div>'
    +'<div class="lad-market-stats">'
    +_nhlMarketStat('Vs '+_nhlSafe(opponent),vs)
    +_nhlMarketStat('Vs avg',record.totA?record.avgA:'—')
    +_nhlMarketStat('L10 '+(record.homeRoad==='H'?'home':'away'),recent)
    +_nhlMarketStat('L10 avg',record.totB?record.avg:'—')
    +'</div><div class="lad-market-games">'+games+'</div>'
    +'<div style="color:#6b7280;font-size:.62rem;margin-top:7px">Recent values · green = over line · under L10 '+under+'</div></article>';
}
function openNhlPlayerSummary(p){
  if(!p)return;
  var records=_nhlRecordsForPlayer(p), opponent=p.opponent||'today\\'s opponent';
  var categories=['Shots on Goal','Points (1+)','Power Play Points (1+)','Assists (1+)','Goals (1+)','Goalie Saves'];
  var cards=categories.map(function(label){return _nhlMarketCard(label,records[label],opponent);}).join('');
  var head='https://assets.nhle.com/mugs/nhl/'+(window.__NHL_SEASON__||'20252026')+'/'+(p.team||'')+'/'+(p.pid||'')+'.png';
  var html='<div class="lad-modal" onclick="event.stopPropagation()">'
    +'<button class="lad-close" onclick="closeNhlLadder()">✕</button>'
    +'<div class="lad-profile"><div class="hs-wrap"><span class="hs-ini">'+_nhlSafe(_initials(p.name))+'</span><img class="hs-img" src="'+_nhlSafe(head)+'" onerror="this.style.display=\\'none\\'"/></div><div><h3>'+_nhlSafe(p.name)+'</h3><div class="lad-profile-team">'+_nhlSafe(p.team||'')+' vs '+_nhlSafe(opponent)+' · '+(p.homeRoad==='H'?'HOME':'AWAY')+'</div></div></div>'
    +'<div style="font-size:.68rem;color:#9ca3af;text-transform:uppercase;letter-spacing:.08em;font-weight:800;margin-bottom:8px">Category history vs '+_nhlSafe(opponent)+'</div>'
    +'<div class="lad-market-grid">'+cards+'</div></div>';
  var ov=document.createElement('div');
  ov.className='lad-ov'; ov.id='nhlLadOv'; ov.onclick=closeNhlLadder;
  ov.innerHTML=html;
  document.body.appendChild(ov);
}
function openNhlLadder(key){
  var p=window.__NHLLAD__[key]; if(!p)return;
  openNhlPlayerSummary(p);
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

function renderNhlGamePredictor(preds){
  if(!preds||!preds.length) return '<div class="no-picks">No game predictions available.</div>';
  var h='<div class="gp-grid">';
  preds.forEach(function(g){
    var t=g.startTime?new Date(g.startTime).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZoneName:'short'}):'';
    var hp=Math.round(g.winProbHome*100), ap=100-hp;
    var isHomePick=g.pickTeam===g.homeTeam;
    var pickFull=isHomePick?(g.homeFull||g.homeTeam):(g.awayFull||g.awayTeam);
    var pickClr=isHomePick?'#f59e0b':'#60a5fa';
    var pickBg=isHomePick
      ?'background:linear-gradient(135deg,rgba(245,158,11,.12),rgba(245,158,11,.04));border:1px solid rgba(245,158,11,.3)'
      :'background:linear-gradient(135deg,rgba(96,165,250,.12),rgba(96,165,250,.04));border:1px solid rgba(96,165,250,.3)';
    var b2bNote=(g.hB2b&&isHomePick?' \u00b7 HOME B2B':g.aB2b&&!isHomePick?' \u00b7 AWAY B2B':'');
    var ouClass=g.ouRec==='OVER'?'ou-over':g.ouRec==='UNDER'?'ou-under':g.ouRec==='PUSH'?'ou-push':'ou-book';
    var ouLbl=g.ouRec==='OVER'?'\u2b06 OVER':g.ouRec==='UNDER'?'\u2b07 UNDER':g.ouRec==='PUSH'?'\u2248 PUSH':'\u2014';
    function sBadge(s){
      if(!s) return '';
      return '<span class="gp-stk '+(s[0]==='W'?'win-stk':'loss-stk')+'">'+s+'</span>';
    }
    var hLogo='https://assets.nhle.com/logos/nhl/svg/'+g.homeTeam+'_light.svg';
    var aLogo='https://assets.nhle.com/logos/nhl/svg/'+g.awayTeam+'_light.svg';
    var mlRow='';
    if(g.homeMl!=null&&g.awayMl!=null){
      var hs=g.homeMl>=0?'+':'', as_=g.awayMl>=0?'+':'';
      mlRow='<div class="gp-ml-row"><span>'+g.awayTeam+' ML: '+as_+g.awayMl+'</span><span>'+g.homeTeam+' ML: '+hs+g.homeMl+'</span></div>';
    }
    h+='<div class="gp-card">'
      +'<div class="gp-head"><span class="gp-mu">'+g.awayTeam+' <span style="color:#4b5563;font-weight:400">@</span> '+g.homeTeam+'</span><span class="gp-time">'+t+'</span></div>'
      +'<div class="gp-pick" style="'+pickBg+'">'
        +'<span style="font-size:1.2rem">\\uD83C\\uDFC6</span>'
        +'<div><div class="gp-pick-team" style="color:'+pickClr+'">'+pickFull+'</div>'
        +'<div class="gp-pick-prob">Confidence '+g.pickProb+'%'+b2bNote+'</div></div>'
      +'</div>'
      +'<div class="gp-bar-wrap">'
        +'<div class="gp-bar-labels"><span style="color:#f59e0b">'+g.homeTeam+' '+hp+'%</span><span style="color:#60a5fa">'+ap+'% '+g.awayTeam+'</span></div>'
        +'<div class="gp-bar-outer"><div class="gp-bar-home" style="width:'+hp+'%"></div><div class="gp-bar-away" style="width:'+ap+'%"></div></div>'
      +'</div>'
      +'<div class="gp-totals">'
        +'<div class="gp-tbox"><div class="gk">Proj Total</div><div class="gv">'+g.projTotal+'</div></div>'
        +(g.bookTotal!=null
          ?'<div class="gp-tbox ou-book"><div class="gk">Book O/U</div><div class="gv" style="color:#9ca3af">'+g.bookTotal+'</div></div>'
           +'<div class="gp-tbox '+ouClass+'"><div class="gk">O/U Pick</div><div class="gv">'+ouLbl+'</div></div>'
          :'')
      +'</div>'
      +'<div class="gp-teams">'
        +'<div class="gp-team">'
          +'<div class="gp-thdr"><img class="gp-tlogo" src="'+hLogo+'" onerror="this.style.display=\\'none\\'"/><span class="gp-tabbr">'+g.homeTeam+'</span><span class="gp-tha h-ha">HOME</span></div>'
          +'<div class="gp-sr"><span class="sk">GF / GA /G</span><span class="sv">'+g.hGfPG+' / '+g.hGaPG+'</span></div>'
          +'<div class="gp-sr"><span class="sk">PP / PK</span><span class="sv">'+g.hPpPct+'% / '+g.hPkPct+'%</span></div>'
          +'<div class="gp-sr"><span class="sk">Home W-L-OT</span><span class="sv">'+g.hHomeRec+'</span></div>'
          +'<div class="gp-sr"><span class="sk">L10</span><span class="sv">'+g.hL10+'</span></div>'
          +'<div class="gp-sr"><span class="sk">Points</span><span class="sv">'+g.hPts+' ('+g.hPctg+'%)</span></div>'
          +'<div class="gp-badges">'+(g.hB2b?'<span class="gp-b2b">B2B</span>':'')+sBadge(g.hStreak)+'</div>'
        +'</div>'
        +'<div class="gp-team">'
          +'<div class="gp-thdr"><img class="gp-tlogo" src="'+aLogo+'" onerror="this.style.display=\\'none\\'"/><span class="gp-tabbr">'+g.awayTeam+'</span><span class="gp-tha a-ha">AWAY</span></div>'
          +'<div class="gp-sr"><span class="sk">GF / GA /G</span><span class="sv">'+g.aGfPG+' / '+g.aGaPG+'</span></div>'
          +'<div class="gp-sr"><span class="sk">PP / PK</span><span class="sv">'+g.aPpPct+'% / '+g.aPkPct+'%</span></div>'
          +'<div class="gp-sr"><span class="sk">Road W-L-OT</span><span class="sv">'+g.aRoadRec+'</span></div>'
          +'<div class="gp-sr"><span class="sk">L10</span><span class="sv">'+g.aL10+'</span></div>'
          +'<div class="gp-sr"><span class="sk">Points</span><span class="sv">'+g.aPts+' ('+g.aPctg+'%)</span></div>'
          +'<div class="gp-badges">'+(g.aB2b?'<span class="gp-b2b">B2B</span>':'')+sBadge(g.aStreak)+'</div>'
        +'</div>'
      +'</div>'
      +mlRow
      +'</div>';
  });
  h+='</div>';
  return h;
}
function _nhlSimRecord(counts){
  if(!counts) return '—';
  return '<span style="color:#4ade80;font-weight:900">'+(counts.wins||0)+'W</span>'
    +'<span style="color:#64748b"> - </span>'
    +'<span style="color:#f87171;font-weight:900">'+(counts.losses||0)+'L</span>'
    +((counts.pushes||0)?'<span style="color:#facc15;font-weight:800"> - '+counts.pushes+'P</span>':'')
    +((counts.voids||0)?'<span style="color:#94a3b8;font-weight:800"> - '+counts.voids+' void</span>':'')
    +((counts.pending||0)?'<span style="color:#94a3b8;font-weight:800"> - '+counts.pending+' pending</span>':'');
}
function _nhlSimPct(counts){
  return counts&&counts.percentage!=null?counts.percentage+'%':'—';
}
function renderNhlSimulationStats(stats){
  if(!stats) return '';
  var team=stats.team||{}, props=stats.player_props||{};
  var rows=(props.by_market||[]).filter(function(m){
    return (m.decided||0)+(m.pushes||0)+(m.voids||0)+(m.pending||0)>0;
  }).map(function(m){
    return '<tr><td style="color:#cbd5e1">'+m.market+'</td>'
      +'<td style="font-weight:800">'+_nhlSimRecord(m)+'</td>'
      +'<td style="font-weight:900;color:#fbbf24">'+_nhlSimPct(m)+'</td></tr>';
  }).join('');
  var detail=rows
    ?'<details style="margin-top:12px"><summary style="cursor:pointer;color:#93c5fd;font-size:.75rem;font-weight:800">Player-prop result breakdown</summary>'
      +'<div style="overflow-x:auto;margin-top:8px"><table class="nhl-trk-tbl"><thead><tr><th>Market</th><th>Record</th><th>Hit Rate</th></tr></thead><tbody>'+rows+'</tbody></table></div></details>'
    :'';
  return '<div style="margin:0 0 16px;padding:15px 16px;background:linear-gradient(135deg,#0c1c2c,#08111d);border:1px solid rgba(56,189,248,.35);border-radius:14px">'
    +'<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap">'
      +'<div><div style="color:#7dd3fc;font-size:.68rem;font-weight:900;letter-spacing:.08em">HISTORICAL SIMULATION RESULTS</div>'
      +'<div style="color:#fff;font-size:1rem;font-weight:900;margin-top:3px">'+(stats.date||'Selected date')+'</div></div>'
      +'<div style="color:#94a3b8;font-size:.7rem;max-width:360px;text-align:right">'+(stats.note||'Simulation results are display only.')+'</div>'
    +'</div>'
    +(stats.lineNote?'<div style="margin-top:10px;padding:8px 10px;border-radius:8px;background:rgba(59,130,246,.12);color:#bfdbfe;font-size:.72rem;font-weight:700">'+stats.lineNote+'</div>':'')
    +'<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:13px">'
      +'<div style="flex:1;min-width:210px;background:#071f17;border:1px solid rgba(52,211,153,.25);border-radius:10px;padding:11px;text-align:center">'
        +'<div style="color:#6ee7b7;font-size:.64rem;font-weight:900;letter-spacing:.07em">TEAM WINNER CALLS</div>'
        +'<div style="margin-top:5px;font-size:1rem">'+_nhlSimRecord(team)+'</div>'
        +'<div style="margin-top:4px;color:#e2e8f0;font-size:.84rem;font-weight:900">'+_nhlSimPct(team)+' hit rate</div>'
      +'</div>'
      +'<div style="flex:1;min-width:210px;background:#07192b;border:1px solid rgba(56,189,248,.25);border-radius:10px;padding:11px;text-align:center">'
        +'<div style="color:#7dd3fc;font-size:.64rem;font-weight:900;letter-spacing:.07em">PLAYER PROPS</div>'
        +'<div style="margin-top:5px;font-size:1rem">'+_nhlSimRecord(props)+'</div>'
        +'<div style="margin-top:4px;color:#e2e8f0;font-size:.84rem;font-weight:900">'+_nhlSimPct(props)+' hit rate</div>'
      +'</div>'
    +'</div>'+detail+'</div>';
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

function _nhlJumpSlug(s){return String(s||'').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'');}
function _nhlGameId(away,home){return 'nhl-game-'+_nhlJumpSlug(away)+'-'+_nhlJumpSlug(home);}
function _nhlScrollTo(id){
  var el=document.getElementById(id);
  if(el)el.scrollIntoView({behavior:'smooth',block:'start'});
}
function nhlJumpToGames(){_nhlScrollTo('nhl-section-games');}
function nhlJumpToShots(){_nhlScrollTo('nhl-section-shots');}
function nhlJumpToPoints(){_nhlScrollTo('nhl-section-points');}
function nhlJumpToPpPoints(){_nhlScrollTo('nhl-section-pp-points');}
function nhlJumpToAssists(){_nhlScrollTo('nhl-section-assists');}
function nhlJumpToGoals(){_nhlScrollTo('nhl-section-goals');}
function nhlJumpToSaves(){_nhlScrollTo('nhl-section-saves');}
function nhlScrollToGameId(id){
  var row=document.getElementById(id);
  if(!row)return;
  var panel=row.querySelector('.nhl-game-panel');
  if(panel&&panel.style.display==='none'){
    var toggleId=panel.id.replace('nhltoggle_','');
    nhlToggle(toggleId);
  }
  row.scrollIntoView({behavior:'smooth',block:'start'});
}

function renderResults(d){
  window.__NHL_RAW__ = d;
  window.__NHL_SEASON__ = d.season || '20252026';
  window.__NHL_DATE__ = d.date || '';
  document.getElementById('out').innerHTML = '<div class="nhl-toolbar"><div class="nhl-lookup"><div class="nhl-lookup-label">Player lookup</div><div class="nhl-lookup-row"><input id="nhlSearch" type="search" autocomplete="off" placeholder="Search a player…" aria-label="Search NHL player" oninput="_nhlPaint(this.value)" onkeydown="if(event.key===\\'Enter\\'){nhlLookupPlayer();}"/><button type="button" class="nhl-lookup-btn" onclick="nhlLookupPlayer()">View stats</button></div><div id="nhlLookupHint" class="nhl-lookup-hint">Search the loaded slate, then view all available category history.</div></div></div><div id="nhlBody"></div>';
  _nhlPaint('');
}
function _nhlSafe(value){
  return String(value==null?'':value).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function _nhlPlayerDirectory(){
  var raw=window.__NHL_RAW__||{}, keys=[
    'playerProfiles',
    'picks','rest','ptsPicks','ptsRest','ppPicks','ppRest','astPicks','astRest',
    'goalPicks','goalRest','savesPicks','savesRest','shotUnders','shotUndersRest',
    'ptsUnders','ptsUndersRest','ppUnders','ppUndersRest','astUnders','astUndersRest',
    'goalUnders','goalUndersRest','savesUnders','savesUndersRest'
  ];
  var seen={}, rows=[];
  keys.forEach(function(k){
    (raw[k]||[]).forEach(function(p){
      if(!p||!p.name)return;
      var id=p.pid!=null?String(p.pid):String(p.name).toLowerCase()+'|'+String(p.team||'');
      var key=id+'|'+String(p.team||'')+'|'+String(p.opponent||'');
      if(seen[key])return;
      seen[key]=p;
      rows.push(p);
    });
  });
  rows.sort(function(a,b){return String(a.name||'').localeCompare(String(b.name||''));});
  return rows;
}
function _nhlSetLookupHint(text, error){
  var hint=document.getElementById('nhlLookupHint');
  if(hint){hint.textContent=text||'';hint.style.color=error?'#f87171':'#9ca3af';}
}
function nhlLookupPlayer(){
  var input=document.getElementById('nhlSearch'), q=input?String(input.value||'').trim():'';
  if(!q){_nhlSetLookupHint('Type a player name to view category history.',true);if(input)input.focus();return;}
  var ql=q.toLowerCase(), rows=_nhlPlayerDirectory();
  var exact=rows.filter(function(p){return String(p.name||'').toLowerCase()===ql;});
  var matches=exact.length?exact:rows.filter(function(p){return String(p.name||'').toLowerCase().indexOf(ql)>=0;});
  if(!matches.length){
    _nhlSetLookupHint('No matching player in the loaded slate. Try a name from the suggestions.',true);
    return;
  }
  var p=matches[0];
  if(input)input.value=p.name;
  _nhlSetLookupHint('Showing '+p.name+' vs '+(p.opponent||'today\\'s opponent')+'.');
  openNhlPlayerSummary(p);
}
// Re-paints the NHL body filtered by player name. The search box lives outside
// #nhlBody so it keeps focus across keystrokes. `d` is aliased to a shallow copy
// whose pick lists are name-filtered, leaving the original render code untouched.
function _nhlPaint(q){
  var raw=window.__NHL_RAW__; if(!raw) return;
  var options=document.getElementById('nhlPlayerOptions');
  if(options){
    options.innerHTML=_nhlPlayerDirectory().map(function(p){
      return '<option value="'+_nhlSafe(p.name)+'">'+_nhlSafe(p.team||'')+(p.opponent?' vs '+_nhlSafe(p.opponent):'')+'</option>';
    }).join('');
  }
  q=(q||'').toLowerCase().trim();
  function _f(a){return q?(a||[]).filter(function(p){return (p.name||'').toLowerCase().indexOf(q)>=0;}):(a||[]);}
  var d={}; for(var _k in raw){ d[_k]=raw[_k]; }
  d.picks=_f(raw.picks); d.ptsPicks=_f(raw.ptsPicks); d.ppPicks=_f(raw.ppPicks); d.astPicks=_f(raw.astPicks); d.goalPicks=_f(raw.goalPicks); d.savesPicks=_f(raw.savesPicks);
  d.rest=_f(raw.rest); d.ptsRest=_f(raw.ptsRest); d.ppRest=_f(raw.ppRest); d.astRest=_f(raw.astRest); d.goalRest=_f(raw.goalRest); d.savesRest=_f(raw.savesRest);
  d.shotUnders=_f(raw.shotUnders); d.ptsUnders=_f(raw.ptsUnders); d.ppUnders=_f(raw.ppUnders); d.astUnders=_f(raw.astUnders); d.goalUnders=_f(raw.goalUnders); d.savesUnders=_f(raw.savesUnders);
  d.shotUndersRest=_f(raw.shotUndersRest); d.ptsUndersRest=_f(raw.ptsUndersRest); d.ppUndersRest=_f(raw.ppUndersRest); d.astUndersRest=_f(raw.astUndersRest); d.goalUndersRest=_f(raw.goalUndersRest); d.savesUndersRest=_f(raw.savesUndersRest);
  var h = '';

  if(d.simulation && d.simulationStats) h += renderNhlSimulationStats(d.simulationStats);

  // Chips
  h += '<div class="chips">' +
    '<div class="chip nhl-jump-chip" onclick="nhlJumpToGames()" role="button" tabindex="0"><div class="val">' + d.games.length + '</div><div class="lbl">Games</div></div>' +
    '<div class="chip nhl-jump-chip" onclick="nhlJumpToShots()" role="button" tabindex="0"><div class="val">' + ((d.picks||[]).length) + '</div><div class="lbl">Shots</div></div>' +
    '<div class="chip nhl-jump-chip" onclick="nhlJumpToPoints()" role="button" tabindex="0"><div class="val">' + ((d.ptsPicks||[]).length) + '</div><div class="lbl">Points</div></div>' +
    '<div class="chip nhl-jump-chip" onclick="nhlJumpToPpPoints()" role="button" tabindex="0"><div class="val">' + ((d.ppPicks||[]).length) + '</div><div class="lbl">PP Points</div></div>' +
    '<div class="chip nhl-jump-chip" onclick="nhlJumpToAssists()" role="button" tabindex="0"><div class="val">' + ((d.astPicks||[]).length) + '</div><div class="lbl">Assists</div></div>' +
    '<div class="chip nhl-jump-chip" onclick="nhlJumpToGoals()" role="button" tabindex="0"><div class="val">' + ((d.goalPicks||[]).length) + '</div><div class="lbl">Goals</div></div>' +
    '<div class="chip nhl-jump-chip" onclick="nhlJumpToSaves()" role="button" tabindex="0"><div class="val">' + ((d.savesPicks||[]).length) + '</div><div class="lbl">Saves</div></div>' +
    '</div>';

  // Games
  h += '<div id="nhl-section-games" class="sec">- Games -- ' + (d.targetDate || '') + '</div><div class="games">';
  d.games.forEach(function(g){
    var t = g.startTime ? new Date(g.startTime).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZoneName:'short'}) : '';
    var lu=g.lineupByTeam||{},a=lu[g.awayTeam]||g.lineupSource||'UNAVAILABLE',hm=lu[g.homeTeam]||g.lineupSource||'UNAVAILABLE';
    function _luText(v){return v==='CONFIRMED'?'confirmed':v==='BOOK_LISTED'?'book-listed': 'unavailable';}
    h += '<div class="gcard nhl-game-jump" data-game-id="' + _nhlGameId(g.awayTeam,g.homeTeam) + '"><div class="mu">' + g.awayTeam + ' @ ' + g.homeTeam + '</div><div class="gt">' + t + '</div>'
      + '<div style="margin-top:4px;color:#94a3b8;font-size:.6rem;font-weight:800;text-transform:uppercase;letter-spacing:.04em">Lineups: '
      + g.awayTeam + ' ' + _luText(a) + ' · ' + g.homeTeam + ' ' + _luText(hm) + '</div></div>';
  });
  h += '</div>';

  // SA Rankings
  h += '<div class="sec">- Shots Against / Game Rankings</div><div class="sa-list">';
  (d.sa_ranks || []).forEach(function(item, i){
    h += '<div class="sa-badge"><span class="rk">#' + (i+1) + ' ' + item[0] + '</span> <span class="sv">' + item[1].toFixed(1) + '</span></div>';
  });
  h += '</div>';

  // ── Game Predictor ────────────────────────────────────────────────────────
  var _gpPreds = d.game_predictions || [];
  if(_gpPreds.length){
    h += '<div class="sec">\\uD83D\\uDD2E Game Predictor \u2014 Win Probability & Projected Totals</div>';
    h += renderNhlGamePredictor(_gpPreds);
  }

  // ── 🔒 80–100% Locks — cross-market picks hitting 80%+ ────────────────
  var _lockAll=[];
  ['picks','ptsPicks','ppPicks','astPicks','goalPicks','savesPicks',
   'shotUnders','ptsUnders','ppUnders','astUnders','goalUnders','savesUnders'].forEach(function(k){
    (d[k]||[]).forEach(function(p){
      var sc=Number(p.dispScore||p.ptsScore||p.score||0);
      if(sc>=80) _lockAll.push(Object.assign({},p,{_lockScore:sc}));
    });
  });
  // De-dup by name+market in case a player appears in two lists
  var _lockSeen={}; _lockAll=_lockAll.filter(function(p){
    var k=(p.name||'')+'|'+(p.mkt||'')+'|'+(p.pick||'OVER');
    if(_lockSeen[k]) return false; _lockSeen[k]=true; return true;
  });
  _lockAll.sort(function(a,b){return b._lockScore-a._lockScore;});
  if(q) _lockAll=_lockAll.filter(function(p){return (p.name||'').toLowerCase().indexOf(q)>=0;});
  var _lockMain=_lockAll.slice(0,10);
  var _lockOvf=_lockAll.slice(10,20);
  if(_lockMain.length){
    h+='<div style="background:linear-gradient(135deg,rgba(245,158,11,.1),rgba(74,222,128,.05));border:1px solid rgba(245,158,11,.4);border-radius:14px;margin-bottom:14px;overflow:hidden">'
      +'<div style="display:flex;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid rgba(245,158,11,.2)">'
      +'<span style="font-size:1.4rem;flex-shrink:0">&#128274;</span>'
      +'<div style="flex:1;min-width:0">'
      +'<div style="font-weight:900;font-size:1rem;color:#f59e0b;letter-spacing:.03em">80–100% Locks</div>'
      +'<div style="font-size:.72rem;color:#9ca3af;margin-top:2px">Picks hitting 80%+ across all tracked samples — top-line performers sorted highest % first</div>'
      +'</div>'
      +'<div style="background:rgba(245,158,11,.2);border:1px solid rgba(245,158,11,.5);border-radius:20px;padding:3px 12px;font-size:.73rem;font-weight:900;color:#f59e0b;flex-shrink:0">'+_lockMain.length+(_lockOvf.length?' + '+_lockOvf.length+' more':'')+' lock'+(_lockMain.length!==1?'s':'')+'</div>'
      +'</div>'
      +nhlCardGrid(_lockMain);
    if(_lockOvf.length){
      h+='<details style="border-top:1px solid rgba(245,158,11,.2)">'
        +'<summary style="padding:10px 16px;cursor:pointer;color:#f59e0b;font-size:.8rem;font-weight:700;user-select:none">&#9660; '+_lockOvf.length+' more lock'+(+_lockOvf.length!==1?'s':'')+' (Overflow)</summary>'
        +nhlCardGrid(_lockOvf)
        +'</details>';
    }
    h+='</div>';
  }

  // SHOTS cards (OVER)
  h += '<div id="nhl-section-shots" class="nhl-scroll-anchor"></div><div class="sec">🏒 Top ' + ((d.picks||[]).length) + ' Shots on Goal — OVER</div>';
  h += nhlCardGrid(d.picks);
  h += nhlRestBlock(d.rest, 'shots', '#4ade80');

  // Shots UNDER cards
  if((d.shotUnders||[]).length){
    h += '<div class="sec">⬇ Top ' + d.shotUnders.length + ' Shots on Goal — UNDER</div>';
    h += nhlUnderGrid(d.shotUnders);
    h += nhlUnderRestBlock(d.shotUndersRest, 'shots under', '#f87171');
  }

  // POINTS cards
  h += '<div id="nhl-section-points" class="nhl-scroll-anchor"></div>';
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
  // POWER PLAY POINTS cards — model 0.5 line
  h += '<div id="nhl-section-pp-points" class="nhl-scroll-anchor"></div>';
  if((d.ppPicks||[]).length){
    h += '<div class="sec">⚡ Top ' + d.ppPicks.length + ' Power Play Points (1+) — MODEL</div>';
    h += nhlCardGrid(d.ppPicks);
    h += nhlRestBlock(d.ppRest, 'power play points', '#c084fc');
  }
  if((d.ppUnders||[]).length){
    h += '<div class="sec">⬇ Top ' + d.ppUnders.length + ' Power Play Points (1+) — UNDER · MODEL</div>';
    h += nhlUnderGrid(d.ppUnders);
    h += nhlUnderRestBlock(d.ppUndersRest, 'power play points under', '#f87171');
  }
  // ASSISTS cards
  h += '<div id="nhl-section-assists" class="nhl-scroll-anchor"></div>';
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
  h += '<div id="nhl-section-goals" class="nhl-scroll-anchor"></div>';
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
  h += '<div id="nhl-section-saves" class="nhl-scroll-anchor"></div>';
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
    .concat(d.ppPicks||[]).concat(d.ppRest||[])
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
      var shots = gamePlays.filter(function(p){return p.mkt==='Shots on Goal';});
      var pts   = gamePlays.filter(function(p){return p.mkt==='Points (1+)';});
      var pp    = gamePlays.filter(function(p){return p.mkt==='Power Play Points (1+)';});
      var ast   = gamePlays.filter(function(p){return p.mkt==='Assists (1+)';});
      var goals = gamePlays.filter(function(p){return p.mkt==='Goals (1+)';});
      var sv    = gamePlays.filter(function(p){return p.mkt==='Goalie Saves';});
       h += '<div id="' + _nhlGameId(g.awayTeam,g.homeTeam) + '" class="nhl-game-row" style="margin-bottom:10px">';
      h += '<div onclick="nhlToggle('+gi+')" style="background:#161616;border:1px solid #262626;border-radius:12px;padding:12px 18px;cursor:pointer;display:flex;align-items:center;justify-content:space-between">';
      h += '<span style="font-weight:700;color:#fff;font-size:.92rem">' + gameName + '</span>';
      h += '<div style="display:flex;align-items:center;gap:10px">';
       h += '<span style="background:rgba(245,158,11,.1);color:#f59e0b;padding:3px 12px;border-radius:999px;font-size:.75rem;font-weight:700">';
       h += shots.length + ' shots | ' + pts.length + ' pts | ' + pp.length + ' pp pts | ' + ast.length + ' ast | ' + goals.length + ' goals | ' + sv.length + ' sv</span>';
      h += '<button id="nhltoggle_btn_'+gi+'" onclick="event.stopPropagation();nhlToggle('+gi+')" style="background:none;border:1px solid #374151;color:#9ca3af;border-radius:6px;padding:3px 12px;font-size:.72rem;cursor:pointer">Expand</button>';
      h += '</div></div>';
       h += '<div id="nhltoggle_'+gi+'" class="nhl-game-panel" style="display:none;margin-top:6px">';
       if(!gamePlays.length) h += '<div style="color:#6b7280;font-size:.78rem;padding:12px">No qualifying plays for this game.</div>';
      if(shots.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Shots on Goal</div>';
        h += buildTable(shots, 1);
      }
      if(pts.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Points</div>';
        h += buildPtsTable(pts, 1);
      }
      if(pp.length){
        h += '<div style="font-size:.72rem;font-weight:700;color:#c084fc;text-transform:uppercase;letter-spacing:.1em;padding:8px 12px 4px">Power Play Points</div>';
        h += buildNormTable(pp, 1);
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
  document.querySelectorAll('#nhlBody .nhl-game-jump').forEach(function(card){
    card.onclick=function(){nhlScrollToGameId(card.getAttribute('data-game-id'));};
  });
  // Parlay pool always reflects the full (unfiltered) slate regardless of search.
  function _nhlParlaySide(arr, side){return (arr||[]).map(function(p){return Object.assign({},p,{_parlaySide:side});});}
  window.__NHL_PLAYS__ = _nhlParlaySide(raw.picks,'OVER').concat(_nhlParlaySide(raw.rest,'OVER'))
    .concat(_nhlParlaySide(raw.ptsPicks,'OVER')).concat(_nhlParlaySide(raw.ptsRest,'OVER'))
    .concat(_nhlParlaySide(raw.astPicks,'OVER')).concat(_nhlParlaySide(raw.astRest,'OVER'))
    .concat(_nhlParlaySide(raw.goalPicks,'OVER')).concat(_nhlParlaySide(raw.goalRest,'OVER'))
    .concat(_nhlParlaySide(raw.savesPicks,'OVER')).concat(_nhlParlaySide(raw.savesRest,'OVER'))
    .concat(_nhlParlaySide(raw.shotUnders,'UNDER')).concat(_nhlParlaySide(raw.shotUndersRest,'UNDER'))
    .concat(_nhlParlaySide(raw.ptsUnders,'UNDER')).concat(_nhlParlaySide(raw.ptsUndersRest,'UNDER'))
    .concat(_nhlParlaySide(raw.astUnders,'UNDER')).concat(_nhlParlaySide(raw.astUndersRest,'UNDER'))
    .concat(_nhlParlaySide(raw.goalUnders,'UNDER')).concat(_nhlParlaySide(raw.goalUndersRest,'UNDER'))
    .concat(_nhlParlaySide(raw.savesUnders,'UNDER')).concat(_nhlParlaySide(raw.savesUndersRest,'UNDER'));
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
// ── NHL Track Record ──────────────────────────────────────────────────────────
var _nhlTrkData=null,_nhlTrkReplay=null,_nhlTrkTabMode='cat',_nhlOvfTabMode='cat';
var _nhlSpTrkData=null,_nhlSpTrkTabMode='cat';
function _nhlTrkDayName(){
  var dp=document.getElementById('nhlTrkDate'),dn=document.getElementById('nhlTrkDayName');
  if(!dp||!dn) return;
  try{var days=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    dn.textContent=days[new Date(dp.value+'T12:00:00').getDay()];}catch(e){dn.textContent='';}
}
function _nhlOvfDayName(){
  var dp=document.getElementById('nhlOvfDate'),dn=document.getElementById('nhlOvfDayName');
  if(!dp||!dn) return;
  try{var days=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    dn.textContent=days[new Date(dp.value+'T12:00:00').getDay()];}catch(e){dn.textContent='';}
}
function _nhlSpDayName(){
  var dp=document.getElementById('nhlSpDate'),dn=document.getElementById('nhlSpDayName');
  if(!dp||!dn)return;
  if(!dp.value){dn.textContent='All Time';return;}
  try{var days=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    dn.textContent=days[new Date(dp.value+'T12:00:00').getDay()];}catch(e){dn.textContent='';}
}
function _nhlRecordDateChanged(sourceId){
  var source=document.getElementById(sourceId);
  if(!source)return;
  var other=document.getElementById(sourceId==='nhlTrkDate'?'nhlOvfDate':'nhlTrkDate');
  if(other)other.value=source.value;
  _nhlTrkDayName();_nhlOvfDayName();
  renderNhlTrackDay();renderNhlOverflowDay();
}
function openNhlOverflowRecord(){
  var section=document.getElementById('nhl-overflow-section');
  if(section)section.scrollIntoView({behavior:'smooth',block:'start'});
  if(_nhlTrkData)renderNhlOverflowDay();
  else loadNhlTrackRecord();
}
function openNhlSpecialRecord(){
  var section=document.getElementById('nhl-special-record-section');
  if(section)section.scrollIntoView({behavior:'smooth',block:'start'});
  if(_nhlSpTrkData)renderNhlSpecialDay();
  else loadNhlSpecialRecord();
}
async function loadNhlTrackRecord(manualGrade){
  var body=document.getElementById('nhlTrkBody');
  var ovfBody=document.getElementById('nhlOvfBody');
  if(body) body.innerHTML='<p style="color:#94a3b8;padding:24px">Loading\u2026</p>';
  if(ovfBody) ovfBody.innerHTML='<p style="color:#94a3b8;padding:24px">Loading\u2026</p>';
  try{
    // Hub-hosted snapshots include a read-only Track Record payload.  That
    // keeps results visible even when the live Render service is waking up or
    // temporarily unavailable.
    if(window.__INITIAL_TRACK_RECORD__&&!manualGrade){
      _nhlTrkData=window.__INITIAL_TRACK_RECORD__;
      _nhlSpTrkData={dates:_nhlTrkData.special_dates||[],stake:_nhlTrkData.stake||20};
      renderNhlTrackDay();
      renderNhlOverflowDay();
      renderNhlSpecialDay();
      return;
    }
    var dp=document.getElementById('nhlTrkDate');
    var qs=manualGrade?'?grade=true&date_str='+encodeURIComponent(dp&&dp.value?dp.value:''):'';
    var r=await fetch('/api/track-record'+qs);
    if(!r.ok) throw new Error(await r.text());
    _nhlTrkData=await r.json();
    _nhlSpTrkData={dates:_nhlTrkData.special_dates||[],stake:_nhlTrkData.stake||20};
    renderNhlTrackDay();
    renderNhlOverflowDay();
    renderNhlSpecialDay();
  }catch(e){
    if(body) body.innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading track record')+'</p>';
    if(ovfBody) ovfBody.innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading overflow track record')+'</p>';
  }
}
async function loadNhlSpecialRecord(manualGrade){
  var body=document.getElementById('nhlSpBody');
  if(body)body.innerHTML='<p style="color:#94a3b8;padding:24px">Loading\u2026</p>';
  try{
    if(window.__INITIAL_TRACK_RECORD__&&!manualGrade){
      _nhlSpTrkData={dates:window.__INITIAL_TRACK_RECORD__.special_dates||[],
                     stake:window.__INITIAL_TRACK_RECORD__.stake||20};
      renderNhlSpecialDay();return;
    }
    var dp=document.getElementById('nhlSpDate');
    var qs=manualGrade?'?grade=true&date_str='+encodeURIComponent(dp&&dp.value?dp.value:''):'';
    var r=await fetch('/api/special-track-record'+qs);
    if(!r.ok)throw new Error(await r.text());
    _nhlSpTrkData=await r.json();
    renderNhlSpecialDay();
  }catch(e){
    if(body)body.innerHTML='<p style="color:#f87171;padding:16px">'+(e.message||'Error loading Special Plays record')+'</p>';
  }
}
function nhlTrkSetTab(tab){
  _nhlTrkTabMode=tab;
  var bc=document.getElementById('nhlTrkBtnCat'),bl=document.getElementById('nhlTrkBtnList');
  if(bc) bc.style.background=tab==='cat'?'#065f46':'#1e293b';
  if(bl) bl.style.background=tab==='list'?'#065f46':'#1e293b';
  renderNhlTrackDay();
}
function nhlOvfSetTab(tab){
  _nhlOvfTabMode=tab;
  var bc=document.getElementById('nhlOvfBtnCat'),bl=document.getElementById('nhlOvfBtnList');
  if(bc) bc.style.background=tab==='cat'?'#b45309':'#1e293b';
  if(bl) bl.style.background=tab==='list'?'#b45309':'#1e293b';
  renderNhlOverflowDay();
}
function _nhlSpSetTab(tab){
  _nhlSpTrkTabMode=tab;
  var bc=document.getElementById('nhlSpBtnCat'),bl=document.getElementById('nhlSpBtnList');
  if(bc)bc.style.background=tab==='cat'?'#854d0e':'#1e293b';
  if(bl)bl.style.background=tab==='list'?'#854d0e':'#1e293b';
  renderNhlSpecialDay();
}
function _nhlSpSetAllTime(){
  var dp=document.getElementById('nhlSpDate');if(dp)dp.value='';
  _nhlSpDayName();renderNhlSpecialDay();
}
function _nhlTrkStake(){
  var n=parseFloat(localStorage.getItem('nhl_track_stake')||'20');
  return isFinite(n)&&n>0?n:20;
}
function _nhlTrkSetStake(input){
  var n=parseFloat(input&&input.value||'');
  if(!isFinite(n)||n<=0){input.value=_nhlTrkStake().toFixed(2);return;}
  localStorage.setItem('nhl_track_stake',String(Math.round(n*100)/100));
  renderNhlTrackDay();
  renderNhlOverflowDay();
  renderNhlSpecialDay();
}
function _nhlTrkProfit(row,stake){
  if(!row||row.result!=='WIN'&&row.result!=='LOSS') return null;
  if(row.odds==null||String(row.odds).trim()===''||String(row.odds)==='0') return null;
  var odds=parseFloat(row.odds);
  if(!isFinite(odds)||odds===0) return null;
  return row.result==='LOSS'?-stake:(odds>0?stake*odds/100:stake*100/Math.abs(odds));
}
function _nhlMainRows(dayData){
  return ((dayData&&dayData.detail)||[]).filter(function(r){
    return !r.is_overflow&&r.category!=='NHL Overflow';
  });
}
function _nhlOverflowRows(dayData){
  if(dayData&&Array.isArray(dayData.overflow_detail))return dayData.overflow_detail;
  return ((dayData&&dayData.detail)||[]).filter(function(r){
    return !!r.is_overflow||r.category==='NHL Overflow';
  });
}
function renderNhlSpecialDay(){
  if(!_nhlSpTrkData)return;
  var dp=document.getElementById('nhlSpDate'),selDate=dp?dp.value:'';
  var dates=_nhlSpTrkData.dates||[];
  var day=selDate?dates.find(function(d){return String(d.date||'').slice(0,10)===selDate;}):null;
  var sumEl=document.getElementById('nhlSpSummary'),bodyEl=document.getElementById('nhlSpBody');
  if(!sumEl||!bodyEl)return;
  if(selDate&&!day){
    sumEl.innerHTML='<p style="color:#facc15;padding:12px;text-align:center">No saved Special Plays snapshot exists for '+selDate+'.</p>';
    bodyEl.innerHTML='';return;
  }
  var rows=[];
  if(day)rows=(day.detail||[]).slice();
  else dates.forEach(function(d){(d.detail||[]).forEach(function(r){rows.push(r);});});
  var decided=rows.filter(function(r){return r.result==='WIN'||r.result==='LOSS';});
  var withOdds=decided.filter(function(r){return r.odds!=null&&String(r.odds).trim()!==''&&String(r.odds)!=='0';});
  var wins=decided.filter(function(r){return r.result==='WIN';}).length,losses=decided.length-wins;
  var pushes=rows.filter(function(r){return r.result==='PUSH';}).length;
  var voids=rows.filter(function(r){return r.result==='VOID';}).length;
  var pending=rows.length-decided.length-pushes-voids,stake=_nhlTrkStake();
  var netPL=withOdds.reduce(function(a,r){return a+(_nhlTrkProfit(r,stake)||0);},0);
  var staked=withOdds.length*stake,roi=staked?netPL/staked*100:null;
  var rate=decided.length?wins/decided.length*100:null,plColor=netPL>=0?'#4ade80':'#f87171';
  var rangeLabel=selDate?selDate:'All Time';
  sumEl.innerHTML='<div style="background:#1c1408;border:1px solid #713f12;border-radius:12px;padding:14px 18px;display:flex;flex-wrap:wrap;gap:18px;align-items:center;margin-bottom:14px">'
    +'<span style="color:#fde047;font-size:.78rem;font-weight:900">'+rangeLabel+'</span>'
    +'<span style="font-size:1.05rem;font-weight:900;color:#fff"><span style="color:#4ade80">'+wins+'</span>/<span style="color:#f87171">'+(wins+losses)+'</span>'
    +(rate!=null?' <span style="color:#94a3b8;font-size:.85rem;font-weight:600">('+rate.toFixed(1)+'%)</span>':'')+'</span>'
    +'<label style="display:flex;align-items:center;gap:6px;color:#cbd5e1;font-size:.76rem;font-weight:700">Bet size ($)<input type="number" min="0.01" step="0.01" value="'+stake.toFixed(2)+'" onchange="_nhlTrkSetStake(this)" style="width:82px;background:#0b1120;border:1px solid #854d0e;border-radius:7px;padding:6px 8px;color:#fff;font-weight:800"></label>'
    +'<span style="font-family:monospace;font-weight:800;color:'+plColor+'">Net '+(netPL>=0?'+$':'-$')+Math.abs(netPL).toFixed(2)+'</span>'
    +(roi!=null?'<span style="font-family:monospace;font-weight:700;color:'+plColor+'">ROI '+(roi>=0?'+':'')+roi.toFixed(1)+'%</span>':'')
    +(pushes?'<span style="color:#facc15;font-size:.8rem;font-weight:800">'+pushes+' push</span>':'')
    +(voids?'<span style="color:#94a3b8;font-size:.8rem;font-weight:800">'+voids+' void</span>':'')
    +(pending?'<span style="color:#facc15;font-size:.8rem;font-weight:800">'+pending+' pending</span>':'')
    +'<span style="color:#64748b;font-size:.8rem">'+rows.length+' saved plays · '+withOdds.length+' priced</span></div>';
  bodyEl.innerHTML=_nhlSpTrkTabMode==='cat'?_nhlTrkCatHtml(rows,stake):_nhlTrkListHtml(rows,true);
}
function renderNhlTrackDay(){
  if(!_nhlTrkData) return;
  var dp=document.getElementById('nhlTrkDate');
  var selDate=dp?dp.value:'';
  var dates=_nhlTrkData.dates||[];
  var savedDay=selDate?dates.find(function(d){return String(d.date||'').slice(0,10)===selDate;}):null;
  var replayDay=selDate&&_nhlTrkReplay&&String(_nhlTrkReplay.date||'').slice(0,10)===selDate?_nhlTrkReplay:null;
  var dayData=replayDay||savedDay;
  var isReplay=!!replayDay;
  var sumEl=document.getElementById('nhlTrkSummary'),bodyEl=document.getElementById('nhlTrkBody');
  if(!sumEl||!bodyEl) return;
  if(selDate&&!dayData){
    sumEl.innerHTML='<div style="padding:12px;text-align:center"><p style="color:#facc15;margin:0 0 10px">No saved official NHL pick snapshot exists for '+selDate+'.</p><p style="color:#94a3b8;font-size:.78rem;margin:0">Choose that date above and click <b style="color:#67e8f9">Get Picks</b> to show its historical pick board and replay Track Record.</p></div>';
    bodyEl.innerHTML='';
    return;
  }
  var rows=[];
  if(dayData) rows=_nhlMainRows(dayData);
  else dates.forEach(function(d){_nhlMainRows(d).forEach(function(r){rows.push(r);});});
  var decided=rows.filter(function(r){return r.result==='WIN'||r.result==='LOSS';});
  var _withOdds=decided.filter(function(r){return r.odds!=null&&String(r.odds).trim()!==''&&String(r.odds)!=='0';});
  if(!rows.length&&selDate){
    sumEl.innerHTML='<p style="color:#94a3b8;padding:12px;text-align:center">No saved player picks for '+selDate+'.</p>';
    bodyEl.innerHTML='';return;
  }
  var stake=_nhlTrkStake();
  var wins=decided.filter(function(r){return r.result==='WIN';}).length;
  var losses=decided.length-wins;
  var pushes=rows.filter(function(r){return r.result==='PUSH';}).length;
  var voids=rows.filter(function(r){return r.result==='VOID';}).length;
  var pending=rows.length-decided.length-pushes-voids;
  var netPL=_withOdds.reduce(function(a,r){return a+(_nhlTrkProfit(r,stake)||0);},0);
  var totalStaked=_withOdds.length*stake;
  var roi=totalStaked?(netPL/totalStaked*100):null;
  var rate=decided.length?(wins/decided.length*100):null;
  var plColor=netPL>=0?'#4ade80':'#f87171';
  var replayNote=isReplay?'<div style="margin-bottom:12px;padding:10px 12px;border:1px solid rgba(56,189,248,.35);border-radius:10px;background:rgba(14,116,144,.1);color:#bae6fd;font-size:.76rem;font-weight:700">'+(dayData.note||'Historical replay only — excluded from the official Track Record.')+'</div>':'';
  var voidNotes=isReplay&&voids&&dayData.summary&&dayData.summary.void_reasons
    ?'<div style="margin:-4px 0 14px;padding:10px 12px;border-radius:10px;background:#111827;color:#cbd5e1;font-size:.76rem"><b style="color:#facc15">'+voids+' void call'+(voids===1?'':'s')+'</b> — '+dayData.summary.void_reasons.map(function(v){return v.count+'× '+v.reason;}).join(' · ')+'</div>'
    :'';
  sumEl.innerHTML=replayNote+'<div style="background:#0f172a;border-radius:12px;padding:14px 18px;display:flex;flex-wrap:wrap;gap:18px;align-items:center;margin-bottom:14px">'
    +'<span style="font-size:1.05rem;font-weight:900;color:#fff"><span style="color:#4ade80">'+wins+'</span>/<span style="color:#f87171">'+(wins+losses)+'</span>'
    +(rate!=null?' <span style="color:#94a3b8;font-size:.85rem;font-weight:600">('+rate.toFixed(1)+'%)</span>':'')+'</span>'
    +'<label style="display:flex;align-items:center;gap:6px;color:#cbd5e1;font-size:.76rem;font-weight:700">Bet size ($)<input id="nhl-trk-stake" type="number" min="0.01" step="0.01" value="'+stake.toFixed(2)+'" onchange="_nhlTrkSetStake(this)" style="width:82px;background:#0b1120;border:1px solid #334155;border-radius:7px;padding:6px 8px;color:#fff;font-weight:800"></label>'
    +'<span style="font-family:monospace;font-weight:800;color:'+plColor+'">Net '+(netPL>=0?'+$':'-$')+Math.abs(netPL).toFixed(2)+'</span>'
    +(roi!=null?'<span style="font-family:monospace;font-weight:700;color:'+plColor+'">ROI '+(roi>=0?'+':'')+roi.toFixed(1)+'%</span>':'')
      +(pushes?'<span style="color:#facc15;font-size:.8rem;font-weight:800">'+pushes+' push</span>':'')
      +(voids?'<span style="color:#94a3b8;font-size:.8rem;font-weight:800">'+voids+' void</span>':'')
     +(pending?'<span style="color:#facc15;font-size:.8rem;font-weight:800">'+pending+' pending</span>':'')
    +'<span style="color:#475569;font-size:.8rem">$'+stake.toFixed(2)+'/play \u00b7 '+_withOdds.length+' priced plays</span>'
      +'</div>'+voidNotes;
  bodyEl.innerHTML=(isReplay&&dayData.gp?_nhlGpHtml(dayData.gp):'')
     +(_nhlTrkTabMode==='cat'?_nhlTrkCatHtml(rows,stake):_nhlTrkListHtml(rows));
}
function renderNhlOverflowDay(){
  if(!_nhlTrkData)return;
  var dp=document.getElementById('nhlOvfDate');
  var selDate=dp?dp.value:'';
  var dates=_nhlTrkData.dates||[];
  var savedDay=selDate?dates.find(function(d){return String(d.date||'').slice(0,10)===selDate;}):null;
  var replayDay=selDate&&_nhlTrkReplay&&String(_nhlTrkReplay.date||'').slice(0,10)===selDate?_nhlTrkReplay:null;
  var dayData=replayDay||savedDay;
  var isReplay=!!replayDay;
  var sumEl=document.getElementById('nhlOvfSummary'),bodyEl=document.getElementById('nhlOvfBody');
  if(!sumEl||!bodyEl)return;
  if(selDate&&!dayData){
    sumEl.innerHTML='<div style="padding:12px;text-align:center"><p style="color:#facc15;margin:0 0 10px">No saved official NHL overflow snapshot exists for '+selDate+'.</p><p style="color:#94a3b8;font-size:.78rem;margin:0">Choose that date above and click <b style="color:#fbbf24">Get Picks</b> to show its historical overflow replay.</p></div>';
    bodyEl.innerHTML='';
    return;
  }
  var rows=[];
  if(dayData)rows=_nhlOverflowRows(dayData);
  else dates.forEach(function(d){_nhlOverflowRows(d).forEach(function(r){rows.push(r);});});
  var decided=rows.filter(function(r){return r.result==='WIN'||r.result==='LOSS';});
  var withOdds=decided.filter(function(r){return r.odds!=null&&String(r.odds).trim()!==''&&String(r.odds)!=='0';});
  if(!rows.length&&selDate){
    sumEl.innerHTML='<p style="color:#94a3b8;padding:12px;text-align:center">No ranks 11–20 overflow picks were recorded for '+selDate+'.</p>';
    bodyEl.innerHTML='';
    return;
  }
  var stake=_nhlTrkStake();
  var wins=decided.filter(function(r){return r.result==='WIN';}).length;
  var losses=decided.length-wins;
  var pushes=rows.filter(function(r){return r.result==='PUSH';}).length;
  var voids=rows.filter(function(r){return r.result==='VOID';}).length;
  var pending=rows.length-decided.length-pushes-voids;
  var netPL=withOdds.reduce(function(a,r){return a+(_nhlTrkProfit(r,stake)||0);},0);
  var totalStaked=withOdds.length*stake;
  var roi=totalStaked?(netPL/totalStaked*100):null;
  var rate=decided.length?(wins/decided.length*100):null;
  var plColor=netPL>=0?'#4ade80':'#f87171';
  var replayNote=isReplay?'<div style="margin-bottom:12px;padding:10px 12px;border:1px solid rgba(245,158,11,.35);border-radius:10px;background:rgba(180,83,9,.1);color:#fde68a;font-size:.76rem;font-weight:700">'+(dayData.note||'Historical overflow replay only — excluded from the official Overflow Track Record.')+'</div>':'';
  sumEl.innerHTML=replayNote+'<div style="background:#1c1408;border:1px solid #5b3d12;border-radius:12px;padding:14px 18px;display:flex;flex-wrap:wrap;gap:18px;align-items:center;margin-bottom:14px">'
    +'<span style="font-size:1.05rem;font-weight:900;color:#fff"><span style="color:#4ade80">'+wins+'</span>/<span style="color:#f87171">'+(wins+losses)+'</span>'
    +(rate!=null?' <span style="color:#94a3b8;font-size:.85rem;font-weight:600">('+rate.toFixed(1)+'%)</span>':'')+'</span>'
    +'<label style="display:flex;align-items:center;gap:6px;color:#cbd5e1;font-size:.76rem;font-weight:700">Bet size ($)<input type="number" min="0.01" step="0.01" value="'+stake.toFixed(2)+'" onchange="_nhlTrkSetStake(this)" style="width:82px;background:#0b1120;border:1px solid #78350f;border-radius:7px;padding:6px 8px;color:#fff;font-weight:800"></label>'
    +'<span style="font-family:monospace;font-weight:800;color:'+plColor+'">Net '+(netPL>=0?'+$':'-$')+Math.abs(netPL).toFixed(2)+'</span>'
    +(roi!=null?'<span style="font-family:monospace;font-weight:700;color:'+plColor+'">ROI '+(roi>=0?'+':'')+roi.toFixed(1)+'%</span>':'')
    +(pushes?'<span style="color:#facc15;font-size:.8rem;font-weight:800">'+pushes+' push</span>':'')
    +(voids?'<span style="color:#94a3b8;font-size:.8rem;font-weight:800">'+voids+' void</span>':'')
    +(pending?'<span style="color:#facc15;font-size:.8rem;font-weight:800">'+pending+' pending</span>':'')
    +'<span style="color:#64748b;font-size:.8rem">$'+stake.toFixed(2)+'/play · '+withOdds.length+' priced plays</span>'
    +'</div>';
  bodyEl.innerHTML=_nhlOvfTabMode==='cat'?_nhlTrkCatHtml(rows,stake):_nhlTrkListHtml(rows,true);
}
 function _nhlTrkCatHtml(allRows,stake){
  if(!allRows.length) return '<p style="color:#475569;padding:20px;text-align:center">No graded picks yet.</p>';
  var cats={},catOrder=['Shots on Goal','Points','Power Play Points','Assists','Goals','Goalie Saves','NHL Overflow','80-100% Locks','Shot Plays','Point Plays','Assist Plays','Goal Plays','Save Plays'];
  allRows.forEach(function(r){
    var cat=r.category||'Other',side=(r.side||'OVER').toUpperCase();
    var key=cat+'|'+side;
    (cats[key]||(cats[key]=[])).push(r);
  });
  Object.keys(cats).forEach(function(key){
    var cat=key.split('|')[0];
    if(catOrder.indexOf(cat)<0)catOrder.push(cat);
  });
  function hasOdds(r){return r.odds!=null&&String(r.odds).trim()!==''&&String(r.odds)!=='0';}
   function money(v){return v==null?'—':(v>=0?'+$':'-$')+Math.abs(Number(v)).toFixed(2);}
  function stats(list){
    var w=list.filter(function(r){return r.result==='WIN';}).length;
    var l=list.filter(function(r){return r.result==='LOSS';}).length;
    var push=list.filter(function(r){return r.result==='PUSH';}).length;
    var voids=list.filter(function(r){return r.result==='VOID';}).length;
    var pending=list.length-w-l-push-voids;
    var priced=list.filter(hasOdds),pl=priced.reduce(function(x,r){return x+(_nhlTrkProfit(r,stake)||0);},0);
    var staked=priced.length*stake,roi=staked?pl/staked*100:null;
    return {w:w,l:l,push:push,voids:voids,pending:pending,pl:pl,roi:roi,rate:(w+l)?w/(w+l)*100:null};
  }
  function catLabel(cat,side){return cat+' ('+(side==='OVER'?'Over':'Under')+')';}
  function row(cat,side,list){
    if(!list.length)return '';
    var s=stats(list), rate=s.rate!=null?s.rate:0;
    var rateColor=s.rate==null?'#64748b':(s.rate>=70?'#4ade80':(s.rate>=55?'#facc15':'#f87171'));
    var plColor=s.pl>=0?'#4ade80':'#f87171';
    var record=s.w+' / '+(s.w+s.l);
    if(s.push) record+=' <span style="color:#facc15;font-size:.68rem">· '+s.push+' push</span>';
    if(s.voids) record+=' <span style="color:#94a3b8;font-size:.68rem">· '+s.voids+' void</span>';
    if(s.pending) record+=' <span style="color:#facc15;font-size:.68rem">· '+s.pending+' pending</span>';
    return '<tr>'
      +'<td style="color:#e2e8f0;font-weight:700">'+catLabel(cat,side)+'</td>'
      +'<td style="font-family:monospace;color:#cbd5e1;font-weight:800">'+record+'</td>'
      +'<td><span class="nhl-trk-bar-wrap"><span class="nhl-trk-bar" style="display:block;width:'+rate+'%;background:'+rateColor+'"></span></span> <span style="color:'+rateColor+';font-family:monospace;font-weight:800">'+(s.rate!=null?s.rate.toFixed(1)+'%':'—')+'</span></td>'
      +'<td style="font-family:monospace;color:'+plColor+';font-weight:800">'+money(s.pl)+'</td>'
      +'<td style="font-family:monospace;color:'+plColor+';font-weight:800">'+(s.roi!=null?(s.roi>=0?'+':'')+s.roi.toFixed(1)+'%':'—')+'</td>'
      +'</tr>';
  }
  var html='<div style="overflow-x:auto"><table class="nhl-trk-tbl"><thead><tr><th>Category</th><th>Record</th><th>Hit Rate</th><th>Net P/L</th><th>ROI</th></tr></thead><tbody>';
  catOrder.forEach(function(cat){
    html+=row(cat,'OVER',cats[cat+'|OVER']||[])+row(cat,'UNDER',cats[cat+'|UNDER']||[]);
  });
  return html+'</tbody></table></div>';
}
function _nhlTrkListHtml(allRows,showRank){
  if(!allRows.length) return '<p style="color:#475569;padding:20px;text-align:center">No graded picks yet.</p>';
  var stake=_nhlTrkStake();
   var catOrder=['Shots on Goal','Points','Power Play Points','Assists','Goals','Goalie Saves','NHL Overflow','80-100% Locks','Shot Plays','Point Plays','Assist Plays','Goal Plays','Save Plays'];
   var catColors={'Shots on Goal':'#fbbf24','Points':'#60a5fa','Power Play Points':'#c084fc','Assists':'#a78bfa','Goals':'#fb7185','Goalie Saves':'#34d399','NHL Overflow':'#f59e0b','80-100% Locks':'#facc15','Shot Plays':'#fde047','Point Plays':'#60a5fa','Assist Plays':'#a78bfa','Goal Plays':'#fb7185','Save Plays':'#34d399'};
   var groups={},order=[];
   allRows.forEach(function(r){
     var cat=r.category||'Other',side=(r.side||'OVER').toUpperCase(),key=cat+'|'+side;
     if(!groups[key]){groups[key]=[];order.push(key);}
     groups[key].push(r);
   });
   function orderKey(key){
     var bits=key.split('|'),idx=catOrder.indexOf(bits[0]);
     return (idx<0?catOrder.length:idx)*2+(bits[1]==='UNDER'?1:0);
   }
   order.sort(function(a,b){return orderKey(a)-orderKey(b);});
   function hasOdds(r){return r.odds!=null&&String(r.odds).trim()!==''&&String(r.odds)!=='0';}
   function money(v){return v==null?'—':(v>=0?'+$':'-$')+Math.abs(Number(v)).toFixed(2);}
   function groupBlock(key){
     var bits=key.split('|'),cat=bits[0],side=bits[1],list=groups[key].slice().sort(function(a,b){
       var ar=a.rank==null?999:Number(a.rank),br=b.rank==null?999:Number(b.rank);
       return ar-br||(a.name||'').localeCompare(b.name||'');
     });
     var w=list.filter(function(r){return r.result==='WIN';}).length;
     var l=list.filter(function(r){return r.result==='LOSS';}).length;
     var push=list.filter(function(r){return r.result==='PUSH';}).length;
     var voids=list.filter(function(r){return r.result==='VOID';}).length;
     var pending=list.length-w-l-push-voids;
     var priced=list.filter(hasOdds),pl=priced.reduce(function(x,r){return x+(_nhlTrkProfit(r,stake)||0);},0);
     var rate=(w+l)?(w/(w+l)*100):null, accent=catColors[cat]||'#22d3ee';
     var meta=w+'W · '+l+'L'+(push?' · '+push+'P':'')+(voids?' · '+voids+'V':'')+(pending?' · '+pending+' pending':'');
     var rows=list.map(function(r){
       var rowProfit=_nhlTrkProfit(r,stake),result=(r.result||'PENDING').toUpperCase();
       var resultClass=result.toLowerCase(),plColor=result==='WIN'?'#4ade80':(result==='LOSS'?'#f87171':'#94a3b8');
       var odds=r.odds!=null&&String(r.odds).trim()!==''?(r.odds>0?'+':'')+r.odds:'—';
       return '<tr>'
         +(showRank?'<td style="color:#fbbf24;font-family:monospace;font-weight:900">#'+(r.rank!=null?r.rank:'—')+'</td>':'')
         +'<td style="color:#f8fafc;font-weight:850;font-size:.96rem">'+r.name+'</td>'
         +'<td style="color:#94a3b8;font-weight:800">'+r.team+'</td>'
         +'<td style="color:#e2e8f0;font-weight:800">'+(r.side||'')+(r.line!=null?' '+r.line:'')+'</td>'
         +'<td style="font-family:monospace;color:#cbd5e1;font-weight:700">'+odds+'</td>'
         +'<td style="color:#cbd5e1;font-weight:700">'+((r.actual!=null)?r.actual:'—')+'</td>'
         +'<td><span class="nhl-trk-result '+resultClass+'">'+result+'</span></td>'
         +'<td style="font-family:monospace;font-weight:900;color:'+plColor+'">'+(rowProfit!=null?money(rowProfit):'—')+'</td>'
         +'<td class="nhl-trk-note">'+(r.void_reason||r.line_source||'—')+'</td>'
         +'</tr>';
     }).join('');
     return '<section class="nhl-trk-group" style="--trk-accent:'+accent+'">'
       +'<div class="nhl-trk-group-head"><div class="nhl-trk-group-title">'
       +'<span class="nhl-trk-group-kicker">Category</span><span class="nhl-trk-group-name">'+cat+'</span><span class="nhl-trk-group-side">'+side+'</span></div>'
       +'<div class="nhl-trk-group-summary"><span>'+meta+'</span><span class="nhl-trk-group-rate">'+(rate!=null?rate.toFixed(1)+'%':'—')+'</span><span class="nhl-trk-group-pl" style="color:'+(pl>=0?'#4ade80':'#f87171')+'">'+money(pl)+'</span></div></div>'
       +'<div class="nhl-trk-table-scroll"><table class="nhl-trk-tbl"><thead><tr>'
       +(showRank?'<th>Rank</th>':'')+'<th>Player</th><th>Team</th><th>Pick</th><th>Odds</th><th>Actual</th><th>Result</th><th>P/L</th><th>Line / Note</th>'
       +'</tr></thead><tbody>'+rows+'</tbody></table></div></section>';
   }
   return order.map(groupBlock).join('');
}
function _nhlGpResult(r, key){
  var v=r&&r[key]; if(!v) return '<span style="color:#64748b">PENDING</span>';
  var c=v==='WIN'?'#4ade80':v==='LOSS'?'#f87171':'#facc15';
  return '<span style="color:'+c+';font-weight:900">'+v+'</span>';
}
function _nhlGpHtml(gp){
  if(!gp) return '';
  var mlTotal=(gp.mlWins||0)+(gp.mlLosses||0);
  var ouTotal=(gp.ouWins||0)+(gp.ouLosses||0);
  var mlRate=gp.mlRate!=null?gp.mlRate.toFixed(1)+'%':'—';
  var ouRate=gp.ouRate!=null?gp.ouRate.toFixed(1)+'%':'—';
  var detail=gp.detail||[];
  var rows=detail.map(function(r){
    var actual=(r.actualAway!=null&&r.actualHome!=null)
      ?r.awayTeam+' '+r.actualAway+' — '+r.actualHome+' '+r.homeTeam:'Pending';
    var totalPick=r.ouRec&&r.ouRec!=='PUSH'
      ?r.ouRec+(r.bookTotal!=null?' '+r.bookTotal:''):'—';
    return '<tr>'
      +'<td style="color:#cbd5e1;font-weight:700">'+r.awayTeam+' @ '+r.homeTeam+'</td>'
      +'<td style="color:#e2e8f0">'+(r.pickTeam||'—')+'</td>'
      +'<td style="color:#94a3b8">'+actual+'</td>'
      +'<td>'+_nhlGpResult(r,'mlResult')+'</td>'
      +'<td style="color:#cbd5e1">'+totalPick+'</td>'
      +'<td style="color:#94a3b8">'+(r.actualTotal!=null?r.actualTotal:'—')+'</td>'
      +'<td>'+_nhlGpResult(r,'ouResult')+'</td>'
      +'</tr>';
  }).join('');
  return '<div style="background:linear-gradient(135deg,rgba(14,116,144,.16),rgba(6,95,70,.12));border:1px solid rgba(45,212,191,.28);border-radius:12px;padding:14px 16px;margin-bottom:14px">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">'
    +'<div><div style="font-weight:900;font-size:1rem;color:#5eead4">🔮 Game Predictor Record</div>'
    +'<div style="font-size:.72rem;color:#94a3b8;margin-top:3px">Read-only model results · no stake tracking</div></div>'
    +'<div style="font-size:.7rem;color:#64748b">'+detail.length+' game'+(detail.length===1?'':'s')+'</div></div>'
    +'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:12px">'
      +'<div style="background:#0f172a;border-radius:8px;padding:8px 12px;color:#e2e8f0;font-size:.8rem"><b style="color:#5eead4">ML</b> '+(gp.mlWins||0)+'-'+(gp.mlLosses||0)+'-'+(gp.mlPushes||0)+' <span style="color:#94a3b8">('+mlRate+')</span></div>'
      +'<div style="background:#0f172a;border-radius:8px;padding:8px 12px;color:#e2e8f0;font-size:.8rem"><b style="color:#fbbf24">Projected Total</b> '+(gp.ouWins||0)+'-'+(gp.ouLosses||0)+'-'+(gp.ouPushes||0)+' <span style="color:#94a3b8">('+ouRate+')</span></div>'
    +'</div>'
    +(mlTotal||ouTotal?'<div style="font-size:.7rem;color:#64748b;margin-top:7px">ML W-L-P · total O/U W-L-P · hit rates exclude pushes</div>':'')
    +(rows?'<div style="overflow-x:auto;margin-top:12px"><table class="nhl-trk-tbl"><thead><tr><th>Matchup</th><th>ML Pick</th><th>Score</th><th>ML</th><th>Total Pick</th><th>Total</th><th>O/U</th></tr></thead><tbody>'+rows+'</tbody></table></div>':'')
    +'</div>';
}
document.addEventListener('DOMContentLoaded',function(){
  var dp=document.getElementById('nhlTrkDate');
  var op=document.getElementById('nhlOvfDate');
  var sp=document.getElementById('nhlSpDate');
  var today=new Date().toISOString().slice(0,10);
  if(dp){dp.value=today;
    dp.addEventListener('change',function(){_nhlRecordDateChanged('nhlTrkDate');});}
  if(op){op.value=today;
    op.addEventListener('change',function(){_nhlRecordDateChanged('nhlOvfDate');});}
  if(sp){sp.value=today;
    sp.addEventListener('change',function(){_nhlSpDayName();renderNhlSpecialDay();});}
  _nhlTrkDayName();_nhlOvfDayName();_nhlSpDayName();
  loadNhlTrackRecord();
  var top=document.getElementById('nhl-btn-top'),bot=document.getElementById('nhl-btn-bot');
  function _sc(){var y=window.pageYOffset||document.documentElement.scrollTop;
    var atBot=(y+window.innerHeight)>=document.body.scrollHeight-50;
    if(top) top.style.display=y>400?'block':'none';
    if(bot) bot.style.display=!atBot?'block':'none';}
  window.addEventListener('scroll',_sc,{passive:true});_sc();
});

// ── Standalone NHL Game Predictor Record ─────────────────────────────────────
var _nhlGpRecordData=null,_nhlGpRecordTab='daily',_nhlGpRecordDate='';
function _nhlGpToday(){return new Date().toISOString().slice(0,10);}
function _nhlGpDateLabel(dt){
  var a=(dt||'').split('-');
  if(a.length!==3) return dt||'';
  var m=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return m[parseInt(a[1],10)-1]+' '+parseInt(a[2],10)+', '+a[0];
}
function _nhlGpPct(w,l){var n=(w||0)+(l||0);return n?Math.round((w||0)/n*100)+'%':'—';}
function _nhlGpWlp(w,l,p){
  return '<span style="color:#4ade80;font-weight:900">'+(w||0)+'W</span>'
    +'<span style="color:#64748b"> - </span><span style="color:#f87171;font-weight:900">'+(l||0)+'L</span>'
    +((p||0)?'<span style="color:#facc15;font-weight:800"> - '+p+'P</span>':'');
}
function _nhlGpResult(v){
  if(!v) return '<span style="color:#64748b;font-weight:800">PENDING</span>';
  var c=v==='WIN'?'#4ade80':(v==='LOSS'?'#f87171':'#facc15');
  return '<span style="color:'+c+';font-weight:900">'+v+'</span>';
}
function _nhlGpInitialFromTrack(d){
  var out=[];
  (d&&d.dates||[]).forEach(function(day){
    var gp=day.gp;if(!gp) return;
    var games=(gp.detail||[]).map(function(p){
      return {
        home_abbr:p.homeTeam||'',away_abbr:p.awayTeam||'',
        home_team:p.homeFull||p.homeTeam||'',away_team:p.awayFull||p.awayTeam||'',
        pick:p.pickTeam||'',pick_prob:p.pickProb,win_prob_home:p.winProbHome!=null?p.winProbHome*100:null,
        proj_home:p.projHome,proj_away:p.projAway,proj_total:p.projTotal,
        book_total:p.bookTotal,total_pick:p.ouRec,home_ml:p.homeMl,away_ml:p.awayMl,
        ml_book:p.mlBook||'',total_book:p.totBook||'',actual_home:p.actualHome,actual_away:p.actualAway,
        actual_total:p.actualTotal,team_result:p.mlResult,ou_result:p.ouResult,start_time:p.startTime||''
      };
    });
    out.push({date:day.date,locked:true,games:games,
      team_w:gp.mlWins||0,team_l:gp.mlLosses||0,team_p:gp.mlPushes||0,
      ou_w:gp.ouWins||0,ou_l:gp.ouLosses||0,ou_p:gp.ouPushes||0});
  });
  out.sort(function(a,b){return b.date.localeCompare(a.date);});
  return {daily:out};
}
function _nhlGpDay(dt){
  var a=(_nhlGpRecordData&&_nhlGpRecordData.daily)||[];
  for(var i=0;i<a.length;i++) if(a[i].date===dt) return a[i];
  return null;
}
function _nhlGpAggregate(days){
  var a={tw:0,tl:0,tp:0,ow:0,ol:0,op:0,homeW:0,homeL:0,awayW:0,awayL:0,overW:0,overL:0,underW:0,underL:0};
  (days||[]).forEach(function(day){(day.games||[]).forEach(function(g){
    if(g.team_result==='WIN') a.tw++; else if(g.team_result==='LOSS') a.tl++; else if(g.team_result==='PUSH') a.tp++;
    if(g.ou_result==='WIN') a.ow++; else if(g.ou_result==='LOSS') a.ol++; else if(g.ou_result==='PUSH') a.op++;
    if(g.team_result==='WIN'||g.team_result==='LOSS'){
      if(g.pick===g.home_abbr) a.homeW+=(g.team_result==='WIN'?1:0),a.homeL+=(g.team_result==='LOSS'?1:0);
      else a.awayW+=(g.team_result==='WIN'?1:0),a.awayL+=(g.team_result==='LOSS'?1:0);
    }
    if(g.ou_result==='WIN'||g.ou_result==='LOSS'){
      if(g.total_pick==='OVER') a.overW+=(g.ou_result==='WIN'?1:0),a.overL+=(g.ou_result==='LOSS'?1:0);
      if(g.total_pick==='UNDER') a.underW+=(g.ou_result==='WIN'?1:0),a.underL+=(g.ou_result==='LOSS'?1:0);
    }
  });});
  return a;
}
function _nhlGpSummary(a){
  return '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:14px">'
    +'<div style="flex:1;min-width:220px;background:#071f17;border:1px solid rgba(52,211,153,.28);border-radius:12px;padding:14px;text-align:center">'
    +'<div style="color:#6ee7b7;font-size:.66rem;font-weight:900;letter-spacing:.08em">TEAM WIN / LOSS</div>'
    +'<div style="font-size:1.15rem;margin-top:5px">'+_nhlGpWlp(a.tw,a.tl,a.tp)+'</div>'
    +'<div style="color:#94a3b8;font-size:.74rem;margin-top:4px">'+_nhlGpPct(a.tw,a.tl)+' hit rate</div></div>'
    +'<div style="flex:1;min-width:220px;background:#07192b;border:1px solid rgba(56,189,248,.28);border-radius:12px;padding:14px;text-align:center">'
    +'<div style="color:#7dd3fc;font-size:.66rem;font-weight:900;letter-spacing:.08em">PROJECTED TOTAL O/U</div>'
    +'<div style="font-size:1.15rem;margin-top:5px">'+_nhlGpWlp(a.ow,a.ol,a.op)+'</div>'
    +'<div style="color:#94a3b8;font-size:.74rem;margin-top:4px">'+_nhlGpPct(a.ow,a.ol)+' hit rate</div></div></div>'
    +'<div style="font-size:.64rem;color:#64748b;margin:-5px 0 12px">Accuracy only · no stake or ROI accounting · pushes excluded from hit rates</div>';
}
function _nhlGpSplits(a){
  function tile(label,w,l){return '<div style="flex:1;min-width:125px;background:#0a1120;border:1px solid #1e293b;border-radius:9px;padding:9px;text-align:center"><div style="color:#94a3b8;font-size:.58rem;font-weight:800">'+label+'</div><div style="margin-top:4px">'+_nhlGpWlp(w,l,0)+'</div><div style="color:#64748b;font-size:.64rem;margin-top:2px">'+_nhlGpPct(w,l)+'</div></div>';}
  return '<div style="font-size:.64rem;color:#a78bfa;font-weight:900;letter-spacing:.08em;margin:10px 0 6px">PREDICTOR SPLITS</div>'
    +'<div style="display:flex;gap:8px;flex-wrap:wrap">'+tile('HOME TEAM PICKS',a.homeW,a.homeL)+tile('AWAY TEAM PICKS',a.awayW,a.awayL)+tile('OVER CALLS',a.overW,a.overL)+tile('UNDER CALLS',a.underW,a.underL)+'</div>';
}
function _nhlGpGamesHtml(games){
  if(!games||!games.length) return '<p style="color:#64748b;padding:18px;text-align:center">No saved games for this date.</p>';
  var rows=games.map(function(g){
    var final=(g.actual_home!=null&&g.actual_away!=null)
      ?g.away_abbr+' '+g.actual_away+' &mdash; '+g.home_abbr+' '+g.actual_home:'Pending';
    var mlOdds=g.pick===g.home_abbr?g.home_ml:g.away_ml;
    var total=(g.total_pick&&g.book_total!=null)?g.total_pick+' '+g.book_total:'No total call';
    return '<tr><td style="color:#e2e8f0;font-weight:800">'+g.away_abbr+' @ '+g.home_abbr+'</td>'
      +'<td style="color:#94a3b8">'+final+'</td><td style="color:#cbd5e1">'+(g.pick||'—')
      +(g.pick_prob!=null?' ('+g.pick_prob+'%)':'')+(mlOdds!=null?' · '+(mlOdds>0?'+':'')+mlOdds:'')+'</td>'
      +'<td>'+_nhlGpResult(g.team_result)+'</td><td style="color:#cbd5e1">'+total+'</td>'
      +'<td style="color:#94a3b8">'+(g.actual_total!=null?g.actual_total:'—')+'</td><td>'+_nhlGpResult(g.ou_result)+'</td></tr>';
  }).join('');
  return '<div style="overflow-x:auto"><table class="nhl-trk-tbl"><thead><tr><th>Matchup</th><th>Final</th><th>ML Pick</th><th>ML</th><th>Total Call</th><th>Total</th><th>O/U</th></tr></thead><tbody>'+rows+'</tbody></table></div>';
}
function _nhlGpToolbar(){
  var tabs=[['daily','Daily'],['weekly','Last 7 Days'],['monthly','This Month'],['alltime','All Time']];
  var h='<div style="display:flex;gap:7px;flex-wrap:wrap;margin-bottom:12px">';
  tabs.forEach(function(t){var on=_nhlGpRecordTab===t[0];h+='<button onclick="_nhlGpSetTab(&#39;'+t[0]+'&#39;)" style="background:'+(on?'#065f46':'#1e293b')+';color:#fff;border:none;border-radius:8px;padding:8px 13px;font-weight:800;font-size:.76rem;cursor:pointer">'+t[1]+'</button>';});
  return h+'</div>';
}
function _nhlGpRender(){
  var head=document.getElementById('nhlGpRecordHead'),body=document.getElementById('nhlGpRecordBody');if(!head||!body)return;
  var daily=(_nhlGpRecordData&&_nhlGpRecordData.daily)||[];
  head.innerHTML=_nhlGpToolbar();
  if(!daily.length){body.innerHTML='<p style="color:#94a3b8;padding:18px">No Game Predictor record has been graded yet. A day is banked after its games are final.</p>';return;}
  if(_nhlGpRecordTab==='daily'){
    var dt=_nhlGpRecordDate||daily[0].date;
    var day=_nhlGpDay(dt);
    var picker='<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:13px"><label style="color:#94a3b8;font-size:.78rem;font-weight:800">DATE <input type="date" value="'+dt+'" max="'+_nhlGpToday()+'" onchange="_nhlGpGotoDay(this.value)" style="margin-left:6px;background:#020617;border:1px solid #334155;color:#fff;border-radius:7px;padding:7px 9px"></label><button onclick="_nhlGpLoad(true)" style="background:#0e7490;color:#fff;border:none;border-radius:7px;padding:7px 12px;font-size:.76rem;font-weight:800;cursor:pointer">&#8635; Grade &amp; Get Results</button></div>';
    if(!day){body.innerHTML=picker+'<p style="color:#94a3b8;padding:14px">No saved Game Predictor slate for '+_nhlGpDateLabel(dt)+'.</p>';return;}
    var a=_nhlGpAggregate([day]);
    body.innerHTML=picker+_nhlGpSummary(a)+_nhlGpGamesHtml(day.games);
    return;
  }
  var today=_nhlGpToday(),days=[];
  if(_nhlGpRecordTab==='weekly'){
    var start=new Date(today+'T12:00:00');start.setDate(start.getDate()-6);
    days=daily.filter(function(d){var x=new Date(d.date+'T12:00:00');return x>=start&&x<=new Date(today+'T23:59:59');});
  }else if(_nhlGpRecordTab==='monthly'){
    var ym=today.slice(0,7);days=daily.filter(function(d){return d.date.slice(0,7)===ym;});
  }else days=daily.slice();
  if(!days.length){body.innerHTML='<p style="color:#94a3b8;padding:18px">No graded Game Predictor days in this range yet.</p>';return;}
  var all=[];days.forEach(function(d){all=all.concat(d.games||[]);});
  var ag=_nhlGpAggregate(days);
  var dayRows=days.map(function(d){var da=_nhlGpAggregate([d]);return '<tr><td>'+_nhlGpDateLabel(d.date)+'</td><td>'+_nhlGpWlp(da.tw,da.tl,da.tp)+'</td><td>'+_nhlGpPct(da.tw,da.tl)+'</td><td>'+_nhlGpWlp(da.ow,da.ol,da.op)+'</td><td>'+_nhlGpPct(da.ow,da.ol)+'</td><td>'+((d.games||[]).length)+'</td></tr>';}).join('');
  body.innerHTML=_nhlGpSummary(ag)+_nhlGpSplits(ag)+'<div style="font-size:.64rem;color:#a78bfa;font-weight:900;letter-spacing:.08em;margin:16px 0 6px">DAILY RESULTS</div><div style="overflow-x:auto"><table class="nhl-trk-tbl"><thead><tr><th>Date</th><th>Team ML</th><th>Rate</th><th>Totals O/U</th><th>Rate</th><th>Games</th></tr></thead><tbody>'+dayRows+'</tbody></table></div>';
}
function _nhlGpSetTab(tab){_nhlGpRecordTab=tab;_nhlGpRender();}
function _nhlGpGotoDay(dt){_nhlGpRecordDate=dt;_nhlGpRecordTab='daily';_nhlGpRender();}
async function _nhlGpLoad(manual){
  var body=document.getElementById('nhlGpRecordBody');if(body)body.innerHTML='<p style="color:#94a3b8;padding:20px">Loading Game Predictor record...</p>';
  try{
    if(!manual&&window.__INITIAL_GP_RECORD__){_nhlGpRecordData=window.__INITIAL_GP_RECORD__;}
    else if(!manual&&window.__INITIAL_TRACK_RECORD__){_nhlGpRecordData=_nhlGpInitialFromTrack(window.__INITIAL_TRACK_RECORD__);}
    else{
      var qs=manual?'?grade=true&date_str='+encodeURIComponent(_nhlGpRecordDate||_nhlGpToday()):'';
      var r=await fetch('/api/gp-record'+qs);if(!r.ok)throw new Error(await r.text()||('HTTP '+r.status));
      _nhlGpRecordData=await r.json();
    }
    var ds=(_nhlGpRecordData.daily||[]);if(!_nhlGpRecordDate&&ds.length)_nhlGpRecordDate=ds[0].date;_nhlGpRender();
  }catch(e){if(body)body.innerHTML='<p style="color:#f87171;padding:18px">Could not load GP Record: '+(e.message||'request failed')+'</p>';}
}
function openNhlGPRecord(){
  var sec=document.getElementById('nhl-gp-record-section');if(!sec)return;
  sec.style.display='block';sec.scrollIntoView({behavior:'smooth',block:'start'});
  if(_nhlGpRecordData)_nhlGpRender();else _nhlGpLoad(false);
}
</script>
<!-- Standalone NHL Game Predictor Record -->
<div id="nhl-gp-record-section" style="display:none;max-width:960px;margin:0 auto;padding:0 16px 24px">
  <div class="card" style="padding:20px 22px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;gap:10px;flex-wrap:wrap">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.4rem;font-weight:700;color:#fff">&#128302; NHL Game Predictor Record</h2>
      <button onclick="document.getElementById('nhl-gp-record-section').style.display='none'" style="background:#1e293b;border:none;color:#94a3b8;border-radius:8px;padding:7px 11px;font-size:.9rem;cursor:pointer">&#215;</button>
    </div>
    <p style="color:#64748b;font-size:.74rem;margin:0 0 14px">Permanent pre-game model accuracy. Team winner and projected total are tracked separately; no stake or ROI accounting.</p>
    <div id="nhlGpRecordHead"></div>
    <div id="nhlGpRecordBody"><p style="color:#94a3b8;padding:18px">Open GP Record to load results.</p></div>
  </div>
</div>
<!-- NHL Track Record — always visible below picks -->
<div id="nhl-track-section" style="max-width:960px;margin:0 auto 0;padding:0 16px 40px">
  <div class="card" style="padding:20px 22px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.4rem;font-weight:700;color:#fff">&#127942; NHL Track Record</h2>
    </div>
    <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:14px">
      <label style="color:#94a3b8;font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em">Date</label>
      <input type="date" id="nhlTrkDate" style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;padding:7px 11px;color:#e2e8f0;font-size:.85rem;outline:none">
      <span id="nhlTrkDayName" style="color:#34d399;font-weight:700;font-size:.9rem"></span>
      <button onclick="loadNhlTrackRecord(true)" style="background:#065f46;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">&#8635; Grade &amp; Get Results</button>
      <button id="nhlTrkBtnCat" onclick="nhlTrkSetTab('cat')" style="background:#065f46;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">By Category</button>
      <button id="nhlTrkBtnList" onclick="nhlTrkSetTab('list')" style="background:#1e293b;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">Full List</button>
    </div>
    <div id="nhlTrkSummary"></div>
    <div id="nhlTrkBody"></div>
  </div>
</div>
<!-- NHL Overflow Track Record — ranks 11-20 only -->
<div id="nhl-overflow-section" style="max-width:960px;margin:0 auto 0;padding:0 16px 40px">
  <div class="card" style="padding:20px 22px;border-color:rgba(245,158,11,.28)">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;gap:10px;flex-wrap:wrap">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.4rem;font-weight:700;color:#fff">&#11088; NHL Overflow Track Record</h2>
    </div>
    <p style="color:#94a3b8;font-size:.74rem;margin:0 0 14px">Ranks 11–20 from every existing NHL player-prop category and side. These plays are kept out of the main Top 10 Track Record.</p>
    <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:14px">
      <label style="color:#94a3b8;font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em">Date</label>
      <input type="date" id="nhlOvfDate" style="background:#0f172a;border:1px solid #78350f;border-radius:8px;padding:7px 11px;color:#e2e8f0;font-size:.85rem;outline:none">
      <span id="nhlOvfDayName" style="color:#fbbf24;font-weight:700;font-size:.9rem"></span>
      <button onclick="loadNhlTrackRecord(true)" style="background:#b45309;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">&#8635; Grade &amp; Get Results</button>
      <button id="nhlOvfBtnCat" onclick="nhlOvfSetTab('cat')" style="background:#b45309;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">By Category</button>
      <button id="nhlOvfBtnList" onclick="nhlOvfSetTab('list')" style="background:#1e293b;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">Full List</button>
    </div>
    <div id="nhlOvfSummary"></div>
    <div id="nhlOvfBody"></div>
  </div>
</div>
<!-- Special — Best Plays Track Record -->
<div id="nhl-special-record-section" style="max-width:960px;margin:0 auto 0;padding:0 16px 40px">
  <div class="card" style="padding:20px 22px;border-color:rgba(250,204,21,.28)">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;gap:10px;flex-wrap:wrap">
      <h2 style="font-family:'Playfair Display',serif;font-size:1.4rem;font-weight:700;color:#fff">&#11088; Special Plays Track Record</h2>
    </div>
    <p style="color:#94a3b8;font-size:.74rem;margin:0 0 14px">Permanent results for every play displayed in Special — Best Plays. Kept separate from the main, Overflow, and Locks totals.</p>
    <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:14px">
      <label style="color:#94a3b8;font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em">Date</label>
      <input type="date" id="nhlSpDate" style="background:#0f172a;border:1px solid #854d0e;border-radius:8px;padding:7px 11px;color:#e2e8f0;font-size:.85rem;outline:none">
      <span id="nhlSpDayName" style="color:#fde047;font-weight:700;font-size:.9rem"></span>
      <button onclick="loadNhlSpecialRecord(true)" style="background:#854d0e;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">&#8635; Grade &amp; Get Results</button>
      <button onclick="_nhlSpSetAllTime()" style="background:#422006;color:#fde68a;border:1px solid #854d0e;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">All Time</button>
      <button id="nhlSpBtnCat" onclick="_nhlSpSetTab('cat')" style="background:#854d0e;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">By Category</button>
      <button id="nhlSpBtnList" onclick="_nhlSpSetTab('list')" style="background:#1e293b;color:#fff;border:none;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:.82rem">Full List</button>
    </div>
    <div id="nhlSpSummary"></div>
    <div id="nhlSpBody"></div>
  </div>
</div>
<!-- Scroll buttons -->
<button id="nhl-btn-top" onclick="window.scrollTo({top:0,behavior:'smooth'})" title="Back to top" style="position:fixed;bottom:76px;right:22px;z-index:9999;display:none;width:48px;height:48px;border-radius:50%;border:none;cursor:pointer;background:#34d399;color:#0a0a0a;font-size:1.4rem;font-weight:900;box-shadow:0 4px 14px rgba(0,0,0,.45);line-height:1">&#8593;</button>
<button id="nhl-btn-bot" onclick="window.scrollTo({top:document.body.scrollHeight,behavior:'smooth'})" title="Scroll to bottom" style="position:fixed;bottom:22px;right:22px;z-index:9999;display:none;width:48px;height:48px;border-radius:50%;border:none;cursor:pointer;background:#0ea5e9;color:#0a0a0a;font-size:1.4rem;font-weight:900;box-shadow:0 4px 14px rgba(0,0,0,.45);line-height:1">&#8595;</button>
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
_NHL_CAT_ORDER = ["Shots on Goal", "Points", "Power Play Points", "Assists", "Goalie Saves"]
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
        if stat_key == "PP_POINTS":
            v = g.get("powerPlayPoints")
            return float(v) if v is not None else None
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


# ─────────────────────────────────────────────────────────────────────────────
#  NHL Track Record — automated daily grading, Supabase-backed
# ─────────────────────────────────────────────────────────────────────────────
_NHL_SB_URL_RAW = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
_NHL_SB_URL = (f"https://{_NHL_SB_URL_RAW}.supabase.co"
               if _NHL_SB_URL_RAW and not _NHL_SB_URL_RAW.startswith("http")
               else _NHL_SB_URL_RAW)
_NHL_SB_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

def _nhl_sb_get(table, params=None):
    if not _NHL_SB_URL or not _NHL_SB_KEY:
        return []
    try:
        r = httpx.get(f"{_NHL_SB_URL}/rest/v1/{table}",
                      headers={"apikey": _NHL_SB_KEY, "Authorization": f"Bearer {_NHL_SB_KEY}"},
                      params=params or {}, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[nhl_sb_get] {e}")
    return []

def _nhl_sb_get_all(table, params=None, page_size=1000):
    """Read a complete Supabase REST result set without a permanent row cap."""
    out, offset = [], 0
    while True:
        page_params = dict(params or {})
        page_params["limit"] = str(page_size)
        page_params["offset"] = str(offset)
        page = _nhl_sb_get(table, page_params)
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out

def _nhl_sb_upsert(table, rows, on_conflict=None):
    if not _NHL_SB_URL or not _NHL_SB_KEY or not rows:
        return False
    try:
        h = {"apikey": _NHL_SB_KEY, "Authorization": f"Bearer {_NHL_SB_KEY}",
             "Content-Type": "application/json",
             "Prefer": "resolution=merge-duplicates,return=minimal"}
        url = f"{_NHL_SB_URL}/rest/v1/{table}"
        if on_conflict:
            url += f"?on_conflict={on_conflict}"
        r = httpx.post(url, headers=h, json=rows, timeout=20)
        return r.status_code in (200, 201, 204)
    except Exception as e:
        print(f"[nhl_sb_upsert] {e}")
    return False

_NHL_TRK_APP   = "nhl"
_NHL_TRK_STAKE = 20.0
_NHL_TRK_TOP   = 10
_NHL_SNAP_CAT  = "__picks__"
_NHL_GP_CAT    = "__gp__"
_NHL_SPECIAL_SNAP_CAT = "__special_picks__"
_NHL_SPECIAL_LEDGER_CAT = "__special_ledger__"
_NHL_SPECIAL_DETAIL_CAT = "__special_detail__"

# (result_key, category_label, stat_key, side, is_overflow)
_NHL_TRK_LISTS = [
    ("picks",           "Shots on Goal", "SHOTS",  "OVER",  False),
    ("rest",            "Shots on Goal", "SHOTS",  "OVER",  True),
    ("shotUnders",      "Shots on Goal", "SHOTS",  "UNDER", False),
    ("shotUndersRest",  "Shots on Goal", "SHOTS",  "UNDER", True),
    ("ptsPicks",        "Points",        "POINTS", "OVER",  False),
    ("ptsRest",         "Points",        "POINTS", "OVER",  True),
    ("ptsUnders",       "Points",        "POINTS", "UNDER", False),
    ("ptsUndersRest",   "Points",        "POINTS", "UNDER", True),
    ("ppPicks",         "Power Play Points", "PP_POINTS", "OVER",  False),
    ("ppRest",          "Power Play Points", "PP_POINTS", "OVER",  True),
    ("ppUnders",        "Power Play Points", "PP_POINTS", "UNDER", False),
    ("ppUndersRest",    "Power Play Points", "PP_POINTS", "UNDER", True),
    ("astPicks",        "Assists",       "ASSISTS","OVER",  False),
    ("astRest",         "Assists",       "ASSISTS","OVER",  True),
    ("astUnders",       "Assists",       "ASSISTS","UNDER", False),
    ("astUndersRest",   "Assists",       "ASSISTS","UNDER", True),
    ("goalPicks",       "Goals",         "GOALS",  "OVER",  False),
    ("goalRest",        "Goals",         "GOALS",  "OVER",  True),
    ("goalUnders",      "Goals",         "GOALS",  "UNDER", False),
    ("goalUndersRest",  "Goals",         "GOALS",  "UNDER", True),
    ("savesPicks",      "Goalie Saves",  "SAVES",  "OVER",  False),
    ("savesRest",       "Goalie Saves",  "SAVES",  "OVER",  True),
    ("savesUnders",     "Goalie Saves",  "SAVES",  "UNDER", False),
    ("savesUndersRest", "Goalie Saves",  "SAVES",  "UNDER", True),
]

_NHL_SPECIAL_LISTS = [
    ("Shot Plays", "picks", "Shots on Goal", "SHOTS"),
    ("Point Plays", "ptsPicks", "Points", "POINTS"),
    ("Assist Plays", "astPicks", "Assists", "ASSISTS"),
    ("Goal Plays", "goalPicks", "Goals", "GOALS"),
    ("Save Plays", "savesPicks", "Goalie Saves", "SAVES"),
]

def _nhl_save_picks_snapshot(date_str: str, result: dict):
    """Freeze all pick lists to Supabase so they survive redeploys and can be graded."""
    flat = []
    for (rkey, cat, sk, side, ovf) in _NHL_TRK_LISTS:
        rank_start = _NHL_TRK_TOP + 1 if ovf else 1
        for rank, p in enumerate(result.get(rkey) or [], rank_start):
            odds = p.get("realOdds") if side == "OVER" else p.get("realUnderOdds")
            line = p.get("realLine") or p.get("line") or p.get("dispLine")
            flat.append({
                "name": p.get("name", ""), "pid": p.get("pid"),
                "team": p.get("team", ""), "category": cat,
                "stat_key": sk, "side": side, "line": line,
                "odds": odds,
                # Every market uses dispScore for the live lock board. Keep
                # that score in the frozen snapshot so grading can create the
                # duplicate 80-100% Locks record alongside the base category.
                "score": p.get("score") or p.get("dispScore") or p.get("ptsScore") or 0,
                "rank": rank, "is_overflow": ovf,
            })
    if flat:
        ok = _nhl_sb_upsert(
            "mpa_track_ledger",
            [{"app": _NHL_TRK_APP, "date": date_str, "category": _NHL_SNAP_CAT,
              "side": "ALL", "wins": 0, "losses": 0, "locked": False, "detail": flat}],
            "app,date,category,side")
        print(f"[nhl_track] snapshot {'saved' if ok else 'FAILED'}: "
              f"{len(flat)} picks -> {date_str}")

    # Special — Best Plays is a curated display, not another market board.
    # Freeze only the rows the UI actually surfaces (top eight per column).
    # A player appearing in Shot Plays and Point Plays is intentionally kept
    # twice because those are separate Special categories.
    special_flat = []
    special_seen = set()
    for special_cat, rkey, source_cat, stat_key in _NHL_SPECIAL_LISTS:
        for rank, p in enumerate((result.get(rkey) or [])[:8], 1):
            side = "OVER"
            line = p.get("realLine") or p.get("line") or p.get("dispLine")
            dedupe_key = (p.get("pid"), special_cat, side, str(line))
            if dedupe_key in special_seen:
                continue
            special_seen.add(dedupe_key)
            special_flat.append({
                "name": p.get("name", ""), "pid": p.get("pid"),
                "team": p.get("team", ""), "opponent": p.get("opponent", ""),
                "category": special_cat, "source_category": source_cat,
                "stat_key": stat_key, "side": side, "line": line,
                "odds": p.get("realOdds"), "line_source": p.get("lineSource", ""),
                "score": p.get("dispScore") or p.get("ptsScore") or p.get("score") or 0,
                "rank": rank, "is_overflow": False,
            })
    existing_special = _nhl_load_special_snapshot(date_str)
    if special_flat and not existing_special:
        ok = _nhl_sb_upsert(
            "mpa_track_ledger",
            [{"app": _NHL_TRK_APP, "date": date_str,
              "category": _NHL_SPECIAL_SNAP_CAT, "side": "ALL",
              "wins": 0, "losses": 0, "locked": False, "detail": special_flat}],
            "app,date,category,side")
        print(f"[nhl_track] special snapshot {'saved' if ok else 'FAILED'}: "
              f"{len(special_flat)} plays -> {date_str}")
    elif existing_special:
        print(f"[nhl_track] special snapshot preserved: "
              f"{len(existing_special)} plays -> {date_str}")


def _nhl_save_gp_snapshot(date_str: str, result: dict):
    """Freeze the day's game-predictor calls in the shared ledger.

    GP is deliberately separate from player picks: it has no stake, odds, or
    player stat key.  Do not overwrite a non-empty pre-game snapshot on a
    later re-run, otherwise a live re-run could change the forecast we grade.
    """
    captured_at = datetime.utcnow().isoformat() + "Z"
    predictions = [
        p for p in (result.get("game_predictions") or [])
        if _nhl_gp_is_pre_game(p)
    ]
    if not predictions:
        print(f"[nhl_track] GP snapshot skipped: no pre-game calls for {date_str}")
        return
    existing = _nhl_sb_get("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": f"eq.{_NHL_GP_CAT}",
        "side": "eq.ALL", "date": f"eq.{date_str}",
        "select": "detail,locked", "limit": "1"})
    if existing and isinstance(existing[0].get("detail"), list) and existing[0]["detail"]:
        return
    detail = []
    for p in predictions:
        detail.append({
            "gameId": p.get("gameId"),
            "homeTeam": p.get("homeTeam", ""), "awayTeam": p.get("awayTeam", ""),
            "homeFull": p.get("homeFull", ""), "awayFull": p.get("awayFull", ""),
            "startTime": p.get("startTime", ""),
            "capturedAt": captured_at,
            "pickTeam": p.get("pickTeam", ""), "pickProb": p.get("pickProb"),
            "winProbHome": p.get("winProbHome"),
            "projHome": p.get("projHome"), "projAway": p.get("projAway"),
            "projTotal": p.get("projTotal"),
            "bookTotal": p.get("bookTotal"), "ouRec": p.get("ouRec"),
            "homeMl": p.get("homeMl"), "awayMl": p.get("awayMl"),
            "totBook": p.get("totBook", ""), "mlBook": p.get("mlBook", ""),
        })
    ok = _nhl_sb_upsert("mpa_track_ledger", [{
        "app": _NHL_TRK_APP, "date": date_str, "category": _NHL_GP_CAT,
        "side": "ALL", "wins": 0, "losses": 0, "locked": False,
        "detail": detail,
    }], "app,date,category,side")
    print(f"[nhl_track] GP snapshot {'saved' if ok else 'FAILED'}: "
          f"{len(detail)} games -> {date_str}")


def _nhl_gp_is_pre_game(prediction: dict, now: Optional[datetime] = None) -> bool:
    """Only track forecasts captured before their scheduled puck drop."""
    start = prediction.get("startTime") or ""
    try:
        start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
        start_at = start_at.replace(tzinfo=None)
        return (now or datetime.utcnow()) < start_at
    except (TypeError, ValueError):
        # A missing start time cannot prove this is a pre-game prediction, so
        # omit it rather than contaminate the accuracy record with hindsight.
        return False


def _nhl_load_gp_snapshots() -> list:
    """Return all GP snapshot rows, including dates not graded yet."""
    return _nhl_sb_get("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": f"eq.{_NHL_GP_CAT}",
        "side": "eq.ALL", "select": "date,detail,locked",
        "limit": "500"}) or []


def _nhl_gp_schedule_scores(date_str: str) -> tuple:
    """Fetch one date of NHL schedule scores.

    The schedule endpoint exposes the final score on each homeTeam/awayTeam
    object.  Keep both game-id and matchup indexes because older saved GP
    rows may predate the gameId field.
    """
    by_id, by_matchup = {}, {}
    try:
        r = httpx.get(f"{NHL_API}/schedule/{date_str}",
                      follow_redirects=True, timeout=20)
        if r.status_code != 200:
            return by_id, by_matchup
        for day in r.json().get("gameWeek", []):
            if day.get("date") != date_str:
                continue
            for g in day.get("games", []):
                home = g.get("homeTeam") or {}
                away = g.get("awayTeam") or {}
                record = {
                    "gameId": g.get("id"),
                    "homeTeam": home.get("abbrev", ""),
                    "awayTeam": away.get("abbrev", ""),
                    "homeScore": home.get("score"),
                    "awayScore": away.get("score"),
                    "gameState": g.get("gameState", ""),
                }
                if record["gameId"] is not None:
                    by_id[str(record["gameId"])] = record
                if record["homeTeam"] and record["awayTeam"]:
                    by_matchup[(record["awayTeam"], record["homeTeam"])] = record
    except Exception as e:
        print(f"[nhl_track] schedule score fetch failed {date_str}: {e}")
    return by_id, by_matchup


def _nhl_grade_gp_date(date_str: str, snapshot: list) -> dict:
    """Grade saved NHL GP calls without odds or stake accounting."""
    by_id, by_matchup = _nhl_gp_schedule_scores(date_str)
    final_states = {"OFF", "FINAL"}
    graded, all_found, all_final = [], True, True
    for p in snapshot:
        game = by_id.get(str(p.get("gameId"))) if p.get("gameId") is not None else None
        if game is None:
            game = by_matchup.get((p.get("awayTeam", ""), p.get("homeTeam", "")))
        row = dict(p)
        row.update({
            "mlResult": None, "ouResult": None, "actualHome": None,
            "actualAway": None, "actualTotal": None,
            "gameState": game.get("gameState", "") if game else "",
        })
        if not game:
            all_found = False
            all_final = False
            graded.append(row)
            continue
        if game.get("gameState") not in final_states:
            # Scores can appear during LIVE/CRIT games. They are informational
            # only until final; never surface them as a graded result.
            all_final = False
            graded.append(row)
            continue
        try:
            hs, aws = game.get("homeScore"), game.get("awayScore")
            if hs is None or aws is None:
                raise ValueError("score unavailable")
            hs, aws = float(hs), float(aws)
            row["actualHome"] = int(hs) if hs.is_integer() else hs
            row["actualAway"] = int(aws) if aws.is_integer() else aws
            row["actualTotal"] = row["actualHome"] + row["actualAway"]
            pick = p.get("pickTeam")
            if pick == p.get("homeTeam"):
                row["mlResult"] = "WIN" if hs > aws else ("PUSH" if hs == aws else "LOSS")
            elif pick == p.get("awayTeam"):
                row["mlResult"] = "WIN" if aws > hs else ("PUSH" if hs == aws else "LOSS")
            total_pick = p.get("ouRec")
            total_line = p.get("bookTotal")
            if total_pick in ("OVER", "UNDER") and total_line is not None:
                line = float(total_line)
                actual_total = hs + aws
                if actual_total == line:
                    row["ouResult"] = "PUSH"
                elif total_pick == "OVER":
                    row["ouResult"] = "WIN" if actual_total > line else "LOSS"
                else:
                    row["ouResult"] = "WIN" if actual_total < line else "LOSS"
        except (TypeError, ValueError):
            all_final = False
        graded.append(row)
    return {"detail": graded, "any_game": bool(snapshot),
            "all_found": all_found, "all_final": all_final}


def _nhl_gp_summary(detail: list) -> dict:
    """Build read-only ML and projected-total counts for the UI."""
    ml = {k: sum(1 for r in detail if r.get("mlResult") == k)
          for k in ("WIN", "LOSS", "PUSH")}
    ou = {k: sum(1 for r in detail if r.get("ouResult") == k)
          for k in ("WIN", "LOSS", "PUSH")}
    ml_decided = ml["WIN"] + ml["LOSS"]
    ou_decided = ou["WIN"] + ou["LOSS"]
    return {
        "mlWins": ml["WIN"], "mlLosses": ml["LOSS"], "mlPushes": ml["PUSH"],
        "mlRate": round(ml["WIN"] / ml_decided * 100, 1) if ml_decided else None,
        "ouWins": ou["WIN"], "ouLosses": ou["LOSS"], "ouPushes": ou["PUSH"],
        "ouRate": round(ou["WIN"] / ou_decided * 100, 1) if ou_decided else None,
        "detail": detail,
    }


def _nhl_gp_record_payload() -> dict:
    """Build the standalone GP Record payload from the existing GP ledger."""
    daily = []
    for saved in _nhl_load_gp_snapshots():
        date_str = saved.get("date")
        if not date_str:
            continue
        detail = saved.get("detail") or []
        games = []
        for p in detail:
            home = p.get("homeTeam", "")
            away = p.get("awayTeam", "")
            pick = p.get("pickTeam", "")
            games.append({
                "game_id": p.get("gameId"),
                "home_abbr": home, "away_abbr": away,
                "home_team": p.get("homeFull") or home,
                "away_team": p.get("awayFull") or away,
                "pick": pick,
                "pick_prob": p.get("pickProb"),
                "win_prob_home": (p.get("winProbHome") * 100
                                  if p.get("winProbHome") is not None
                                  else None),
                "proj_home": p.get("projHome"),
                "proj_away": p.get("projAway"),
                "proj_total": p.get("projTotal"),
                "book_total": p.get("bookTotal"),
                "total_pick": p.get("ouRec"),
                "home_ml": p.get("homeMl"),
                "away_ml": p.get("awayMl"),
                "ml_book": p.get("mlBook", ""),
                "total_book": p.get("totBook", ""),
                "actual_home": p.get("actualHome"),
                "actual_away": p.get("actualAway"),
                "actual_total": p.get("actualTotal"),
                "team_result": p.get("mlResult"),
                "ou_result": p.get("ouResult"),
                "start_time": p.get("startTime", ""),
            })
        summary = _nhl_gp_summary(detail)
        daily.append({
            "date": date_str,
            "locked": bool(saved.get("locked")),
            "games": games,
            "team_w": summary["mlWins"], "team_l": summary["mlLosses"],
            "team_p": summary["mlPushes"],
            "ou_w": summary["ouWins"], "ou_l": summary["ouLosses"],
            "ou_p": summary["ouPushes"],
        })
    daily.sort(key=lambda x: x["date"], reverse=True)
    return {"daily": daily, "updated_at": datetime.utcnow().isoformat() + "Z"}


def _nhl_update_gp_ledger(include_date: str = ""):
    """Grade unlocked historical GP snapshots and lock completed dates."""
    today = date.today().isoformat()
    rows = _nhl_load_gp_snapshots()
    for saved in rows:
        d = saved.get("date")
        if not d or d > today or (d == today and d != include_date) or saved.get("locked"):
            continue
        snapshot = saved.get("detail") or []
        if not isinstance(snapshot, list) or not snapshot:
            continue
        try:
            graded = _nhl_grade_gp_date(d, snapshot)
            if not graded.get("any_game"):
                continue
            _nhl_sb_upsert("mpa_track_ledger", [{
                "app": _NHL_TRK_APP, "date": d, "category": _NHL_GP_CAT,
                "side": "ALL",
                "wins": sum(1 for r in graded["detail"] if r.get("mlResult") == "WIN"),
                "losses": sum(1 for r in graded["detail"] if r.get("mlResult") == "LOSS"),
                "locked": bool(graded.get("all_final")),
                "locked_at": (datetime.utcnow().isoformat() + "Z"
                              if graded.get("all_final") else None),
                "detail": graded["detail"],
            }], "app,date,category,side")
        except Exception as e:
            print(f"[nhl_track] GP grade failed {d}: {e}")

def _nhl_load_picks_snapshot(date_str: str) -> list:
    rows = _nhl_sb_get("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": f"eq.{_NHL_SNAP_CAT}",
        "side": "eq.ALL", "date": f"eq.{date_str}", "select": "detail", "limit": "1"})
    if rows:
        d = rows[0].get("detail") or []
        return d if isinstance(d, list) else []
    return []

def _nhl_load_special_snapshot(date_str: str) -> list:
    rows = _nhl_sb_get("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": f"eq.{_NHL_SPECIAL_SNAP_CAT}",
        "side": "eq.ALL", "date": f"eq.{date_str}", "select": "detail", "limit": "1"})
    if rows:
        d = rows[0].get("detail") or []
        return d if isinstance(d, list) else []
    return []

def _nhl_list_snap_dates() -> list:
    rows = _nhl_sb_get("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": f"eq.{_NHL_SNAP_CAT}",
        "side": "eq.ALL", "select": "date", "limit": "365"})
    return sorted({r["date"] for r in rows if r.get("date")})

def _nhl_list_special_snap_dates() -> list:
    rows = _nhl_sb_get_all("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": f"eq.{_NHL_SPECIAL_SNAP_CAT}",
        "side": "eq.ALL", "select": "date", "order": "date.desc"})
    return sorted({r["date"] for r in rows if r.get("date")})

def _nhl_grade_pick_stat(g: dict, stat_key: str):
    """Like _nhl_extract_stat but also handles GOALS for anytime-scorer market."""
    if stat_key == "GOALS":
        v = g.get("goals")
        return float(v) if v is not None else None
    return _nhl_extract_stat(g, stat_key)

def _nhl_grade_date(date_str: str, snap: list) -> dict:
    from collections import defaultdict
    need: dict = {}
    for p in snap:
        pid = p.get("pid")
        if pid:
            need.setdefault(str(pid), []).append(p)
    pid_games: dict = {}
    all_found = True
    for pid in need:
        merged: dict = {}
        for s in _nhl_seasons_for(date_str):
            try:
                games, ok = _nhl_player_games_raw(pid, s)
                merged.update(games)
                if not ok:
                    all_found = False
            except Exception as e:
                print(f"[nhl_track] grade {pid}/{s}: {e}")
                all_found = False
        pid_games[pid] = merged
    any_game = bool(pid_games)
    by_group: dict = defaultdict(list)
    for p in snap:
        pid = str(p.get("pid") or "")
        if pid in pid_games:
            by_group[(p.get("category","?"), (p.get("side") or "OVER").upper(),
                      bool(p.get("is_overflow")))].append(p)
    main_rows: list = []
    ovf_rows: list = []
    lock_rows: list = []
    for (cat, side, is_ovf), ps in by_group.items():
        ps.sort(key=lambda x: float(x.get("score") or 0), reverse=True)
        for relative_rank, p in enumerate(ps, 1):
            pid = str(p.get("pid") or "")
            g = pid_games.get(pid, {}).get(date_str)
            sk = p.get("stat_key")
            line_raw = p.get("line")
            odds = p.get("odds")
            result_val = actual = profit = None
            if g and line_raw is not None and sk:
                actual = _nhl_grade_pick_stat(g, sk)
                if actual is not None:
                    try:
                        fl = float(line_raw)
                        if actual == fl:
                            result_val = "PUSH"
                        elif side == "OVER":
                            result_val = "WIN" if actual > fl else "LOSS"
                        else:
                            result_val = "WIN" if actual < fl else "LOSS"
                        if result_val and odds is not None:
                            profit = round(_nhl_american_profit(odds, _NHL_TRK_STAKE, result_val), 2)
                    except Exception:
                        pass
            try:
                rank = int(p.get("rank"))
            except (TypeError, ValueError):
                rank = relative_rank
            if is_ovf and rank <= _NHL_TRK_TOP:
                # Snapshots saved before the dedicated overflow tracker used
                # relative ranks 1-10 for their ranks 11-20 lists.
                rank += _NHL_TRK_TOP
            row = {"name": p.get("name",""), "team": p.get("team",""),
                   "category": cat, "side": side, "stat_key": sk,
                   "line": line_raw, "odds": odds, "rank": rank,
                   "result": result_val, "actual": actual, "profit": profit}
            row["is_overflow"] = bool(is_ovf)
            if not is_ovf:
                main_rows.append(row)
            else:
                ovf_rows.append(row)
            # Cross-market 80-100% Locks category
            lock_score = float(p.get("dispScore") or p.get("ptsScore") or p.get("score") or 0)
            if lock_score >= 80:
                lock_rows.append({**row, "category": "80-100% Locks",
                                  "is_overflow": bool(is_ovf)})
    return {"any_game": any_game, "all_final": all_found,
            "main": main_rows, "overflow": ovf_rows, "locks": lock_rows}

def _nhl_grade_special_date(date_str: str, snap: list) -> dict:
    """Grade the separately persisted rows shown in Special — Best Plays."""
    need = {}
    for p in snap:
        pid = p.get("pid")
        if pid:
            need.setdefault(str(pid), True)
    pid_games = {}
    all_found = True
    for pid in need:
        merged = {}
        for s in _nhl_seasons_for(date_str):
            try:
                games, ok = _nhl_player_games_raw(pid, s)
                merged.update(games)
                if not ok:
                    all_found = False
            except Exception as e:
                print(f"[nhl_special] grade {pid}/{s}: {e}")
                all_found = False
        pid_games[pid] = merged

    detail = []
    try:
        old_enough_to_void = (
            date.today() - date.fromisoformat(date_str)
        ).days >= 2
    except Exception:
        old_enough_to_void = False
    for p in snap:
        pid = str(p.get("pid") or "")
        g = pid_games.get(pid, {}).get(date_str)
        stat_key = p.get("stat_key")
        line_raw = p.get("line")
        odds = p.get("odds")
        result_val = actual = profit = void_reason = None
        if g and line_raw is not None and stat_key:
            actual = _nhl_grade_pick_stat(g, stat_key)
            if actual is not None:
                try:
                    line = float(line_raw)
                    if actual == line:
                        result_val = "PUSH"
                    elif (p.get("side") or "OVER").upper() == "OVER":
                        result_val = "WIN" if actual > line else "LOSS"
                    else:
                        result_val = "WIN" if actual < line else "LOSS"
                    if odds not in (None, "", "0") and result_val:
                        profit = round(
                            _nhl_american_profit(odds, _NHL_TRK_STAKE, result_val), 2)
                except (TypeError, ValueError):
                    pass
            elif old_enough_to_void and all_found:
                result_val = "VOID"
                void_reason = "The saved stat could not be graded"
        elif old_enough_to_void and all_found:
            result_val = "VOID"
            if not g:
                void_reason = "Player did not appear in the game log"
            elif line_raw is None:
                void_reason = "No frozen line was available"
            else:
                void_reason = "The saved stat could not be graded"
        detail.append({
            "name": p.get("name", ""), "pid": p.get("pid"),
            "team": p.get("team", ""), "opponent": p.get("opponent", ""),
            "category": p.get("category", "Special Plays"),
            "source_category": p.get("source_category", ""),
            "side": (p.get("side") or "OVER").upper(),
            "stat_key": stat_key, "line": line_raw, "odds": odds,
            "line_source": p.get("line_source", ""), "rank": p.get("rank"),
            "result": result_val, "actual": actual, "profit": profit,
            "void_reason": void_reason,
        })
    terminal = all(
        row.get("result") in ("WIN", "LOSS", "PUSH", "VOID")
        for row in detail
    )
    return {"any_game": bool(pid_games), "all_final": all_found and terminal,
            "detail": detail}

def _nhl_special_aggregate_graded(detail: list) -> dict:
    agg = {}
    for row in detail:
        cat = row.get("category", "Special Plays")
        side = (row.get("side") or "OVER").upper()
        e = agg.setdefault(cat, {}).setdefault(
            side, {"wins": 0, "losses": 0, "pushes": 0, "voids": 0, "pending": 0})
        result = row.get("result")
        if result == "WIN":
            e["wins"] += 1
        elif result == "LOSS":
            e["losses"] += 1
        elif result == "PUSH":
            e["pushes"] += 1
        elif result == "VOID":
            e["voids"] += 1
        else:
            e["pending"] += 1
    return agg

def _nhl_aggregate_graded(graded: dict) -> dict:
    agg: dict = {}
    for row in graded.get("main", []) + graded.get("overflow", []) + graded.get("locks", []):
        if row.get("result") not in ("WIN","LOSS"):
            continue
        rec = agg.setdefault(row["category"], {}).setdefault(row.get("side","OVER"), [0,0])
        if row["result"] == "WIN":
            rec[0] += 1
        else:
            rec[1] += 1
    # Sentinel lets the next deployment re-grade older locked snapshots once
    # so their stored overflow metadata is reflected in the split record.
    agg["__ovf_v1__"] = {"ALL": [0, 0]}
    return agg

def _nhl_detail_graded(graded: dict) -> list:
    out = []
    for row in graded.get("main", []) + graded.get("overflow", []) + graded.get("locks", []):
        if row.get("result") not in ("WIN","LOSS"):
            continue
        out.append({k: row.get(k) for k in
                    ("name","team","category","side","stat_key","line","odds","rank",
                     "result","actual","profit","is_overflow")})
    return out

_NHL_TRK_LOCK = _bt_th.Lock()

def _nhl_update_track_ledger(include_date: str = ""):
    from datetime import date as _d
    today = _d.today().isoformat()
    with _NHL_TRK_LOCK:
        locked_rows = _nhl_sb_get("mpa_track_ledger", {
            "app": f"eq.{_NHL_TRK_APP}", "category": "eq.__ledger__",
            "locked": "eq.true", "select": "date,detail", "limit": "500"}) or []
        locked = {
            r["date"] for r in locked_rows
            if r.get("date") and isinstance(r.get("detail"), dict)
            and "__ovf_v1__" in r.get("detail", {})
        }
        special_locked_rows = _nhl_sb_get_all("mpa_track_ledger", {
            "app": f"eq.{_NHL_TRK_APP}",
            "category": f"eq.{_NHL_SPECIAL_LEDGER_CAT}",
            "locked": "eq.true", "select": "date", "order": "date.desc"}) or []
        special_locked = {
            r["date"] for r in special_locked_rows if r.get("date")
        }
        upserts = []
        for d in _nhl_list_snap_dates():
            if d >= today or d in locked:
                continue
            snap = _nhl_load_picks_snapshot(d)
            if not snap:
                continue
            try:
                graded = _nhl_grade_date(d, snap)
            except Exception as e:
                print(f"[nhl_track] grade failed {d}: {e}")
                continue
            if not graded.get("any_game"):
                continue
            try:
                from datetime import date as _dd
                old_enough = (_dd.today() - _dd.fromisoformat(d)).days >= 2
            except Exception:
                old_enough = False
            if not graded.get("all_final") and not old_enough:
                continue
            agg = _nhl_aggregate_graded(graded)
            det = _nhl_detail_graded(graded)
            upserts += [
                {"app":_NHL_TRK_APP,"date":d,"category":"__ledger__","side":"ALL",
                 "wins":0,"losses":0,"locked":True,"detail":agg},
                {"app":_NHL_TRK_APP,"date":d,"category":"__detail__","side":"ALL",
                 "wins":0,"losses":0,"locked":True,"detail":det},
            ]
        # Special Plays have their own snapshot, grade, summary, and lock
        # namespace. They never enter the regular or Overflow aggregates.
        for d in _nhl_list_special_snap_dates():
            if d >= today or d in special_locked:
                continue
            snap = _nhl_load_special_snapshot(d)
            if not snap:
                continue
            try:
                graded_special = _nhl_grade_special_date(d, snap)
            except Exception as e:
                print(f"[nhl_special] grade failed {d}: {e}")
                continue
            if not graded_special.get("any_game"):
                continue
            try:
                from datetime import date as _dd
                old_enough = (_dd.today() - _dd.fromisoformat(d)).days >= 2
            except Exception:
                old_enough = False
            # Date age may turn a successfully fetched DNP into VOID inside
            # the grader, but it must never override an upstream fetch failure.
            # Only terminal rows may become immutable.
            if not graded_special.get("all_final"):
                continue
            special_detail = graded_special.get("detail") or []
            upserts += [
                {"app": _NHL_TRK_APP, "date": d,
                 "category": _NHL_SPECIAL_LEDGER_CAT, "side": "ALL",
                 "wins": 0, "losses": 0, "locked": True,
                 "detail": _nhl_special_aggregate_graded(special_detail)},
                {"app": _NHL_TRK_APP, "date": d,
                 "category": _NHL_SPECIAL_DETAIL_CAT, "side": "ALL",
                 "wins": 0, "losses": 0, "locked": True,
                 "detail": special_detail},
            ]
            print(f"[nhl_special] locked {len(special_detail)} plays -> {d}")
        if upserts:
            for i in range(0, len(upserts), 10):
                _nhl_sb_upsert("mpa_track_ledger", upserts[i:i+10], "app,date,category,side")
            print(f"[nhl_track] wrote {len(upserts)//2} regular/special date records")
    # GP uses its own row and read-only summary; grade it independently from
    # player-pick stake accounting.
    try:
        _nhl_update_gp_ledger(include_date)
    except Exception as e:
        print(f"[nhl_track] GP background error: {e}")

def _nhl_trk_bg():
    try:
        _nhl_update_track_ledger()
    except Exception as e:
        print(f"[nhl_track] bg error: {e}")

_bt_th.Thread(target=_nhl_trk_bg, daemon=True).start()


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

@app.get("/api/gp-record")
async def nhl_gp_record(grade: bool = False, date_str: str = ""):
    """Standalone, read-only NHL Game Predictor record."""
    if grade:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _nhl_update_gp_ledger, date_str)
    else:
        _bt_th.Thread(target=_nhl_update_gp_ledger, args=(date_str,), daemon=True).start()
    return JSONResponse(_nhl_gp_record_payload())

@app.get("/api/track-record")
async def nhl_track_record(grade: bool = False, date_str: str = ""):
    """NHL player-pick and read-only Game Predictor records by date."""
    if grade:
        # The Track Record button is a deliberate manual grade step.  Run it
        # off the event loop so users receive the newly graded GP data in this
        # same response instead of needing to click twice.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _nhl_update_track_ledger, date_str)
    else:
        _bt_th.Thread(target=_nhl_trk_bg, daemon=True).start()
    return JSONResponse(_nhl_track_record_payload())

@app.get("/api/special-track-record")
async def nhl_special_track_record(grade: bool = False, date_str: str = ""):
    """Permanent record for only the plays displayed in Special — Best Plays."""
    if grade:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _nhl_update_track_ledger, date_str)
    else:
        _bt_th.Thread(target=_nhl_trk_bg, daemon=True).start()
    return JSONResponse(_nhl_special_track_record_payload())


def _nhl_special_track_record_payload() -> dict:
    det_rows = _nhl_sb_get_all("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}",
        "category": f"eq.{_NHL_SPECIAL_DETAIL_CAT}",
        "locked": "eq.true", "select": "date,detail", "order": "date.desc"}) or []
    detail_by_date = {
        r["date"]: (r.get("detail") or []) for r in det_rows if r.get("date")
    }
    snap_rows = _nhl_sb_get_all("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}",
        "category": f"eq.{_NHL_SPECIAL_SNAP_CAT}", "side": "eq.ALL",
        "select": "date,detail", "order": "date.desc"}) or []
    snapshot_by_date = {
        r["date"]: (r.get("detail") or []) for r in snap_rows if r.get("date")
    }
    dates = sorted(set(detail_by_date) | set(snapshot_by_date), reverse=True)
    cat_order = [x[0] for x in _NHL_SPECIAL_LISTS]
    result = []
    for d in dates:
        det = [dict(r or {}) for r in detail_by_date.get(d, [])]
        locked = d in detail_by_date
        if not det and snapshot_by_date.get(d):
            for p in snapshot_by_date[d]:
                det.append({
                    "name": p.get("name", ""), "pid": p.get("pid"),
                    "team": p.get("team", ""), "opponent": p.get("opponent", ""),
                    "category": p.get("category", "Special Plays"),
                    "source_category": p.get("source_category", ""),
                    "side": p.get("side", "OVER"), "stat_key": p.get("stat_key"),
                    "line": p.get("line"), "odds": p.get("odds"),
                    "line_source": p.get("line_source", ""), "rank": p.get("rank"),
                    "result": None, "actual": None, "profit": None,
                })
        decided = [r for r in det if r.get("result") in ("WIN", "LOSS")]
        wins = sum(1 for r in decided if r.get("result") == "WIN")
        losses = len(decided) - wins
        pushes = sum(1 for r in det if r.get("result") == "PUSH")
        voids = sum(1 for r in det if r.get("result") == "VOID")
        pending = len(det) - len(decided) - pushes - voids
        priced = [
            r for r in decided
            if r.get("odds") is not None
            and str(r.get("odds")).strip() not in ("", "0")
        ]
        net_pl = round(sum(r.get("profit") or 0 for r in priced), 2)
        staked = len(priced) * _NHL_TRK_STAKE
        by_cat = []
        for cat in cat_order:
            rows = [r for r in det if r.get("category") == cat]
            cat_decided = [r for r in rows if r.get("result") in ("WIN", "LOSS")]
            cw = sum(1 for r in cat_decided if r.get("result") == "WIN")
            cl = len(cat_decided) - cw
            cp = sum(
                (r.get("profit") or 0) for r in cat_decided
                if r.get("odds") is not None
                and str(r.get("odds")).strip() not in ("", "0")
            )
            cpriced = sum(
                1 for r in cat_decided
                if r.get("odds") is not None
                and str(r.get("odds")).strip() not in ("", "0")
            )
            by_cat.append({
                "category": cat, "wins": cw, "losses": cl,
                "pushes": sum(1 for r in rows if r.get("result") == "PUSH"),
                "voids": sum(1 for r in rows if r.get("result") == "VOID"),
                "pending": sum(1 for r in rows if not r.get("result")),
                "net_pl": round(cp, 2),
                "roi": round(cp / (cpriced * _NHL_TRK_STAKE) * 100, 1)
                if cpriced else None,
                "rate": round(cw / len(cat_decided) * 100, 1)
                if cat_decided else None,
            })
        result.append({
            "date": d, "locked": locked, "wins": wins, "losses": losses,
            "pushes": pushes, "voids": voids, "pending": pending,
            "net_pl": net_pl,
            "roi": round(net_pl / staked * 100, 1) if staked else None,
            "by_cat": by_cat, "detail": det,
        })
    return {"dates": result, "stake": _NHL_TRK_STAKE}


def _nhl_track_record_payload() -> dict:
    """Build the read-only payload used by both the API and hub snapshots."""
    det_rows = _nhl_sb_get("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": "eq.__detail__",
        "locked": "eq.true", "select": "date,detail", "limit": "365"})
    detail_by_date = {r["date"]: (r.get("detail") or []) for r in (det_rows or [])}
    snap_rows = _nhl_sb_get("mpa_track_ledger", {
        "app": f"eq.{_NHL_TRK_APP}", "category": f"eq.{_NHL_SNAP_CAT}",
        "side": "eq.ALL", "select": "date,detail", "limit": "365"})
    snapshot_by_date = {r["date"]: (r.get("detail") or []) for r in (snap_rows or [])}
    gp_by_date = {
        r["date"]: (r.get("detail") or [])
        for r in _nhl_load_gp_snapshots() if r.get("date")
    }
    dates = sorted(set(detail_by_date) | set(snapshot_by_date) | set(gp_by_date), reverse=True)

    def _same_line(a, b):
        try:
            return float(a) == float(b)
        except (TypeError, ValueError):
            return str(a or "") == str(b or "")

    def _split_detail(rows, snapshot):
        """Classify old detail rows from their frozen snapshot metadata.

        New detail rows carry is_overflow directly. Older locked detail rows
        predate that field, but their same-day snapshot still retains the
        source category, rank, side, line, and player identity.
        """
        out = []
        snapshot = snapshot or []
        for original in rows or []:
            row = dict(original or {})
            if "is_overflow" in row:
                row["is_overflow"] = bool(row.get("is_overflow"))
                out.append(row)
                continue
            candidates = []
            for saved in snapshot:
                if row.get("name") and saved.get("name") != row.get("name"):
                    continue
                if row.get("team") and saved.get("team") != row.get("team"):
                    continue
                if str(row.get("side") or "OVER").upper() != str(saved.get("side") or "OVER").upper():
                    continue
                if row.get("stat_key") and saved.get("stat_key") != row.get("stat_key"):
                    continue
                if row.get("rank") is not None and saved.get("rank") is not None:
                    try:
                        if int(row.get("rank")) != int(saved.get("rank")):
                            continue
                    except (TypeError, ValueError):
                        continue
                if row.get("line") is not None and saved.get("line") is not None:
                    if not _same_line(row.get("line"), saved.get("line")):
                        continue
                if row.get("category") not in ("NHL Overflow", "80-100% Locks"):
                    if saved.get("category") != row.get("category"):
                        continue
                candidates.append(saved)
            if candidates:
                flags = {bool(saved.get("is_overflow")) for saved in candidates}
                row["is_overflow"] = True if flags == {True} else False
                if row.get("category") == "NHL Overflow":
                    row["category"] = candidates[0].get("category") or row["category"]
            else:
                # A legacy overflow label is still safer than allowing it
                # back into the main record when its snapshot is incomplete.
                row["is_overflow"] = row.get("category") == "NHL Overflow"
            out.append(row)
        return out

    result = []
    for d in dates:
        det = _split_detail(detail_by_date.get(d, []), snapshot_by_date.get(d, []))
        if not det and snapshot_by_date.get(d):
            # A saved player snapshot is the source of truth for an ungraded
            # slate. Keep its line and odds, mark the outcome pending, and
            # mirror qualifying rows into Locks exactly as the grader will.
            det = []
            for p in snapshot_by_date[d]:
                pending_row = {
                    "name": p.get("name", ""), "team": p.get("team", ""),
                    "category": p.get("category", "?"), "side": p.get("side", "OVER"),
                    "stat_key": p.get("stat_key"), "line": p.get("line"),
                    "odds": p.get("odds"), "rank": p.get("rank"),
                    "is_overflow": bool(p.get("is_overflow")),
                    "result": None, "actual": None, "profit": None,
                }
                det.append(pending_row)
                try:
                    lock_score = float(p.get("score") or 0)
                except (TypeError, ValueError):
                    lock_score = 0.0
                if lock_score >= 80:
                    det.append({**pending_row, "category": "80-100% Locks"})
        main_det = [row for row in det if not row.get("is_overflow")]
        overflow_det = [row for row in det if row.get("is_overflow")]
        gp = _nhl_gp_summary(gp_by_date[d]) if d in gp_by_date else None
        decided = [r for r in main_det if r.get("result") in ("WIN","LOSS")]
        wins = sum(1 for r in decided if r["result"] == "WIN")
        losses = len(decided) - wins
        priced = [r for r in decided
                  if r.get("odds") is not None and str(r.get("odds")).strip() not in ("", "0")]
        net_pl = round(sum(r.get("profit") or 0 for r in priced), 2)
        staked = len(priced) * _NHL_TRK_STAKE
        roi = round(net_pl / staked * 100, 1) if staked else None
        cats: dict = {}
        for r in decided:
            cat = r.get("category","?")
            e = cats.setdefault(cat, {"wins":0,"losses":0,"pl":0.0,"staked":0.0})
            if r["result"] == "WIN": e["wins"] += 1
            else: e["losses"] += 1
            if r.get("odds") is not None and str(r.get("odds")).strip() not in ("", "0"):
                e["pl"] = round(e["pl"] + (r.get("profit") or 0), 2)
                e["staked"] += _NHL_TRK_STAKE
        by_cat = []
        for cat, e in cats.items():
            total = e["wins"] + e["losses"]
            by_cat.append({"category": cat, "wins": e["wins"], "losses": e["losses"],
                           "net_pl": e["pl"],
                           "roi": round(e["pl"]/e["staked"]*100,1) if e["staked"] else None,
                           "rate": round(e["wins"]/total*100,1) if total else None})
        by_cat.sort(key=lambda x: (x.get("roi") or -999), reverse=True)
        overflow_decided = [
            r for r in overflow_det if r.get("result") in ("WIN", "LOSS")
        ]
        overflow_wins = sum(1 for r in overflow_decided if r["result"] == "WIN")
        overflow_losses = len(overflow_decided) - overflow_wins
        result.append({"date":d,"wins":wins,"losses":losses,
                       "net_pl":net_pl,"roi":roi,"by_cat":by_cat,
                       "detail":main_det, "overflow_detail":overflow_det,
                       "overflow_wins":overflow_wins,
                       "overflow_losses":overflow_losses,
                       "gp":gp})
    special_payload = _nhl_special_track_record_payload()
    return {"dates": result, "stake": _NHL_TRK_STAKE,
            "special_dates": special_payload.get("dates", [])}


_DASHBOARD_FALLBACK_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NHL Money Shots</title></head>
<body style="margin:0;min-height:100vh;display:grid;place-items:center;background:#0f0f0f;color:#f3f4f6;font-family:system-ui,sans-serif;text-align:center;padding:24px">
<main><h1 style="color:#f59e0b">NHL Money Shots is reconnecting</h1>
<p>The live dashboard is being restored. Please refresh in a moment.</p></main>
</body></html>"""


def _render_dashboard_shell(is_admin: bool) -> str:
    """Render the member page without allowing a template error to become 500."""
    try:
        page = HTML
        if not isinstance(page, str) or not page.strip():
            raise RuntimeError("NHL dashboard HTML is empty or invalid")
        js_flag = "true" if is_admin else "false"
        rendered = page.replace(
            "</head>",
            f"<script>window.IS_ADMIN = {js_flag};</script></head>",
            1,
        )
        if rendered == page:
            raise RuntimeError("NHL dashboard HTML is missing its closing head tag")
        return rendered
    except Exception:
        logger.exception("NHL dashboard HTML render failed; serving recovery page")
        return _DASHBOARD_FALLBACK_HTML


@app.on_event("startup")
async def validate_dashboard_shell():
    """Emit a startup signal that makes template regressions obvious in logs."""
    rendered = _render_dashboard_shell(False)
    if rendered == _DASHBOARD_FALLBACK_HTML:
        logger.error("NHL dashboard startup validation failed")
    else:
        logger.info("NHL dashboard startup validation passed (%d bytes)", len(rendered))


@app.get("/", response_class=HTMLResponse)
async def index(admin: str = "", token: str = ""):
    is_admin = (bool(admin) and admin == os.environ.get("INTERNAL_API_TOKEN", "__none__")) or _is_admin_token(token)
    return HTMLResponse(_render_dashboard_shell(is_admin))

@app.get("/api/picks")
async def api_picks(request: Request, target_date: str = None, token: str = "",
                    simulate: bool = False):
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _verify_hub_token(tok):
        raise HTTPException(status_code=401, detail="Subscription required — please log in via moneypicksarena.com")
    if simulate:
        try:
            replay_date = date.fromisoformat(target_date or "")
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="A valid completed date is required")
        if replay_date >= date.today():
            raise HTTPException(status_code=400, detail="Historical replays are available only for completed dates")
        target_date = replay_date.isoformat()
    key = target_date or date.today().isoformat()
    cached = None if simulate else _cache_get("nhl", key)
    if cached:
        return JSONResponse(cached)
    result = await run_picks(target_date, simulate=simulate)
    if "error" not in result and not simulate:
        _cache_set("nhl", key, result)
    return JSONResponse(result)


@app.get("/api/historical-track-replay")
async def nhl_historical_track_replay(request: Request, date_str: str,
                                      token: str = ""):
    """Return a view-only historical replay; never save it to the ledger."""
    tok = token or request.headers.get("Authorization", "").replace("Bearer ", "").strip()
    if not _verify_hub_token(tok) or not _is_admin_token(tok):
        raise HTTPException(status_code=403, detail="Admin only")
    try:
        replay_date = date.fromisoformat(date_str)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="A valid historical date is required")
    if replay_date >= date.today():
        raise HTTPException(status_code=400, detail="Choose a completed NHL date")
    result = await run_picks(date_str, simulate=True)
    if result.get("error") or result.get("no_games"):
        raise HTTPException(status_code=400, detail=result.get("error") or result.get("message") or
                            "The historical replay could not be generated")
    return JSONResponse(_nhl_historical_replay_payload(result))


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
        # Daily cron runs also settle the prior dates' player and GP records.
        # Keep the blocking NHL/Supabase calls out of FastAPI's event loop.
        await asyncio.get_running_loop().run_in_executor(None, _nhl_update_track_ledger)
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

@app.get("/api/odds-debug")
async def api_odds_debug(dt: str = None, admin: str = ""):
    """Admin-gated diagnostic: surfaces exactly what The Odds API returns for
    NHL player props so silent fetch failures (bad key/plan/markets/region) are
    visible without Render logs. Visit ?admin=INTERNAL_API_TOKEN&dt=YYYY-MM-DD."""
    if admin != os.environ.get("INTERNAL_API_TOKEN", "__none__"):
        return JSONResponse({"error": "admin token required"}, status_code=403)
    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        return JSONResponse({"error": "ODDS_API_KEY not set on this service"})
    target_date = dt or date.today().isoformat()
    tomorrow = (date.fromisoformat(target_date) + timedelta(days=1)).isoformat()
    out = {"target_date": target_date, "tomorrow": tomorrow,
           "key_tail": api_key[-4:], "sport_keys": {}}
    MKTS = ("player_shots_on_goal,player_points,player_assists,"
            "player_total_saves,player_goal_scorer")
    try:
        async with httpx.AsyncClient(timeout=25) as c:
            for sport_key in ["icehockey_nhl", "icehockey_nhl_championship"]:
                sk = {"events_status": None, "events_total": 0,
                      "events_in_window": 0, "sample_events": [], "per_event": []}
                r = await c.get(f"{ODDS_API}/sports/{sport_key}/events",
                                params={"apiKey": api_key, "dateFormat": "iso"})
                sk["events_status"] = r.status_code
                sk["quota_remaining"] = r.headers.get("x-requests-remaining")
                if r.status_code == 200:
                    evs = r.json()
                    sk["events_total"] = len(evs)
                    win = [e for e in evs
                           if e.get("commence_time", "")[:10] in (target_date, tomorrow)]
                    sk["events_in_window"] = len(win)
                    sk["sample_events"] = [
                        {"home": e.get("home_team"), "away": e.get("away_team"),
                         "commence": e.get("commence_time")} for e in evs[:6]]
                    for ev in win[:3]:
                        r2 = await c.get(
                            f"{ODDS_API}/sports/{sport_key}/events/{ev['id']}/odds",
                            params={"apiKey": api_key, "regions": "us,us2,eu,ca",
                                    "markets": MKTS, "oddsFormat": "american"})
                        info = {"event": f"{ev.get('away_team')} @ {ev.get('home_team')}",
                                "odds_status": r2.status_code,
                                "body_head": (r2.text[:300] if r2.status_code != 200 else None),
                                "books": [], "market_counts": {}, "sample_players": []}
                        if r2.status_code == 200:
                            for book in r2.json().get("bookmakers", []):
                                info["books"].append(book.get("key"))
                                for mkt in book.get("markets", []):
                                    mk = mkt.get("key")
                                    n = len(mkt.get("outcomes", []))
                                    info["market_counts"][mk] = info["market_counts"].get(mk, 0) + n
                                    for oc in mkt.get("outcomes", [])[:3]:
                                        nm = oc.get("description", "")
                                        if nm and nm not in info["sample_players"]:
                                            info["sample_players"].append(nm)
                        sk["per_event"].append(info)
                else:
                    sk["body_head"] = r.text[:300]
                out["sport_keys"][sport_key] = sk
    except Exception as e:
        out["exception"] = f"{type(e).__name__}: {e}"
    return JSONResponse(out)

@app.get("/api/progress")
async def api_progress():
    return JSONResponse(_progress)

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
