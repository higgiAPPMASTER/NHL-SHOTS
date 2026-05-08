#!/usr/bin/env python3
"""
NHL Shots on Goal Picks — main.py
Step 1 : The Odds API  (player shots on goal lines ≥ 1.5)
Step 2 : StatMuse      (career H/A shots vs today's opponent ≥ 80%)
Step 3 : StatMuse      (last 10 H/A games shots ≥ 80%)
Step 4 : Rank & top 10
Deployed on Render (FastAPI + Playwright + curl_cffi)
"""

import os, hmac, asyncio, re, unicodedata
from datetime import date, datetime
from typing import List, Dict, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession as CFSession
from playwright.async_api import async_playwright
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, JSONResponse

# ─────────────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────────────

app      = FastAPI(title="NHL Shots Picks")
security = HTTPBasic()

NHL_API      = "https://api-web.nhle.com/v1"
NHL_STATS    = "https://api.nhle.com/stats/rest/en"
STATMUSE_NHL = "https://www.statmuse.com/nhl/ask"

MIN_SPG  = 1.5   # shots/game season average to qualify
MIN_GP   = 10    # minimum games played for valid average

MIN_GAMES   = 3      # min games required for hit-rate calc
HIT_THRESH  = 80.0   # % hit rate to qualify
TOP_N       = 10     # final picks count
SEM_NHL     = 8      # concurrent NHL API calls
SEM_SM      = 4      # concurrent StatMuse scrapes (be polite)

# StatMuse team slugs  (abbrev → URL slug)
TEAM_SLUGS: Dict[str, str] = {
    "ANA": "anaheim-ducks",       "BOS": "boston-bruins",
    "BUF": "buffalo-sabres",      "CGY": "calgary-flames",
    "CAR": "carolina-hurricanes", "CHI": "chicago-blackhawks",
    "COL": "colorado-avalanche",  "CBJ": "columbus-blue-jackets",
    "DAL": "dallas-stars",        "DET": "detroit-red-wings",
    "EDM": "edmonton-oilers",     "FLA": "florida-panthers",
    "LAK": "los-angeles-kings",   "MIN": "minnesota-wild",
    "MTL": "montreal-canadiens",  "NSH": "nashville-predators",
    "NJD": "new-jersey-devils",   "NYI": "new-york-islanders",
    "NYR": "new-york-rangers",    "OTT": "ottawa-senators",
    "PHI": "philadelphia-flyers", "PIT": "pittsburgh-penguins",
    "SJS": "san-jose-sharks",     "STL": "st-louis-blues",
    "TBL": "tampa-bay-lightning", "TOR": "toronto-maple-leafs",
    "UTA": "utah-hockey-club",    "VAN": "vancouver-canucks",
    "VGK": "vegas-golden-knights","WSH": "washington-capitals",
    "WPG": "winnipeg-jets",       "SEA": "seattle-kraken",
}

# ─────────────────────────────────────────────────────────────────────────────
#  HTTP Basic Auth
# ─────────────────────────────────────────────────────────────────────────────

def _parse_users() -> Dict[str, str]:
    raw = os.environ.get("USERS", "admin:changeme")
    out = {}
    for entry in raw.split(","):
        parts = entry.strip().split(":", 1)
        if len(parts) == 2:
            out[parts[0].strip()] = parts[1].strip()
    return out

def verify_user(creds: HTTPBasicCredentials = Depends(security)) -> str:
    users = _parse_users()
    stored = users.get(creds.username, "")
    if not stored or not hmac.compare_digest(creds.password.encode(), stored.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return creds.username

# ─────────────────────────────────────────────────────────────────────────────
#  StatMuse Session (Playwright login — cached for the process lifetime)
# ─────────────────────────────────────────────────────────────────────────────

_sm_cookie: Optional[str] = None
_sm_lock   = asyncio.Lock()

async def get_sm_cookie() -> str:
    global _sm_cookie
    async with _sm_lock:
        if not _sm_cookie:
            _sm_cookie = await _statmuse_login()
    return _sm_cookie

async def _statmuse_login() -> str:
    """Launch headless Chromium, log into StatMuse, return cookie string."""
    email    = os.environ.get("STATMUSE_EMAIL", "")
    password = os.environ.get("STATMUSE_PASSWORD", "")
    print("[StatMuse] Logging in…")

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
            await page.goto(
                "https://www.statmuse.com/auth/sign-in",
                wait_until="domcontentloaded",
                timeout=30_000,
            )
            await page.fill("input[type='email']",    email)
            await page.fill("input[type='password']", password)
            await page.click("button[type='submit']")
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception as e:
            print(f"[StatMuse] Login warning: {e}")

        cookies = await ctx.cookies()
        await browser.close()

    cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    print(f"[StatMuse] Login OK — {len(cookies)} cookies")
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
#  NHL Skater Stats — season shot averages (replaces sportsbook props)
# ─────────────────────────────────────────────────────────────────────────────

async def get_shot_qualified_players(
    games: List[Dict],
    sa_map: Dict[str, float],
    sem: asyncio.Semaphore,
    season: str = "20242025",
) -> List[Dict]:
    """Return skaters on today's teams averaging ≥ MIN_SPG shots/game."""

    # Build team → game context
    team_ctx: Dict[str, Dict] = {}
    for g in games:
        team_ctx[g["homeTeam"]] = {"opponent": g["awayTeam"], "homeRoad": "H"}
        team_ctx[g["awayTeam"]] = {"opponent": g["homeTeam"],  "homeRoad": "R"}

    pool: List[Dict] = []
    seen: set = set()

    async def _fetch_team(team: str):
        async with sem:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(
                    f"{NHL_STATS}/skater/summary",
                    params={
                        "limit": 100,
                        "start": 0,
                        "cayenneExp": f"gameTypeId=2 and seasonId={season} and teamAbbrevs='{team}'",
                    },
                )
        if r.status_code != 200:
            print(f"[NHL] Skater stats {team} → {r.status_code}")
            return

        ctx = team_ctx.get(team, {})
        opp = ctx.get("opponent", "")
        hr  = ctx.get("homeRoad", "")

        for p in r.json().get("data", []):
            pid   = p.get("playerId")
            gp    = p.get("gamesPlayed", 0)
            shots = p.get("shots", 0)
            pos   = p.get("positionCode", "")

            if pos == "G" or gp < MIN_GP:
                continue
            spg = shots / gp
            if spg < MIN_SPG or pid in seen:
                continue

            seen.add(pid)
            est = _est_line(spg)
            pool.append({
                "name":     p["skaterFullName"],
                "pid":      pid,
                "team":     team,
                "opponent": opp,
                "homeRoad": hr,
                "line":     1.5,
                "estLine":  est,
                "spg":      round(spg, 2),
                "gp":       gp,
                "oppSA":    sa_map.get(opp, 0.0),
            })

    await asyncio.gather(*[_fetch_team(t) for t in team_ctx], return_exceptions=True)
    pool.sort(key=lambda x: x["oppSA"], reverse=True)
    print(f"[NHL] {len(pool)} skaters averaging ≥{MIN_SPG} S/G on today's teams")
    return pool

# ─────────────────────────────────────────────────────────────────────────────
#  StatMuse scraping helpers
# ─────────────────────────────────────────────────────────────────────────────

def _player_slug(name: str) -> str:
    """'Connor McDavid' → 'connor-mcdavid'"""
    nfd = unicodedata.normalize("NFD", name)
    ascii_ = nfd.encode("ascii", "ignore").decode("ascii")
    slug = ascii_.lower().replace(" ", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    return re.sub(r"-+", "-", slug).strip("-")

def _ha_word(home_road: str) -> str:
    return "home" if home_road == "H" else "road"

def _sm_url_career(player: str, home_road: str, opp_abbrev: str) -> str:
    """Career H/A shots vs specific opponent."""
    ps   = _player_slug(player)
    ts   = TEAM_SLUGS.get(opp_abbrev, opp_abbrev.lower())
    ha   = _ha_word(home_road)
    return f"{STATMUSE_NHL}/{ps}-shots-on-goal-in-{ha}-games-vs-{ts}"

def _sm_url_last10(player: str, home_road: str) -> str:
    """Last 10 H/A games shots."""
    ps = _player_slug(player)
    ha = _ha_word(home_road)
    return f"{STATMUSE_NHL}/{ps}-shots-on-goal-last-10-{ha}-games"


async def _scrape_sm(url: str, cookie: str, sem: asyncio.Semaphore) -> List[int]:
    """Scrape a StatMuse NHL page and return list of shot totals per game."""
    hdrs = {
        "Cookie":  cookie,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":   "text/html,application/xhtml+xml",
        "Referer":  "https://www.statmuse.com/",
    }
    async with sem:
        try:
            async with CFSession(impersonate="chrome110") as s:
                r = await s.get(url, headers=hdrs, timeout=20)
        except Exception as e:
            print(f"[SM] Fetch error {url}: {e}")
            return []

    if r.status_code != 200:
        print(f"[SM] HTTP {r.status_code} for {url}")
        return []

    soup  = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    hdrs_row = [td.get_text(strip=True) for td in rows[0].find_all(["th", "td"])]
    try:
        shots_idx = hdrs_row.index("S")
    except ValueError:
        return []

    totals = []
    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) > shots_idx:
            try:
                totals.append(int(cells[shots_idx]))
            except ValueError:
                pass
    return totals


def _est_line(spg: float) -> float:
    """Estimate sportsbook line from season shots/game average."""
    if spg >= 3.0:
        return 3.5
    elif spg >= 2.0:
        return 2.5
    return 1.5

def _hit_rate(totals: List[int], line: float) -> Tuple[int, int, float]:
    total = len(totals)
    if total == 0:
        return 0, 0, 0.0
    hits = sum(1 for s in totals if s > line)
    return hits, total, round(hits / total * 100, 1)



# ─────────────────────────────────────────────────────────────────────────────
#  Main algorithm
# ─────────────────────────────────────────────────────────────────────────────

async def run_picks(target_date: str = None) -> Dict:
    sem_nhl = asyncio.Semaphore(SEM_NHL)
    sem_sm  = asyncio.Semaphore(SEM_SM)

    target_date = target_date or date.today().isoformat()
    season = get_season_for_date(date.fromisoformat(target_date))

    # ── Step 1 — fetch games, SA map, StatMuse cookie in parallel ────────────
    games, sa_map, cookie = await asyncio.gather(
        get_today_games(target_date),
        get_team_sa_map(season),
        get_sm_cookie(),
    )

    if not games:
        return {"error": f"No NHL games found for {target_date}.", "picks": [], "games": []}

    # SA rankings for display
    playing = list({g["homeTeam"] for g in games} | {g["awayTeam"] for g in games})
    sa_ranks = sorted(
        [(t, sa_map.get(t, 0.0)) for t in playing],
        key=lambda x: x[1], reverse=True
    )

    # Build player pool from NHL skater season averages
    pool = await get_shot_qualified_players(games, sa_map, sem_nhl, season)

    if not pool:
        return {"error": f"No skaters averaging ≥{MIN_SPG} S/G on today's teams.", "picks": [], "games": games}

    # ── Steps 2 & 3 — StatMuse hit-rate analysis ──────────────────────────────
    async def analyze(p: Dict) -> Optional[Dict]:
        url2 = _sm_url_career(p["name"], p["homeRoad"], p["opponent"])
        url3 = _sm_url_last10(p["name"], p["homeRoad"])

        t2_raw, t3_raw = await asyncio.gather(
            _scrape_sm(url2, cookie, sem_sm),
            _scrape_sm(url3, cookie, sem_sm),
        )

        # Step 3 — last 10 H/A games
        t3_list = t3_raw[:10]
        t3 = len(t3_list)
        if t3 < MIN_GAMES:
            return None
        h3, _, r3 = _hit_rate(t3_list, p["line"])
        ha10avg  = round(sum(t3_list) / t3, 2)  # avg shots in last 10 H/A games

        # Step 2 — career H/A vs today's opponent
        t2 = len(t2_raw)
        h2, _, r2 = _hit_rate(t2_raw, p["line"])
        opp_avg = round(sum(t2_raw) / t2, 2) if t2 > 0 else p["spg"]  # avg shots vs this opp H/A

        # DQ logic
        s2_ok = (t2 < MIN_GAMES) or (r2 >= HIT_THRESH)
        s3_ok = r3 >= HIT_THRESH
        if not s2_ok or not s3_ok:
            return None

        score = round((r2 + r3) / 2 if t2 >= MIN_GAMES else r3, 1)

        return {
            **p,
            "step2Hits":  h2,
            "step2Total": t2,
            "step2Rate":  r2,
            "step3Hits":  h3,
            "step3Total": t3,
            "step3Rate":  r3,
            "oppAvg":     opp_avg,
            "ha10avg":    ha10avg,
            "score":      score,
        }

    results_raw = await asyncio.gather(*[analyze(p) for p in pool])
    picks = [r for r in results_raw if r is not None]

    # ── Step 4 — rank & top 10 ────────────────────────────────────────────────
    picks.sort(key=lambda x: (x["score"], x["oppSA"]), reverse=True)

    return {
        "picks":     picks[:TOP_N],
        "rest":      picks[TOP_N:],
        "games":     games,
        "sa_ranks":  sa_ranks,
        "poolSize":  len(pool),
        "qualified": len(picks),
        "targetDate": target_date,
        "runTime":   datetime.utcnow().isoformat() + "Z",
    }

# ─────────────────────────────────────────────────────────────────────────────
#  HTML
# ─────────────────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>🏒 NHL Money Shots</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800;900&family=Barlow:wght@400;500;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0c0c0e;color:#f0f0f0;font-family:'Barlow',system-ui,sans-serif;min-height:100vh}

/* ── Header ── */
.hdr{position:relative;overflow:hidden;background:#000;padding:0;border-bottom:3px solid #FFB81C}
.hdr-inner{position:relative;z-index:2;padding:32px 20px 24px;text-align:center}
.hdr::before{content:'';position:absolute;inset:0;
  background:linear-gradient(160deg,#1a0000 0%,#000 40%,#1a1100 100%);z-index:0}
.hdr::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,#FFB81C,#c8102e,#FFB81C,transparent);z-index:1}
.hdr h1{font-family:'Barlow Condensed',sans-serif;font-size:3rem;font-weight:900;
  letter-spacing:6px;color:#fff;text-transform:uppercase;
  text-shadow:0 0 40px rgba(255,184,28,.4),0 2px 0 #FFB81C}
.hdr h1 span{color:#FFB81C}
.hdr-sub{color:#888;margin-top:6px;font-size:.85rem;letter-spacing:3px;text-transform:uppercase}
.hdr-bar{display:flex;justify-content:center;gap:32px;margin-top:16px;flex-wrap:wrap}
.hdr-stat{text-align:center}
.hdr-stat .v{font-family:'Barlow Condensed',sans-serif;font-size:1.5rem;font-weight:800;color:#FFB81C}
.hdr-stat .l{font-size:.68rem;color:#555;text-transform:uppercase;letter-spacing:1px}

/* ── Layout ── */
.wrap{max-width:1400px;margin:0 auto;padding:28px 20px}

/* ── Controls ── */
.controls{display:flex;align-items:center;justify-content:center;gap:16px;
  flex-wrap:wrap;margin-bottom:24px;padding:20px;
  background:#111;border:1px solid #222;border-radius:12px}
.date-wrap{display:flex;align-items:center;gap:10px}
.date-wrap label{color:#FFB81C;font-weight:700;font-size:.9rem;text-transform:uppercase;letter-spacing:1px}
.date-wrap input[type=date]{background:#1a1a1a;color:#f0f0f0;border:1px solid #333;
  border-radius:8px;padding:10px 16px;font-size:.95rem;font-family:'Barlow',sans-serif;
  cursor:pointer;outline:none}
.date-wrap input[type=date]:focus{border-color:#FFB81C}
.sm-dot{font-size:.82rem;font-weight:600;padding:6px 14px;border-radius:20px;
  background:#1a1a1a;border:1px solid #333}

/* ── Run Button ── */
.btn{padding:14px 52px;background:linear-gradient(135deg,#c8102e,#8b0000);
  border:none;border-radius:8px;color:#fff;font-size:1rem;
  font-family:'Barlow Condensed',sans-serif;font-weight:800;
  letter-spacing:2px;text-transform:uppercase;cursor:pointer;
  transition:all .2s;box-shadow:0 4px 16px rgba(200,16,46,.3)}
.btn:hover{transform:translateY(-2px);box-shadow:0 8px 24px rgba(200,16,46,.5);background:linear-gradient(135deg,#e01535,#a00010)}
.btn:disabled{opacity:.5;cursor:not-allowed;transform:none}

.status{text-align:center;color:#666;margin:12px 0 24px;font-size:.85rem}

/* ── Summary chips ── */
.chips{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px;margin-bottom:32px}
.chip{background:#111;border:1px solid #222;border-radius:10px;padding:16px 12px;text-align:center;
  position:relative;overflow:hidden}
.chip::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:#FFB81C}
.chip .val{font-family:'Barlow Condensed',sans-serif;font-size:2rem;font-weight:900;color:#FFB81C}
.chip .lbl{font-size:.65rem;color:#555;margin-top:4px;text-transform:uppercase;letter-spacing:.8px}

/* ── Section headers ── */
.sec{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:800;
  color:#fff;margin:28px 0 14px;padding:10px 16px;
  background:#111;border-left:4px solid #FFB81C;
  text-transform:uppercase;letter-spacing:2px;border-radius:0 6px 6px 0}

/* ── Games grid ── */
.games{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;margin-bottom:28px}
.gcard{background:#111;border:1px solid #222;border-radius:10px;padding:14px;text-align:center;
  transition:border-color .2s}
.gcard:hover{border-color:#FFB81C}
.gcard .mu{font-family:'Barlow Condensed',sans-serif;font-size:1.1rem;font-weight:700;color:#fff;letter-spacing:1px}
.gcard .gt{font-size:.74rem;color:#555;margin-top:6px}
.gcard .puck{font-size:1.2rem;margin-bottom:4px}

/* ── SA badges ── */
.sa-list{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:28px}
.sa-badge{background:#111;border:1px solid #222;border-radius:6px;padding:6px 12px;font-size:.78rem;font-family:'Barlow Condensed',sans-serif}
.sa-badge .rk{color:#FFB81C;font-weight:800;font-size:.9rem}
.sa-badge .sv{color:#c8102e;font-weight:700}

/* ── Table ── */
.tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid #222;margin-bottom:8px}
table{width:100%;border-collapse:collapse;background:#0c0c0e}
thead tr{background:#111;border-bottom:2px solid #FFB81C}
th{padding:12px 14px;text-align:left;font-family:'Barlow Condensed',sans-serif;
  font-size:.8rem;text-transform:uppercase;letter-spacing:1.5px;color:#FFB81C;white-space:nowrap}
td{padding:11px 14px;border-bottom:1px solid #1a1a1a;font-size:.88rem;white-space:nowrap}
tr:nth-child(even) td{background:#0f0f11}
tr:hover td{background:#161618}
tr:last-child td{border-bottom:none}

.rk-num{font-family:'Barlow Condensed',sans-serif;font-weight:900;color:#FFB81C;font-size:1.2rem}
.pname{font-weight:600;color:#fff}
.badge{padding:3px 10px;border-radius:4px;font-size:.74rem;font-weight:700;letter-spacing:.5px}
.t-badge{background:#1a1a1a;color:#aaa;border:1px solid #2a2a2a}
.home{background:#0d2b0d;color:#4ade80;border:1px solid #1a4a1a}
.away{background:#2b0d0d;color:#f87171;border:1px solid #4a1a1a}
.line-v{color:#FFB81C;font-weight:700;font-family:'Barlow Condensed',sans-serif;font-size:1rem}
.rate-hi{color:#22c55e;font-weight:700}
.rate-md{color:#FFB81C;font-weight:700}
.rate-lo{color:#ef4444;font-weight:700}
.score-v{font-family:'Barlow Condensed',sans-serif;color:#fff;font-weight:900;font-size:1.1rem}
.sa-v{color:#888;font-size:.82rem}
.est-line{background:#1a1400;color:#FFB81C;border:1px solid #3a2a00;padding:3px 10px;
  border-radius:4px;font-size:.8rem;font-weight:700;font-family:'Barlow Condensed',sans-serif}

/* ── States ── */
.loading{text-align:center;padding:80px 20px}
.spin{width:56px;height:56px;border:4px solid #1a1a1a;border-top:4px solid #FFB81C;
  border-radius:50%;animation:spin .7s linear infinite;margin:0 auto 20px}
@keyframes spin{to{transform:rotate(360deg)}}
.err{background:#200;border:1px solid #c8102e;border-radius:10px;
  padding:24px;text-align:center;color:#f87171;font-weight:600}
.empty{text-align:center;padding:60px;color:#444}

footer{text-align:center;padding:32px;color:#333;font-size:.78rem;border-top:1px solid #1a1a1a;margin-top:32px}
footer span{color:#FFB81C}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-inner">
    <h1>🏒 NHL <span>Money</span> Shots</h1>
    <div class="hdr-sub">Shots on Goal Daily Analyzer</div>
  </div>
</div>

<div class="wrap">

  <div class="controls">
    <div class="date-wrap">
      <label>📅 Date</label>
      <input type="date" id="datePicker" max=""/>
    </div>
    <button class="btn" id="runBtn" onclick="run()">▶ Run Picks</button>
    <span id="sm-status" class="sm-dot" style="color:#555">● StatMuse</span>
  </div>

  <div id="status" class="status"></div>
  <div id="out"></div>
</div>

<footer>NHL <span>Money Shots</span> · StatMuse + NHL Stats API · higgiAPPMASTER</footer>

<script>
document.addEventListener('DOMContentLoaded',()=>{
  const dp=document.getElementById('datePicker');
  const today=new Date().toISOString().split('T')[0];
  dp.value=today; dp.max=today;
});

async function run(){
  const btn=document.getElementById('runBtn');
  const st=document.getElementById('status');
  const out=document.getElementById('out');
  const picksDate = document.getElementById('datePicker').value || new Date().toISOString().split('T')[0];
  btn.disabled=true; btn.textContent='⏳ Analyzing…';
  st.textContent=`Fetching games for ${picksDate}…`;
  out.innerHTML='<div class="loading"><div class="spin"></div><p style="color:#888">Logging into StatMuse — analyzing player histories…<br><small style="color:#555">Allow 60–90 seconds</small></p></div>';
  const smEl=document.getElementById('sm-status');
  smEl.innerHTML='● Connecting…'; smEl.style.color='#888';
  try{
    const res=await fetch(`/api/picks?target_date=${picksDate}`);
    const d=await res.json();
    smEl.innerHTML='● StatMuse Connected'; smEl.style.color='#22c55e';
    if(d.error){out.innerHTML=`<div class="err">⚠️ ${d.error}</div>`;return;}
    render(d);
    st.textContent=`✅ ${d.qualified} players qualified · ${d.picks.length} top picks · ${picksDate}`;
  }catch(e){
    smEl.innerHTML='● StatMuse Error'; smEl.style.color='#c8102e';
    out.innerHTML=`<div class="err">❌ ${e.message}</div>`;
  }
  finally{btn.disabled=false;btn.textContent='▶ Run Picks';}
}

function rc(r){return r>=90?'rate-hi':r>=80?'rate-md':'rate-lo';}

function render(d){
  let h='';

  h+=`<div class="chips">
    <div class="chip"><div class="val">${d.games.length}</div><div class="lbl">Games</div></div>
    <div class="chip"><div class="val">${d.poolSize}</div><div class="lbl">≥1.5 S/G Pool</div></div>
    <div class="chip"><div class="val">${d.qualified}</div><div class="lbl">Qualified</div></div>
    <div class="chip"><div class="val">${d.picks.length}</div><div class="lbl">Top Picks</div></div>
    <div class="chip"><div class="val">80%</div><div class="lbl">Min Hit Rate</div></div>
  </div>`;

  h+=`<div class="sec">🏒 Games — ${d.targetDate||''}</div><div class="games">`;
  for(const g of d.games){
    const t=g.startTime?new Date(g.startTime).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZoneName:'short'}):'';
    h+=`<div class="gcard"><div class="puck">🪬</div><div class="mu">${g.awayTeam} @ ${g.homeTeam}</div><div class="gt">${t}</div></div>`;
  }
  h+=`</div>`;

  h+=`<div class="sec">🛡️ Shots Against / Game Rankings</div><div class="sa-list">`;
  (d.sa_ranks||[]).forEach(([team,sa],i)=>{
    h+=`<div class="sa-badge"><span class="rk">#${i+1} ${team}</span> <span class="sv">${sa.toFixed(1)}</span></div>`;
  });
  h+=`</div>`;

  h+=`<div class="sec">🎯 Top ${d.picks.length} Money Shots</div>`;
  if(!d.picks.length){
    h+=`<div class="empty">No players met the 80%+ hit rate threshold for this date.</div>`;
  }else{
    const thead=`<thead><tr>
      <th>#</th><th>Player</th><th>Team</th><th>Opp</th><th>H/A</th>
      <th>Avg vs Opp H/A</th><th>L10 H/A Avg</th><th>Est. Line</th>
      <th>Career vs Opp 1.5 S</th><th>Last 10 H/A 1.5 S</th>
      <th>Score</th><th>Opp SA/G</th>
    </tr></thead>`;

    // Top 10
    h+=`<div class="tbl-wrap"><table>${thead}<tbody>`;
    d.picks.forEach((p,i)=>{ h+=pickRow(p,i,true); });
    h+=`</tbody></table></div>`;

    // Rest of qualified players
    if(d.rest && d.rest.length){
      h+=`<div class="sec" style="margin-top:32px">📋 Also Qualified — ${d.rest.length} more players</div>`;
      h+=`<div class="tbl-wrap"><table>${thead}<tbody>`;
      d.rest.forEach((p,i)=>{ h+=pickRow(p,i,false); });
      h+=`</tbody></table></div>`;
    }
  }

  document.getElementById('out').innerHTML=h;
}

function pickRow(p,i,showRank){
  const ha=p.homeRoad==='H';
  return `<tr>
    ${showRank?`<td><span class="rk-num">${i+1}</span></td>`:'<td><span style="color:#334155">${i+11}</span></td>'}
    <td><span class="pname">${p.name}</span></td>
    <td><span class="badge t-badge">${p.team}</span></td>
    <td><span class="badge t-badge">${p.opponent}</span></td>
    <td><span class="badge ${ha?'home':'away'}">${ha?'HOME':'AWAY'}</span></td>
    <td><span class="line-v">${p.oppAvg}</span></td>
    <td><span class="line-v">${p.ha10avg}</span></td>
    <td><span class="est-line">~${p.estLine}</span></td>
    <td><span class="${rc(p.step2Rate)}">${p.step2Hits}/${p.step2Total} (${p.step2Rate}%)</span></td>
    <td><span class="${rc(p.step3Rate)}">${p.step3Hits}/${p.step3Total} (${p.step3Rate}%)</span></td>
    <td><span class="score-v">${p.score}</span></td>
    <td><span class="sa-v">${p.oppSA.toFixed(1)}</span></td>
  </tr>`;
}
</script>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────────────────────
#  Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(_: str = Depends(verify_user)):
    return HTML

@app.get("/api/picks")
async def api_picks(target_date: str = None, _: str = Depends(verify_user)):
    result = await run_picks(target_date)
    return JSONResponse(result)

@app.get("/api/status")
async def api_status(_: str = Depends(verify_user)):
    """Check StatMuse connection status."""
    global _sm_cookie
    connected = _sm_cookie is not None
    if not connected:
        try:
            await get_sm_cookie()
            connected = True
        except Exception:
            connected = False
    return {
        "statmuse": "connected" if connected else "disconnected",
        "time": datetime.utcnow().isoformat(),
    }

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
