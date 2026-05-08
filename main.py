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


async def get_today_games() -> List[Dict]:
    today = date.today().isoformat()
    async with httpx.AsyncClient(follow_redirects=True) as c:
        data = await _fetch(f"{NHL_API}/schedule/{today}", c)
    if not data:
        return []
    games = []
    for day in data.get("gameWeek", []):
        if day.get("date") == today:
            for g in day.get("games", []):
                if g.get("gameState", "") in ("FUT", "PRE", "LIVE", "CRIT"):
                    games.append({
                        "gameId":    g["id"],
                        "homeTeam":  g["homeTeam"]["abbrev"],
                        "awayTeam":  g["awayTeam"]["abbrev"],
                        "homeFull":  g["homeTeam"].get("commonName", {}).get("default", ""),
                        "awayFull":  g["awayTeam"].get("commonName", {}).get("default", ""),
                        "startTime": g.get("startTimeUTC", ""),
                    })
    return games


async def get_team_sa_map() -> Dict[str, float]:
    """Shots Against Per Game — joins /standings (abbrev) + /team/summary (SA/G)."""
    import urllib.parse
    sort_p = urllib.parse.quote('[{"property":"shotsAgainstPerGame","direction":"DESC"}]')
    summary_url = (
        f"{NHL_STATS}/team/summary"
        f"?isAggregate=false&isGame=false&sort={sort_p}"
        f"&start=0&limit=50&factCayenneExp=gamesPlayed>=1"
        f"&cayenneExp=gameTypeId=2 and seasonId<=20242025 and seasonId>=20242025"
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
                        "cayenneExp": f"gameTypeId=2 and seasonId=20242025 and teamAbbrevs='{team}'",
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
            pool.append({
                "name":     p["skaterFullName"],
                "pid":      pid,
                "team":     team,
                "opponent": opp,
                "homeRoad": hr,
                "line":     1.5,
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


def _hit_rate(totals: List[int], line: float) -> Tuple[int, int, float]:
    total = len(totals)
    if total == 0:
        return 0, 0, 0.0
    hits = sum(1 for s in totals if s > line)
    return hits, total, round(hits / total * 100, 1)



# ─────────────────────────────────────────────────────────────────────────────
#  Main algorithm
# ─────────────────────────────────────────────────────────────────────────────

async def run_picks() -> Dict:
    sem_nhl = asyncio.Semaphore(SEM_NHL)
    sem_sm  = asyncio.Semaphore(SEM_SM)

    # ── Step 1 — fetch games, SA map, StatMuse cookie in parallel ────────────
    games, sa_map, cookie = await asyncio.gather(
        get_today_games(),
        get_team_sa_map(),
        get_sm_cookie(),
    )

    if not games:
        return {"error": "No NHL games scheduled today.", "picks": [], "games": []}

    # SA rankings for display
    playing = list({g["homeTeam"] for g in games} | {g["awayTeam"] for g in games})
    sa_ranks = sorted(
        [(t, sa_map.get(t, 0.0)) for t in playing],
        key=lambda x: x[1], reverse=True
    )

    # Build player pool from NHL skater season averages
    pool = await get_shot_qualified_players(games, sa_map, sem_nhl)

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

        # Step 2 — career H/A vs today's opponent
        t2 = len(t2_raw)
        h2, _, r2 = _hit_rate(t2_raw, p["line"])

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
            "score":      score,
        }

    results_raw = await asyncio.gather(*[analyze(p) for p in pool])
    picks = [r for r in results_raw if r is not None]

    # ── Step 4 — rank & top 10 ────────────────────────────────────────────────
    picks.sort(key=lambda x: (x["score"], x["oppSA"]), reverse=True)

    return {
        "picks":     picks[:TOP_N],
        "games":     games,
        "sa_ranks":  sa_ranks,
        "poolSize":  len(pool),
        "qualified": len(picks),
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
<title>🏒 NHL Shots Picks</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#07090f;color:#e2e8f0;font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh}

.hdr{background:linear-gradient(135deg,#001f5c 0%,#003fa3 50%,#001f5c 100%);
     padding:28px 20px;text-align:center;border-bottom:3px solid #1e90ff}
.hdr h1{font-size:2.4rem;font-weight:900;letter-spacing:3px;text-shadow:0 0 30px rgba(30,144,255,.6)}
.hdr p{color:#90c8ff;margin-top:8px;font-size:.95rem;letter-spacing:1px}

.wrap{max-width:1280px;margin:0 auto;padding:28px 16px}

.btn{display:block;margin:0 auto 28px;padding:16px 56px;
     background:linear-gradient(135deg,#003fa3,#1e90ff);
     border:none;border-radius:10px;color:#fff;font-size:1.15rem;
     font-weight:800;cursor:pointer;letter-spacing:1px;
     transition:transform .2s,box-shadow .2s}
.btn:hover{transform:translateY(-3px);box-shadow:0 10px 28px rgba(30,144,255,.45)}
.btn:disabled{opacity:.55;cursor:not-allowed;transform:none}

.status{text-align:center;color:#90c8ff;margin-bottom:20px;font-size:.88rem}

.chips{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:28px}
.chip{background:#0d1426;border:1px solid #1e3060;border-radius:10px;
      padding:14px 20px;flex:1;min-width:130px;text-align:center}
.chip .val{font-size:1.8rem;font-weight:900;color:#1e90ff}
.chip .lbl{font-size:.72rem;color:#4a6080;margin-top:3px;text-transform:uppercase;letter-spacing:.5px}

.sec{font-size:1.15rem;font-weight:800;color:#1e90ff;
     margin:28px 0 14px;padding-left:14px;border-left:4px solid #1e90ff;letter-spacing:.5px}

.games{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;margin-bottom:32px}
.gcard{background:#0d1426;border:1px solid #1e3060;border-radius:10px;padding:14px;text-align:center}
.gcard .mu{font-size:1rem;font-weight:700}
.gcard .gt{font-size:.78rem;color:#4a6080;margin-top:5px}

.sa-list{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:28px}
.sa-badge{background:#0d1426;border:1px solid #1e3060;border-radius:20px;padding:6px 14px;font-size:.8rem}
.sa-badge .rk{color:#1e90ff;font-weight:800}
.sa-badge .sv{color:#f59e0b;font-weight:700}

.tbl-wrap{overflow-x:auto;border-radius:12px;border:1px solid #1e3060}
table{width:100%;border-collapse:collapse;background:#0a0e1a}
thead{background:linear-gradient(135deg,#001f5c,#003fa3)}
th{padding:13px 14px;text-align:left;font-size:.75rem;text-transform:uppercase;
   letter-spacing:.6px;color:#90c8ff;white-space:nowrap}
td{padding:12px 14px;border-bottom:1px solid #141e36;font-size:.9rem;white-space:nowrap}
tr:last-child td{border-bottom:none}
tr:hover td{background:#0d1426}

.rk-num{font-weight:900;color:#1e90ff;font-size:1.1rem}
.pname{font-weight:700;color:#f1f5f9}
.badge{padding:3px 9px;border-radius:5px;font-size:.76rem;font-weight:700}
.t-badge{background:#0a2050;color:#60a5fa}
.home{background:#052e16;color:#4ade80}
.away{background:#3b0764;color:#c084fc}
.line-v{color:#f59e0b;font-weight:800;font-size:1rem}
.odds-v{color:#64748b;font-size:.82rem}
.rate-hi{color:#22c55e;font-weight:700}
.rate-md{color:#f59e0b;font-weight:700}
.rate-lo{color:#ef4444;font-weight:700}
.score-v{color:#a78bfa;font-weight:900;font-size:1.05rem}
.sa-v{color:#38bdf8;font-size:.82rem}

.loading{text-align:center;padding:70px 20px}
.spin{width:52px;height:52px;border:4px solid #141e36;border-top:4px solid #1e90ff;
      border-radius:50%;animation:spin .8s linear infinite;margin:0 auto 18px}
@keyframes spin{to{transform:rotate(360deg)}}
.err{background:#2d0707;border:1px solid #dc2626;border-radius:10px;
     padding:22px;text-align:center;color:#fca5a5;font-weight:600}
.empty{text-align:center;padding:50px;color:#334155}

footer{text-align:center;padding:28px;color:#1e3060;font-size:.78rem;margin-top:20px}
</style>
</head>
<body>

<div class="hdr">
  <h1>🏒 NHL SHOTS PICKS</h1>
  <p id="hdr-date">Shots on Goal Daily Analyzer</p>
</div>

<div class="wrap">
  <button class="btn" id="runBtn" onclick="run()">▶ &nbsp;RUN TODAY'S PICKS</button>
  <div id="status" class="status"></div>
  <div id="out"></div>
</div>

<footer>NHL Shots Picks · The Odds API + StatMuse + NHL Stats API</footer>

<script>
async function run(){
  const btn=document.getElementById('runBtn');
  const st=document.getElementById('status');
  const out=document.getElementById('out');
  btn.disabled=true; btn.textContent='⏳  Analyzing…';
  st.textContent='Logging into StatMuse, fetching props & player histories…';
  out.innerHTML='<div class="loading"><div class="spin"></div><p style="color:#90c8ff">Running algorithm — allow 60–120 seconds</p></div>';
  try{
    const res=await fetch('/api/picks');
    const d=await res.json();
    if(d.error){out.innerHTML=`<div class="err">⚠️ ${d.error}</div>`;return;}
    render(d);
    document.getElementById('hdr-date').textContent=
      new Date().toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric',year:'numeric'});
    st.textContent=`✅ Done — ${d.qualified} players qualified · ${d.picks.length} picks returned`;
  }catch(e){out.innerHTML=`<div class="err">❌ ${e.message}</div>`;}
  finally{btn.disabled=false;btn.textContent='🔄  REFRESH PICKS';}
}

function rc(r){return r>=90?'rate-hi':r>=80?'rate-md':'rate-lo';}

function render(d){
  let h='';

  h+=`<div class="chips">
    <div class="chip"><div class="val">${d.games.length}</div><div class="lbl">Games Today</div></div>
    <div class="chip"><div class="val">${d.poolSize}</div><div class="lbl">Avg ≥1.5 S/G</div></div>
    <div class="chip"><div class="val">${d.qualified}</div><div class="lbl">Qualified</div></div>
    <div class="chip"><div class="val">${d.picks.length}</div><div class="lbl">Top Picks</div></div>
    <div class="chip"><div class="val">80%</div><div class="lbl">Min Hit Rate</div></div>
  </div>`;

  h+=`<div class="sec">📅 Today's Games</div><div class="games">`;
  for(const g of d.games){
    const t=g.startTime?new Date(g.startTime).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZoneName:'short'}):'';
    h+=`<div class="gcard"><div class="mu">${g.awayTeam} @ ${g.homeTeam}</div><div class="gt">${t}</div></div>`;
  }
  h+=`</div>`;

  h+=`<div class="sec">🛡️ Shots Against Rankings — Today's Teams</div><div class="sa-list">`;
  (d.sa_ranks||[]).forEach(([team,sa],i)=>{
    h+=`<div class="sa-badge"><span class="rk">#${i+1} ${team}</span> <span class="sv">${sa.toFixed(1)} SA/G</span></div>`;
  });
  h+=`</div>`;

  h+=`<div class="sec">🎯 Top ${d.picks.length} Picks</div>`;
  if(!d.picks.length){
    h+=`<div class="empty">No players met the 80%+ hit rate threshold today.</div>`;
  }else{
    h+=`<div class="tbl-wrap"><table>
      <thead><tr>
        <th>#</th><th>Player</th><th>Team</th><th>Opp</th><th>H/A</th>
        <th>Avg S/G</th>
        <th>Career vs Opp</th><th>Last 10 H/A</th>
        <th>Score</th><th>Opp SA/G</th>
      </tr></thead><tbody>`;
    d.picks.forEach((p,i)=>{
      const ha=p.homeRoad==='H';
      h+=`<tr>
        <td><span class="rk-num">${i+1}</span></td>
        <td><span class="pname">${p.name}</span></td>
        <td><span class="badge t-badge">${p.team}</span></td>
        <td><span class="badge t-badge">${p.opponent}</span></td>
        <td><span class="badge ${ha?'home':'away'}">${ha?'HOME':'AWAY'}</span></td>
        <td><span class="line-v">${p.spg}</span></td>
        <td><span class="${rc(p.step2Rate)}">${p.step2Hits}/${p.step2Total} (${p.step2Rate}%)</span></td>
        <td><span class="${rc(p.step3Rate)}">${p.step3Hits}/${p.step3Total} (${p.step3Rate}%)</span></td>
        <td><span class="score-v">${p.score}</span></td>
        <td><span class="sa-v">${p.oppSA.toFixed(1)}</span></td>
      </tr>`;
    });
    h+=`</tbody></table></div>`;
  }

  document.getElementById('out').innerHTML=h;
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
async def api_picks(_: str = Depends(verify_user)):
    result = await run_picks()
    return JSONResponse(result)

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
