"""
MoneyBall FastAPI Backend
Runs the full 4-step MLB Daily Picks pipeline
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests, re, time, json, os
from bs4 import BeautifulSoup
from datetime import date as _date
from typing import Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class PicksRequest(BaseModel):
    date: str
    manual_players: Optional[str] = ""

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
MIN_S1_AB   = 5
MIN_S1_BA   = 0.250
MIN_S2_BA   = 0.250
MIN_S3_BA   = 0.250
MIN_DN_BA   = 0.200

# ── Cache ─────────────────────────────────────────────────────────────
def get_fic_players(run_date):
    cache = f"/tmp/fic_{run_date.replace('-','')}.json"
    if os.path.exists(cache):
        with open(cache) as f:
            return json.load(f)
    url = f"https://www.fantasyinfocentral.com/mlb/daily-matchups?date={run_date}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", id="searchable")
    if not table:
        return []
    results = []
    for row in table.find("tbody").find_all("tr"):
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 13: continue
        batter = cols[0].split(",")[0].strip()
        pos_m  = re.search(r",([^(]+)", cols[0])
        pos    = pos_m.group(1).strip() if pos_m else ""
        pitcher = re.sub(r"\([^)]*\)", "", cols[1]).strip()
        try:
            ab = int(cols[6]); ba = float(cols[12])
        except: continue
        if ab >= MIN_S1_AB and ba >= MIN_S1_BA:
            results.append({"batter": batter, "pos": pos, "pitcher": pitcher, "ba": ba})
    results.sort(key=lambda x: x["ba"], reverse=True)
    with open(cache, "w") as f:
        json.dump(results, f)
    return results

# ── ESPN Schedule ──────────────────────────────────────────────────────
def get_espn_schedule(date_nodash):
    r = requests.get(
        f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_nodash}",
        timeout=15)
    sched = {}
    for event in r.json().get("events", []):
        comps = event.get("competitions", [{}])[0]
        home = away = None
        for t in comps.get("competitors", []):
            if t["homeAway"] == "home": home = t["team"]
            else: away = t["team"]
        if home and away:
            gt = event.get("date", "")
            gtype = "day"
            if gt:
                from datetime import datetime
                dt = datetime.fromisoformat(gt.replace("Z", "+00:00"))
                gtype = "day" if dt.hour < 21 else "night"
            sched[home["displayName"]] = {"side":"HOME","opponent":away["displayName"],"opp_slug":away["displayName"].lower().replace(" ","-"),"game_type":gtype}
            sched[away["displayName"]] = {"side":"AWAY","opponent":home["displayName"],"opp_slug":home["displayName"].lower().replace(" ","-"),"game_type":gtype}
    return sched

# ── MLB Roster ─────────────────────────────────────────────────────────
def get_player_team(full_name):
    parts = full_name.strip().split()
    last = parts[-1] if parts else full_name
    try:
        r = requests.get("https://statsapi.mlb.com/api/v1/people/search",
            params={"names": last, "sportId": 1}, timeout=8)
        people = [p for p in r.json().get("people", []) if p.get("active")]
        for p in people:
            if p.get("firstName","").lower().startswith(parts[0][0].lower()):
                pid = p["id"]
                r2 = requests.get(f"https://statsapi.mlb.com/api/v1/people/{pid}",
                    params={"hydrate": "currentTeam"}, timeout=8)
                person = r2.json()["people"][0]
                team = person.get("currentTeam", {}).get("name", "")
                fname = person.get("firstName","")
                lname = person.get("lastName","")
                slug = f"{fname}-{lname}".lower().replace(" ","-")
                slug = slug.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
                return team, slug
    except: pass
    return "", ""

# ── StatMuse ───────────────────────────────────────────────────────────
def statmuse_ba(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        # Check for career highlight
        if soup.find(class_=lambda c: c and "bg-team-primary" in c):
            return None, "career"
        el = soup.find("p", class_=lambda c: c and "text-" in str(c))
        if el:
            m = re.search(r"(\.\d{3})", el.get_text())
            if m: return float(m.group(1)), "last10"
        # Try any .XXX pattern
        m = re.search(r"batting average.*?(\.\d{3})", r.text, re.IGNORECASE)
        if m: return float(m.group(1)), "last10"
        return None, "na"
    except:
        return None, "na"

def fetch_s2(first, last, side, opp):
    keyword = "away" if side == "AWAY" else "home"
    url = f"https://www.statmuse.com/mlb/ask/{first}-{last}-batting-average-in-last-10-{keyword}-games-vs-{opp}"
    return statmuse_ba(url)

def fetch_s3(first, last, side):
    keyword = "away" if side == "AWAY" else "home"
    url = f"https://www.statmuse.com/mlb/ask/{first}-{last}-batting-average-in-last-10-{keyword}-games-in-2026"
    return statmuse_ba(url)

# ── ESPN Day/Night ─────────────────────────────────────────────────────
def get_espn_id(full_name):
    try:
        url = f"https://site.web.api.espn.com/apis/search/v2?query={full_name.replace(' ','+')}&limit=5&sport=mlb"
        r = requests.get(url, headers=HEADERS, timeout=8)
        for result in r.json().get("results", []):
            if result.get("type") == "player":
                for c in result.get("contents", [])[:2]:
                    m = re.search(r"a:(\d+)", c.get("uid",""))
                    if m: return m.group(1)
    except: pass
    return None

def fetch_dn_ba(espn_id, game_type):
    if not espn_id: return None
    label = "Day" if game_type == "day" else "Night"
    try:
        url = f"https://site.web.api.espn.com/apis/common/v3/sports/baseball/mlb/athletes/{espn_id}/splits"
        r = requests.get(url, headers=HEADERS, timeout=10)
        for cat in r.json().get("splitCategories", []):
            if cat.get("displayName") == "Breakdown":
                splits = cat.get("splits", [])
                for s in splits:
                    if s.get("displayName") == label:
                        stats = s.get("stats", [])
                        if len(stats) > 12:
                            try: return float(stats[12])
                            except: pass
                # fallback
                other = "Night" if label == "Day" else "Day"
                for s in splits:
                    if s.get("displayName") == other:
                        stats = s.get("stats", [])
                        if len(stats) > 12:
                            try: return float(stats[12])
                            except: pass
    except: pass
    return None

# ── Main Pipeline ──────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "message": "MoneyBall API is running!"}

@app.post("/picks")
def get_picks(req: PicksRequest):
    run_date    = req.date
    manual      = req.manual_players.strip() if req.manual_players else ""
    date_nodash = run_date.replace("-", "")

    # Step 1
    if manual:
        names = [n.strip() for n in re.split(r"[,\n]+", manual) if n.strip()]
        players = [{"batter": n, "pos": "", "pitcher": "", "ba": 0.0} for n in names]
        is_manual = True
    else:
        players = get_fic_players(run_date)[:30]
        is_manual = False

    if not players:
        raise HTTPException(status_code=404, detail="No players found for this date")

    sched = get_espn_schedule(date_nodash)
    results, dq_list = [], []

    for p in players:
        name = p["batter"]
        team, slug = get_player_team(name)
        game = sched.get(team, {})
        side = game.get("side", "")
        if not side or not slug:
            continue

        parts = slug.split("-")
        first = parts[0]; last = "-".join(parts[1:])
        opp_slug  = game.get("opp_slug", "")
        opp_name  = game.get("opponent", "")
        game_type = game.get("game_type", "night")

        # Steps 2 & 3
        s2_ba, s2_flag = fetch_s2(first, last, side, opp_slug); time.sleep(0.2)
        s3_ba, s3_flag = fetch_s3(first, last, side);           time.sleep(0.2)

        # DQ check
        dq = []
        if s2_flag == "last10" and s2_ba and s2_ba < MIN_S2_BA: dq.append(f"S2 {s2_ba:.3f}")
        if s3_flag == "last10" and s3_ba and s3_ba < MIN_S3_BA: dq.append(f"S3 {s3_ba:.3f}")

        if dq:
            dq_list.append({"name": name, "dq_reason": " & ".join(dq)})
            continue

        # Step 4
        espn_id = get_espn_id(name)
        dn_ba   = fetch_dn_ba(espn_id, game_type)
        dn_label = "DAY" if game_type == "day" else "NIGHT"

        if dn_ba is not None and dn_ba < MIN_DN_BA:
            dq_list.append({"name": name, "dq_reason": f"Step4 {dn_label} {dn_ba:.3f}"})
            continue

        # Score
        s1_pts = round(p["ba"] * 1000) if not is_manual else 0
        s2_pts = round(s2_ba * 1000) if s2_ba and s2_flag == "last10" else 0
        s3_pts = round(s3_ba * 1000) if s3_ba and s3_flag == "last10" else 0
        total  = s1_pts + s2_pts + s3_pts

        results.append({
            "name":     name,
            "pos":      p["pos"],
            "side":     side,
            "opponent": opp_name,
            "s1":       round(p["ba"], 3),
            "s2":       f"{s2_ba:.3f}" if s2_ba else "N/A",
            "s3":       f"{s3_ba:.3f}" if s3_ba else "N/A",
            "dn":       f"{dn_ba:.3f}" if dn_ba else "N/A",
            "dn_label": dn_label,
            "total":    total,
        })

    results.sort(key=lambda x: x["total"], reverse=True)
    top9 = results[:9]

    return {
        "date":         run_date,
        "top9":         top9,
        "disqualified": dq_list,
        "is_manual":    is_manual,
        "picks_count":  len(top9),
    }
