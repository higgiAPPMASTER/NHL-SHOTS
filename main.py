
"""
under_picks.py — Under Picks via The Odds API (batter_hits 1.5 line).
Replaces the DraftKings scraper. Requires ODDS_API_KEY env var.

Algorithm per candidate (must pass ALL gates; cutoff < .250 each):
  S1  Career BA vs today's probable pitcher                    — N/A / 0 AB passes; DQ if >= .250
  S2  BA over last 10 (or fewer) H/A games vs TODAY'S opponent — data required AND < .250
  S3  BA over last 10 (or fewer) H/A games vs ANY opponent     — data required AND < .250
  L7  BA over last 7 games (general, any side/opp)             — N/A passes; DQ if >= .250
  All four gates apply to every player — no bypasses.
  Facing a top-30 ERA ace shows a display chip on the card only (does not affect qualification).
  Qualifiers ranked coldest first (lowest S2 + S3 + L7 combined BA).
"""
import os
import requests
import time
from datetime import date, datetime, timezone

from concurrent.futures import ThreadPoolExecutor, as_completed

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

_PLAYER_MAP:    dict = {}
_PITCHER_CACHE: dict = {}

# Populated by _fetch_hits_lines: normalized player name -> Over price on the 0.5 hits line
# (i.e. the standard "to record a hit" prop). Read by pipeline.py to enrich top9 picks.
import unicodedata as _ud
import re as _re
def _norm_name(s: str) -> str:
    s = "".join(c for c in _ud.normalize("NFKD", s or "") if not _ud.combining(c)).lower().strip()
    # strip suffixes: jr, sr, ii, iii, iv
    s = _re.sub(r'\b(jr\.?|sr\.?|ii|iii|iv)$', '', s).strip().rstrip(',').strip()
    # normalize hyphens to space (ha-seong → ha seong)
    s = s.replace('-', ' ')
    # collapse multiple spaces
    s = _re.sub(r'\s+', ' ', s).strip()
    return s
HIT_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name -> {name, line, home_team,
# away_team, over, under} for the batter_runs_scored (Over/Under ~0.5) market.
# Read by run_runs_picks. Parallel to HIT_ODDS; first game seen per name wins.
RUNS_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, line, home_team,
# away_team, tb_under_odds} for players with a posted batter_total_bases
# Under 1.5 price. Read by run_tb_under_picks. Cleared each call.
TB_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, tb_over_odds, …}
# for players with a posted batter_total_bases Over 1.5 price.
TB_OVER_ODDS: dict = {}
# Populated by _fetch_hits_lines: normalized name → {name, line, home_team,
# away_team, over, under} for the batter_rbis (Over/Under ~0.5) market.
# Read by run_rbi_picks. Cleared each call.
RBI_ODDS: dict = {}


def _log(emit, msg, type_="log"):
    if emit:
        emit({"type": type_, "msg": msg})


def _team_match(a: str, b: str) -> bool:
    a, b = a.lower().strip(), b.lower().strip()
    if a == b: return True
    al = a.split()[-1] if a else ""
    bl = b.split()[-1] if b else ""
    if al and al == bl: return True
    return (a in b) or (b in a)


def _build_player_map(season: int):
    global _PLAYER_MAP
    if _PLAYER_MAP: return
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/sports/1/players",
            params={"season": season, "gameType": "R"}, timeout=15)
        for p in r.json().get("people", []):
            name = p.get("fullName", "").lower().strip()
            pid  = p.get("id")
            if name and pid:
                _PLAYER_MAP[name] = pid
    except Exception:
        pass


def _resolve_id(name: str):
    key = name.lower().strip()
    if key in _PLAYER_MAP: return _PLAYER_MAP[key]
    last = key.split()[-1] if key else ""
    for k, v in _PLAYER_MAP.items():
        if k.endswith(last) and abs(len(k) - len(key)) <= 6:
            return v
    return None


def _get_teams_batch(player_ids: list) -> dict:
    if not player_ids: return {}
    result = {}
    # Chunk to keep the personIds URL short — the candidate pool is now the full
    # hit-odds set (~300 players), not just the ~57 on the 1.5 line.
    for i in range(0, len(player_ids), 100):
        chunk = player_ids[i:i + 100]
        try:
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/people",
                params={"personIds": ",".join(str(x) for x in chunk), "hydrate": "currentTeam"},
                timeout=12)
            for p in r.json().get("people", []):
                pid  = p.get("id")
                team = p.get("currentTeam", {}).get("name", "")
                if pid: result[pid] = team
        except Exception:
            continue
    return result


def _get_probable_pitchers(run_date: str) -> dict:
    if run_date in _PITCHER_CACHE: return _PITCHER_CACHE[run_date]
    result = {}
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": run_date,
                    "hydrate": "probablePitcher,team", "gameType": "R"},
            timeout=12)
        for d in r.json().get("dates", []):
            for game in d.get("games", []):
                for side in ("home", "away"):
                    t         = game.get("teams", {}).get(side, {})
                    team_name = t.get("team", {}).get("name", "")
                    pitcher   = t.get("probablePitcher", {})
                    if team_name and pitcher:
                        result[team_name] = {"name": pitcher.get("fullName", "TBD"),
                                             "id":   pitcher.get("id")}
    except Exception:
        pass
    _PITCHER_CACHE[run_date] = result
    return result


def _get_s1_vs_pitcher(batter_id, pitcher_id) -> dict:
    if not batter_id or not pitcher_id:
        return {"ba": None, "display": "N/A", "ab": 0}
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats",
            params={"stats": "vsPlayer", "opposingPlayerId": pitcher_id,
                    "group": "hitting", "gameType": "R"}, timeout=10)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits: return {"ba": None, "display": "N/A", "ab": 0}
        stat = splits[0].get("stat", {})
        ab = int(stat.get("atBats", 0) or 0)
        h  = int(stat.get("hits",   0) or 0)
        if ab == 0: return {"ba": None, "display": "N/A", "ab": 0}
        ba = h / ab
        return {"ba": ba, "display": f".{int(ba*1000):03d} ({ab}AB)", "ab": ab}
    except Exception:
        return {"ba": None, "display": "N/A", "ab": 0}


def _get_last7_ba(batter_id) -> dict:
    if not batter_id:
        return {"ba": None, "display": "N/A", "ab": 0}
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats",
            params={"stats": "lastXGames", "group": "hitting",
                    "gameType": "R", "limit": 7}, timeout=10)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits: return {"ba": None, "display": "N/A", "ab": 0}
        stat = splits[0].get("stat", {})
        ab = int(stat.get("atBats", 0) or 0)
        h  = int(stat.get("hits",   0) or 0)
        if ab == 0: return {"ba": None, "display": "0H/0AB", "ab": 0}
        ba = h / ab
        return {"ba": ba, "display": f".{int(ba*1000):03d} ({h}H/{ab}AB)", "ab": ab}
    except Exception:
        return {"ba": None, "display": "N/A", "ab": 0}


def _last10_ba(player_id, side: str, opp_name: str = "", max_games: int = 10) -> dict:
    """BA over the most recent max_games H/A games (up to 5 seasons back), counting
       only games with >=1 AB and matching the side the player is on today. When
       opp_name is set, restrict to games vs THAT team. Returns {ba, display, games}."""
    if not player_id:
        return {"ba": None, "display": "N/A", "games": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        hits = abs_ = g = 0
        done = False
        for season in range(cy, cy - 5, -1):
            for sp in reversed(_get_game_logs(player_id, season)):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                hits += int(stat.get("hits", 0) or 0)
                abs_ += ab
                g += 1
                if g >= max_games:
                    done = True
                    break
            if done:
                break
        if abs_ == 0:
            return {"ba": None, "display": "N/A", "games": 0}
        ba = hits / abs_
        return {"ba": ba, "display": f".{int(ba*1000):03d} ({g}G)", "games": g}
    except Exception:
        return {"ba": None, "display": "N/A", "games": 0}


def _fetch_hits_lines(run_date: str, emit=None) -> list:
    if not ODDS_API_KEY:
        _log(emit, "⚠️  ODDS_API_KEY not set — Under Picks skipped")
        return []

    # Fresh runs odds each call so the in-process scheduler (11/14/17:40 ET) and
    # any next-day run can't serve a first-seen matchup/price. (HIT_ODDS predates
    # this and is left as-is.)
    RUNS_ODDS.clear()
    TB_ODDS.clear()
    TB_OVER_ODDS.clear()
    RBI_ODDS.clear()
    PREFERRED = ["draftkings", "betmgm", "espnbet", "hardrockbet", "fanduel", "williamhill_us", "pointsbetus"]
    tomorrow  = (time.strftime("%Y-%m-%d",
                  time.gmtime(time.mktime(time.strptime(run_date, "%Y-%m-%d")) + 86400)))
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY, "dateFormat": "iso"}, timeout=15)
        if r.status_code != 200:
            _log(emit, f"⚠️  Odds API events returned {r.status_code}")
            return []

        def _is_run_date_game(ct: str) -> bool:
            """True if this game belongs to run_date (handles UTC rollover for late PT games)."""
            if not ct: return False
            day = ct[:10]
            if day == run_date: return True
            # Late-night PT games (10pm PT = 1am UTC next day) — cap at 09:00 UTC
            if day == tomorrow:
                try:
                    hour = int(ct[11:13])
                    return hour < 9
                except Exception:
                    return False
            return False
        events = [e for e in r.json() if _is_run_date_game(e.get("commence_time", ""))]
        _log(emit, f"  Odds API: {len(events)} games for {run_date}")
        seen:  dict = {}
        hit05: dict = {}  # nk -> {name, home_team, away_team} for every 0.5-line player

        now_utc = datetime.now(timezone.utc)
        for ev in events:
            ct = ev.get("commence_time", "")
            if ct:
                try:
                    game_start = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                    if game_start < now_utc:
                        continue  # game already started — skip live odds
                except Exception:
                    pass
            home_team = ev.get("home_team", "")
            away_team = ev.get("away_team", "")
            r2 = requests.get(
                f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{ev['id']}/odds",
                params={"apiKey": ODDS_API_KEY, "regions": "us,us2",
                        "markets": "batter_hits,batter_hits_alternate,batter_total_bases,batter_total_bases_alternate,batter_runs_scored,batter_rbis",
                        "oddsFormat": "american"}, timeout=15)
            if r2.status_code != 200: continue
            all_bms = r2.json().get("bookmakers", [])
            # Scan ALL books for both the 0.5 hit odds and the 1.5-line candidates.
            _bm_map = {b.get("key"): b for b in all_bms}
            # Collect 0.5-line Over odds from every bookmaker (first seen per player)
            for bm_any in all_bms:
                for mkt in bm_any.get("markets", []):
                    if mkt.get("key") not in ("batter_hits", "batter_hits_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt is None or side != "Over": continue
                        nk = _norm_name(player)
                        if pt == 0.5 and price is not None:
                            if nk not in HIT_ODDS:
                                HIT_ODDS[nk] = price
                            hit05.setdefault(nk, {"name": player,
                                                  "home_team": home_team,
                                                  "away_team": away_team})
            # Build 1.5-line candidates from ALL books — a player qualifies if ANY
            # book posts his 1.5 line (stops part-time players from blinking in/out
            # based on a single book's coverage). Honor PREFERRED order for the
            # displayed price, and backfill the Under side from a lower-priority
            # book when the preferred book only posts an Over.
            #
            # Aggregation is scoped to THIS event and keyed by normalized name so:
            #   • name-variant spellings across books merge into one candidate, and
            #   • a name appearing in another game can't backfill odds/teams here.
            # Cross-event dedup keeps the first game seen (matches prior behavior).
            ordered_books = ([_bm_map[k] for k in PREFERRED if k in _bm_map]
                             + [b for b in all_bms if b.get("key") not in PREFERRED])
            event_entries: dict = {}
            for book in ordered_books:
                for mkt in book.get("markets", []):
                    if mkt.get("key") not in ("batter_hits", "batter_hits_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 1.5 or price is None: continue
                        nk = _norm_name(player)
                        if nk in seen: continue  # already locked to an earlier game
                        entry = event_entries.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": 1.5,
                                     "home_team": home_team, "away_team": away_team,
                                     "over_odds": None, "under_odds": None}
                            event_entries[nk] = entry
                        if side == "Over" and entry["over_odds"] is None:
                            entry["over_odds"] = price
                        elif side == "Under" and entry["under_odds"] is None:
                            entry["under_odds"] = price
            # Under 1.5 TOTAL BASES odds for the same players, shown alongside the
            # hits line (pays more because a double/HR busts it even on one hit).
            # Same all-books union + PREFERRED order; first Under price seen wins.
            tb_under: dict = {}
            tb_over_map: dict = {}
            for book in ordered_books:
                for mkt in book.get("markets", []):
                    if mkt.get("key") not in ("batter_total_bases", "batter_total_bases_alternate"): continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 1.5 or price is None: continue
                        nk = _norm_name(player)
                        if side == "Under" and nk not in tb_under:
                            tb_under[nk] = price
                        elif side == "Over" and nk not in tb_over_map:
                            tb_over_map[nk] = price
            for nk, entry in event_entries.items():
                entry["tb_under_odds"] = tb_under.get(nk)
                entry["tb_over_odds"]  = tb_over_map.get(nk)
                seen.setdefault(nk, entry)
                if entry.get("tb_under_odds") is not None:
                    TB_ODDS.setdefault(nk, entry)
                if entry.get("tb_over_odds") is not None:
                    TB_OVER_ODDS.setdefault(nk, entry)
            # Batter runs scored (Over/Under, line ~0.5) for the Runs Picks category.
            # Same all-books union + PREFERRED order; first price per side wins, stored
            # in the module-global RUNS_ODDS (parallel to HIT_ODDS), first game seen wins.
            # ZERO extra Odds API calls beyond the one market added to the request above.
            for book in ordered_books:
                for mkt in book.get("markets", []):
                    if mkt.get("key") != "batter_runs_scored": continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        # This is the Over/Under 0.5 ("to score a run") market only.
                        if not player or pt != 0.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = RUNS_ODDS.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": pt,
                                     "home_team": home_team, "away_team": away_team,
                                     "over": None, "under": None}
                            RUNS_ODDS[nk] = entry
                        if side == "Over" and entry["over"] is None:
                            entry["over"] = price; entry["line"] = pt
                        elif side == "Under" and entry["under"] is None:
                            entry["under"] = price

            # Batter RBIs (Over/Under 0.5) for the RBI Picks category.
            # ZERO extra Odds API calls — market added to the same per-game request.
            for book in ordered_books:
                for mkt in book.get("markets", []):
                    if mkt.get("key") != "batter_rbis": continue
                    for oc in mkt.get("outcomes", []):
                        player = oc.get("description", "").strip()
                        pt     = oc.get("point")
                        side   = oc.get("name", "")
                        price  = oc.get("price")
                        if not player or pt != 0.5 or price is None: continue
                        nk = _norm_name(player)
                        entry = RBI_ODDS.get(nk)
                        if entry is None:
                            entry = {"name": player, "line": pt,
                                     "home_team": home_team, "away_team": away_team,
                                     "over": None, "under": None}
                            RBI_ODDS[nk] = entry
                        if side == "Over" and entry["over"] is None:
                            entry["over"] = price; entry["line"] = pt
                        elif side == "Under" and entry["under"] is None:
                            entry["under"] = price

        _log(emit, f"  ✅ {len(seen)} players on 1.5 hits line | {len(HIT_ODDS)} players with 0.5 hit odds | {len(RUNS_ODDS)} with runs odds | {len(TB_ODDS)} with TB under odds | {len(RBI_ODDS)} with RBI odds")
        # Scan ALL players who have any posted hit odds (the 0.5 set), not just
        # the ~57 with a 1.5 line. Players who DO have a 1.5 line keep their
        # Under 1.5 / total-bases odds; 0.5-only players are still evaluated as
        # potential unders but carry no 1.5/TB price (no book posted one). This
        # adds ZERO Odds API calls — both lines come from the per-game odds
        # already fetched above; only the (free) MLB Stats scan grows.
        candidates = list(seen.values())
        for nk, info in hit05.items():
            if nk in seen: continue
            candidates.append({"name": info["name"], "line": 1.5,
                               "home_team": info["home_team"], "away_team": info["away_team"],
                               "over_odds": None, "under_odds": None, "tb_under_odds": None})
        _log(emit, f"  ▸ Scanning {len(candidates)} players for unders (was {len(seen)} on the 1.5 line)")
        return candidates
    except Exception as exc:
        _log(emit, f"⚠️  Odds API error: {exc}")
        return []


def run_under_picks(run_date: str, team_schedule: dict, emit=None,
                    top_era=None, top_era_list=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ Under Picks — Fetching 1.5 hits lines from The Odds API", "section")
    season = int(run_date[:4])

    candidates = _fetch_hits_lines(run_date, emit)
    if not candidates: return []

    _log(emit, "  Loading probable pitchers…")
    pitchers = _get_probable_pitchers(run_date)
    _log(emit, f"  ✅ {len(pitchers)} probable pitchers found")

    _log(emit, "  Building MLB player ID map…")
    _build_player_map(season)
    _log(emit, f"  ✅ {len(_PLAYER_MAP)} active players indexed")

    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid: id_map[c["name"]] = pid

    _log(emit, f"  Looking up teams for {len(id_map)} players…")
    team_map = _get_teams_batch(list(id_map.values()))
    _log(emit, "  ✅ Teams resolved")
    _log(emit, f"  Evaluating {len(candidates)} candidates…")

    # Evaluate candidates in parallel (≤8 threads). Each worker is independent —
    # it does up to 4 MLB Stats API calls with short-circuit filters — and the
    # shared splits _CACHE / id maps are GIL-safe (worst case = harmless dup
    # fetch). Logs are emitted from the main thread as futures complete so the
    # live progress feed stays intact.
    def _eval_candidate(c):
        name      = c["name"]
        home_team = c["home_team"]
        away_team = c["away_team"]
        batter_id   = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team: return None
        if _team_match(player_team, home_team):
            side, opp_name = "HOME", away_team
        elif _team_match(player_team, away_team):
            side, opp_name = "AWAY", home_team
        else:
            return None
        pitcher_name, pitcher_id = "TBD", None
        for pteam, pinfo in pitchers.items():
            if _team_match(pteam, opp_name):
                pitcher_name = pinfo["name"]
                pitcher_id   = pinfo.get("id")
                break
        # Facing a top-30 ERA ace is DISPLAY ONLY — it shows a chip on the card
        # but does NOT bypass or affect any qualification gate.
        def _plast(nm):
            nm = (nm or "").strip()
            if not nm or nm.upper() == "TBD": return ""
            return (nm.split(".")[-1] if "." in nm else nm.split()[-1]).strip().lower()
        p_last = _plast(pitcher_name)
        ace = bool(top_era) and p_last != "" and p_last in top_era
        ace_era = None
        if ace and top_era_list:
            ace_era = next((q["era"] for q in top_era_list
                            if q.get("name", "").lower().endswith(p_last)), None)
        s1 = _get_s1_vs_pitcher(batter_id, pitcher_id)
        # S1: career BA vs today's pitcher. N/A / 0 AB passes. DQ if >= .250.
        if s1["ba"] is not None and s1["ab"] > 0 and s1["ba"] >= 0.250: return None
        # S2: H/A games vs TODAY'S opponent. Data required AND < .250.
        s2 = _last10_ba(batter_id, side, opp_name, 10)
        if s2["ba"] is None or s2["ba"] >= 0.250: return None
        # S3: H/A games vs ANY opponent. Data required AND < .250.
        s3 = _last10_ba(batter_id, side, "", 10)
        if s3["ba"] is None or s3["ba"] >= 0.250: return None
        # L7: last 7 games (general). N/A passes; DQ if >= .250.
        l7 = _get_last7_ba(batter_id)
        if l7["ba"] is not None and l7["ba"] >= 0.250: return None
        # Coldest first: lower under_score ranks higher.
        def _ba(x, fb=0.250): return x["ba"] if x and x["ba"] is not None else fb
        l7_ba = l7["ba"] if l7["ba"] is not None else _ba(s3, _ba(s1))
        under_score = round((_ba(s2) + _ba(s3) + l7_ba) * 1000)
        return {"name": name, "team": player_team, "pos": "—", "side": side, "opp": opp_name,
                "pitcher": pitcher_name, "s1_disp": s1["display"],
                "s1_ab": s1["ab"], "s2": s2, "s3": s3, "l7": l7,
                "lineup_status": "TBD", "under_score": under_score,
                "batter_id": batter_id, "under_basis": "vs-ace" if ace else "recent",
                "ace_era": ace_era,
                "under_odds": c.get("under_odds"), "over_odds": c.get("over_odds"),
                "tb_under_odds": c.get("tb_under_odds")}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval_candidate, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pick = _fut.result()
            except Exception as _exc:
                pick = None
                _log(emit, f"  ⚠️ {_futs[_fut].get('name', '?')} — eval failed: {_exc}")
            if pick:
                picks.append(pick)
                _log(emit, f"  ✅ UNDER: {pick['name']:<22}  S1:{pick['s1_disp']:<14}  S2:{pick['s2']['display']}  S3:{pick['s3']['display']}")

    # Sort coldest first; name as a deterministic tie-breaker since workers now
    # finish out of order.
    picks.sort(key=lambda x: (x["under_score"], x["name"]))
    _log(emit, f"✅ Under Picks: {len(picks)} picks found")
    return picks


# ── Runs Picks (Batter Runs Scored, Over/Under 0.5) ────────────────────────
# A full over/under category mirroring the hit list, driven by how often a batter
# scores a run (H/A vs the opponent, falling back to L10 H/A any opp when there's
# no head-to-head sample). Ranked by the Wilson lower bound of that rate so a proven
# sample outranks a thin lucky one. Odds come from RUNS_ODDS, populated by
# _fetch_hits_lines (no extra Odds API calls).
import math as _math

def _wilson_lb(hits: int, games: int, z: float = 1.96) -> float:
    """Lower bound of a 95% Wilson interval — rewards sample size."""
    if not games:
        return 0.0
    p = hits / games
    den = 1.0 + z * z / games
    centre = p + z * z / (2 * games)
    margin = z * _math.sqrt(p * (1 - p) / games + z * z / (4 * games * games))
    return (centre - margin) / den


def _runs_consistency(player_id, side: str, opp_name: str = "",
                      max_games: int = 10) -> dict:
    """Last max_games career H/A games (5 seasons back) counting games with 1+ run.
       When opp_name is set, restrict to games vs THAT opponent."""
    if not player_id:
        return {"runs_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                runs = int(stat.get("runs", 0) or 0)
                matching.append(1 if runs >= 1 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        runs_games = sum(matching)
        if games == 0:
            return {"runs_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"runs_games": runs_games, "games": games,
                "display": f"{runs_games}/{games}",
                "score": round(runs_games / games * 100)}
    except Exception:
        return {"runs_games": 0, "games": 0, "display": "ERR", "score": 0}


def _runs_rate(player_id, side: str, opp_name: str) -> dict:
    """Runs-scored rate vs THIS opponent (H/A); fall back to L10 H/A any opp when
       there's no head-to-head sample. Returns the consistency dict + a `basis`."""
    vs = _runs_consistency(player_id, side, opp_name, 10)
    if vs["games"] > 0:
        vs["basis"] = "vs opp"
        return vs
    la = _runs_consistency(player_id, side, "", 10)
    la["basis"] = "L10 H/A"
    return la


def _recent_runs_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, runs, hits, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "r":   int(stat.get("runs", 0) or 0),
                    "h":   int(stat.get("hits", 0) or 0),
                    "opp": (sp.get("opponent", {}) or {}).get("name", ""),
                    "ha":  "H" if sp.get("isHome") else "A",
                })
                if len(games) >= n:
                    break
            if len(games) >= n:
                break
        return games
    except Exception:
        return []


# Pick qualifies as OVER when the runs-scored rate is high, UNDER when low.
RUNS_OVER_CUT  = 70   # >= this % → likely to score a run (vs opp)
RUNS_UNDER_CUT = 30   # <= this % → likely NOT to score (vs opp)
RUNS_MIN_GAMES = 3    # minimum head-to-head games vs THIS opponent to qualify
RUNS_TOP_N     = 20   # cap per side (top N overs / top N unders)


def run_runs_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ Runs Picks — Batter Runs Scored (Over/Under 0.5)", "section")
    season = int(run_date[:4])

    if not RUNS_ODDS:
        _fetch_hits_lines(run_date, emit)   # populates RUNS_ODDS as a side effect
    candidates = list(RUNS_ODDS.values())
    if not candidates:
        _log(emit, "  No batter runs-scored lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a runs line")

    _build_player_map(season)
    id_map = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        rate = _runs_rate(batter_id, side, opp_name)
        # Vs-opponent ONLY (no L10 any-opp fallback) and minimum head-to-head games.
        if rate.get("basis") != "vs opp" or rate["games"] < RUNS_MIN_GAMES:
            return None
        score = rate["score"]
        if score >= RUNS_OVER_CUT:
            pick = "OVER"
        elif score <= RUNS_UNDER_CUT:
            pick = "UNDER"
        else:
            return None
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": pick, "line": c.get("line", 0.5),
                "rate_disp": rate["display"], "score": score,
                "games": rate["games"], "basis": rate.get("basis", ""),
                "wilson": round(_wilson_lb(rate["runs_games"], rate["games"]), 4),
                "over_odds": c.get("over"), "under_odds": c.get("under"),
                "batter_id": batter_id,
                "recent_runs_log": _recent_runs_log(batter_id)}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    # OVERs first (highest confidence-adjusted rate), then UNDERs (coldest first).
    picks.sort(key=lambda p: (
        0 if p["pick"] == "OVER" else 1,
        -p["wilson"] if p["pick"] == "OVER" else p["score"],
        -p["games"],
    ))
    # Cap to the top RUNS_TOP_N on each side (overs / unders).
    overs  = [p for p in picks if p["pick"] == "OVER"][:RUNS_TOP_N]
    unders = [p for p in picks if p["pick"] == "UNDER"][:RUNS_TOP_N]
    picks = overs + unders
    _log(emit, f"✅ Runs Picks: {len(picks)} "
               f"({sum(1 for p in picks if p['pick']=='OVER')} over / "
               f"{sum(1 for p in picks if p['pick']=='UNDER')} under)")
    return picks


# ── RBI Picks (Batter RBIs, Over/Under 0.5) ────────────────────────────────
# Full over/under category: OVER when batter drives in runs at ≥70% H/A vs opp,
# UNDER when ≤30%. Vs-opp only (min 3 games). Ranked by Wilson lower bound.
# Odds from RBI_ODDS (batter_rbis market), zero extra Odds API calls.

RBI_OVER_CUT  = 70   # >= this % → likely to drive in a run
RBI_UNDER_CUT = 30   # <= this % → likely NOT to drive in a run
RBI_MIN_GAMES = 3    # minimum head-to-head games vs THIS opponent to qualify
RBI_TOP_N     = 20   # cap per side


def _rbi_consistency(player_id, side: str, opp_name: str = "",
                     max_games: int = 10) -> dict:
    """Last max_games career H/A games counting games with 1+ RBI."""
    if not player_id:
        return {"rbi_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                rbi = int(stat.get("rbi", 0) or 0)
                matching.append(1 if rbi >= 1 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        rbi_games = sum(matching)
        if games == 0:
            return {"rbi_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"rbi_games": rbi_games, "games": games,
                "display": f"{rbi_games}/{games}",
                "score": round(rbi_games / games * 100)}
    except Exception:
        return {"rbi_games": 0, "games": 0, "display": "ERR", "score": 0}


def _rbi_rate(player_id, side: str, opp_name: str) -> dict:
    """RBI rate vs THIS opponent (H/A); vs-opp only (no fallback)."""
    vs = _rbi_consistency(player_id, side, opp_name, 10)
    vs["basis"] = "vs opp"
    return vs


def _recent_rbi_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, rbi, hits, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "rbi": int(stat.get("rbi", 0) or 0),
                    "h":   int(stat.get("hits", 0) or 0),
                    "opp": (sp.get("opponent", {}) or {}).get("name", ""),
                    "ha":  "H" if sp.get("isHome") else "A",
                })
                if len(games) >= n:
                    break
            if len(games) >= n:
                break
        return games
    except Exception:
        return []


def run_rbi_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ RBI Picks — Batter RBIs (Over/Under 0.5)", "section")
    season = int(run_date[:4])

    if not RBI_ODDS:
        _fetch_hits_lines(run_date, emit)   # populates RBI_ODDS as a side effect
    candidates = list(RBI_ODDS.values())
    if not candidates:
        _log(emit, "  No batter RBI lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with an RBI line")

    _build_player_map(season)
    id_map = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        rate = _rbi_rate(batter_id, side, opp_name)
        if rate["games"] < RBI_MIN_GAMES:
            return None
        score = rate["score"]
        if score >= RBI_OVER_CUT:
            pick = "OVER"
        elif score <= RBI_UNDER_CUT:
            pick = "UNDER"
        else:
            return None
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": pick, "line": c.get("line", 0.5),
                "rate_disp": rate["display"], "score": score,
                "games": rate["games"], "basis": rate.get("basis", ""),
                "wilson": round(_wilson_lb(rate["rbi_games"], rate["games"]), 4),
                "over_odds": c.get("over"), "under_odds": c.get("under"),
                "batter_id": batter_id,
                "recent_rbi_log": _recent_rbi_log(batter_id)}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (
        0 if p["pick"] == "OVER" else 1,
        -p["wilson"] if p["pick"] == "OVER" else p["score"],
        -p["games"],
    ))
    overs  = [p for p in picks if p["pick"] == "OVER"][:RBI_TOP_N]
    unders = [p for p in picks if p["pick"] == "UNDER"][:RBI_TOP_N]
    picks = overs + unders
    _log(emit, f"✅ RBI Picks: {len(picks)} "
               f"({sum(1 for p in picks if p['pick']=='OVER')} over / "
               f"{sum(1 for p in picks if p['pick']=='UNDER')} under)")
    return picks


# ─── Total Bases Under ─────────────────────────────────────────────────────
# Players who frequently go Under 1.5 Total Bases (TB < 2 = 0 hits or exactly
# 1 single). TB = hits + doubles + 2*triples + 3*HR.  Picks use the same Odds
# API data already fetched (TB_ODDS populated by _fetch_hits_lines, zero extra
# calls). Only UNDER picks — qualify at ≥TB_UNDER_CUT% of H/A career games.
TB_UNDER_CUT = 70   # % of games with TB < 2 to qualify
TB_MIN_VS    = 2    # minimum games vs THIS opponent (preferred path)
TB_MIN_ANY   = 5    # minimum games any-opp (fallback path)
TB_TOP_N     = 20   # cap (unders only)


def _tb_consistency(player_id, side: str, opp_name: str = "",
                    max_games: int = 10) -> dict:
    """Last max_games career H/A games; count games where total bases < 2."""
    if not player_id:
        return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h  = int(stat.get("hits",     0) or 0)
                d  = int(stat.get("doubles",  0) or 0)
                t  = int(stat.get("triples",  0) or 0)
                hr = int(stat.get("homeRuns", 0) or 0)
                tb = h + d + 2 * t + 3 * hr   # singles×1 + D×2 + T×3 + HR×4
                matching.append(1 if tb < 2 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        tb_games = sum(matching)
        if games == 0:
            return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"tb_games": tb_games, "games": games,
                "display": f"{tb_games}/{games}",
                "score": round(tb_games / games * 100)}
    except Exception:
        return {"tb_games": 0, "games": 0, "display": "ERR", "score": 0}


def _recent_tb_log(player_id, n: int = 5) -> list:
    """Last n games (any opp), newest-first: date, hits, total_bases, opp, H/A."""
    if not player_id:
        return []
    try:
        from mlb_stats_splits import _get_game_logs
        from datetime import date as _dt
        cy = _dt.today().year
        games = []
        for season in range(cy, cy - 2, -1):
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h  = int(stat.get("hits",     0) or 0)
                d  = int(stat.get("doubles",  0) or 0)
                t  = int(stat.get("triples",  0) or 0)
                hr = int(stat.get("homeRuns", 0) or 0)
                tb = h + d + 2 * t + 3 * hr
                games.append({
                    "d":   (sp.get("date") or "")[5:],
                    "h":   h,
                    "tb":  tb,
                    "opp": (sp.get("opponent", {}) or {}).get("name", ""),
                    "ha":  "H" if sp.get("isHome") else "A",
                })
                if len(games) >= n:
                    break
            if len(games) >= n:
                break
        return games
    except Exception:
        return []


def _tb_consistency_over(player_id, side: str, opp_name: str = "",
                         max_games: int = 10) -> dict:
    """Last max_games career H/A games vs opp; count games where total bases >= 2 (OVER)."""
    if not player_id:
        return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
    try:
        from mlb_stats_splits import _get_game_logs, _team_name_match
        from datetime import date as _dt
        cy = _dt.today().year
        seasons = list(range(cy, cy - 5, -1))
        matching = []
        for season in seasons:
            splits = _get_game_logs(player_id, season)
            for sp in reversed(splits):
                is_home = sp.get("isHome", False)
                if (side.upper() == "HOME") != is_home:
                    continue
                if opp_name:
                    opp = sp.get("opponent", {}).get("name", "")
                    if not _team_name_match(opp, opp_name):
                        continue
                stat = sp.get("stat", {})
                ab = int(stat.get("atBats", 0) or 0)
                if ab < 1:
                    continue
                h  = int(stat.get("hits",     0) or 0)
                d  = int(stat.get("doubles",  0) or 0)
                t  = int(stat.get("triples",  0) or 0)
                hr = int(stat.get("homeRuns", 0) or 0)
                tb = h + d + 2 * t + 3 * hr
                matching.append(1 if tb >= 2 else 0)
                if len(matching) >= max_games:
                    break
            if len(matching) >= max_games:
                break
        games = len(matching)
        tb_games = sum(matching)
        if games == 0:
            return {"tb_games": 0, "games": 0, "display": "N/A", "score": 0}
        return {"tb_games": tb_games, "games": games,
                "display": f"{tb_games}/{games}",
                "score": round(tb_games / games * 100)}
    except Exception:
        return {"tb_games": 0, "games": 0, "display": "ERR", "score": 0}


TB_OVER_CUT    = 60   # >= this % → likely to get 1.5+ total bases (vs opp H/A)
TB_OVER_MIN_VS = 3    # minimum head-to-head H/A games vs opponent (no fallback)
TB_OVER_TOP_N  = 20   # cap (overs only)


def run_tb_over_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ TB Over Picks — Batter Total Bases Over 1.5", "section")
    season = int(run_date[:4])

    if not TB_OVER_ODDS:
        _fetch_hits_lines(run_date, emit)
    candidates = list(TB_OVER_ODDS.values())
    if not candidates:
        _log(emit, "  No batter total-bases over lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a TB over line")

    _build_player_map(season)
    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        vs = _tb_consistency_over(batter_id, side, opp_name, 10)
        if vs["games"] < TB_OVER_MIN_VS:
            return None
        if vs["score"] < TB_OVER_CUT:
            return None
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": "OVER", "line": 1.5,
                "rate_disp": vs["display"], "score": vs["score"],
                "games": vs["games"], "basis": "vs opp",
                "wilson": round(_wilson_lb(vs["tb_games"], vs["games"]), 4),
                "tb_over_odds": c.get("tb_over_odds"),
                "batter_id": batter_id,
                "recent_tb_log": _recent_tb_log(batter_id)}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (-p["wilson"], -p["games"]))
    picks = picks[:TB_OVER_TOP_N]
    _log(emit, f"✅ TB Over Picks: {len(picks)} qualifying")
    return picks


def run_tb_under_picks(run_date: str, team_schedule: dict, emit=None) -> list:
    _log(emit, "", "log")
    _log(emit, "▸ TB Under Picks — Batter Total Bases Under 1.5", "section")
    season = int(run_date[:4])

    if not TB_ODDS:
        _fetch_hits_lines(run_date, emit)
    candidates = list(TB_ODDS.values())
    if not candidates:
        _log(emit, "  No batter total-bases under lines posted today.")
        return []
    _log(emit, f"  {len(candidates)} players with a TB under line")

    _build_player_map(season)
    id_map: dict = {}
    for c in candidates:
        pid = _resolve_id(c["name"])
        if pid:
            id_map[c["name"]] = pid
    team_map = _get_teams_batch(list(id_map.values()))

    def _eval(c):
        name = c["name"]
        batter_id = id_map.get(name)
        player_team = team_map.get(batter_id, "") if batter_id else ""
        if not batter_id or not player_team:
            return None
        if _team_match(player_team, c["home_team"]):
            side, opp_name = "HOME", c["away_team"]
        elif _team_match(player_team, c["away_team"]):
            side, opp_name = "AWAY", c["home_team"]
        else:
            return None
        vs = _tb_consistency(batter_id, side, opp_name, 10)
        if vs["games"] >= TB_MIN_VS:
            rate = vs; rate["basis"] = "vs opp"
        else:
            any_opp = _tb_consistency(batter_id, side, "", 10)
            if any_opp["games"] < TB_MIN_ANY:
                return None
            rate = any_opp; rate["basis"] = "L10 H/A"
        if rate["score"] < TB_UNDER_CUT:
            return None
        return {"name": name, "team": player_team, "side": side, "opp": opp_name,
                "pick": "UNDER", "line": 1.5,
                "rate_disp": rate["display"], "score": rate["score"],
                "games": rate["games"], "basis": rate.get("basis", ""),
                "wilson": round(_wilson_lb(rate["tb_games"], rate["games"]), 4),
                "tb_under_odds": c.get("tb_under_odds"),
                "batter_id": batter_id,
                "recent_tb_log": _recent_tb_log(batter_id)}

    picks = []
    with ThreadPoolExecutor(max_workers=8) as _ex:
        _futs = {_ex.submit(_eval, c): c for c in candidates}
        for _fut in as_completed(_futs):
            try:
                pk = _fut.result()
            except Exception:
                pk = None
            if pk:
                picks.append(pk)

    picks.sort(key=lambda p: (-p["wilson"], -p["games"]))
    picks = picks[:TB_TOP_N]
    _log(emit, f"✅ TB Under Picks: {len(picks)} qualifying")
    return picks
