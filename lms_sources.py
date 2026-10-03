"""Bounded, cached public-stat enrichment for LMS, only during host runs.

No paid odds calls and no import-time work. Failed sources leave required
inputs unavailable; opponent allowances are never replaced with own-team stats.
"""
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from lms import number

_CACHE, _LOCK = {}, threading.Lock()


def get_json(url, *, nba=False):
    with _LOCK:
        old = _CACHE.get(url)
        if old and time.monotonic() < old[0]:
            return old[1]
    headers = {"Accept": "application/json"}
    if nba:
        headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://www.nba.com/",
                        "Origin": "https://www.nba.com"})
    try:
        with urlopen(Request(url, headers=headers), timeout=4) as response:
            data = json.loads(response.read())
    except Exception:
        data = None
    with _LOCK:
        _CACHE[url] = (time.monotonic() + (600 if data is not None else 30), data)
    return data


def parallel(urls, *, nba=False):
    with ThreadPoolExecutor(max_workers=8) as pool:
        return list(pool.map(lambda u: get_json(u, nba=nba), urls))


def rank(rows, column, *, descending=True):
    vals = [(key, number(row.get(column))) for key, row in rows.items()]
    vals = [(key, value) for key, value in vals if value is not None]
    ordered = sorted(vals, key=lambda pair: pair[1], reverse=descending)
    out, previous, previous_rank = {}, None, 0
    for i, (key, value) in enumerate(ordered, 1):
        if value != previous:
            previous_rank, previous = i, value
        out[key] = previous_rank
    return out


def nba_table(payload):
    if not isinstance(payload, dict):
        return []
    sets = payload.get("resultSets") or []
    if isinstance(sets, dict):
        sets = [sets]
    out = []
    for group in sets:
        headers = group.get("headers") or []
        for row in group.get("rowSet") or []:
            if len(row) == len(headers):
                out.append(dict(zip(headers, row)))
    return out


def nba_facts(context, games, ds):
    season_start = int(ds[:4]) if int(ds[5:7]) >= 10 else int(ds[:4]) - 1
    params = {"Season": f"{season_start}-{str(season_start + 1)[-2:]}",
              "SeasonType": "Regular Season", "PerMode": "PerGame", "LeagueID": "00",
              "LastNGames": 0, "Month": 0, "OpponentTeamID": 0, "PaceAdjust": "N",
              "PlusMinus": "N", "Rank": "N", "TeamID": 0, "Period": 0,
              "DateTo": ds, "DateFrom": ""}
    base = "https://stats.nba.com/stats/"
    urls = [base + "leaguedashteamstats?" + urlencode({**params, "MeasureType": "Base"}),
            base + "leaguedashteamstats?" + urlencode({**params, "MeasureType": "Opponent"}),
            base + "leaguedashplayerstats?" + urlencode({**params, "MeasureType": "Base"})]
    payloads = parallel(urls, nba=True)
    own = {str(r["TEAM_ID"]): r for r in nba_table(payloads[0]) if r.get("TEAM_ID")}
    opp = {str(r["TEAM_ID"]): r for r in nba_table(payloads[1]) if r.get("TEAM_ID")}
    players = nba_table(payloads[2])
    fga_rank = rank(own, "FGA") if len(own) >= 30 else {}
    fg_rank = rank(own, "FG_PCT") if len(own) >= 30 else {}
    oreb_rank = rank(opp, "OPP_OREB", descending=False) if len(opp) >= 30 else {}
    dreb_rank = rank(opp, "OPP_DREB", descending=False) if len(opp) >= 30 else {}
    leaders = {}
    for p in players:
        tid, reb = str(p.get("TEAM_ID") or ""), number(p.get("REB"))
        if reb is not None and tid and tid != "0":
            leaders[tid] = max(leaders.get(tid, 0), reb)
    # Explicit starter flags in the actual game box are the only automatic
    # starter evidence. A depth projection/minutes average is not confirmation.
    event_games = [g for g in games if g.get("event_id")]
    summaries = parallel(["https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event="
                          + str(g["event_id"]) for g in event_games])
    starters = {}
    for g, response in zip(event_games, summaries):
        if not isinstance(response, dict):
            continue
        ids = set()
        for team in (response.get("boxscore") or {}).get("players") or []:
            for group in team.get("statistics") or []:
                for athlete in group.get("athletes") or []:
                    if athlete.get("starter") is True:
                        ids.add(str((athlete.get("athlete") or {}).get("id") or ""))
        starters[str(g["event_id"])] = ids
    for c in context:
        g = next((g for g in games if str(g.get("event_id")) == c.get("event_id")), {})
        home = c["team"] == g.get("home")
        # ESPN team/player IDs are not NBA Stats IDs. Match exact official
        # full names, then use the provider's own IDs internally.
        from lms_data import name_key
        own_name = g.get("home_name") if home else g.get("away_name")
        opp_name = g.get("away_name") if home else g.get("home_name")
        tid = next((key for key, r in own.items()
                    if name_key(r.get("TEAM_NAME")) == name_key(own_name)), "")
        otid = next((key for key, r in own.items()
                     if name_key(r.get("TEAM_NAME")) == name_key(opp_name)), "")
        facts = c["facts"]
        for ranks, key in ((fga_rank, "opponent_fga_rank"),
                           (fg_rank, "opponent_fg_pct_rank"),
                           (oreb_rank, "opponent_offensive_rebound_allowance_rank"),
                           (dreb_rank, "opponent_defensive_rebound_allowance_rank")):
            if otid in ranks:
                facts[key] = ranks[otid]
        matching = [p for p in players if str(p.get("TEAM_ID")) == tid and
                    name_key(p.get("PLAYER_NAME")) == name_key(c["name"])]
        player = matching[0] if len(matching) == 1 else {}
        reb = number(player.get("REB"))
        if str(player.get("TEAM_ID")) == tid and reb is not None and tid in leaders:
            facts["team_rebound_leader"] = reb == leaders[tid]
        if c["player_id"] in starters.get(c.get("event_id"), set()):
            facts["verified_starter"] = True


def nhl_facts(context, games, ds):
    year = int(ds[:4]) if int(ds[5:7]) >= 7 else int(ds[:4]) - 1
    season = str(year) + str(year + 1)
    base = "https://api.nhle.com/stats/rest/en/"
    param = urlencode({"cayenneExp": "seasonId=" + season + " and gameTypeId=2",
                       "isAggregate": "false", "isGame": "false", "limit": -1})
    summary, penalties, skaters = parallel([base + report + "?" + param
                                            for report in ("team/summary", "team/penalties", "skater/summary")])
    def complete(payload):
        if not isinstance(payload, dict):
            return []
        rows = payload.get("data") or []
        if number(payload.get("total")) is not None and len(rows) < payload["total"]:
            return []
        return rows
    teams = {str(r.get("teamId")): r for r in complete(summary) if r.get("teamId")}
    penalty = {str(r.get("teamId")): r for r in complete(penalties) if r.get("teamId")}
    pp = rank(teams, "powerPlayPct") if len(teams) >= 32 else {}
    pk = rank(teams, "penaltyKillPct") if len(teams) >= 32 else {}
    ga = rank(teams, "goalsAgainstPerGame", descending=False) if len(teams) >= 32 else {}
    pim = rank(penalty, "penaltyMinutesPerGame") if len(penalty) >= 32 else {}
    full_to_id = {str(r.get("teamFullName") or ""): tid for tid, r in teams.items()}
    abbrev_to_id = {}
    for g in games:
        for side in ("home", "away"):
            abbr = g.get(side + "Team")
            tid = full_to_id.get(str(g.get(side + "Full") or ""))
            if abbr and tid:
                abbrev_to_id[abbr] = tid
    player_rows = complete(skaters)
    pts_by_team = {}
    for p in player_rows:
        team = str(p.get("teamAbbrevs") or "")
        # Multi-team aggregates cannot establish current-team scoring rank.
        if not team or "," in team:
            continue
        pid = str(p.get("playerId") or "")
        points = number(p.get("points"))
        if points is not None:
            pts_by_team.setdefault(team, {})[pid] = {"points": points}
    pts_ranks = {team: rank(rows, "points") for team, rows in pts_by_team.items()}
    for c in context:
        facts = c["facts"]
        tid, otid = abbrev_to_id.get(c["team"]), abbrev_to_id.get(c["opponent"])
        for table, key, identity in ((pp, "team_power_play_rank", tid),
                                     (pk, "opponent_penalty_kill_rank", otid),
                                     (ga, "opponent_goals_allowed_rank", otid),
                                     (pim, "opponent_pim_rank", otid)):
            if identity in table:
                facts[key] = table[identity]
        if c["player_id"] in pts_ranks.get(c["team"], {}):
            facts["team_points_rank"] = pts_ranks[c["team"]][c["player_id"]]
        # No invented numerical definition of "high-volume". The member
        # page requests explicit source verification if the host lacks it.


def enrich(sport, candidates, games, ds):
    if not candidates:
        return
    if sport == "nba":
        nba_facts(candidates, games, ds)
    elif sport == "nhl":
        nhl_facts(candidates, games, ds)


def mlb_profiles(ds):
    """Independent quoted-player universe with batch official game logs.

    Does not inherit the host's hitter qualification gates or display caps.
    Missing identities, team membership, dates or stats are not inferred.
    """
    from lms_quotes import load, matches
    from lms_data import MARKETS, name_key, profile
    from lms import timestamp
    quotes = [q for q in load("mlb", ds) if
              MARKETS.get(q["source_market"].replace("_alternate", "")) in
              ("hits", "home_runs", "hits_allowed")]
    if not quotes:
        return []
    base = "https://statsapi.mlb.com/api/v1/"
    people_data, schedule = parallel([
        base + "sports/1/players?season=" + ds[:4] + "&hydrate=currentTeam",
        base + "schedule?sportId=1&hydrate=team&date=" + ds])
    if not isinstance(people_data, dict) or not isinstance(schedule, dict):
        return []
    people = people_data.get("people") or []
    games = [g for d in schedule.get("dates") or [] for g in d.get("games") or []
             if g.get("gameType") in ("R", "F", "D", "L", "W")]
    selected = {}
    for q in quotes:
        found = [p for p in people if matches(p.get("fullName"), q["name"]) and
                 (p.get("currentTeam") or {}).get("id")]
        if len(found) != 1:
            continue
        p = found[0]
        tid = p["currentTeam"]["id"]
        applicable = []
        for g in games:
            h, a = g["teams"]["home"]["team"], g["teams"]["away"]["team"]
            if tid not in (h["id"], a["id"]):
                continue
            if {name_key(h.get("name")), name_key(a.get("name"))} != {
                    name_key(q.get("home_name")), name_key(q.get("away_name"))}:
                continue
            start, quote_start = timestamp(g.get("gameDate")), timestamp(q["game_start"])
            if start and quote_start and abs((start - quote_start).total_seconds()) <= 1800:
                applicable.append(g)
        if len(applicable) == 1:
            selected[(str(p["id"]), str(applicable[0]["gamePk"]))] = (p, applicable[0])
    ids = sorted({pid for pid, gid in selected})
    urls = []
    for season in (int(ds[:4]), int(ds[:4]) - 1):
        for i in range(0, len(ids), 50):
            urls.append(base + "people?" + urlencode({
                "personIds": ",".join(ids[i:i + 50]),
                "hydrate": f"stats(group=[hitting,pitching],type=[gameLog],season={season})"}))
    logs = {}
    for payload in parallel(urls):
        if not isinstance(payload, dict):
            continue
        for person in payload.get("people") or []:
            pid = str(person.get("id") or "")
            for group in person.get("stats") or []:
                if (group.get("type") or {}).get("displayName") != "gameLog":
                    continue
                kind = (group.get("group") or {}).get("displayName")
                for split in group.get("splits") or []:
                    d = str(split.get("date") or "")
                    if len(d) != 10 or d >= ds:
                        continue
                    logs.setdefault((pid, kind), {})[(d, str((split.get("game") or {}).get("gamePk")))] = split
    result = []
    for (pid, gid), (p, g) in selected.items():
        home = (p.get("currentTeam") or {}).get("id") == g["teams"]["home"]["team"]["id"]
        team = g["teams"]["home" if home else "away"]["team"]
        opponent = g["teams"]["away" if home else "home"]["team"]
        for market, group, column in (("hits", "hitting", "hits"),
                                       ("home_runs", "hitting", "homeRuns"),
                                       ("hits_allowed", "pitching", "hits")):
            splits = list(logs.get((pid, group), {}).values())
            if group == "pitching":
                splits = [s for s in splits if number((s.get("stat") or {}).get("gamesStarted")) == 1]
            splits.sort(key=lambda s: str(s.get("date")), reverse=True)
            vals = [number((s.get("stat") or {}).get(column)) for s in splits[:10]]
            vals = [v for v in vals if v is not None]
            row = {"name": p.get("fullName"), "player_id": pid,
                   "team": team.get("abbreviation"), "opponent": opponent.get("abbreviation"),
                   "game_start": g["gameDate"]}
            item = profile(row, market, vals)
            if item:
                item["event_id"] = gid
                item["history_source"] = "MLB official gameLog; pre-date last ten games/starts"
                result.append(item)
    return result