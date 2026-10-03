"""Authoritative, final-only LMS grading. Missing statistics stay pending.

Read requests are cached and grouped by event. Nothing runs at import time.
Actual parlays are not manufactured or settled as independent legs here.
"""
from datetime import datetime, timezone
from lms import number
from lms_data import name_key
from lms_sources import get_json


def team_key(value):
    s = str(value or "").upper()
    return {"LA": "LAR", "WAS": "WSH", "AZ": "ARI", "CWS": "CHW",
            "GS": "GSW", "NY": "NYK", "NO": "NOP", "SA": "SAS"}.get(s, s)


def games(sport, ds):
    out = []
    if sport in ("nba", "nfl"):
        path = "basketball/nba" if sport == "nba" else "football/nfl"
        data = get_json("https://site.api.espn.com/apis/site/v2/sports/" + path +
                        "/scoreboard?dates=" + ds.replace("-", ""))
        if not isinstance(data, dict):
            return None
        for event in data.get("events") or []:
            competition = next(iter(event.get("competitions") or []), {})
            home = next((c for c in competition.get("competitors") or [] if c.get("homeAway") == "home"), {})
            away = next((c for c in competition.get("competitors") or [] if c.get("homeAway") == "away"), {})
            status = ((event.get("status") or {}).get("type") or {})
            out.append({"id": str(event.get("id") or ""),
                        "home": team_key((home.get("team") or {}).get("abbreviation")),
                        "away": team_key((away.get("team") or {}).get("abbreviation")),
                        "home_name": (home.get("team") or {}).get("displayName"),
                        "away_name": (away.get("team") or {}).get("displayName"),
                        "home_score": number(home.get("score")), "away_score": number(away.get("score")),
                        "final": status.get("completed") is True,
                        "void": status.get("name") in ("STATUS_POSTPONED", "STATUS_CANCELED", "STATUS_CANCELLED")})
    elif sport == "mlb":
        data = get_json("https://statsapi.mlb.com/api/v1/schedule?sportId=1&hydrate=team&date=" + ds)
        if not isinstance(data, dict):
            return None
        for day in data.get("dates") or []:
            for game in day.get("games") or []:
                if game.get("gameType") not in ("R", "F", "D", "L", "W"):
                    continue
                home = game["teams"]["home"]
                away = game["teams"]["away"]
                state = (game.get("status") or {}).get("detailedState")
                out.append({"id": str(game.get("gamePk") or ""),
                            "home": team_key(home["team"].get("abbreviation")),
                            "away": team_key(away["team"].get("abbreviation")),
                            "home_name": home["team"].get("name"), "away_name": away["team"].get("name"),
                            "home_score": number(home.get("score")), "away_score": number(away.get("score")),
                            "final": (game.get("status") or {}).get("abstractGameState") == "Final",
                            "void": state in ("Postponed", "Cancelled", "Canceled")})
    else:
        data = get_json("https://api-web.nhle.com/v1/schedule/" + ds)
        if not isinstance(data, dict):
            return None
        for day in data.get("gameWeek") or []:
            if day.get("date") != ds:
                continue
            for game in day.get("games") or []:
                if game.get("gameType") not in (2, 3):
                    continue
                home, away = game["homeTeam"], game["awayTeam"]
                out.append({"id": str(game.get("id") or ""),
                            "home": home.get("abbrev"), "away": away.get("abbrev"),
                            "home_score": number(home.get("score")), "away_score": number(away.get("score")),
                            "final": game.get("gameState") in ("OFF", "FINAL"),
                            "void": game.get("gameState") in ("PPD", "CNCL")})
    return out


def resolve(row, slate):
    direct = [g for g in slate if g["id"] == str(row.get("event_id"))]
    if len(direct) == 1:
        return direct[0]
    team, opponent = team_key(row.get("team")), team_key(row.get("opponent"))
    matches = [g for g in slate if
               ({team, opponent} == {g["home"], g["away"]}) or
               ({name_key(row.get("team")), name_key(row.get("opponent"))} ==
                {name_key(g.get("home_name")), name_key(g.get("away_name"))})]
    # Doubleheaders without an authoritative event id are never guessed.
    return matches[0] if len(matches) == 1 else None


def stat_value(value):
    if isinstance(value, str):
        value = value.strip()
        if "/" in value:
            value = value.split("/")[0]
    return number(value)


def espn_players(sport, gid):
    path = "basketball/nba" if sport == "nba" else "football/nfl"
    data = get_json("https://site.api.espn.com/apis/site/v2/sports/" + path + "/summary?event=" + gid)
    if not isinstance(data, dict):
        return None
    out = {}
    for team in (data.get("boxscore") or {}).get("players") or []:
        abbr = team_key((team.get("team") or {}).get("abbreviation"))
        for group in team.get("statistics") or []:
            label = str(group.get("name") or "").lower()
            labels = [str(v).upper().replace(" ", "") for v in
                      (group.get("labels") or group.get("names") or [])]
            for row in group.get("athletes") or []:
                athlete = row.get("athlete") or {}
                pid = str(athlete.get("id") or "")
                name = name_key(athlete.get("displayName"))
                if not pid or not name:
                    continue
                player = out.setdefault(pid, {"name": name, "team": abbr, "stats": {}, "dnp": False})
                if row.get("didNotPlay") is True or row.get("active") is False:
                    player["dnp"] = True
                raw = row.get("stats") or []
                fields = {key: raw[i] for i, key in enumerate(labels) if i < len(raw)}
                if sport == "nba":
                    for key, market in (("PTS", "points"), ("REB", "rebounds"), ("AST", "assists")):
                        val = stat_value(fields.get(key))
                        if val is not None:
                            player["stats"][market] = val
                else:
                    pairs = []
                    if label == "passing":
                        pairs = [("YDS", "passing_yards"), ("TD", "passing_tds")]
                    elif label == "rushing":
                        pairs = [("YDS", "rushing_yards")]
                    elif label == "receiving":
                        pairs = [("YDS", "receiving_yards")]
                    elif label == "kicking":
                        fg, xp = stat_value(fields.get("FG")), stat_value(fields.get("XP"))
                        if fg is not None and xp is not None:
                            player["stats"]["kicker_points"] = 3 * fg + xp
                    for key, market in pairs:
                        val = stat_value(fields.get(key))
                        if val is not None:
                            player["stats"][market] = val
    return out if out else None


def nhl_players(gid):
    data = get_json("https://api-web.nhle.com/v1/gamecenter/" + gid + "/boxscore")
    if not isinstance(data, dict) or not isinstance(data.get("playerByGameStats"), dict):
        return None
    out = {}
    for side in ("homeTeam", "awayTeam"):
        abbr = (data.get(side) or {}).get("abbrev")
        for category, players in (data["playerByGameStats"].get(side) or {}).items():
            if not isinstance(players, list):
                continue
            for p in players:
                pid = str(p.get("playerId") or "")
                if not pid:
                    continue
                raw_name = p.get("name") or {}
                name = raw_name.get("default", "") if isinstance(raw_name, dict) else raw_name
                stats = {}
                for source, market in (("sog", "shots"), ("points", "points"), ("saves", "saves")):
                    val = stat_value(p.get(source))
                    if val is not None:
                        stats[market] = val
                if "points" not in stats and number(p.get("goals")) is not None and number(p.get("assists")) is not None:
                    stats["points"] = number(p["goals"]) + number(p["assists"])
                if category == "goalies" and "saves" not in stats:
                    saves = stat_value(p.get("saveShotsAgainst"))
                    if saves is not None:
                        stats["saves"] = saves
                out[pid] = {"name": name_key(name), "team": abbr, "stats": stats,
                            "dnp": p.get("toi") == "00:00"}
    return out if out else None


def mlb_players(gid):
    data = get_json("https://statsapi.mlb.com/api/v1/game/" + gid + "/boxscore")
    if not isinstance(data, dict) or not isinstance(data.get("teams"), dict):
        return None
    out = {}
    for team in data["teams"].values():
        abbr = team_key((team.get("team") or {}).get("abbreviation"))
        if not isinstance(team.get("players"), dict):
            return None
        batters, pitchers = set(map(str, team.get("batters") or [])), set(map(str, team.get("pitchers") or []))
        for p in team["players"].values():
            person = p.get("person") or {}
            pid = str(person.get("id") or "")
            if not pid:
                continue
            raw = p.get("stats") or {}
            batting, pitching = raw.get("batting") or {}, raw.get("pitching") or {}
            stats = {}
            for source, market in (("hits", "hits"), ("homeRuns", "home_runs")):
                val = number(batting.get(source))
                if val is not None:
                    stats[market] = val
            val = number(pitching.get("hits"))
            if val is not None:
                stats["hits_allowed"] = val
            out[pid] = {"name": name_key(person.get("fullName")), "team": abbr, "stats": stats,
                        "dnp_markets": {"hits": pid not in batters, "home_runs": pid not in batters,
                                        "hits_allowed": pid not in pitchers}, "dnp": False}
    return out if out else None


def actual_value(sport, row, game, cache):
    market = row["market"]
    if market in ("total_goals", "total_runs", "total_points", "team_runs"):
        h, a = game["home_score"], game["away_score"]
        if h is None or a is None:
            return None, False
        if market == "team_runs":
            team = team_key(row.get("team"))
            return (h if team == game["home"] else a if team == game["away"] else None), False
        return h + a, False
    if market == "first_period_goals":
        data = get_json("https://api-web.nhle.com/v1/gamecenter/" + game["id"] + "/landing")
        scoring = (data or {}).get("summary", {}).get("scoring")
        if not isinstance(scoring, list):
            return None, False
        first = next((p for p in scoring if (p.get("periodDescriptor") or {}).get("number") == 1), None)
        # Empty/missing period is not blindly converted to zero.
        return len(first["goals"]) if first and isinstance(first.get("goals"), list) else None, False
    gid = game["id"]
    if gid not in cache:
        cache[gid] = (espn_players(sport, gid) if sport in ("nfl", "nba")
                      else nhl_players(gid) if sport == "nhl" else mlb_players(gid))
    players = cache[gid]
    if players is None:
        return None, False
    player = players.get(str(row.get("player_id") or ""))
    if not player:
        matches = [p for p in players.values()
                   if p["name"] == name_key(row.get("name")) and
                   (not row.get("team") or team_key(p.get("team")) == team_key(row["team"]))]
        player = matches[0] if len(matches) == 1 else None
    if not player:
        return None, False
    dnp = player.get("dnp") or player.get("dnp_markets", {}).get(market, False)
    return player["stats"].get(market), bool(dnp)


def settle_rows(sport, rows):
    slates, boxes = {}, {}
    for frozen in rows:
        row = dict(frozen)
        if row.get("result") in ("WIN", "LOSS", "PUSH", "VOID"):
            continue
        ds = row["date"]
        if ds not in slates:
            slates[ds] = games(sport, ds)
        slate = slates[ds]
        game = resolve(row, slate) if slate is not None else None
        row["last_grade_attempt"] = datetime.now(timezone.utc).isoformat()
        if not game:
            row["pending_reason"] = "Authoritative event unavailable or ambiguous; no zero/DNP was inferred."
            yield row
            continue
        if game["void"]:
            row.update(result="VOID", actual=None, profit=0, pending_reason=None)
        elif not game["final"]:
            row["pending_reason"] = "Game not officially final."
            yield row
            continue
        else:
            actual, dnp = actual_value(sport, row, game, boxes)
            if dnp:
                row.update(result="VOID", actual=None, profit=0, pending_reason=None)
            elif actual is None:
                row["pending_reason"] = "Final game but required stat/participation evidence is unavailable."
                yield row
                continue
            else:
                line, side = row["line"], row["side"]
                result = "PUSH" if actual == line else (
                    "WIN" if (actual > line if side == "OVER" else actual < line) else "LOSS")
                odds, stake = number(row.get("odds")), number(row.get("stake")) or 1
                profit = (0 if result == "PUSH" else -stake if result == "LOSS" else
                          stake * (100 / -odds if odds < 0 else odds / 100)) if odds else None
                row.update(result=result, actual=actual, profit=profit, pending_reason=None)
        row["graded_at"] = row["last_grade_attempt"]
        row["grading_source"] = "Official final box score/schedule"
        yield row