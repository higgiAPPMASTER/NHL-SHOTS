"""Read-only adapters for host data; no odds requests, pipelines or imports run here.

Only real quotes are normalised. Rates are recalculated at each exact line;
standard-line probabilities are never transferred to an alternate threshold.
"""
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from lms import clean, implied, number, timestamp, today


MARKETS = {
    "PTS": "points", "REB": "rebounds", "AST": "assists",
    "POINTS": "points", "SHOTS": "shots", "ASSISTS": "assists",
    "SAVES": "saves", "HITS": "hits", "HR": "home_runs",
    "player_points": "points", "player_rebounds": "rebounds",
    "player_assists": "assists", "player_shots_on_goal": "shots",
    "player_saves": "saves", "batter_hits": "hits",
    "player_total_saves": "saves",
    "batter_home_runs": "home_runs", "pitcher_hits_allowed": "hits_allowed",
    "player_rush_yds": "rushing_yards", "player_reception_yds": "receiving_yards",
    "player_pass_yds": "passing_yards", "player_pass_tds": "passing_tds",
    "player_kicking_points": "kicker_points", "player_rushing_yards": "rushing_yards",
    "player_receiving_yards": "receiving_yards", "player_passing_yards": "passing_yards",
    "player_passing_touchdowns": "passing_tds",
    "rushing_yards": "rushing_yards", "receiving_yards": "receiving_yards",
    "passing_yards": "passing_yards", "passing_tds": "passing_tds",
    "kicker_points": "kicker_points", "hits_allowed": "hits_allowed",
    "points": "points", "shots": "shots", "saves": "saves",
}
LABELS = {"shots on goal": "shots", "points": "points", "points (1+)": "points",
          "assists": "assists", "goalie saves": "saves", "hits": "hits",
          "hits allowed": "hits_allowed", "home runs": "home_runs",
          "rushing yards": "rushing_yards", "receiving yards": "receiving_yards",
          "passing yards": "passing_yards", "passing touchdowns": "passing_tds",
          "kicking points": "kicker_points"}


def name_key(value):
    raw = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    s = re.sub(r"[^\w\s]", "", raw.lower())
    s = re.sub(r"\s+(jr|sr|ii|iii|iv|v)$", "", s.strip())
    return re.sub(r"\s+", " ", s).strip()


def first(row, *keys):
    for key in keys:
        if row.get(key) is not None and row.get(key) != "":
            return row[key]
    return None


def market_of(row):
    for key in ("market", "stat", "stat_key", "mkt", "label", "stat_label"):
        value = str(row.get(key) or "")
        if value in MARKETS:
            return MARKETS[value]
        if value.lower() in LABELS:
            return LABELS[value.lower()]
    return None


def values(rows, ds, stat=None):
    out = []
    for row in rows or []:
        if isinstance(row, dict):
            d = str(first(row, "d", "date", "gameDate") or "")[:10]
            if re.match(r"^\d{4}-\d{2}-\d{2}$", d) and d >= ds:
                continue
            v = first(row, "v", stat or "_unused", "value")
        else:
            v = row
        v = number(v)
        if v is not None:
            out.append(v)
    return out


def probability(vs, line, side):
    decisive = [v for v in vs if v != line]
    if not decisive:
        return None, 0
    hits = sum(v > line if side == "OVER" else v < line for v in decisive)
    return hits / len(decisive), len(decisive)


def append_quotes(out, row, market, history, ds, game=None, facts=None):
    if not market or not isinstance(row, dict):
        return
    game = game or {}
    name = str(first(row, "name", "full_name", "player") or "")
    pid = first(row, "pid", "player_id", "batter_id", "pitcher_id")
    team = str(first(row, "team", "team_abbr") or "")
    start = first(row, "game_start", "tipoff", "startTime") or first(game, "startTime", "tipoff", "start", "game_start")
    if not name or not start:
        return
    if not timestamp(start):
        return
    start = timestamp(start).isoformat()
    event = str(first(game, "event_id", "gameId", "id", "gamePk") or
                first(row, "event_id", "game_id", "gamePk") or "")
    identity = str(pid or name_key(name)) + "|" + team + "|" + str(start)
    ln = number(first(row, "realLine", "line", "dk_line", "fd_line"))
    if ln is None:
        return
    for side in ("OVER", "UNDER"):
        price = first(row, "over_odds", "realOdds", "dk_over_odds") if side == "OVER" else first(row, "under_odds", "realUnderOdds", "dk_under_odds")
        book = first(row, "over_book", "odds_book", "bookmaker_label", "bookmaker") if side == "OVER" else first(row, "under_book", "bookmaker_label", "bookmaker")
        # Single-side native quotes are accepted only for their explicit side.
        explicit = str(first(row, "pick", "dir", "side", "line_rec") or "").upper()
        if not book and explicit == side:
            book = row.get("book")
        if price is None and explicit == side:
            price = first(row, "odds", "hit_odds" if side == "OVER" else "under_odds")
        if implied(price) is None or not book or name_key(book).replace(" ", "") in (
                "oddsapi", "theoddsapi", "model", "na", "unpriced", "unknown"):
            continue
        rate, sample = probability(history, ln, side)
        if rate is None:
            continue
        fs = {**(facts or {})}
        avg = sum(history) / len(history) if history else None
        if avg is not None:
            fs["line_below_average"] = ln < avg
        if number(fs.get("carry_ypc_baseline")) is not None:
            fs["baseline_below_line"] = fs["carry_ypc_baseline"] < ln
        if number(fs.get("rush_projection")) is not None:
            fs["rush_projection_above_line"] = fs["rush_projection"] > ln
        out.append({"entity_id": identity, "name": name, "player_id": str(pid or ""),
                    "team": team, "opponent": str(first(row, "opponent", "opp", "opp_name") or ""),
                    "game_start": str(start), "event_id": event, "market": market,
                    "side": side, "line": ln, "odds": number(price), "book": str(book),
                    "probability": rate, "sample_size": sample,
                    "probability_kind": "Exact-line pre-game historical hit rate; not calibrated",
                    "history_values": history, "facts": fs})


def game_for(team, games, start=None):
    for game in games or []:
        teams = [str(game.get(k) or "") for k in
                 ("home", "away", "homeTeam", "awayTeam", "home_abbr", "away_abbr")]
        if team not in teams:
            continue
        if start:
            gs = first(game, "startTime", "tipoff", "start", "game_start")
            if gs and timestamp(gs) != timestamp(start):
                continue
        return game
    return {}


def nba_prepare(result, local, ds):
    out = []
    games = result.get("games") or local.get("games") or []
    logs = local.get("logs_by_player") or {}
    rosters = local.get("rosters") or {}
    props = list(local.get("odds_props") or [])
    roster = {str(p["id"]): {**p, "team_id": str(tid)}
              for tid, rows in rosters.items() for p in rows}
    by_name = {name_key(p["name"]): (pid, p) for pid, p in roster.items()}
    for quote in props:
        match = by_name.get(name_key(quote.get("player")))
        if not match:
            continue
        pid, player = match
        game = next((g for g in games if player["team_id"] in
                     (str(g.get("home_id")), str(g.get("away_id")))), {})
        team = game.get("home") if player["team_id"] == str(game.get("home_id")) else game.get("away")
        opponent = game.get("away") if team == game.get("home") else game.get("home")
        stat = quote.get("stat")
        raw = [g for g in logs.get(pid, logs.get(player.get("id"), []))
               if str(g.get("date") or "")[:10] < ds and
               (not g.get("player_team") or g["player_team"] == team)]
        raw.sort(key=lambda g: str(g.get("date") or ""), reverse=True)
        venue = "Home" if team == game.get("home") else "Away"
        versus = [g for g in raw if g.get("opp") == opponent and g.get("location") == venue][:10]
        history = values(versus or [g for g in raw if g.get("location") == venue][:10], ds, stat)
        append_quotes(out, {**quote, "name": quote["player"], "player_id": pid,
                           "team": team, "opponent": opponent}, MARKETS.get(stat), history, ds, game)
    if not out:
        # New boards retain an uncapped LMS context. This is only an old-cache
        # compatibility path, visibly labelled, not a purported complete pool.
        for p in result.get("all_picks") or []:
            history = values(p.get("glog") or p.get("recent_glog"), ds)
            append_quotes(out, p, market_of(p), history, ds, game_for(p.get("team"), games))
    return out


def nfl_prepare(result, local, ds):
    out = []
    games = local.get("espn_games") or result.get("games") or []
    if not isinstance(games, list):
        games = []
    frame = next((v for v in local.values() if
                  hasattr(v, "columns") and "player_id" in v.columns and
                  "opponent_team" in v.columns and "season" in v.columns), None)
    teams, league = {}, None
    if frame is not None and not frame.empty:
        try:
            columns = [c for c in ("opponent_team", "week", "season", "season_type",
                                   "rushing_yards", "carries", "passing_yards",
                                   "completions", "passing_tds") if c in frame.columns]
            league = frame[columns].copy()
            if "season_type" in league:
                league = league[league["season_type"] == "REG"]
            league = league[league["season"] == league["season"].max()]
            numeric = ["rushing_yards", "carries", "passing_yards", "completions", "passing_tds"]
            cols = [c for c in numeric if c in league.columns]
            grouped = league.groupby(["opponent_team", "week"])[cols].sum()
            average = grouped.groupby("opponent_team").mean()
            sums = grouped.groupby("opponent_team").sum()
            for team, row in average.iterrows():
                facts = {}
                if len(average) >= 32:
                    if "rushing_yards" in average:
                        facts["rush_defense_rank"] = int(average["rushing_yards"].rank(method="min")[team])
                    if "passing_yards" in average:
                        facts["pass_defense_rank"] = int(average["passing_yards"].rank(method="min")[team])
                if "passing_tds" in average:
                    facts["opponent_passing_td_per_game"] = number(row["passing_tds"])
                if "completions" in sums and sums.loc[team, "completions"] > 0:
                    facts["opponent_yards_per_completion"] = float(sums.loc[team, "passing_yards"] / sums.loc[team, "completions"])
                if "carries" in sums and sums.loc[team, "carries"] > 0:
                    facts["opponent_rush_ypc"] = float(sums.loc[team, "rushing_yards"] / sums.loc[team, "carries"])
                teams[str(team)] = facts
        except Exception:
            teams = {}
    for p in result.get("all") or []:
        market = market_of(p)
        if not market:
            continue
        facts = {"position": p.get("position")}
        facts.update(teams.get(str(p.get("opponent")), {}))
        if p.get("rookieVerified") is True:
            facts["verified_nonrookie"] = p.get("isRookie") is False
        if p.get("depthChart") and number(p.get("depthRank")) is not None:
            facts["verified_starter"] = number(p["depthRank"]) == 1
        history = []
        if frame is not None and p.get("pid") and market in frame.columns:
            try:
                rows = frame[(frame["player_id"].astype(str) == str(p["pid"])) &
                             (frame["recent_team"] == p.get("team"))]
                if p.get("position") and "season_type" in rows and ds[5:7] not in ("01", "02"):
                    rows = rows[rows["season_type"] == "REG"]
                rows = rows.sort_values(["season", "week"], ascending=False)
                versus = rows[rows["opponent_team"] == p.get("opponent")]
                history = [float(v) for v in (versus if not versus.empty else rows).head(10)[market].dropna()]
                if not rows.empty:
                    latest = rows[rows["season"] == rows["season"].max()]
                    if "target_share" in latest:
                        facts["target_share"] = number(latest["target_share"].dropna().mean())
                    if "carries" in latest and latest["carries"].sum() > 0 and market == "rushing_yards":
                        carries = float(latest["carries"].mean())
                        own_ypc = float(latest["rushing_yards"].sum() / latest["carries"].sum())
                        baseline = carries * own_ypc
                        facts["carry_ypc_baseline"] = baseline
                        line = number(p.get("line"))
                        if line is not None:
                            facts["baseline_below_line"] = baseline < line
                            if facts.get("opponent_rush_ypc") is not None:
                                facts["rush_projection"] = carries * (own_ypc + facts["opponent_rush_ypc"]) / 2
                                facts["rush_projection_above_line"] = facts["rush_projection"] > line
            except Exception:
                history = []
        source_history = [g for g in (p.get("vs_opp_log") or [])
                          if g.get("ha") == p.get("homeRoad")][:10]
        source_history = source_history or [g for g in (p.get("glog") or [])
                                            if g.get("ha") == p.get("homeRoad")][:10]
        native_history = values(source_history, ds)
        if native_history:
            history = native_history
        elif not history:
            history = values(first(p, "glog", "recentLog", "vsOppLog", "logB") or [], ds)
        append_quotes(out, p, market, history, ds, game_for(p.get("team"), games, p.get("game_start")), facts)
    return out


def nhl_prepare(result, local, ds):
    out = []
    games = result.get("games") or []
    # Full quote-bearing pre-cap arrays, not the board's ten or twenty rows.
    sources = []
    for key in ("results_raw", "pts_all", "pts_unders", "saves_all", "saves_unders"):
        sources.extend(p for p in (local.get(key) or []) if isinstance(p, dict))
    if not sources:
        for key in ("picks", "rest", "ptsPicks", "ptsRest", "ptsUnders", "ptsUndersRest",
                    "savesPicks", "savesRest", "savesUnders", "savesUndersRest"):
            sources.extend(result.get(key) or [])
    for p in sources:
        market = market_of(p)
        if market not in ("shots", "points", "saves"):
            continue
        profile = next((r for r in result.get("playerProfiles") or []
                        if r.get("pid") == p.get("pid") and market_of(r) == market), {})
        history = values(profile.get("glog") or p.get("glog") or p.get("recentLog") or [], ds)
        facts = {}
        if market == "saves":
            # These arrays already obey the host's confirmed-starter rule.
            facts["verified_starter"] = True
        append_quotes(out, p, market, history, ds, game_for(p.get("team"), games), facts)
    return out


def mlb_prepare(result, local, ds):
    out = []
    # Never relabel a TB/other Under as a Hits Under merely because the row
    # has under_odds. Batter candidates come from exact raw market tuples.
    for key, bucket in (result.get("pitcher_props") or {}).items():
        if key != "pitcher_hits_allowed":
            continue
        for p in (bucket.get("all") or bucket.get("picks") or []):
            history = values(p.get("recent_log") or p.get("vs_opp_log") or [], ds)
            append_quotes(out, p, "hits_allowed", history, ds)
    return out


def profile(row, market, history, games=None):
    name = str(first(row, "name", "full_name", "player") or "")
    pid = str(first(row, "pid", "player_id", "batter_id", "pitcher_id") or "")
    team = str(first(row, "team", "team_abbr") or "")
    game = game_for(team, games or [], row.get("game_start"))
    start = first(row, "game_start", "tipoff") or first(game, "startTime", "tipoff", "start")
    if not name or not history or not start:
        return None
    if not timestamp(start):
        return None
    start = timestamp(start).isoformat()
    return {"entity_id": str(pid or name_key(name)) + "|" + team + "|" + str(start),
            "name": name, "player_id": pid, "team": team,
            "opponent": str(first(row, "opponent", "opp", "opp_name") or ""),
            "market": market, "game_start": str(start),
            "event_id": str(first(game, "event_id", "gameId", "id", "gamePk") or ""),
            "history_values": history, "facts": {},
            "probability_kind": "Exact-line pre-game historical hit rate; not calibrated"}


def unpriced_profiles(sport, result, local, ds, candidates=None):
    out = []
    if sport == "nfl":
        # Include the complete analysed pool, not the display slice.
        out.extend(candidates or [])
        for p in result.get("all") or []:
            market = market_of(p)
            history = values(p.get("glog") or [], ds)
            item = profile(p, market, history, local.get("espn_games") or [])
            if item and market:
                item["facts"]["position"] = p.get("position")
                if not any(c["entity_id"] == item["entity_id"] and c["market"] == market for c in out):
                    out.append(item)
    elif sport == "nhl":
        for p in result.get("playerProfiles") or []:
            market = market_of(p)
            if market not in ("shots", "points", "saves"):
                continue
            item = profile(p, market, values(p.get("glog") or [], ds), result.get("games"))
            if item:
                out.append(item)
    elif sport == "nba":
        logs = local.get("logs_by_player") or {}
        for tid, players in (local.get("rosters") or {}).items():
            game = next((g for g in result.get("games") or [] if str(tid) in
                         (str(g.get("home_id")), str(g.get("away_id")))), {})
            home = str(tid) == str(game.get("home_id"))
            team, opponent = game.get("home" if home else "away"), game.get("away" if home else "home")
            for p in players:
                pid = str(p.get("id") or "")
                raw = [g for g in logs.get(pid, logs.get(p.get("id"), []))
                       if str(g.get("date") or "")[:10] < ds and
                       (not g.get("player_team") or g["player_team"] == team)]
                raw.sort(key=lambda g: str(g.get("date") or ""), reverse=True)
                ha = "Home" if home else "Away"
                versus = [g for g in raw if g.get("opp") == opponent and g.get("location") == ha][:10]
                window = versus or [g for g in raw if g.get("location") == ha][:10]
                row = {"name": p.get("name"), "player_id": pid, "team": team,
                       "opponent": opponent, "tipoff": game.get("tipoff")}
                for stat in ("PTS", "REB", "AST"):
                    item = profile(row, MARKETS[stat], values(window, ds, stat), [game])
                    if item:
                        out.append(item)
    elif sport == "mlb" and ds >= today():
        from lms_sources import mlb_profiles
        out.extend(mlb_profiles(ds))
    return out


def prepare(sport, result, namespace, local):
    ds = str(result.get("date") or local.get("target_date") or today())[:10]
    if result.get("_lms_context") and not local:
        return result["_lms_context"]
    official = not bool(result.get("simulation") or result.get("historical_replay") or
                        result.get("historical_snapshot") or result.get("preseason"))
    if sport == "nhl":
        official = official and result.get("officialCaptureAllowed") is True
    if sport == "nfl":
        games = local.get("espn_games") or []
        official = official and local.get("capture_official") is not False and bool(games) and all(
            str(g.get("lms_season_type")) in ("2", "3") for g in games)
    if sport == "nba":
        official = official and bool(result.get("games")) and all(
            str(g.get("lms_season_type")) in ("2", "3") for g in result.get("games") or [])
    candidates = {"nba": nba_prepare, "nfl": nfl_prepare,
                  "nhl": nhl_prepare, "mlb": mlb_prepare}[sport](result, local, ds)
    from lms_quotes import augment, persist
    from lms_sources import enrich
    profiles = unpriced_profiles(sport, result, local, ds, candidates)
    candidates = augment(sport, ds, candidates, profiles)
    if official and ds >= today():
        games = result.get("games") or local.get("espn_games") or []
        if not isinstance(games, list):
            games = []
        enrich(sport, candidates, games, ds)
    persist(sport, ds)
    # Keep the raw source pool in the sport's existing snapshot, so reloads
    # never need to rerun a pipeline merely to render/rebuild LMS.
    context = clean({"official": official, "candidates": candidates, "profiles": profiles,
                     "note": "LMS uses uncapped native data where available. Missing exact quotes, "
                     "history or verified specialist inputs are explicitly withheld."})
    result["_lms_context"] = context
    return context