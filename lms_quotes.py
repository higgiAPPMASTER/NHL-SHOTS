"""Independent exact-quote collection from requests the host already makes.

No HTTP calls, no import-time jobs, and no changes to existing line selection.
"""
import json
import threading
from pathlib import Path
from lms import clean, implied, number, timestamp

_LOCK = threading.RLock()
_DATA = {}
ROOT = Path(__file__).parent / ".lms_quote_cache"


def collect(sport, ds, payload):
    if not isinstance(payload, dict):
        return
    start = payload.get("commence_time")
    if not timestamp(start):
        return
    rows = []
    for book in payload.get("bookmakers") or []:
        book_name = book.get("title") or book.get("key")
        if not book_name:
            continue
        for market in book.get("markets") or []:
            market_key = market.get("key") or ""
            for outcome in market.get("outcomes") or []:
                side = str(outcome.get("name") or "").upper()
                ln, odds = number(outcome.get("point")), number(outcome.get("price"))
                if side not in ("OVER", "UNDER") or ln is None or implied(odds) is None:
                    continue
                rows.append({"name": outcome.get("description") or "",
                             "source_market": market_key, "side": side,
                             "line": ln, "odds": odds, "book": book_name,
                             "game_start": start, "quote_event_id": payload.get("id"),
                             "home_name": payload.get("home_team"), "away_name": payload.get("away_team")})
    with _LOCK:
        key = (sport, str(ds)[:10])
        saved = _DATA.setdefault(key, {})
        for row in rows:
            identity = "|".join(map(str, (row["quote_event_id"], row["name"],
                                         row["source_market"], row["side"], row["line"], row["book"])))
            saved[identity] = row
        dates = sorted(k for k in _DATA if k[0] == sport)
        for old in dates[:-7]:
            del _DATA[old]


def load(sport, ds):
    with _LOCK:
        rows = _DATA.get((sport, ds))
        if rows is not None:
            return list(rows.values())
    path = ROOT / f"{sport}_{ds}.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, list) else []
    except (OSError, ValueError):
        return []


def persist(sport, ds):
    rows = load(sport, ds)
    if not rows:
        return
    ROOT.mkdir(exist_ok=True)
    path = ROOT / f"{sport}_{ds}.json"
    tmp = path.with_suffix(".tmp")
    with _LOCK:
        tmp.write_text(json.dumps(clean(rows), allow_nan=False), encoding="utf-8")
        tmp.replace(path)


def matches(a, b):
    from lms_data import name_key
    a, b = name_key(a), name_key(b)
    if a == b:
        return True
    aa, bb = a.split(), b.split()
    return bool(len(aa) >= 2 and len(bb) >= 2 and aa[-1] == bb[-1] and
                aa[0][0] == bb[0][0] and (len(aa[0]) == 1 or len(bb[0]) == 1))


def augment(sport, ds, candidates, profiles=None):
    from lms_data import MARKETS, probability
    result = list(candidates)
    bases = list(candidates) + list(profiles or [])
    for q in load(sport, ds):
        raw = q["source_market"].replace("_alternate", "")
        market = MARKETS.get(raw)
        if not market or not q.get("name"):
            continue
        possible = [c for c in bases if c["market"] == market and
                    matches(c["name"], q["name"]) and
                    timestamp(c["game_start"]) and timestamp(q["game_start"]) and
                    abs((timestamp(c["game_start"]) - timestamp(q["game_start"])).total_seconds()) <= 1800]
        entities = {c["entity_id"] for c in possible}
        if len(entities) != 1:
            continue
        base = possible[0]
        p, n = probability(base.get("history_values") or [], q["line"], q["side"])
        if p is None:
            continue
        facts = dict(base.get("facts") or {})
        history = base["history_values"]
        if history:
            facts["line_below_average"] = q["line"] < sum(history) / len(history)
        if number(facts.get("carry_ypc_baseline")) is not None:
            facts["baseline_below_line"] = facts["carry_ypc_baseline"] < q["line"]
        if number(facts.get("rush_projection")) is not None:
            facts["rush_projection_above_line"] = facts["rush_projection"] > q["line"]
        result.append({**base, "side": q["side"], "line": q["line"],
                       "odds": q["odds"], "book": q["book"], "probability": p,
                       "sample_size": n, "quote_event_id": q["quote_event_id"],
                       "facts": facts})
    return result