#!/usr/bin/env python3
"""Signal collectors #2-4: Kalshi markets, Binance order flow, Fear&Greed,
plus a fuzzy Kalshi<->Polymarket macro matcher that logs price gaps."""
import json
import os
import re
import urllib.parse
import sqlite3
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "data.db")
UA = {"User-Agent": "poly-research/0.1"}

BINANCE_HOSTS = ["https://data-api.binance.vision",
                 "https://api.binance.com",
                 "https://api1.binance.com"]

STRONG = {"fed", "fomc", "rate", "rates", "cut", "cuts", "hike", "hikes",
          "bps", "cpi", "inflation", "gdp", "unemployment", "jobs",
          "shutdown", "taiwan", "china", "bitcoin", "btc", "ethereum",
          "eth", "election", "president", " pope", "solana"}
MONTHS = {"january", "february", "march", "april", "may", "june", "july",
          "august", "september", "october", "november", "december",
          "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct",
          "nov", "dec"}
STOP = {"will", "the", "a", "an", "of", "in", "on", "to", "for", "by",
        "at", "be", "is", "are", "after", "before", "between", "vs",
        "and", "or", "not", "this", "that", "there", "than", "then",
        "from", "with", "end", "by", "during", "what"}


def get(url, timeout=30):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def jget(url):
    return json.loads(get(url))


def init_db(db):
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS kalshi_markets(
            ts INTEGER, ticker TEXT, title TEXT, yes_bid INTEGER,
            yes_ask INTEGER, last_price INTEGER, volume INTEGER,
            volume_24h INTEGER, close_time TEXT,
            PRIMARY KEY(ts, ticker));
        CREATE TABLE IF NOT EXISTS binance_klines(
            symbol TEXT, tf TEXT, t INTEGER, open REAL, high REAL,
            low REAL, close REAL, vol REAL, taker_buy_ratio REAL,
            PRIMARY KEY(symbol, tf, t));
        CREATE TABLE IF NOT EXISTS binance_depth(
            ts INTEGER, symbol TEXT, bid_sum REAL, ask_sum REAL,
            spread REAL, PRIMARY KEY(ts, symbol));
        CREATE TABLE IF NOT EXISTS fng(
            t INTEGER PRIMARY KEY, value INTEGER, classification TEXT);
        CREATE TABLE IF NOT EXISTS match_log(
            ts INTEGER, poly_market_id TEXT, poly_q TEXT, kalshi_ticker TEXT,
            kalshi_title TEXT, poly_yes REAL, kalshi_mid REAL, gap REAL,
            PRIMARY KEY(ts, poly_market_id, kalshi_ticker));
        """
    )
    cols = [r[1] for r in db.execute("PRAGMA table_info(poly_markets)")]
    if "outcomes" not in cols:
        db.execute("ALTER TABLE poly_markets ADD COLUMN outcomes TEXT")


def kalshi_quote(m):
    """Kalshi dollars-markets: yes_bid/yes_ask are cents-strings under
    *_dollars; legacy integer fields are always 0. Returns (bid_c, ask_c,
    last_c) as integers or 0."""
    def cents(x):
        try:
            return int(round(float(x) * 100))
        except (TypeError, ValueError):
            return 0
    bid = cents(m.get("yes_bid_dollars") or m.get("yes_bid"))
    ask = cents(m.get("yes_ask_dollars") or m.get("yes_ask"))
    last = cents(m.get("last_price_dollars") or m.get("last_price"))
    if last == 0 and (bid or ask):
        last = (bid + ask) // 2
    return bid, ask, last


def collect_kalshi(db):
    """Two passes: (1) targeted macro/crypto series pulls (nested markets
    carry real quotes), (2) one page of the general open-market scan."""
    ts = int(time.time())
    rows = []
    SERIES = [
        "KXFED", "KXFEDFUNDSYEAR", "KXCPI", "KXGDPYEAR", "KXUNEMPLOY",
        "KXLFPRATEEOY", "KXBTC", "KXETH", "KXINX", "KXSP500", "KXNAS",
        "KXTREASCHG", "KXGOVBORROW", "KXDEBTCEILING", "KXSHUTDOWN",
    ]
    for s in SERIES:
        try:
            d = jget("https://api.elections.kalshi.com/trade-api/v2/events"
                     f"?status=open&series_ticker={s}"
                     "&with_nested_markets=true&limit=100")
            for ev in d.get("events", []):
                for m in ev.get("markets", []):
                    bid, ask, last = kalshi_quote(m)
                    rows.append((
                        ts, m.get("ticker", ""),
                        ev.get("title", "") + " — " + m.get("title", ""),
                        bid, ask, last,
                        m.get("volume") or 0, m.get("volume_24h") or 0,
                        m.get("close_time", "")))
        except Exception as e:
            print(f"[kalshi] series {s} failed: {e}")
    n_series = len(rows)
    try:
        d = jget("https://api.elections.kalshi.com/trade-api/v2/markets"
                 "?status=open&limit=200")
        rows += [
            (ts, m.get("ticker", ""), m.get("title", ""),
             m.get("yes_bid") or 0, m.get("yes_ask") or 0,
             m.get("last_price") or 0, m.get("volume") or 0,
             m.get("volume_24h") or 0, m.get("close_time", ""))
            for m in d.get("markets", [])]
    except Exception as e:
        print(f"[kalshi] general page failed: {e}")
    db.executemany(
        "INSERT OR IGNORE INTO kalshi_markets VALUES(?,?,?,?,?,?,?,?,?)",
        rows)
    print(f"[kalshi] {n_series} series markets + "
          f"{len(rows) - n_series} general = {len(rows)} snapshotted")


def binance_get(path):
    last = None
    for host in BINANCE_HOSTS:
        try:
            return jget(host + path)
        except Exception as e:
            last = e
    raise last


def collect_binance(db):
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        for tf, mins in (("1m", 500), ("1h", 200)):
            try:
                kl = binance_get(f"/api/v3/klines?symbol={symbol}"
                                 f"&interval={tf}&limit={mins}")
                rows = []
                for k in kl:
                    tb = float(k[9])
                    tot = float(k[5])
                    rows.append((symbol, tf, k[0] // 1000, float(k[1]),
                                 float(k[2]), float(k[3]), float(k[4]),
                                 tot, tb / tot if tot else 0.0))
                db.executemany(
                    "INSERT OR IGNORE INTO binance_klines "
                    "VALUES(?,?,?,?,?,?,?,?,?)", rows)
                print(f"[binance] {symbol} {tf}: {len(rows)} candles")
            except Exception as e:
                print(f"[binance] {symbol} {tf} failed: {e}")
    try:
        depth = binance_get("/api/v3/depth?symbol=BTCUSDT&limit=100")
        bid = sum(float(x[0]) * float(x[1]) for x in depth["bids"])
        ask = sum(float(x[0]) * float(x[1]) for x in depth["asks"])
        spread = float(depth["asks"][0][0]) - float(depth["bids"][0][0])
        db.execute("INSERT OR IGNORE INTO binance_depth VALUES(?,?,?,?,?)",
                   (int(time.time()), "BTCUSDT", bid, ask, spread))
        print(f"[binance] depth: bid ${bid:,.0f} vs ask ${ask:,.0f} "
              f"(imbalance {bid / (bid + ask):.2f})")
    except Exception as e:
        print(f"[binance] depth failed: {e}")


def collect_fng(db):
    try:
        d = jget("https://api.alternative.me/fng/?limit=30")
        rows = [(int(e["timestamp"]), int(e["value"]),
                 e["value_classification"]) for e in d["data"]]
        db.executemany("INSERT OR IGNORE INTO fng VALUES(?,?,?)", rows)
        print(f"[fng] latest {rows[0][1]} ({rows[0][2]}), {len(rows)} days")
    except Exception as e:
        print(f"[fng] failed: {e}")


def keywords(q):
    words = set(re.findall(r"[a-z0-9]+", q.lower()))
    return {w for w in words if len(w) > 2 and w not in STOP}


def poly_yes_price(outcomes_json, prices_json):
    try:
        outs = json.loads(outcomes_json or "[]")
        prices = json.loads(prices_json or "[]")
        for o, p in zip(outs, prices):
            if str(o).strip().lower() == "yes":
                return float(p)
        return float(prices[0]) if prices else None
    except Exception:
        return None


def latest_poly(db, n=20):
    ts = db.execute("SELECT MAX(ts) FROM poly_markets").fetchone()[0]
    if not ts:
        return []
    return db.execute(
        "SELECT market_id, question, outcomes, outcome_prices "
        "FROM poly_markets WHERE ts=? "
        "ORDER BY volume24h DESC LIMIT ?", (ts, n)).fetchall()


def strike_mids(kal_rows):
    """Return list of (strike_float, mid, ticker, title) for T-strike ladders."""
    out = []
    for ticker, title, bid, ask, *_ in kal_rows:
        m = re.search(r'-T(-?\d+\.?\d*)$', ticker)
        if not m:
            continue
        mid = (bid + ask) / 200.0
        out.append((float(m.group(1)), mid, ticker, title))
    return sorted(out)


def infer_current_rate(ladder):
    """Highest strike still trading >0.5 = current target upper bound."""
    above = [s for s, mid, *_ in ladder if mid > 0.5]
    return max(above) if above else None


def poly_fed_action(q):
    """Map a Poly Fed question to (target_delta, month_token) or None."""
    ql = q.lower()
    if 'fed' not in ql and 'fomc' not in ql:
        return None
    month = next((mo for mo in MONTHS if mo in ql), None)
    if not month:
        return None
    for kw, d in ((('decrease', 'cut'), -0.25), (('increase', 'hike'), 0.25)):
        if any(k in ql for k in kw):
            if '50' in ql:
                return (d * 2, month)
            return (d, month)
    if 'no change' in ql or 'unchanged' in ql or 'hold' in ql:
        return (0.0, month)
    return None


DOMAIN_MAP = {
    'KXCPI': ('cpi inflation',),
    'KXGDPYEAR': ('gdp',),
    'KXUNEMPLOY': ('unemployment jobs payrolls',),
    'KXBTC': ('bitcoin btc',),
    'KXETH': ('ethereum eth',),
    'KXSHUTDOWN': ('shutdown',),
    'KXDEBTCEILING': ('debt ceiling',),
}


def match_and_log(db):
    kal_ts = db.execute("SELECT MAX(ts) FROM kalshi_markets").fetchone()[0]
    if not kal_ts:
        return []
    kal = db.execute(
        "SELECT ticker, title, yes_bid, yes_ask, volume_24h "
        "FROM kalshi_markets WHERE ts=?", (kal_ts,)).fetchall()
    poly = latest_poly(db)
    now = int(time.time())
    out = []

    def log_pair(mid, q, py, ktick, ktitle, kmid):
        gap = abs(py - kmid) if kmid is not None else None
        if gap is None:
            return
        db.execute(
            "INSERT OR REPLACE INTO match_log VALUES(?,?,?,?,?,?,?,?)",
            (now, mid, q[:120], ktick[:80], ktitle[:120], py, kmid, gap))
        out.append((q, py, ktick, ktitle, kmid, gap))

    # Fed strike-ladder pairing. Kalshi KXFED strikes quote
    # P(final target-range upper bound > T); cur = highest strike still
    # trading >0.5 = current upper bound (e.g. 3.75 for band 3.50-3.75).
    # A Poly action with target delta d maps to X = cur + d, priced by
    # CDF difference: P(upper = X) = mid(X - 0.25) - mid(X).
    # E.g. hike25 from 3.50-3.75: P(>3.75) - P(>4.00).
    fed_rows = [k for k in kal if k[0].startswith('KXFED-')]
    ladders = {}
    for k in fed_rows:
        m = re.match(r'KXFED-(\d{2})([A-Z]{3})-T(-?\d+\.?\d*)$', k[0])
        if m:
            ladders.setdefault(m.group(1) + m.group(2), []).append(
                (float(m.group(3)), (k[2] + k[3]) / 200.0, k[0]))
    # nearest upcoming meeting = smallest (yy, month-index) >= today
    MI = {m: i + 1 for i, m in enumerate(
        ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
         'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'))}
    now_tm = time.gmtime()
    key = lambda ek: (2000 + int(ek[:2]), MI[ek[2:]])
    upcoming = sorted((ek for ek in ladders if key(ek) >= (now_tm.tm_year, now_tm.tm_mon)), key=key)
    if upcoming:
        near = upcoming[0]
        lad = sorted(ladders[near])
        above = [s for s, mid, *_ in lad if mid > 0.5]
        cur = max(above) if above else (lad[0][0] if lad else None)
        for mid, q, outs, op in poly:
            m = poly_fed_action(q)
            if not m or cur is None:
                continue
            delta, month = m
            py = poly_yes_price(outs, op)
            if py is None:
                continue
            ek = next((e for e in upcoming
                       if e[2:].lower().startswith(month[:3])), None)
            if ek is None:
                continue  # never pair across meetings
            smap = {s: mid for s, mid, _ in sorted(ladders[ek])}

            def smid(s):
                if not smap:
                    return None
                s2 = min(smap, key=lambda k: abs(k - s))
                return smap[s2] if abs(s2 - s) < 0.26 else None
            # P(final rate = X) = mid(X-0.25) - mid(X)  [strikes are CDFs]
            X = cur + delta
            lo, hi = smid(X - 0.25), smid(X)
            kmid = lo - hi if (lo is not None and hi is not None) else None
            if kmid is None or kmid < -0.02:
                continue
            log_pair(mid, q, py, f"KXFED-{ek}", f"target={X:.2f}", kmid)

    # generic domain matching (CPI, BTC, shutdown, ...)
    kw_by_series = {s: set(v[0].split()) for s, v in DOMAIN_MAP.items()}
    for mid, q, outs, op in poly:
        py = poly_yes_price(outs, op)
        if py is None:
            continue
        qk = keywords(q)
        for series, dom in kw_by_series.items():
            if not (qk & dom):
                continue
            best, best_score = None, 0
            for tick, title, bid, ask, v in kal:
                if not tick.startswith(series):
                    continue
                tk = keywords(title) | keywords(ticker_words(tick))
                score = len(qk & tk)
                if score > best_score:
                    best, best_score = (tick, title, (bid + ask) / 200.0), score
            if best and best_score >= 2:
                log_pair(mid, q, py, best[0], best[1], best[2])
            break
    return sorted(out, key=lambda x: -x[5])


def ticker_words(t):
    return re.sub(r'[^A-Za-z0-9]+', ' ', t)


def main():
    db = sqlite3.connect(DB)
    init_db(db)
    collect_kalshi(db)
    collect_binance(db)
    collect_fng(db)

    print("\n== Kalshi <-> Polymarket macro disagreement ==")
    pairs = match_and_log(db)
    if not pairs:
        print("  no confident macro matches this pass")
    for q, py, ktick, ktitle, kmid, gap in pairs:
        flag = "  <-- GAP" if gap >= 0.05 else ""
        print(f"  gap {gap * 100:4.1f}c | poly {py:.2f} vs kalshi {kmid:.2f}"
              f" | {q[:38]} ~ {ktick[:30]}{flag}")

    print("\n== Binance order flow (BTCUSDT) ==")
    rows = db.execute(
        "SELECT t, taker_buy_ratio FROM binance_klines "
        "WHERE symbol='BTCUSDT' AND tf='1m' ORDER BY t DESC LIMIT 30"
    ).fetchall()
    if rows:
        avg = sum(r[1] for r in rows) / len(rows)
        print(f"  last 30m taker-buy ratio: {avg:.3f} -> "
              f"{'buyers' if avg > 0.5 else 'sellers'} aggressive")
    d = db.execute("SELECT ts, bid_sum, ask_sum, spread FROM binance_depth "
                   "ORDER BY ts DESC LIMIT 1").fetchone()
    if d:
        imb = d[1] / (d[1] + d[2])
        print(f"  book imbalance: {imb:.2f} "
              f"({'bid' if imb > 0.5 else 'ask'}-heavy), spread ${d[3]:.2f}")

    print("\n== Fear & Greed ==")
    rows = db.execute("SELECT t, value FROM fng ORDER BY t DESC LIMIT 7"
                      ).fetchall()
    if rows:
        vals = [r[1] for r in rows]
        z = ("extreme fear" if vals[0] < 25 else "fear" if vals[0] < 45
             else "neutral" if vals[0] < 55 else "greed" if vals[0] < 75
             else "extreme greed")
        print(f"  now: {vals[0]} ({z}), 7d {vals[-1]} -> {vals[0]}")
    db.commit()
    db.close()


if __name__ == "__main__":
    sys.exit(main())
