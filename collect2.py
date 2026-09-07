#!/usr/bin/env python3
"""Signal collectors #2-4: Kalshi markets, Binance order flow, Fear&Greed,
plus a fuzzy Kalshi<->Polymarket macro matcher that logs price gaps."""
import json
import os
import re
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


def collect_kalshi(db):
    ts = int(time.time())
    rows = []
    cursor = 100
    while cursor < 1000:
        try:
            d = jget("https://api.elections.kalshi.com/trade-api/v2/markets"
                     f"?status=open&limit=200&cursor={cursor}")
        except Exception as e:
            print(f"[kalshi] page {cursor} failed: {e}")
            break
        ms = d.get("markets", [])
        rows += [
            (ts, m.get("ticker", ""), m.get("title", ""),
             m.get("yes_bid") or 0, m.get("yes_ask") or 0,
             m.get("last_price") or 0, m.get("volume") or 0,
             m.get("volume_24h") or 0, m.get("close_time", ""))
            for m in ms]
        cur = d.get("cursor")
        if not ms or cur in (None, "", cursor):
            break
        cursor += 100
    db.executemany(
        "INSERT OR IGNORE INTO kalshi_markets VALUES(?,?,?,?,?,?,?,?,?)",
        rows)
    print(f"[kalshi] {len(rows)} open markets snapshotted")


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


def match_and_log(db):
    kal_ts = db.execute("SELECT MAX(ts) FROM kalshi_markets").fetchone()[0]
    if not kal_ts:
        return []
    kal = db.execute(
        "SELECT ticker, title, yes_bid, yes_ask, volume_24h "
        "FROM kalshi_markets WHERE ts=?", (kal_ts,)).fetchall()
    kal_kw = [(k, keywords(k[1])) for k in kal if k[1]]
    poly = latest_poly(db)
    now = int(time.time())
    out = []
    for mid, q, outs, op in poly:
        py = poly_yes_price(outs, op)
        if py is None:
            continue
        qk = keywords(q)
        strong_q = qk & STRONG
        time_q = qk & MONTHS | (qk & {"2026", "2027"})
        best, best_score = None, 0
        for k, kk in kal_kw:
            strong_j = len(strong_q & kk)
            if not strong_j:
                continue
            time_j = len(time_q & (kk & MONTHS | kk & {"2026", "2027"}))
            score = strong_j * 2 + time_j
            if score > best_score:
                best, best_score = k, score
        if not best or best_score < 3:
            continue
        kmid = (best[2] + best[3]) / 200.0
        gap = abs(py - kmid)
        db.execute(
            "INSERT OR REPLACE INTO match_log VALUES(?,?,?,?,?,?,?,?)",
            (now, mid, q[:120], best[0], best[1][:120], py, kmid, gap))
        out.append((q, py, best, kmid, gap))
    return sorted(out, key=lambda x: -x[4])


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
    for q, py, k, kmid, gap in pairs:
        flag = "  <-- GAP" if gap >= 0.05 else ""
        print(f"  gap {gap * 100:4.1f}c | poly {py:.2f} vs kalshi {kmid:.2f}"
              f" | {q[:38]} ~ {k[1][:30]}{flag}")

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
