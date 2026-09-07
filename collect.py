#!/usr/bin/env python3
"""Snapshot collector: Polymarket + CoinGecko + GDELT news tone -> sqlite.

Run repeatedly (cron); every run appends deduped rows so time-series
accumulate and cross-source patterns become computable.
"""
import json
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from calendar import timegm

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db")
UA = {"User-Agent": "poly-research/0.1 (data collector)"}

COINS = ["bitcoin", "ethereum", "solana"]
GDELT_QUERIES = ["bitcoin", "ethereum", "polymarket", "crypto"]


def get(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode()


def jget(url):
    return json.loads(get(url))


def init_db(db):
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS poly_markets(
            ts INTEGER, market_id TEXT, question TEXT, slug TEXT,
            volume24h REAL, liquidity REAL, outcome_prices TEXT,
            end_date TEXT, PRIMARY KEY(ts, market_id));
        CREATE TABLE IF NOT EXISTS poly_prices(
            token_id TEXT, tf TEXT, t INTEGER, price REAL,
            PRIMARY KEY(token_id, tf, t));
        CREATE TABLE IF NOT EXISTS cg_prices(
            coin TEXT, tf TEXT, t INTEGER, price REAL,
            PRIMARY KEY(coin, tf, t));
        CREATE TABLE IF NOT EXISTS news_tone(
            query TEXT, tf TEXT, t INTEGER, tone REAL,
            PRIMARY KEY(query, tf, t));
        CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY, value TEXT);
        """
    )
    cols = [r[1] for r in db.execute("PRAGMA table_info(poly_markets)")]
    if "outcomes" not in cols:
        db.execute("ALTER TABLE poly_markets ADD COLUMN outcomes TEXT")
    pcols = [r[1] for r in db.execute("PRAGMA table_info(poly_prices)")]
    if "outcome" not in pcols:
        db.execute("ALTER TABLE poly_prices ADD COLUMN outcome TEXT")


def collect_polymarkets(db):
    ts = int(time.time())
    url = ("https://gamma-api.polymarket.com/markets?active=true&closed=false"
           "&order=volume24hr&ascending=false&limit=20")
    markets = jget(url)
    rows = [
        (ts, m["id"], m.get("question", ""), m.get("slug", ""),
         float(m.get("volume24hr") or 0), float(m.get("liquidity") or 0),
         m.get("outcomePrices", "[]"), m.get("endDate", ""),
         m.get("outcomes", "[]"))
        for m in markets
    ]
    db.executemany(
        "INSERT OR REPLACE INTO poly_markets VALUES(?,?,?,?,?,?,?,?,?)",
        rows)
    print(f"[poly] {len(rows)} markets snapshotted")

    # hourly price history for the 5 highest-volume markets
    n = 0
    for m in markets[:5]:
        try:
            token_ids = json.loads(m["clobTokenIds"])
            hist = jget("https://clob.polymarket.com/prices-history"
                        f"?market={token_ids[0]}&interval=1d&fidelity=60")
            pts = [(token_ids[0], "1h", p["t"], p["p"])
                   for p in hist.get("history", [])]
            db.executemany(
                "INSERT OR IGNORE INTO poly_prices VALUES(?,?,?,?)", pts)
            n += len(pts)
            time.sleep(0.4)
        except Exception as e:
            print(f"[poly] history failed for {m.get('slug')}: {e}")
    print(f"[poly] {n} hourly price points (top 5 markets)")


def collect_coingecko(db):
    for coin in COINS:
        try:
            for tf, extra in (("1h", "&days=7"), ("1d", "&days=90&interval=daily")):
                d = jget("https://api.coingecko.com/api/v3/coins/"
                         f"{coin}/market_chart?vs_currency=usd{extra}")
                pts = [(coin, tf, ms // 1000, p)
                       for ms, p in d.get("prices", [])]
                db.executemany(
                    "INSERT OR IGNORE INTO cg_prices VALUES(?,?,?,?)", pts)
                print(f"[cg] {coin} {tf}: {len(pts)} points")
                time.sleep(1.2)
        except Exception as e:
            print(f"[cg] {coin} failed: {e}")
    try:
        g = jget("https://api.coingecko.com/api/v3/global")["data"]
        db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)",
                   ("btc_dominance", str(g["market_cap_percentage"]["btc"])))
        db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)",
                   ("total_mcap_usd", str(g["total_market_cap"]["usd"])))
        print("[cg] global: btc dominance "
              f"{g['market_cap_percentage']['btc']:.1f}%")
    except Exception as e:
        print(f"[cg] global failed: {e}")


def gdelt_epoch(s):
    return timegm(time.strptime(s, "%Y%m%dT%H%M%SZ"))


def collect_gdelt(db):
    for q in GDELT_QUERIES:
        try:
            url = ("https://api.gdeltproject.org/api/v2/doc/doc?query="
                   + urllib.parse.quote(f"{q} sourcelang:english")
                   + "&mode=timelinetone&format=json&timespan=7d")
            d = jget(url)
            pts = []
            for series in d.get("timeline", []):
                pts = [(q, "1h", gdelt_epoch(p["date"]), p["value"])
                       for p in series.get("data", [])]
            db.executemany(
                "INSERT OR IGNORE INTO news_tone VALUES(?,?,?,?)", pts)
            print(f"[gdelt] {q}: {len(pts)} tone points")
            time.sleep(1.0)
        except Exception as e:
            print(f"[gdelt] {q} failed: {e}")


def main():
    db = sqlite3.connect(DB)
    init_db(db)
    collect_polymarkets(db)
    collect_coingecko(db)
    collect_gdelt(db)
    db.execute("INSERT OR REPLACE INTO meta VALUES('last_run',?)",
               (str(int(time.time())),))
    db.commit()
    for t in ("poly_markets", "poly_prices", "cg_prices", "news_tone"):
        n = db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"[db] {t}: {n} rows total")
    db.close()


if __name__ == "__main__":
    sys.exit(main())
