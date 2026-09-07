#!/usr/bin/env python3
"""Cross-source pattern analysis over data.db (see collect.py).

Prints a report of the correlations/patterns found so far. The sample
grows with every collect.py run, so re-run daily.
"""
import json
import os
import sqlite3
import statistics
import time

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db")


def pearson(xs, ys):
    n = min(len(xs), len(ys))
    if n < 12:
        return None
    xs, ys = xs[:n], ys[:n]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy = sum((y - my) ** 2 for y in ys) ** 0.5
    return num / (dx * dy) if dx and dy else None


def pct(xs):
    return [100.0 * (b - a) / a for a, b in zip(xs, xs[1:])]


def hourly(coin):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT t, price FROM cg_prices WHERE coin=? AND tf='1h' "
        "ORDER BY t", (coin,)).fetchall()
    db.close()
    return [r[0] for r in rows], [r[1] for r in rows]


def tone(query):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT t, tone FROM news_tone WHERE query=? AND tf='1h' "
        "ORDER BY t", (query,)).fetchall()
    db.close()
    return [r[0] for r in rows], [r[1] for r in rows]


def align(ts_a, ts_b, vals_b):
    """Return (a_idx_sorted_shared, vals) aligned on common hourly stamps."""
    b = dict(zip(ts_b, vals_b))
    shared = [t for t in ts_a if t in b]
    return shared, [b[t] for t in shared]


def tone_vs_returns(coin, query):
    ts_p, p = hourly(coin)
    ts_n, n = tone(query)
    shared, tones = align(ts_p, ts_n, n)
    if len(shared) < 14:
        return None
    prices = dict(zip(ts_p, p))
    px = [prices[t] for t in shared]
    rets = pct(px)
    tones = tones[1:]  # tones aligned to rets[0..]
    out = {}
    r0 = pearson(tones, rets)
    out["same_hour"] = r0
    if len(tones) > 13 and len(rets) > 13:
        out["next_hour"] = pearson(tones[:-1], rets[1:])
        out["next_3h"] = pearson(tones[:-3], rets[3:])
    out["n"] = len(rets)
    return out


def latest_snapshot_markets(db, n=5):
    ts = db.execute("SELECT MAX(ts) FROM poly_markets").fetchone()[0]
    if not ts:
        return []
    return db.execute(
        "SELECT market_id, question, outcomes, outcome_prices "
        "FROM poly_markets WHERE ts=? ORDER BY volume24h DESC LIMIT ?",
        (ts, n)).fetchall()


def polymarket_vs_spot():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT token_id, t, price FROM poly_prices WHERE tf='1h' ORDER BY t"
    ).fetchall()
    if not rows:
        db.close()
        return
    by_tok = {}
    for tok, t, p in rows:
        by_tok.setdefault(tok, []).append((t, p))
    ts_btc, p_btc = hourly("bitcoin")
    btc = dict(zip(ts_btc, p_btc))
    # tokens were stored in volume-desc order of the latest snapshot;
    # token #1 of each market is outcomes[0]
    markets = latest_snapshot_markets(db, 5)
    db.close()
    print("\n== Polymarket top-market price vs spot BTC (hourly) ==")
    for i, (tok, pts) in enumerate(by_tok.items()):
        if len(pts) < 6:
            continue
        both = [(t, p) for t, p in pts if t in btc]
        if len(both) < 6:
            continue
        dm = pct([p for _, p in both])
        ds = pct([btc[t] for t, _ in both])
        n = min(len(dm), len(ds))
        agree = sum(1 for a, b in zip(dm[:n], ds[:n])
                    if (a > 0) == (b > 0) and (a != 0 or b != 0))
        r = pearson(dm[:n], ds[:n])
        label = markets[i][1][:44] if i < len(markets) else tok[:14]
        rtxt = "n/a" if r is None else f"{r:+.3f}"
        print(f"  {label:44s} r={rtxt} sign-agree={agree}/{n} "
              f"last={both[-1][1]:.3f}")


def top_movers():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT question, slug, outcomes, outcome_prices, volume24h "
        "FROM poly_markets WHERE ts=(SELECT MAX(ts) FROM poly_markets) "
        "ORDER BY volume24h DESC LIMIT 10").fetchall()
    db.close()
    print("\n== Top Polymarket markets (24h volume) ==")
    for q, slug, outs, op, v in rows:
        try:
            prices = [float(x) for x in json.loads(op or "[]")]
            labels = json.loads(outs or "[]")
        except Exception:
            prices, labels = [], []
        if len(labels) == len(prices):
            head = ", ".join(f"{l}={p:.2f}" for l, p in zip(labels, prices))
        else:
            head = ", ".join(f"{p:.2f}" for p in prices[:2])
        print(f"  {q[:58]:58s} [{head}] vol24h=${v:,.0f}")


def main():
    print(f"Pattern report @ {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}")
    print("=" * 66)
    top_movers()
    polymarket_vs_spot()
    print("\n== News tone (GDELT hourly) vs coin returns ==")
    for coin, query in (("bitcoin", "bitcoin"), ("ethereum", "ethereum")):
        res = tone_vs_returns(coin, query)
        if not res:
            print(f"  {coin}: sample still small - keep collecting")
            continue
        fmt = lambda r: "n/a" if r is None else f"{r:+.3f}"
        print(f"  {coin} vs '{query}' tone (n={res['n']}): "
              f"same-hour r={fmt(res['same_hour'])}  "
              f"next-hour r={fmt(res.get('next_hour'))}  "
              f"next-3h r={fmt(res.get('next_3h'))}")
    db = sqlite3.connect(DB)
    dom = db.execute(
        "SELECT value FROM meta WHERE key='btc_dominance'").fetchone()
    db.close()
    if dom:
        print(f"\nBTC dominance: {float(dom[0]):.1f}%")


if __name__ == "__main__":
    main()
