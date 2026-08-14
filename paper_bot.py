import time
import requests
import json
from datetime import datetime, timezone
from collections import defaultdict

# ====================== HYBRID SETTINGS ======================
STARTING_BALANCE   = 3000.0
MAX_PER_MARKET     = 45.0          # max $ to spend per market
MAX_OPEN_MARKETS   = 3
MIN_EDGE_TRADE     = 0.025         # 2.5% → actually trade
MIN_EDGE_LOG       = 0.015         # 1.5% → only log (WATCH)
MIN_DEPTH_USD      = 35.0          # both sides need at least this much depth
POLL_SECONDS       = 8
RUN_MINUTES        = 180           # 3 hours (change as you like)
# =============================================================

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
CLOB_BOOK    = "https://clob.polymarket.com/book"

balance = STARTING_BALANCE
locked  = 0.0
open_positions = []          # list of dicts
seen_markets   = set()
trade_log      = []

session = requests.Session()
session.headers.update({"User-Agent": "paper-bot-hybrid/2.0"})

def get_order_book(token_id: str):
    try:
        r = session.get(CLOB_BOOK, params={"token_id": token_id}, timeout=4)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def best_ask(book):
    if not book or not book.get("asks"):
        return None
    try:
        # asks are usually sorted low → high
        return float(book["asks"][0]["price"])
    except Exception:
        return None

def depth_usd(book, limit_price: float, max_levels=8):
    """How much USD is available on the ask side up to limit_price"""
    if not book or not book.get("asks"):
        return 0.0
    total = 0.0
    for level in book["asks"][:max_levels]:
        price = float(level["price"])
        size  = float(level["size"])
        if price > limit_price + 0.015:
            break
        total += price * size
    return total

def discover_btc_updown_markets(limit=40):
    """Find currently active Bitcoin Up or Down short-term markets"""
    markets = []
    try:
        # Pull recent active events and filter
        params = {
            "active": "true",
            "closed": "false",
            "limit": 80,
            "order": "startDate",
            "ascending": "false"
        }
        r = session.get(GAMMA_EVENTS, params=params, timeout=10)
        r.raise_for_status()
        events = r.json()

        for ev in events:
            title = (ev.get("title") or "").lower()
            if "bitcoin up or down" not in title and "btc up or down" not in title:
                continue

            for m in ev.get("markets", []):
                if not m.get("active") or m.get("closed"):
                    continue
                tokens = m.get("clobTokenIds")
                if isinstance(tokens, str):
                    try:
                        tokens = json.loads(tokens)
                    except Exception:
                        continue
                if not tokens or len(tokens) < 2:
                    continue

                outcomes = m.get("outcomes")
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except Exception:
                        outcomes = ["Up", "Down"]

                # Map Up / Down token
                up_token = down_token = None
                for i, outcome in enumerate(outcomes):
                    if str(outcome).lower() == "up":
                        up_token = tokens[i]
                    elif str(outcome).lower() == "down":
                        down_token = tokens[i]

                if up_token and down_token:
                    markets.append({
                        "condition_id": m.get("conditionId"),
                        "title": m.get("question") or ev.get("title"),
                        "up_token": up_token,
                        "down_token": down_token,
                        "end_date": m.get("endDate")
                    })
    except Exception as e:
        print(f"[ERROR] Discovery failed: {e}")

    return markets

def evaluate(up_book, down_book):
    """
    Returns: action ("TRADE" | "WATCH" | "SKIP"), edge, reason
    """
    up_ask = best_ask(up_book)
    down_ask = best_ask(down_book)

    if up_ask is None or down_ask is None:
        return "SKIP", 0.0, "missing ask"

    combined = up_ask + down_ask

    # Phantom / corrupt book protection
    if combined < 0.72 or combined > 1.12:
        return "SKIP", 0.0, f"phantom book (sum={combined:.3f})"

    edge = 1.0 - combined

    # Depth check
    up_depth = depth_usd(up_book, up_ask)
    down_depth = depth_usd(down_book, down_ask)

    if up_depth < MIN_DEPTH_USD or down_depth < MIN_DEPTH_USD:
        return "SKIP", edge, f"low depth (Up ${up_depth:.0f} / Down ${down_depth:.0f})"

    if edge >= MIN_EDGE_TRADE:
        return "TRADE", edge, "good edge + depth"
    if edge >= MIN_EDGE_LOG:
        return "WATCH", edge, "interesting edge"
    return "SKIP", edge, "edge too small"

def try_open(market):
    global balance, locked

    cid = market["condition_id"]
    if cid in seen_markets:
        return
    if len(open_positions) >= MAX_OPEN_MARKETS:
        return

    up_book = get_order_book(market["up_token"])
    down_book = get_order_book(market["down_token"])

    action, edge, reason = evaluate(up_book, down_book)
    title = (market["title"] or "")[:55]

    if action == "WATCH":
        print(f"[WATCH] {title}")
        print(f"        Edge: {edge*100:.2f}% | {reason}")
        return

    if action != "TRADE":
        return

    # Calculate size
    spend = min(MAX_PER_MARKET, balance * 0.35)
    if spend < 18 or balance - spend < 400:
        return

    up_ask = best_ask(up_book)
    down_ask = best_ask(down_book)
    half = spend / 2

    up_shares = half / up_ask
    down_shares = half / down_ask
    pairs = min(up_shares, down_shares)

    pos = {
        "id": cid,
        "title": title,
        "open_time": datetime.now(timezone.utc),
        "up_ask": up_ask,
        "down_ask": down_ask,
        "combined": up_ask + down_ask,
        "edge": edge,
        "spent": spend,
        "pairs": pairs,
        "status": "OPEN"
    }

    open_positions.append(pos)
    seen_markets.add(cid)
    balance -= spend
    locked += spend

    print("\n" + "="*70)
    print(f"[TRADE] OPENED  |  Edge {edge*100:.2f}%")
    print(f"Market : {title}")
    print(f"Up ask : {up_ask:.4f}  |  Down ask: {down_ask:.4f}  |  Combined: {up_ask+down_ask:.4f}")
    print(f"Spent  : ${spend:.2f}   |  Pairs: {pairs:.1f}")
    print(f"Balance: ${balance:.2f}  |  Locked: ${locked:.2f}")
    print("="*70 + "\n")

def check_resolutions():
    global balance, locked
    now = datetime.now(timezone.utc)
    still_open = []

    for pos in open_positions:
        age_min = (now - pos["open_time"]).total_seconds() / 60
        if age_min >= 5.5:          # resolve after ~5.5 min
            payout = pos["pairs"] * 1.0
            profit = payout - pos["spent"]
            balance += payout
            locked -= pos["spent"]

            pos["status"] = "CLOSED"
            pos["payout"] = payout
            pos["profit"] = profit
            trade_log.append(pos)

            print("\n" + "-"*70)
            print(f"[RESOLVED] {pos['title']}")
            print(f"Spent ${pos['spent']:.2f} → Payout ${payout:.2f} | Profit ${profit:+.2f}")
            print(f"New Balance: ${balance:.2f}")
            print("-"*70 + "\n")
        else:
            still_open.append(pos)

    open_positions[:] = still_open

def print_status():
    equity = balance + locked
    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
          f"Balance: ${balance:.2f} | Locked: ${locked:.2f} | "
          f"Equity: ${equity:.2f} | Open: {len(open_positions)} | "
          f"Closed trades: {len(trade_log)}")

def main():
    print("="*70)
    print("PAPER BOT – HYBRID (Option C)")
    print(f"Starting Balance : ${STARTING_BALANCE:.2f}")
    print(f"Trade when edge  ≥ {MIN_EDGE_TRADE*100:.1f}% + depth")
    print(f"Log when edge    ≥ {MIN_EDGE_LOG*100:.1f}%")
    print(f"Min depth        : ${MIN_DEPTH_USD:.0f} each side")
    print("="*70 + "\n")

    end = time.time() + RUN_MINUTES * 60

    try:
        while time.time() < end:
            markets = discover_btc_updown_markets()
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning {len(markets)} BTC Up/Down markets...")

            for m in markets:
                try_open(m)

            check_resolutions()
            print_status()
            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopped by user.")

    # Final report
    print("\n" + "="*70)
    print("SESSION FINISHED")
    print(f"Starting Balance : ${STARTING_BALANCE:.2f}")
    print(f"Final Balance    : ${balance:.2f}")
    print(f"Profit           : ${balance - STARTING_BALANCE:+.2f}")
    print(f"Trades closed    : {len(trade_log)}")
    if trade_log:
        wins = sum(1 for t in trade_log if t.get("profit", 0) > 0)
        print(f"Win rate         : {wins}/{len(trade_log)}")
    print("="*70)

if __name__ == "__main__":
    main()
