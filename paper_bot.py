# Polymarket Crypto Arbitrage Bot v7.0
# Hybrid: Gamma discovery + market WS trigger + authoritative CLOB /book
# + authenticated user WS + REST reconciliation.
#
# PAPER is default. LIVE_TRADING=1 enables live execution.
# LIVE_DRY_RUN=1 exercises the execution/reconciliation path without real orders.

import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, UTC
from typing import Optional, Any

import requests
import websockets

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    HAS_CLOB_CLIENT = True
except ImportError:
    ClobClient = OrderArgs = OrderType = None
    BUY, SELL = "BUY", "SELL"
    HAS_CLOB_CLIENT = False

INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "3000"))
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "45"))
LIVE_MAX_TRADE_SIZE = float(os.getenv("LIVE_MAX_TRADE_SIZE", str(MAX_TRADE_SIZE)))
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.025"))
MIN_DEPTH_USD = float(os.getenv("MIN_DEPTH_USD", "2.00"))
MIN_COMBINED_PRICE = float(os.getenv("MIN_COMBINED_PRICE", "0.92"))
MAX_LATENCY_MS = float(os.getenv("MAX_LATENCY_MS", "80"))
OPPORTUNITY_COOLDOWN_S = float(os.getenv("OPPORTUNITY_COOLDOWN_S", "3"))
RECONCILE_TIMEOUT_S = float(os.getenv("RECONCILE_TIMEOUT_S", "3"))
LEG_RISK_PRICE_BUFFER = float(os.getenv("LEG_RISK_PRICE_BUFFER", "0.05"))
LEG_RISK_CIRCUIT_BREAKER_LIMIT = int(os.getenv("LEG_RISK_CIRCUIT_BREAKER_LIMIT", "3"))
CRYPTO_TAKER_FEE_RATE = float(os.getenv("CRYPTO_TAKER_FEE_RATE", "0.07"))
EST_GAS_FEE_USD = float(os.getenv("EST_GAS_FEE_USD", "0.01"))
DISCOVERY_INTERVAL_S = float(os.getenv("DISCOVERY_INTERVAL_S", "60"))
HEALTH_INTERVAL_S = float(os.getenv("HEALTH_INTERVAL_S", "30"))

CSV_FILENAME = os.getenv("CSV_FILENAME", "paper_trades_v7.csv")
LIVE_CSV_FILENAME = os.getenv("LIVE_CSV_FILENAME", "live_trades_v7.csv")
RECON_CSV_FILENAME = os.getenv("RECON_CSV_FILENAME", "live_reconciled_v7.csv")

LIVE_TRADING = os.getenv("LIVE_TRADING", "0") == "1"
LIVE_DRY_RUN = os.getenv("LIVE_DRY_RUN", "0") == "1"

CRYPTO_KEYWORDS = ["btc", "eth", "sol", "bitcoin", "ethereum",
                   "updown", "up/down", "5m", "15m", "crypto"]

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
WS_CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_USER_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
CHAIN_ID = 137

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    stream=sys.stdout)
logger = logging.getLogger("polymarket-arb-v7")

session = requests.Session()
session.headers.update({"User-Agent": "PolymarketCryptoArbBot/7.0"})

balance = INITIAL_BALANCE
reserved_capital = 0.0
active_markets: dict[str, dict] = {}
token_to_market: dict[str, tuple[str, str]] = {}
pending_arbs: dict[str, dict] = {}
pending_orders: dict[str, dict] = {}
market_dirty_ts: dict[str, float] = {}
last_evaluation_ts: dict[str, float] = {}
last_opportunity_ts: dict[str, float] = {}
seen_trade_events: dict[str, float] = {}

trades_history: list[dict] = []
live_trades_history: list[dict] = []
reconciled_history: list[dict] = []

clob_client: Optional[Any] = None
api_creds: Optional[dict] = None
live_trading_halted = False
leg_risk_unresolved_count = 0
shutdown_requested = False

stats = {k: 0 for k in (
    "ws_messages", "book_events", "price_change_events", "ws_errors",
    "rest_book_requests", "rest_book_success", "evaluations", "no_book",
    "phantom_reject", "edge_reject", "depth_reject", "latency_reject",
    "cooldown_reject", "paper_trades", "live_submissions"
)}
stats["ws_connected"] = False

def now_ts(): return time.time()
def utc_string(): return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
def safe_float(v, default=0.0):
    try: return float(v)
    except (TypeError, ValueError): return default

def available_capital():
    return max(0.0, balance - reserved_capital)

def append_csv(record, filename):
    try:
        exists = os.path.isfile(filename)
        with open(filename, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=record.keys())
            if not exists: w.writeheader()
            w.writerow(record)
    except Exception as e:
        logger.warning("CSV write failed (%s): %s", filename, e)

def flush_history():
    for rows, filename in ((trades_history, CSV_FILENAME),
                           (live_trades_history, LIVE_CSV_FILENAME),
                           (reconciled_history, RECON_CSV_FILENAME)):
        if not rows: continue
        try:
            with open(filename, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=rows[0].keys())
                w.writeheader(); w.writerows(rows)
        except Exception as e:
            logger.error("Flush failed for %s: %s", filename, e)

def signal_handler(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested; flushing logs...")
    flush_history()
    raise SystemExit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def parse_tokens(market):
    tokens = market.get("clobTokenIds")
    if isinstance(tokens, str):
        try: tokens = json.loads(tokens)
        except Exception: return None
    if not isinstance(tokens, list) or len(tokens) < 2: return None
    return [str(tokens[0]), str(tokens[1])]

def is_crypto_market(market):
    title = (market.get("question") or market.get("title") or "").lower()
    return any(k in title for k in CRYPTO_KEYWORDS)

def normalize_market(market):
    tokens = parse_tokens(market)
    if not tokens: return None
    return {
        "id": str(market.get("id")),
        "title": market.get("question") or market.get("title") or "Unknown",
        "up_token": tokens[0], "down_token": tokens[1],
        "condition_id": market.get("conditionId") or market.get("condition_id"),
        "neg_risk": bool(market.get("negRisk") or market.get("neg_risk") or False),
        "tick_size": str(market.get("minimum_tick_size") or
                          market.get("orderPriceMinTickSize") or "0.01"),
    }

def get_btc_5m_slugs():
    t = int(time.time()); w = t - (t % 300)
    return [f"btc-updown-5m-{w}", f"btc-updown-5m-{w + 300}"]

def fetch_market_by_slug(slug):
    try:
        r = session.get(f"{GAMMA_API_URL}/events",
                        params={"slug": slug}, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                ms = data[0].get("markets") or []
                return ms[0] if ms else None
    except Exception as e:
        logger.debug("Slug lookup failed: %s", e)
    return None

def fetch_crypto_markets():
    markets, tokens = {}, {}
    for slug in get_btc_5m_slugs():
        m = fetch_market_by_slug(slug)
        n = normalize_market(m) if m and is_crypto_market(m) else None
        if n:
            markets[n["id"]] = n
            tokens[n["up_token"]] = (n["id"], "UP")
            tokens[n["down_token"]] = (n["id"], "DOWN")
    try:
        params = {"tag_id": 21, "limit": 25, "active": "true",
                  "closed": "false", "order": "volume24hr", "ascending": "false"}
        r = session.get(f"{GAMMA_API_URL}/markets", params=params, timeout=7)
        if r.status_code == 200:
            for m in r.json() if isinstance(r.json(), list) else []:
                n = normalize_market(m) if is_crypto_market(m) else None
                if n:
                    markets[n["id"]] = n
                    tokens[n["up_token"]] = (n["id"], "UP")
                    tokens[n["down_token"]] = (n["id"], "DOWN")
    except Exception as e:
        logger.warning("Discovery failed: %s", e)
    return markets, tokens

def update_discovery(new_markets, new_tokens):
    global active_markets, token_to_market
    old_ids = set(active_markets)
    for mid, data in new_markets.items():
        if mid in active_markets:
            active_markets[mid].update(data)
        else:
            active_markets[mid] = data
    token_to_market.update(new_tokens)
    for mid in old_ids - set(new_markets):
        m = active_markets.pop(mid, None)
        if m:
            token_to_market.pop(m["up_token"], None)
            token_to_market.pop(m["down_token"], None)
        market_dirty_ts.pop(mid, None)
        last_evaluation_ts.pop(mid, None)

def fetch_order_book(token_id):
    if not token_id: return None
    stats["rest_book_requests"] += 1
    try:
        r = session.get(f"{CLOB_API_URL}/book",
                        params={"token_id": token_id}, timeout=4)
        if r.status_code == 200 and isinstance(r.json(), dict):
            stats["rest_book_success"] += 1
            return r.json()
    except Exception as e:
        logger.debug("Book fetch failed: %s", e)
    return None

def best_ask_with_depth(book):
    levels = []
    for x in book.get("asks") or []:
        p, s = safe_float(x.get("price")), safe_float(x.get("size"))
        if 0.02 <= p <= 0.99 and s > 0:
            levels.append((p, s))
    levels.sort()
    for p, s in levels:
        if p * s >= MIN_DEPTH_USD:
            return p, s, p * s
    return None, 0.0, 0.0

def price_for_shares(book, target):
    if target <= 0: return None
    levels = []
    for x in book.get("asks") or []:
        p, s = safe_float(x.get("price")), safe_float(x.get("size"))
        if 0.02 <= p <= 0.99 and s > 0: levels.append((p, s))
    levels.sort()
    remaining, cost = target, 0.0
    for p, s in levels:
        take = min(remaining, s); cost += take * p; remaining -= take
        if remaining <= 1e-9: return cost / target
    return None

def calculate_vwap(book, target_usd):
    levels = []
    for x in book.get("asks") or []:
        p, s = safe_float(x.get("price")), safe_float(x.get("size"))
        if 0.02 <= p <= 0.99 and s > 0: levels.append((p, s))
    levels.sort()
    remaining, shares, cost = target_usd, 0.0, 0.0
    for p, s in levels:
        take_usd = min(remaining, p * s)
        take_shares = take_usd / p
        shares += take_shares; cost += take_usd; remaining -= take_usd
        if remaining <= 1e-9: return cost / shares, shares, cost
    return None, 0.0, 0.0

def taker_fee(shares, price):
    return shares * CRYPTO_TAKER_FEE_RATE * price * (1.0 - price)

def evaluate_books(up_book, down_book, target_usd):
    up_best, _, up_depth = best_ask_with_depth(up_book)
    dn_best, _, dn_depth = best_ask_with_depth(down_book)
    if up_best is None or dn_best is None: return None
    _, up_sh, _ = calculate_vwap(up_book, target_usd)
    _, dn_sh, _ = calculate_vwap(down_book, target_usd)
    shares = min(up_sh, dn_sh)
    if shares <= 0: return None
    up_px, dn_px = price_for_shares(up_book, shares), price_for_shares(down_book, shares)
    if up_px is None or dn_px is None: return None
    combined = up_px + dn_px
    fees = taker_fee(shares, up_px) + taker_fee(shares, dn_px)
    cost = shares * combined
    net = shares - cost - fees - EST_GAS_FEE_USD
    return {"up_best": up_best, "down_best": dn_best,
            "up_depth": up_depth, "down_depth": dn_depth,
            "up_price": up_px, "down_price": dn_px, "shares": shares,
            "combined": combined, "gross_edge": 1.0 - combined,
            "fees": fees, "cost": cost, "net": net}

def execute_paper(market, result, age_ms):
    global balance
    if result["net"] <= 0: return
    if available_capital() < result["cost"]: return
    balance += result["net"]
    rec = {"timestamp": utc_string(), "market_title": market["title"],
           "up_price": round(result["up_price"], 6),
           "down_price": round(result["down_price"], 6),
           "combined": round(result["combined"], 6),
           "shares": round(result["shares"], 6),
           "spend_usd": round(result["cost"], 6),
           "fees_usd": round(result["fees"], 6),
           "payout_usd": round(result["shares"], 6),
           "net_profit": round(result["net"], 6),
           "gross_edge_pct": round(result["gross_edge"] * 100, 3),
           "signal_age_ms": round(age_ms, 2),
           "new_balance": round(balance, 6)}
    trades_history.append(rec); append_csv(rec, CSV_FILENAME)
    stats["paper_trades"] += 1
    logger.info("[PAPER TRADE] %s | %.4f + %.4f = %.4f | "
                "shares %.2f | net +$%.4f | balance $%.2f",
                market["title"], result["up_price"], result["down_price"],
                result["combined"], result["shares"], result["net"], balance)

def init_clob_client():
    global api_creds
    if not HAS_CLOB_CLIENT:
        logger.error("py-clob-client is not installed.")
        return None
    pk = os.getenv("PRIVATE_KEY")
    if not pk:
        logger.error("PRIVATE_KEY required for live mode.")
        return None
    try:
        client = ClobClient(CLOB_HOST, key=pk, chain_id=CHAIN_ID,
                            signature_type=int(os.getenv("SIGNATURE_TYPE", "0")),
                            funder=os.getenv("FUNDER_ADDRESS") or None)
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        if hasattr(creds, "api_key"):
            api_creds = {"apiKey": creds.api_key, "secret": creds.api_secret,
                         "passphrase": creds.api_passphrase}
        elif isinstance(creds, dict):
            api_creds = {"apiKey": creds.get("apiKey") or creds.get("api_key"),
                         "secret": creds.get("secret") or creds.get("api_secret"),
                         "passphrase": creds.get("passphrase") or creds.get("api_passphrase")}
        return client
    except Exception as e:
        logger.error("CLOB init failed: %s", e)
        return None

async def submit_limit_fok(client, token_id, price, shares, side=BUY):
    start = time.perf_counter()
    res = {"success": False, "order_id": None, "status": None,
           "error": None, "size_matched": 0.0, "fill_price": price,
           "latency_ms": 0.0}
    if LIVE_DRY_RUN:
        await asyncio.sleep(0.02)
        res.update(success=True, order_id=f"dry-{token_id[-8:]}-{int(time.time()*1000)}",
                   status="dry_run", size_matched=shares)
        res["latency_ms"] = (time.perf_counter() - start) * 1000
        return res
    def post():
        args = OrderArgs(token_id=token_id, price=round(price, 4),
                         size=round(shares, 4), side=side)
        signed = client.create_order(args)
        return client.post_order(signed, OrderType.FOK)
    try:
        response = await asyncio.to_thread(post)
        if isinstance(response, dict):
            oid = response.get("orderID") or response.get("orderId") or response.get("id") or response.get("order_id")
            res["order_id"] = str(oid) if oid else None
            res["status"] = response.get("status")
            res["error"] = response.get("errorMsg") or response.get("error")
            res["size_matched"] = safe_float(response.get("size_matched") or response.get("sizeMatched"))
            res["success"] = bool(response.get("success") or oid)
        else:
            res["success"] = True; res["status"] = "ok"
    except Exception as e:
        res["error"] = str(e); res["status"] = "exception"
    res["latency_ms"] = (time.perf_counter() - start) * 1000
    return res

def register_pending_arb(arb_id, ctx):
    pending_arbs[arb_id] = {**ctx, "created_at": now_ts(), "reconciled": False,
                            "up_matched": 0.0, "down_matched": 0.0,
                            "up_fill_price": None, "down_fill_price": None,
                            "unwind": {"action": "none", "resolved": True}}
    for leg in ("up", "down"):
        oid = ctx.get(f"{leg}_order_id")
        if oid: pending_orders[str(oid)] = {"arb_id": arb_id, "leg": leg}

def apply_fill(order_id, matched_size, price=None, status=""):
    meta = pending_orders.get(str(order_id))
    if not meta: return
    arb = pending_arbs.get(meta["arb_id"])
    if not arb or arb.get("reconciled"): return
    leg = meta["leg"]
    size = max(0.0, safe_float(matched_size))
    if size > arb[f"{leg}_matched"]:
        arb[f"{leg}_matched"] = size
        if price is not None: arb[f"{leg}_fill_price"] = safe_float(price)
    if status: arb[f"{leg}_status"] = status
    if arb["up_matched"] > 0 and arb["down_matched"] > 0:
        asyncio.create_task(finalize_arb(meta["arb_id"]))

async def unwind_leg_risk(arb):
    global leg_risk_unresolved_count, live_trading_halted
    diff = arb["up_matched"] - arb["down_matched"]
    if abs(diff) <= 1e-9:
        arb["unwind"] = {"action": "none", "resolved": True}; return
    if LIVE_DRY_RUN or clob_client is None:
        arb["unwind"] = {"action": "not_attempted", "resolved": False}; return
    if diff > 0:
        short, excess, needed = "down", "up", diff
    else:
        short, excess, needed = "up", "down", -diff
    ref = arb.get(f"{short}_fill_price") or arb.get(f"{short}_price") or .5
    retry = await submit_limit_fok(clob_client, arb[f"{short}_token"],
                                   min(.99, ref + LEG_RISK_PRICE_BUFFER), needed, BUY)
    got = safe_float(retry.get("size_matched"))
    arb[f"{short}_matched"] += got
    needed -= got
    if needed > 1e-6:
        ref = arb.get(f"{excess}_fill_price") or arb.get(f"{excess}_price") or .5
        flatten = await submit_limit_fok(clob_client, arb[f"{excess}_token"],
                                         max(.01, ref - LEG_RISK_PRICE_BUFFER),
                                         needed, SELL)
        sold = safe_float(flatten.get("size_matched"))
        arb[f"{excess}_matched"] = max(0.0, arb[f"{excess}_matched"] - sold)
        needed -= sold
    resolved = needed <= 1e-6
    arb["unwind"] = {"action": "resolved" if resolved else "unresolved",
                     "resolved": resolved, "remaining": max(0.0, needed)}
    if not resolved:
        leg_risk_unresolved_count += 1
        if leg_risk_unresolved_count >= LEG_RISK_CIRCUIT_BREAKER_LIMIT:
            live_trading_halted = True
            logger.critical("[CIRCUIT BREAKER] New live trades HALTED.")

async def finalize_arb(arb_id):
    global balance, reserved_capital
    arb = pending_arbs.get(arb_id)
    if not arb or arb.get("reconciled"): return
    arb["reconciled"] = True
    if abs(arb["up_matched"] - arb["down_matched"]) > 1e-9:
        await unwind_leg_risk(arb)
    up, down = arb["up_matched"], arb["down_matched"]
    paired = min(up, down)
    un_up, un_down = max(0, up-down), max(0, down-up)
    up_px = arb.get("up_fill_price") or arb.get("up_price") or 0
    dn_px = arb.get("down_fill_price") or arb.get("down_price") or 0
    cost = paired * (up_px + dn_px)
    fees = taker_fee(paired, up_px) + taker_fee(paired, dn_px)
    net = paired - cost - fees - EST_GAS_FEE_USD - un_up*up_px - un_down*dn_px
    reserved_capital = max(0.0, reserved_capital - arb.get("reserved_usd", 0.0))
    if not LIVE_DRY_RUN: balance += net
    rec = {"timestamp": utc_string(), "arb_id": arb_id, "market_title": arb["title"],
           "up_matched": round(up, 6), "down_matched": round(down, 6),
           "paired_shares": round(paired, 6), "unpaired_up": round(un_up, 6),
           "unpaired_down": round(un_down, 6), "up_fill_price": round(up_px, 6),
           "down_fill_price": round(dn_px, 6), "cost_usd": round(cost, 6),
           "fees_usd": round(fees, 6), "payout_usd": round(paired, 6),
           "net_profit": round(net, 6), "leg_risk": bool(un_up or un_down),
           "unwind_action": arb["unwind"].get("action"),
           "unwind_resolved": arb["unwind"].get("resolved"),
           "balance": round(balance, 6)}
    reconciled_history.append(rec); append_csv(rec, RECON_CSV_FILENAME)
    logger.info("[RECON] %s | UP %.2f DOWN %.2f | paired %.2f | net %+.4f | leg_risk=%s",
                arb["title"], up, down, paired, net, rec["leg_risk"])
    for oid in (arb.get("up_order_id"), arb.get("down_order_id")):
        if oid: pending_orders.pop(str(oid), None)
    pending_arbs.pop(arb_id, None)

async def rest_poll_fills(arb_id):
    arb = pending_arbs.get(arb_id)
    if not arb or not clob_client: return
    for leg in ("up", "down"):
        oid = arb.get(f"{leg}_order_id")
        if not oid: continue
        try:
            if hasattr(clob_client, "get_order"):
                info = await asyncio.to_thread(clob_client.get_order, oid)
                if isinstance(info, dict):
                    apply_fill(str(oid),
                               info.get("size_matched") or info.get("sizeMatched") or 0,
                               info.get("price"),
                               str(info.get("status") or ""))
        except Exception as e:
            logger.debug("REST fill poll failed: %s", e)

async def reconcile_timeout_watcher():
    while not shutdown_requested:
        await asyncio.sleep(.5)
        for aid, arb in list(pending_arbs.items()):
            if not arb.get("reconciled") and now_ts() - arb["created_at"] >= RECONCILE_TIMEOUT_S:
                if clob_client and not LIVE_DRY_RUN: await rest_poll_fills(aid)
                await finalize_arb(aid)

async def execute_live_arb(market, result, signal_ts):
    global reserved_capital
    age_ms = (now_ts() - signal_ts) * 1000
    if age_ms > MAX_LATENCY_MS or live_trading_halted: return
    reserved = result["cost"]
    if available_capital() < reserved: return
    reserved_capital += reserved
    t0 = time.perf_counter()
    up_task = asyncio.create_task(submit_limit_fok(clob_client, market["up_token"],
                                                    result["up_price"], result["shares"], BUY))
    dn_task = asyncio.create_task(submit_limit_fok(clob_client, market["down_token"],
                                                    result["down_price"], result["shares"], BUY))
    up, dn = await asyncio.gather(up_task, dn_task)
    arb_id = f'{market["id"]}-{int(time.time()*1000)}'
    stats["live_submissions"] += 1
    rec = {"timestamp": utc_string(), "arb_id": arb_id, "market_title": market["title"],
           "up_price": result["up_price"], "down_price": result["down_price"],
           "shares": result["shares"], "reserved_usd": reserved,
           "up_success": up["success"], "down_success": dn["success"],
           "up_order_id": up.get("order_id"), "down_order_id": dn.get("order_id"),
           "total_latency_ms": (time.perf_counter()-t0)*1000, "signal_age_ms": age_ms,
           "up_error": up.get("error"), "down_error": dn.get("error")}
    live_trades_history.append(rec); append_csv(rec, LIVE_CSV_FILENAME)
    register_pending_arb(arb_id, {"title": market["title"], "market_id": market["id"],
        "up_token": market["up_token"], "down_token": market["down_token"],
        "up_price": result["up_price"], "down_price": result["down_price"],
        "expected_shares": result["shares"], "reserved_usd": reserved,
        "up_order_id": up.get("order_id"), "down_order_id": dn.get("order_id")})
    if up.get("order_id") and up.get("size_matched"):
        apply_fill(up["order_id"], up["size_matched"], up.get("fill_price"), up.get("status") or "")
    if dn.get("order_id") and dn.get("size_matched"):
        apply_fill(dn["order_id"], dn["size_matched"], dn.get("fill_price"), dn.get("status") or "")
    if LIVE_DRY_RUN:
        apply_fill(up["order_id"], result["shares"], result["up_price"], "dry_run")
        apply_fill(dn["order_id"], result["shares"], result["down_price"], "dry_run")
    logger.info("[LIVE SUBMIT] %s | UP=%s DOWN=%s | %.1fms",
                market["title"], "OK" if up["success"] else "FAIL",
                "OK" if dn["success"] else "FAIL", rec["total_latency_ms"])

def handle_user_event(ev):
    et = str(ev.get("event_type") or ev.get("type") or "").lower()
    if et == "order":
        oid = ev.get("id") or ev.get("order_id")
        if oid:
            apply_fill(str(oid), ev.get("size_matched") or ev.get("sizeMatched") or 0,
                       ev.get("price"), str(ev.get("status") or ev.get("orderEventType") or "").upper())
    elif et == "trade":
        key = ev.get("id") or ev.get("trade_id") or ev.get("tradeId")
        if key:
            key = str(key)
            if key in seen_trade_events: return
            seen_trade_events[key] = now_ts()
        oid = ev.get("taker_order_id") or ev.get("takerOrderId")
        if oid:
            apply_fill(str(oid), ev.get("size") or 0, ev.get("price"), "MATCHED")
        for mo in ev.get("maker_orders") or []:
            oid = mo.get("order_id") or mo.get("orderId")
            if oid:
                apply_fill(str(oid), mo.get("matched_amount") or mo.get("matchedAmount") or 0,
                           mo.get("price"), "MATCHED")

async def user_channel_listener():
    if not api_creds:
        logger.warning("[USER WS] No API credentials.")
        return
    while not shutdown_requested:
        try:
            async with websockets.connect(WS_USER_URL, ping_interval=None,
                                          ping_timeout=None, open_timeout=10) as ws:
                await ws.send(json.dumps({"auth": api_creds, "type": "user"}))
                logger.info("[USER WS] Connected/subscribed.")
                async def heartbeat():
                    while True:
                        try: await ws.send("PING"); await asyncio.sleep(10)
                        except Exception: return
                hb = asyncio.create_task(heartbeat())
                try:
                    async for raw in ws:
                        if raw == "PONG": continue
                        try: msg = json.loads(raw)
                        except Exception: continue
                        for ev in (msg if isinstance(msg, list) else [msg]):
                            if isinstance(ev, dict): handle_user_event(ev)
                finally: hb.cancel()
        except Exception as e:
            logger.warning("[USER WS] disconnected: %s; retrying in 3s", e)
            await asyncio.sleep(3)

async def market_websocket_listener():
    while not shutdown_requested:
        try:
            async with websockets.connect(WS_CLOB_URL, ping_interval=20,
                                          ping_timeout=10, open_timeout=10) as ws:
                stats["ws_connected"] = True
                assets = list(token_to_market)
                if assets:
                    await ws.send(json.dumps({"type": "market", "assets_ids": assets,
                                              "custom_feature_enabled": True}))
                logger.info("[WS] Connected; subscribed to %d tokens", len(assets))
                async for raw in ws:
                    stats["ws_messages"] += 1
                    if raw == "PONG": continue
                    try: msg = json.loads(raw)
                    except Exception: continue
                    for ev in (msg if isinstance(msg, list) else [msg]):
                        if not isinstance(ev, dict): continue
                        et = ev.get("event_type")
                        if et == "book":
                            stats["book_events"] += 1
                            aid = str(ev.get("asset_id", ""))
                            if aid in token_to_market:
                                market_dirty_ts[token_to_market[aid][0]] = now_ts()
                        elif et == "price_change":
                            stats["price_change_events"] += 1
                            for ch in ev.get("price_changes") or []:
                                aid = str(ch.get("asset_id", ""))
                                if aid in token_to_market:
                                    market_dirty_ts[token_to_market[aid][0]] = now_ts()
        except Exception as e:
            stats["ws_errors"] += 1
            logger.warning("[WS] disconnected: %s; retrying in 3s", e)
            await asyncio.sleep(3)
        finally:
            stats["ws_connected"] = False

async def evaluate_market(mid, signal_ts=None):
    stats["evaluations"] += 1
    market = active_markets.get(mid)
    if not market: return
    signal_ts = signal_ts or now_ts()
    if now_ts() - last_evaluation_ts.get(mid, 0) < .15: return
    last_evaluation_ts[mid] = now_ts()
    if now_ts() - last_opportunity_ts.get(mid, 0) < OPPORTUNITY_COOLDOWN_S:
        stats["cooldown_reject"] += 1; return
    up_book, dn_book = await asyncio.gather(
        asyncio.to_thread(fetch_order_book, market["up_token"]),
        asyncio.to_thread(fetch_order_book, market["down_token"]))
    if not up_book or not dn_book:
        stats["no_book"] += 1; return
    cap = LIVE_MAX_TRADE_SIZE if (LIVE_TRADING or LIVE_DRY_RUN) else MAX_TRADE_SIZE
    result = evaluate_books(up_book, dn_book, cap / 2)
    if not result:
        stats["depth_reject"] += 1; return
    if result["combined"] < MIN_COMBINED_PRICE:
        stats["phantom_reject"] += 1; return
    if result["gross_edge"] < MIN_EDGE or result["net"] <= 0:
        stats["edge_reject"] += 1; return
    age_ms = (now_ts() - signal_ts) * 1000
    last_opportunity_ts[mid] = now_ts()
    if LIVE_TRADING or LIVE_DRY_RUN:
        if age_ms > MAX_LATENCY_MS or live_trading_halted: return
        if available_capital() < result["cost"]: return
        asyncio.create_task(execute_live_arb(market, result, signal_ts))
    else:
        execute_paper(market, result, age_ms)

async def dirty_market_loop():
    while not shutdown_requested:
        await asyncio.sleep(.25)
        mids = [mid for mid, ts in list(market_dirty_ts.items())
                if now_ts() - ts < 5]
        if mids:
            await asyncio.gather(*(evaluate_market(mid, market_dirty_ts[mid])
                                   for mid in mids), return_exceptions=True)

async def fallback_scan_loop():
    while not shutdown_requested:
        await asyncio.sleep(10)
        mids = list(active_markets)[:25]
        if mids:
            logger.info("[FALLBACK SCAN] %d markets (WS=%s)",
                        len(mids), "UP" if stats["ws_connected"] else "DOWN")
            for mid in mids:
                await evaluate_market(mid, now_ts())

async def discovery_loop():
    while not shutdown_requested:
        try:
            markets, tokens = await asyncio.to_thread(fetch_crypto_markets)
            update_discovery(markets, tokens)
            logger.info("[DISCOVERY] %d markets | %d tokens",
                        len(active_markets), len(token_to_market))
        except Exception as e:
            logger.warning("Discovery error: %s", e)
        await asyncio.sleep(DISCOVERY_INTERVAL_S)

async def health_loop():
    while not shutdown_requested:
        await asyncio.sleep(30)
        logger.info(
            "[HEALTH] markets=%d tokens=%d ws=%s ws_msgs=%d books=%d "
            "price_changes=%d rest_books=%d/%d eval=%d no_book=%d "
            "depth=%d phantom=%d edge=%d paper=%d live=%d pending=%d "
            "balance=$%.2f reserved=$%.2f available=$%.2f",
            len(active_markets), len(token_to_market),
            "UP" if stats["ws_connected"] else "DOWN",
            stats["ws_messages"], stats["book_events"], stats["price_change_events"],
            stats["rest_book_success"], stats["rest_book_requests"], stats["evaluations"],
            stats["no_book"], stats["depth_reject"], stats["phantom_reject"],
            stats["edge_reject"], stats["paper_trades"], stats["live_submissions"],
            len(pending_arbs), balance, reserved_capital, available_capital())

async def main():
    global clob_client
    if LIVE_TRADING and LIVE_DRY_RUN:
        logger.error("Do not set LIVE_TRADING=1 and LIVE_DRY_RUN=1 together.")
        return
    mode = "LIVE" if LIVE_TRADING else ("DRY-RUN" if LIVE_DRY_RUN else "PAPER")
    print("=" * 78, flush=True)
    print(f" POLYMARKET CRYPTO ARBITRAGE BOT v7.0 [{mode}]", flush=True)
    print("=" * 78, flush=True)
    print(" Hybrid architecture: WebSocket trigger + REST /book validation", flush=True)
    if LIVE_TRADING or LIVE_DRY_RUN:
        clob_client = init_clob_client()
        if LIVE_TRADING and clob_client is None: return
    markets, tokens = await asyncio.to_thread(fetch_crypto_markets)
    update_discovery(markets, tokens)
    logger.info("[STARTUP] Watching %d markets / %d tokens",
                len(active_markets), len(token_to_market))
    tasks = [
        asyncio.create_task(discovery_loop()),
        asyncio.create_task(market_websocket_listener()),
        asyncio.create_task(dirty_market_loop()),
        asyncio.create_task(fallback_scan_loop()),
        asyncio.create_task(health_loop()),
    ]
    if LIVE_TRADING or LIVE_DRY_RUN:
        tasks += [asyncio.create_task(user_channel_listener()),
                  asyncio.create_task(reconcile_timeout_watcher())]
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks: t.cancel()
        flush_history()

if __name__ == "__main__":
    asyncio.run(main())
