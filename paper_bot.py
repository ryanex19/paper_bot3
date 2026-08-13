"""
Polymarket Crypto Arb Bot v6.6
--------------------------------
Builds on v6.5 with:
  - Authenticated user WebSocket for real-time order / trade events
  - Pending-trade registry keyed by order_id
  - Post-trade reconciliation (actual fill size, fees, leg risk)
  - Optional REST fallback poll via get_order / get_trades

Paper mode remains default. Live requires credentials + LIVE_TRADING=1.

Env (live):
  PRIVATE_KEY, FUNDER_ADDRESS, SIGNATURE_TYPE (0/1/2)
  LIVE_TRADING=1 | LIVE_DRY_RUN=1
  MAX_LATENCY_MS=80
  LIVE_MAX_TRADE_SIZE=45
  RECONCILE_TIMEOUT_S=3    how long to wait for fill confirmation before
                           attempting leg-risk unwind
  LEG_RISK_PRICE_BUFFER=0.05        price slippage tolerated to force-fill
                                     the missing leg / force-sell the excess
  LEG_RISK_CIRCUIT_BREAKER_LIMIT=3  unresolved unwinds allowed before all
                                     new live trades are halted
"""

import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, UTC
from typing import Any, Dict, Optional, Tuple

import requests
import websockets

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType, MarketOrderArgs
    from py_clob_client.order_builder.constants import BUY, SELL
    HAS_CLOB_CLIENT = True
except ImportError:
    # py_clob_client is only required for LIVE_TRADING / LIVE_DRY_RUN.
    # Fall back to harmless placeholders so paper mode still imports and
    # runs cleanly without the dependency installed.
    HAS_CLOB_CLIENT = False
    ClobClient = None  # type: ignore
    OrderArgs = OrderType = MarketOrderArgs = None  # type: ignore
    BUY, SELL = "BUY", "SELL"  # type: ignore

# ==========================================
# CONFIG
# ==========================================
INITIAL_BALANCE = 3000.0
MAX_TRADE_SIZE = 45.0
MIN_EDGE = 0.025
EST_GAS_FEE_USD = 0.01
CRYPTO_TAKER_FEE_RATE = 0.07
CSV_FILENAME = "paper_trades_ws_fees.csv"
LIVE_CSV_FILENAME = "live_trades_ws_fees.csv"
RECON_CSV_FILENAME = "live_reconciled.csv"

MARKET_STALE_SECONDS = 300
MAX_LATENCY_MS = int(os.getenv("MAX_LATENCY_MS", "80"))
LIVE_MAX_TRADE_SIZE = float(os.getenv("LIVE_MAX_TRADE_SIZE", str(MAX_TRADE_SIZE)))
RECONCILE_TIMEOUT_S = float(os.getenv("RECONCILE_TIMEOUT_S", "3"))

# --- Leg-risk unwind ---
# How much worse (in price) we're willing to go to force-fill the missing
# leg, or to force-sell the excess leg, when only one side of an arb fills.
LEG_RISK_PRICE_BUFFER = float(os.getenv("LEG_RISK_PRICE_BUFFER", "0.05"))
# How many unresolved/failed unwinds we tolerate in a session before halting
# all new live trades. Repeated leg risk means something structural is
# wrong (latency, sizing, stale books) — not bad luck.
LEG_RISK_CIRCUIT_BREAKER_LIMIT = int(os.getenv("LEG_RISK_CIRCUIT_BREAKER_LIMIT", "3"))

LIVE_TRADING = os.getenv("LIVE_TRADING", "0") == "1"
LIVE_DRY_RUN = os.getenv("LIVE_DRY_RUN", "0") == "1"

CRYPTO_KEYWORDS = [
    "btc", "eth", "sol", "bitcoin", "ethereum",
    "updown", "up/down", "5m", "15m", "crypto",
]

GAMMA_API_URL = "https://gamma-api.polymarket.com"
WS_CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_USER_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
CHAIN_ID = 137

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Global state
balance = INITIAL_BALANCE
trades_history: list = []
live_trades_history: list = []
reconciled_history: list = []
active_markets: Dict[str, dict] = {}
token_to_market: Dict[str, Tuple[str, str]] = {}

# order_id -> pending trade context (for fill matching)
pending_orders: Dict[str, dict] = {}
# trade_group_id -> both legs (for arb-level reconciliation)
pending_arbs: Dict[str, dict] = {}

clob_client: Optional[Any] = None
api_creds: Optional[dict] = None  # {apiKey, secret, passphrase}

# Leg-risk circuit breaker
leg_risk_unresolved_count = 0
live_trading_halted = False
_halt_logged = False

session = requests.Session()
session.headers.update({"User-Agent": "PolymarketCryptoWSBot/6.6"})


# ==========================================
# ORDER BOOK
# ==========================================
def book_from_levels(levels):
    book = {}
    for lvl in levels or []:
        try:
            price = float(lvl.get("price", 0))
            size = float(lvl.get("size", 0))
        except (TypeError, ValueError):
            continue
        if price <= 0 or size <= 0:
            continue
        book[f"{price:.6f}"] = size
    return book


def update_book_level(book, price, size):
    try:
        p = float(price)
        s = float(size) if size is not None else 0.0
    except (TypeError, ValueError):
        return
    if p <= 0:
        return
    key = f"{p:.6f}"
    if s <= 0:
        book.pop(key, None)
    else:
        book[key] = s


def book_to_sorted_asks(book):
    if not book:
        return []
    items = []
    for p_str, size in book.items():
        try:
            items.append({"price": float(p_str), "size": float(size)})
        except (TypeError, ValueError):
            continue
    items.sort(key=lambda x: x["price"])
    return items


# ==========================================
# PRICING / FEES
# ==========================================
def calculate_vwap_and_slippage(asks, target_usd):
    if not asks:
        return None, 0.0, 0.0, 0.0
    sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 1.0)))
    best_ask = float(sorted_asks[0].get("price", 0))
    if best_ask <= 0:
        return None, 0.0, 0.0, 0.0
    remaining_usd = target_usd
    total_shares = total_cost = 0.0
    for ask in sorted_asks:
        price = float(ask.get("price", 0))
        size = float(ask.get("size", 0))
        if price <= 0 or size <= 0:
            continue
        level_usd = price * size
        if remaining_usd <= level_usd:
            shares = remaining_usd / price
            total_shares += shares
            total_cost += remaining_usd
            remaining_usd = 0
            break
        total_shares += size
        total_cost += level_usd
        remaining_usd -= level_usd
    if remaining_usd > 0 or total_shares == 0:
        return None, 0.0, 0.0, 0.0
    vwap = total_cost / total_shares
    slip = ((vwap - best_ask) / best_ask) * 100 if best_ask > 0 else 0.0
    return vwap, slip, best_ask, total_shares


def price_for_shares(asks, target_shares):
    if not asks or target_shares <= 0:
        return None
    sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 1.0)))
    remaining = target_shares
    total_cost = 0.0
    for ask in sorted_asks:
        price = float(ask.get("price", 0))
        size = float(ask.get("size", 0))
        if price <= 0 or size <= 0:
            continue
        if remaining <= size:
            total_cost += remaining * price
            remaining = 0
            break
        total_cost += size * price
        remaining -= size
    if remaining > 0:
        return None
    return total_cost / target_shares


def calculate_polymarket_taker_fee(shares, price):
    return shares * CRYPTO_TAKER_FEE_RATE * price * (1.0 - price)


# ==========================================
# CSV HELPERS
# ==========================================
def append_csv(record, filename):
    try:
        exists = os.path.isfile(filename)
        with open(filename, mode="a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=record.keys())
            if not exists:
                w.writeheader()
            w.writerow(record)
    except Exception as e:
        logger.warning(f"CSV write failed ({filename}): {e}")


def flush_all():
    for hist, name in (
        (trades_history, CSV_FILENAME),
        (live_trades_history, LIVE_CSV_FILENAME),
        (reconciled_history, RECON_CSV_FILENAME),
    ):
        if not hist:
            continue
        try:
            with open(name, mode="w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=hist[0].keys())
                w.writeheader()
                w.writerows(hist)
            logger.info(f"Flushed {len(hist)} rows → {name}")
        except Exception as e:
            logger.error(f"Flush {name} failed: {e}")


def signal_handler(sig, frame):
    logger.info("Shutting down, flushing...")
    flush_all()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ==========================================
# DISCOVERY
# ==========================================
def is_crypto_market(market):
    title = (market.get("question") or market.get("title") or "").lower()
    return any(k in title for k in CRYPTO_KEYWORDS)


def fetch_crypto_markets():
    markets_dict, tokens_map = {}, {}
    now = datetime.now(UTC).timestamp()
    try:
        params = {
            "tag_id": 21,
            "limit": 25,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        resp = session.get(f"{GAMMA_API_URL}/markets", params=params, timeout=5)
        if resp.status_code == 200:
            for m in resp.json():
                if not is_crypto_market(m):
                    continue
                m_id = m.get("id")
                clob_tokens = m.get("clobTokenIds")
                if isinstance(clob_tokens, str):
                    try:
                        clob_tokens = json.loads(clob_tokens)
                    except Exception:
                        continue
                if not clob_tokens or len(clob_tokens) < 2:
                    continue
                up_t, down_t = str(clob_tokens[0]), str(clob_tokens[1])
                markets_dict[m_id] = {
                    "title": m.get("question") or m.get("title"),
                    "up_token": up_t,
                    "down_token": down_t,
                    "up_asks": {},
                    "down_asks": {},
                    "last_seen": now,
                    "condition_id": m.get("conditionId") or m.get("condition_id"),
                    "neg_risk": bool(m.get("negRisk") or m.get("neg_risk") or False),
                    "tick_size": str(
                        m.get("minimum_tick_size")
                        or m.get("orderPriceMinTickSize")
                        or "0.01"
                    ),
                }
                tokens_map[up_t] = (m_id, "UP")
                tokens_map[down_t] = (m_id, "DOWN")
    except Exception as e:
        logger.warning(f"Discovery failed: {e}")
    return markets_dict, tokens_map


def prune_stale_markets(seen_ids):
    global active_markets, token_to_market
    now = datetime.now(UTC).timestamp()
    stale = []
    for mid, data in list(active_markets.items()):
        if mid in seen_ids:
            data["last_seen"] = now
            continue
        if (now - data.get("last_seen", 0)) > MARKET_STALE_SECONDS:
            stale.append(mid)
    for mid in stale:
        data = active_markets.pop(mid, None)
        if data:
            token_to_market.pop(data.get("up_token"), None)
            token_to_market.pop(data.get("down_token"), None)
    if stale:
        logger.info(f"[DISCOVERY] Pruned {len(stale)} stale markets")


# ==========================================
# CLOB CLIENT + LIVE SUBMIT
# ==========================================
def init_clob_client():
    global api_creds
    if not HAS_CLOB_CLIENT:
        logger.error("pip install py-clob-client required for live mode")
        return None
    pk = os.getenv("PRIVATE_KEY")
    if not pk:
        logger.error("PRIVATE_KEY required")
        return None
    funder = os.getenv("FUNDER_ADDRESS") or None
    sig_type = int(os.getenv("SIGNATURE_TYPE", "0"))
    try:
        client = ClobClient(
            CLOB_HOST,
            key=pk,
            chain_id=CHAIN_ID,
            signature_type=sig_type,
            funder=funder,
        )
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        # Store for user-channel auth
        if hasattr(creds, "api_key"):
            api_creds = {
                "apiKey": creds.api_key,
                "secret": creds.api_secret,
                "passphrase": creds.api_passphrase,
            }
        elif isinstance(creds, dict):
            api_creds = {
                "apiKey": creds.get("apiKey") or creds.get("api_key"),
                "secret": creds.get("secret") or creds.get("api_secret"),
                "passphrase": creds.get("passphrase") or creds.get("api_passphrase"),
            }
        logger.info(f"CLOB client ready (sig_type={sig_type})")
        return client
    except Exception as e:
        logger.error(f"CLOB init failed: {e}")
        return None


async def submit_order(client, token_id, price, size, side=BUY, order_type="FOK"):
    """
    Submit a single leg order. side=BUY for opening a leg, side=SELL for
    flattening an already-filled leg during leg-risk unwind.

    NOTE: for market SELL orders, `amount` semantics on MarketOrderArgs may
    differ between share-count and USD depending on py_clob_client version —
    this assumes `amount` means "shares to sell" for SELL and "USD to spend"
    for BUY, matching common usage, but verify against the installed
    py_clob_client version / API docs before relying on this in size-critical
    situations (e.g. during a LIVE_DRY_RUN soak test).
    """
    start = time.perf_counter()
    result = {
        "token_id": token_id,
        "price": price,
        "size": size,
        "side": "BUY" if side == BUY else "SELL",
        "success": False,
        "order_id": None,
        "status": None,
        "error": None,
        "latency_ms": 0.0,
        "size_matched": 0.0,
    }

    def _post():
        if hasattr(client, "create_market_order") and order_type in ("FOK", "FAK"):
            amount = round(price * size, 4) if side == BUY else round(size, 4)
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=side,
                order_type=getattr(OrderType, order_type, OrderType.FOK),
            )
            signed = client.create_market_order(mo)
            return client.post_order(signed, getattr(OrderType, order_type, OrderType.FOK))
        args = OrderArgs(
            token_id=token_id,
            price=round(price, 4),
            size=round(size, 4),
            side=side,
        )
        signed = client.create_order(args)
        return client.post_order(signed, getattr(OrderType, order_type, OrderType.FOK))

    try:
        if LIVE_DRY_RUN:
            await asyncio.sleep(0.035)
            result.update(
                success=True,
                order_id=f"dry-{token_id[-8:]}-{int(time.time()*1000)%100000}",
                status="dry_run",
                size_matched=size,
            )
        else:
            resp = await asyncio.to_thread(_post)
            if isinstance(resp, dict):
                oid = resp.get("orderID") or resp.get("id") or resp.get("order_id")
                result["order_id"] = oid
                result["success"] = bool(oid or resp.get("success"))
                result["status"] = resp.get("status") or ("ok" if result["success"] else "error")
                result["error"] = resp.get("errorMsg") or resp.get("error")
                # Some responses include matched size immediately
                for k in ("size_matched", "takingAmount", "makingAmount", "matched"):
                    if k in resp and resp[k] is not None:
                        try:
                            result["size_matched"] = float(resp[k])
                        except (TypeError, ValueError):
                            pass
            else:
                result["success"] = True
                result["status"] = "ok"
    except Exception as e:
        result["error"] = str(e)
        result["status"] = "exception"
    result["latency_ms"] = (time.perf_counter() - start) * 1000
    return result


async def submit_leg(client, token_id, price, size, order_type="FOK"):
    """Backward-compatible BUY wrapper around submit_order."""
    return await submit_order(client, token_id, price, size, side=BUY, order_type=order_type)


async def attempt_leg_retry(client, token_id, needed_shares, reference_price):
    """
    Try to complete the missing leg of an arb by buying `needed_shares` at a
    worse (more marketable) price than the original attempt, to maximize the
    chance of a fill within the unwind window.
    """
    worse_price = min(0.99, reference_price + LEG_RISK_PRICE_BUFFER)
    logger.info(
        f"[UNWIND] Retrying missing leg: buy {needed_shares:.2f} sh of {token_id[:12]}... "
        f"@ up to {worse_price:.4f}"
    )
    return await submit_order(client, token_id, worse_price, needed_shares, side=BUY)


async def flatten_leg(client, token_id, shares, reference_price):
    """
    Force-sell an excess/naked leg to close out exposure, at a worse (more
    marketable) price than the fill price, to maximize fill probability.
    """
    worse_price = max(0.01, reference_price - LEG_RISK_PRICE_BUFFER)
    logger.info(
        f"[UNWIND] Flattening excess leg: sell {shares:.2f} sh of {token_id[:12]}... "
        f"@ down to {worse_price:.4f}"
    )
    return await submit_order(client, token_id, worse_price, shares, side=SELL)


# ==========================================
# RECONCILIATION
# ==========================================
def register_pending_arb(arb_id: str, ctx: dict):
    """ctx holds both legs + expected sizes/prices."""
    pending_arbs[arb_id] = {
        **ctx,
        "created_at": time.time(),
        "up_fill": None,
        "down_fill": None,
        "up_matched": 0.0,
        "down_matched": 0.0,
        "reconciled": False,
    }
    for leg in ("up", "down"):
        oid = ctx.get(f"{leg}_order_id")
        if oid:
            pending_orders[oid] = {"arb_id": arb_id, "leg": leg}


def apply_fill_to_pending(order_id: str, matched_size: float, price: Optional[float] = None, status: str = ""):
    """Called from user-channel or REST poll when we learn about a fill."""
    meta = pending_orders.get(order_id)
    if not meta:
        return
    arb_id = meta["arb_id"]
    leg = meta["leg"]
    arb = pending_arbs.get(arb_id)
    if not arb or arb.get("reconciled"):
        return

    arb[f"{leg}_matched"] = max(arb[f"{leg}_matched"], float(matched_size or 0))
    if price is not None:
        arb[f"{leg}_fill_price"] = float(price)
    if status:
        arb[f"{leg}_status"] = status

    # Both legs have some confirmation?
    up_done = arb["up_matched"] > 0 or arb.get("up_status") in (
        "MATCHED", "CONFIRMED", "MINED", "dry_run", "ok", "live"
    )
    down_done = arb["down_matched"] > 0 or arb.get("down_status") in (
        "MATCHED", "CONFIRMED", "MINED", "dry_run", "ok", "live"
    )
    # Also treat explicit failure as "done" so we can close the arb
    up_failed = arb.get("up_status") in ("FAILED", "CANCELLED", "CANCELLATION", "exception")
    down_failed = arb.get("down_status") in ("FAILED", "CANCELLED", "CANCELLATION", "exception")

    if (up_done or up_failed) and (down_done or down_failed):
        asyncio.create_task(finalize_arb(arb_id))


async def unwind_leg_risk(arb: dict) -> dict:
    """
    Called from finalize_arb when up_matched != down_matched. Tries, in
    order:
      1. Retry-buy the short leg (complete the hedge, even at a worse price).
      2. If that fails or only partially fills, flatten the excess leg by
         selling it back (bounding the loss instead of holding it naked).
    Mutates and returns arb with updated *_matched / *_fill_price fields and
    an "unwind" summary dict describing what happened.
    """
    global leg_risk_unresolved_count, live_trading_halted, _halt_logged

    up_m = arb.get("up_matched", 0.0)
    down_m = arb.get("down_matched", 0.0)
    diff = up_m - down_m

    unwind = {"action": "none", "retry_matched": 0.0, "flatten_matched": 0.0, "resolved": True}

    if abs(diff) < 1e-9:
        arb["unwind"] = unwind
        return arb

    if diff > 0:
        short_leg, excess_leg = "down", "up"
        short_token = arb.get("down_token")
        excess_token = arb.get("up_token")
        needed = diff
    else:
        short_leg, excess_leg = "up", "down"
        short_token = arb.get("up_token")
        excess_token = arb.get("down_token")
        needed = -diff

    if LIVE_DRY_RUN or clob_client is None or not short_token or not excess_token:
        # Nothing we can actually submit (dry run has no real book to fill
        # against, or client/tokens missing) — leave as unresolved leg risk
        # for the accounting layer to record, but don't pretend we unwound it.
        unwind.update(action="skipped_dry_run_or_no_client", resolved=(diff == 0))
        arb["unwind"] = unwind
        return arb

    # 1. Try to complete the missing leg
    ref_price = arb.get(f"{short_leg}_fill_price", arb.get(f"{short_leg}_price", 0.5))
    retry_res = await attempt_leg_retry(clob_client, short_token, needed, ref_price)
    retry_matched = retry_res.get("size_matched", 0.0) if retry_res.get("success") else 0.0
    unwind["retry_matched"] = retry_matched
    unwind["retry_error"] = retry_res.get("error")

    if retry_matched > 0:
        arb[f"{short_leg}_matched"] = arb.get(f"{short_leg}_matched", 0.0) + retry_matched
        needed -= retry_matched

    if needed > 1e-6:
        # 2. Retry didn't fully close the gap — flatten the excess side instead
        excess_ref_price = arb.get(f"{excess_leg}_fill_price", arb.get(f"{excess_leg}_price", 0.5))
        flatten_res = await flatten_leg(clob_client, excess_token, needed, excess_ref_price)
        flatten_matched = flatten_res.get("size_matched", 0.0) if flatten_res.get("success") else 0.0
        unwind["flatten_matched"] = flatten_matched
        unwind["flatten_error"] = flatten_res.get("error")

        if flatten_matched > 0:
            arb[f"{excess_leg}_matched"] = max(0.0, arb.get(f"{excess_leg}_matched", 0.0) - flatten_matched)
            needed -= flatten_matched

    if needed > 1e-6:
        # Neither retry nor flatten fully closed the gap — real unresolved
        # exposure remains. Count it against the circuit breaker.
        unwind["action"] = "unresolved"
        unwind["resolved"] = False
        leg_risk_unresolved_count += 1
        logger.critical(
            f"[UNWIND] Could not fully resolve leg risk for {arb.get('title')}: "
            f"{needed:.2f} sh still unhedged. Unresolved count = "
            f"{leg_risk_unresolved_count}/{LEG_RISK_CIRCUIT_BREAKER_LIMIT}"
        )
        if leg_risk_unresolved_count >= LEG_RISK_CIRCUIT_BREAKER_LIMIT and not live_trading_halted:
            live_trading_halted = True
            logger.critical(
                "[CIRCUIT BREAKER] Repeated unresolved leg risk — halting all new live trades. "
                "Restart the process (after investigating) to resume."
            )
    else:
        unwind["action"] = "retried" if retry_matched > 0 else "flattened"
        unwind["resolved"] = True

    arb["unwind"] = unwind
    return arb


async def finalize_arb(arb_id: str):
    global balance
    arb = pending_arbs.get(arb_id)
    if not arb or arb.get("reconciled"):
        return
    arb["reconciled"] = True

    # If one leg filled and the other didn't, try to resolve it before
    # locking in the final accounting.
    up_m0 = arb.get("up_matched", 0.0)
    down_m0 = arb.get("down_matched", 0.0)
    if abs(up_m0 - down_m0) > 1e-9:
        arb = await unwind_leg_risk(arb)

    up_m = arb.get("up_matched", 0.0)
    down_m = arb.get("down_matched", 0.0)
    # For a pure arb we can only lock the min matched on both sides
    paired = min(up_m, down_m)
    unpaired_up = max(0.0, up_m - down_m)
    unpaired_down = max(0.0, down_m - up_m)

    up_px = arb.get("up_fill_price", arb.get("up_price", 0))
    down_px = arb.get("down_fill_price", arb.get("down_price", 0))

    cost = paired * (up_px + down_px)
    fees = (
        calculate_polymarket_taker_fee(paired, up_px)
        + calculate_polymarket_taker_fee(paired, down_px)
    )
    # Unpaired inventory is directional risk — mark at cost for now.
    # This is subtracted from net regardless of whether paired > 0: a naked
    # leg costs real money whether or not the other leg also filled.
    unpaired_cost = unpaired_up * up_px + unpaired_down * down_px
    payout = paired * 1.0
    net = payout - cost - fees - EST_GAS_FEE_USD - unpaired_cost

    if not LIVE_DRY_RUN:
        balance += net

    record = {
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "arb_id": arb_id,
        "market_title": arb.get("title"),
        "mode": "dry_run" if LIVE_DRY_RUN else "live",
        "up_order_id": arb.get("up_order_id"),
        "down_order_id": arb.get("down_order_id"),
        "up_matched": round(up_m, 4),
        "down_matched": round(down_m, 4),
        "paired_shares": round(paired, 4),
        "unpaired_up": round(unpaired_up, 4),
        "unpaired_down": round(unpaired_down, 4),
        "up_fill_price": round(up_px, 4),
        "down_fill_price": round(down_px, 4),
        "cost_usd": round(cost, 4),
        "fees_usd": round(fees, 4),
        "payout_usd": round(payout, 4),
        "net_profit": round(net, 4),
        "leg_risk": unpaired_up > 0 or unpaired_down > 0,
        "unwind_action": arb.get("unwind", {}).get("action", "none"),
        "unwind_resolved": arb.get("unwind", {}).get("resolved", True),
        "new_balance": round(balance, 2),
        "signal_age_ms": arb.get("signal_age_ms"),
        "total_latency_ms": arb.get("total_latency_ms"),
    }
    reconciled_history.append(record)
    append_csv(record, RECON_CSV_FILENAME)

    if not record["unwind_resolved"]:
        risk_flag = " 🛑 UNRESOLVED LEG RISK"
    elif record["unwind_action"] not in ("none",):
        risk_flag = f" ⚠ LEG RISK (unwound: {record['unwind_action']})"
    else:
        risk_flag = ""
    print("\n" + "-" * 75, flush=True)
    print(f"✅ [RECONCILED]{risk_flag} {arb.get('title')}", flush=True)
    print(
        f"   Paired {paired:.2f} sh | UP filled {up_m:.2f} @ {up_px:.4f} | "
        f"DOWN filled {down_m:.2f} @ {down_px:.4f}",
        flush=True,
    )
    print(
        f"   Cost ${cost:.4f} + fees ${fees:.4f} → payout ${payout:.4f} | "
        f"Net {net:+.4f} | Bal ${balance:.2f}",
        flush=True,
    )
    print("-" * 75 + "\n", flush=True)

    # Cleanup order map
    for oid in (arb.get("up_order_id"), arb.get("down_order_id")):
        pending_orders.pop(oid, None)

    # Cleanup the arb entry itself. reconcile_timeout_watcher already
    # skips reconciled entries, but without this pending_arbs grows
    # unbounded over a long-running live session.
    pending_arbs.pop(arb_id, None)


async def reconcile_timeout_watcher():
    """Close arbs that never got full confirmation."""
    while True:
        await asyncio.sleep(0.5)
        now = time.time()
        for arb_id, arb in list(pending_arbs.items()):
            if arb.get("reconciled"):
                continue
            if (now - arb["created_at"]) >= RECONCILE_TIMEOUT_S:
                logger.warning(
                    f"[RECON] Timeout on {arb_id} ({arb.get('title')}) – finalizing with known fills"
                )
                # Try one REST poll if client available
                if clob_client and not LIVE_DRY_RUN:
                    await rest_poll_fills(arb_id)
                await finalize_arb(arb_id)


async def rest_poll_fills(arb_id: str):
    """Best-effort REST lookup of order / trades for a pending arb."""
    arb = pending_arbs.get(arb_id)
    if not arb or not clob_client:
        return
    for leg, oid in (("up", arb.get("up_order_id")), ("down", arb.get("down_order_id"))):
        if not oid or arb.get(f"{leg}_matched", 0) > 0:
            continue
        try:
            if hasattr(clob_client, "get_order"):
                info = await asyncio.to_thread(clob_client.get_order, oid)
                if isinstance(info, dict):
                    matched = float(info.get("size_matched") or info.get("sizeMatched") or 0)
                    if matched > 0:
                        apply_fill_to_pending(oid, matched, status=info.get("status", "MATCHED"))
        except Exception as e:
            logger.debug(f"REST poll {oid}: {e}")


# ==========================================
# LIVE EXECUTION (with registration)
# ==========================================
async def execute_live_arb(
    market_id, title, up_token, down_token,
    up_price, down_price, shares, signal_ts,
    tick_size="0.01", neg_risk=False,
):
    global clob_client

    age_ms = (time.time() - signal_ts) * 1000
    if age_ms > MAX_LATENCY_MS:
        logger.info(f"[LIVE SKIP] {title}: age {age_ms:.0f} ms > {MAX_LATENCY_MS} ms")
        return

    if clob_client is None and not LIVE_DRY_RUN:
        logger.error("[LIVE] No client")
        return

    t0 = time.perf_counter()
    up_task = asyncio.create_task(submit_leg(clob_client, up_token, up_price, shares))
    down_task = asyncio.create_task(submit_leg(clob_client, down_token, down_price, shares))
    up_res, down_res = await asyncio.gather(up_task, down_task)
    total_ms = (time.perf_counter() - t0) * 1000

    arb_id = f"{market_id}-{int(time.time()*1000)}"
    both_ok = up_res["success"] and down_res["success"]
    leg_risk = up_res["success"] != down_res["success"]

    # Immediate estimate (will be corrected by reconcile)
    trade_cost = shares * (up_price + down_price)
    fees = (
        calculate_polymarket_taker_fee(shares, up_price)
        + calculate_polymarket_taker_fee(shares, down_price)
    )
    est_net = (shares - trade_cost - fees - EST_GAS_FEE_USD) if both_ok else 0.0

    live_rec = {
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "arb_id": arb_id,
        "market_title": title,
        "mode": "dry_run" if LIVE_DRY_RUN else "live",
        "up_price": round(up_price, 4),
        "down_price": round(down_price, 4),
        "shares": round(shares, 4),
        "up_success": up_res["success"],
        "down_success": down_res["success"],
        "up_order_id": up_res.get("order_id"),
        "down_order_id": down_res.get("order_id"),
        "up_latency_ms": round(up_res["latency_ms"], 1),
        "down_latency_ms": round(down_res["latency_ms"], 1),
        "total_latency_ms": round(total_ms, 1),
        "signal_age_ms": round(age_ms, 1),
        "leg_risk": leg_risk,
        "est_net_profit": round(est_net, 4),
        "up_error": up_res.get("error"),
        "down_error": down_res.get("error"),
    }
    live_trades_history.append(live_rec)
    append_csv(live_rec, LIVE_CSV_FILENAME)

    status = "BOTH OK" if both_ok else ("LEG RISK" if leg_risk else "BOTH FAIL")
    print("\n" + "=" * 75, flush=True)
    print(f"⚡ [LIVE {status}] {title}", flush=True)
    print(
        f"   UP  ${up_price:.4f} x {shares:.2f} → "
        f"{'OK' if up_res['success'] else 'FAIL'} ({up_res['latency_ms']:.0f} ms) id={up_res.get('order_id')}",
        flush=True,
    )
    print(
        f"   DOWN ${down_price:.4f} x {shares:.2f} → "
        f"{'OK' if down_res['success'] else 'FAIL'} ({down_res['latency_ms']:.0f} ms) id={down_res.get('order_id')}",
        flush=True,
    )
    print(f"   RTT {total_ms:.0f} ms | Age {age_ms:.0f} ms | Est net {est_net:+.4f}", flush=True)
    print("=" * 75 + "\n", flush=True)

    # Register for fill confirmation
    register_pending_arb(
        arb_id,
        {
            "title": title,
            "market_id": market_id,
            "up_order_id": up_res.get("order_id"),
            "down_order_id": down_res.get("order_id"),
            "up_token": up_token,
            "down_token": down_token,
            "up_price": up_price,
            "down_price": down_price,
            "expected_shares": shares,
            "signal_age_ms": age_ms,
            "total_latency_ms": total_ms,
            "up_status": up_res.get("status"),
            "down_status": down_res.get("status"),
        },
    )
    # Seed matched size if the POST response already gave it
    if up_res.get("size_matched"):
        apply_fill_to_pending(up_res["order_id"], up_res["size_matched"], up_price, up_res.get("status"))
    if down_res.get("size_matched"):
        apply_fill_to_pending(down_res["order_id"], down_res["size_matched"], down_price, down_res.get("status"))
    # Dry-run: immediately treat as fully filled
    if LIVE_DRY_RUN:
        if up_res.get("order_id"):
            apply_fill_to_pending(up_res["order_id"], shares, up_price, "dry_run")
        if down_res.get("order_id"):
            apply_fill_to_pending(down_res["order_id"], shares, down_price, "dry_run")


# ==========================================
# USER CHANNEL (fill stream)
# ==========================================
async def user_channel_listener():
    """Authenticated WS for order + trade events → drives reconciliation."""
    if not api_creds:
        logger.warning("[USER WS] No API creds – fill confirmation disabled")
        return

    while True:
        try:
            async with websockets.connect(
                WS_USER_URL, ping_interval=None, ping_timeout=None
            ) as ws:
                # Auth subscribe (all markets)
                sub = {
                    "auth": {
                        "apiKey": api_creds["apiKey"],
                        "secret": api_creds["secret"],
                        "passphrase": api_creds["passphrase"],
                    },
                    "type": "user",
                }
                await ws.send(json.dumps(sub))
                logger.info("[USER WS] Subscribed to user channel")

                # Application-level heartbeat
                async def heartbeat():
                    while True:
                        try:
                            await ws.send("PING")
                        except Exception:
                            break
                        await asyncio.sleep(10)

                hb = asyncio.create_task(heartbeat())
                try:
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        events = msg if isinstance(msg, list) else [msg]
                        for ev in events:
                            if not isinstance(ev, dict):
                                continue
                            handle_user_event(ev)
                finally:
                    hb.cancel()
        except Exception as e:
            logger.warning(f"[USER WS] disconnected: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)


def handle_user_event(ev: dict):
    """
    Process order / trade events from the user channel.
    """
    et = (ev.get("event_type") or ev.get("type") or "").lower()

    if et == "order":
        oid = ev.get("id") or ev.get("order_id")
        if not oid:
            return
        size_matched = ev.get("size_matched") or ev.get("sizeMatched") or 0
        status = (
            ev.get("type")  # PLACEMENT / UPDATE / CANCELLATION
            or ev.get("status")
            or ev.get("orderEventType")
            or ""
        )
        try:
            matched = float(size_matched)
        except (TypeError, ValueError):
            matched = 0.0
        price = None
        try:
            if ev.get("price") is not None:
                price = float(ev["price"])
        except (TypeError, ValueError):
            pass
        apply_fill_to_pending(str(oid), matched, price, str(status).upper())

    elif et == "trade":
        # Trade events can carry the taker order id and matched size
        taker_oid = ev.get("taker_order_id") or ev.get("takerOrderId")
        size = ev.get("size") or 0
        price = ev.get("price")
        status = ev.get("status") or "MATCHED"
        try:
            matched = float(size)
        except (TypeError, ValueError):
            matched = 0.0
        try:
            px = float(price) if price is not None else None
        except (TypeError, ValueError):
            px = None

        if taker_oid:
            apply_fill_to_pending(str(taker_oid), matched, px, str(status).upper())

        # Also check maker_orders for our resting orders (less common for this bot)
        for mo in ev.get("maker_orders") or []:
            moid = mo.get("order_id") or mo.get("orderId")
            if not moid:
                continue
            try:
                mamt = float(mo.get("matched_amount") or mo.get("matchedAmount") or 0)
            except (TypeError, ValueError):
                mamt = 0.0
            try:
                mpx = float(mo["price"]) if mo.get("price") is not None else None
            except (TypeError, ValueError):
                mpx = None
            apply_fill_to_pending(str(moid), mamt, mpx, "MATCHED")


# ==========================================
# ARB ENGINE
# ==========================================
def evaluate_arbitrage(market_id: str, signal_ts: Optional[float] = None):
    global balance
    if signal_ts is None:
        signal_ts = time.time()

    m = active_markets.get(market_id)
    if not m:
        return

    title = m.get("title")
    max_size = LIVE_MAX_TRADE_SIZE if (LIVE_TRADING or LIVE_DRY_RUN) else MAX_TRADE_SIZE
    target = max_size / 2.0

    up_asks = book_to_sorted_asks(m.get("up_asks", {}))
    down_asks = book_to_sorted_asks(m.get("down_asks", {}))

    up_vwap, up_slip, up_best, up_sh = calculate_vwap_and_slippage(up_asks, target)
    down_vwap, down_slip, down_best, down_sh = calculate_vwap_and_slippage(down_asks, target)
    if up_vwap is None or down_vwap is None:
        return

    combined = up_vwap + down_vwap
    if combined < 0.90:
        logger.info(f"[SKIP] {title}: combined={combined:.3f} implausible")
        return
    if (1.0 - combined) < MIN_EDGE or balance < max_size:
        return

    executable = min(up_sh, down_sh)
    if executable <= 0:
        return

    if executable == up_sh:
        actual_up, actual_down = up_vwap, price_for_shares(down_asks, executable)
    else:
        actual_down, actual_up = down_vwap, price_for_shares(up_asks, executable)
    if actual_up is None or actual_down is None:
        return

    cost = executable * (actual_up + actual_down)
    fees = (
        calculate_polymarket_taker_fee(executable, actual_up)
        + calculate_polymarket_taker_fee(executable, actual_down)
    )
    net = executable - cost - fees - EST_GAS_FEE_USD
    if net <= 0:
        return

    if LIVE_TRADING or LIVE_DRY_RUN:
        global _halt_logged
        if live_trading_halted:
            if not _halt_logged:
                logger.critical(
                    "[CIRCUIT BREAKER] Live trading is halted due to repeated unresolved "
                    "leg risk. Not opening new trades. Restart the process to resume."
                )
                _halt_logged = True
            return
        asyncio.create_task(
            execute_live_arb(
                market_id, title,
                m["up_token"], m["down_token"],
                actual_up, actual_down, executable, signal_ts,
                m.get("tick_size", "0.01"), m.get("neg_risk", False),
            )
        )
        return

    # Paper
    balance += net
    rec = {
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "market_title": title,
        "up_best_ask": round(up_best, 3),
        "up_vwap": round(actual_up, 3),
        "up_slippage_pct": round(up_slip, 2),
        "down_best_ask": round(down_best, 3),
        "down_vwap": round(actual_down, 3),
        "down_slippage_pct": round(down_slip, 2),
        "spend_usd": round(cost, 2),
        "taker_fee_usd": round(fees, 4),
        "gas_fee_usd": EST_GAS_FEE_USD,
        "payout_usd": round(executable, 2),
        "net_profit": round(net, 2),
        "net_edge_pct": round((net / cost) * 100, 2),
        "new_balance": round(balance, 2),
    }
    trades_history.append(rec)
    append_csv(rec, CSV_FILENAME)
    print("\n" + "=" * 75, flush=True)
    print(f"⚡ [PAPER] {title}", flush=True)
    print(
        f"   UP ${up_best:.3f}/{actual_up:.3f} | DOWN ${down_best:.3f}/{actual_down:.3f} | "
        f"Net +${net:.2f} | Bal ${balance:.2f}",
        flush=True,
    )
    print("=" * 75 + "\n", flush=True)


# ==========================================
# MARKET WS + DISCOVERY LOOPS
# ==========================================
async def subscription_manager(ws):
    subscribed = set()
    while True:
        try:
            current = list(token_to_market.keys())
            if set(current) - subscribed and current:
                logger.info(f"[WS] Subscribe {len(current)} assets")
                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": current,
                    "custom_feature_enabled": True,
                }))
                subscribed = set(current)
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Sub mgr: {e}")
            await asyncio.sleep(5)


async def market_discovery_loop():
    global active_markets, token_to_market
    while True:
        await asyncio.sleep(60)
        new_m, new_t = fetch_crypto_markets()
        if new_m:
            for mid, data in new_m.items():
                if mid in active_markets:
                    active_markets[mid]["title"] = data["title"]
                    active_markets[mid]["last_seen"] = data["last_seen"]
                    active_markets[mid]["tick_size"] = data.get("tick_size", "0.01")
                    active_markets[mid]["neg_risk"] = data.get("neg_risk", False)
                else:
                    active_markets[mid] = data
            token_to_market.update(new_t)
            prune_stale_markets(set(new_m.keys()))
            logger.info(f"[DISCOVERY] {len(active_markets)} markets")


async def websocket_listener():
    while True:
        try:
            async with websockets.connect(WS_CLOB_URL, ping_interval=20, ping_timeout=10) as ws:
                logger.info("Connected to market WS")
                sub = asyncio.create_task(subscription_manager(ws))
                try:
                    async for message in ws:
                        signal_ts = time.time()
                        raw = json.loads(message)
                        events = raw if isinstance(raw, list) else [raw]
                        for data in events:
                            if not isinstance(data, dict):
                                continue
                            et = data.get("event_type")
                            if et == "book":
                                aid = str(data.get("asset_id", ""))
                                if aid in token_to_market:
                                    mid, side = token_to_market[aid]
                                    if mid in active_markets:
                                        key = "up_asks" if side == "UP" else "down_asks"
                                        active_markets[mid][key] = book_from_levels(data.get("asks", []))
                                        evaluate_arbitrage(mid, signal_ts)
                            elif et == "price_change":
                                touched = set()
                                for ch in data.get("price_changes", []):
                                    aid = str(ch.get("asset_id", ""))
                                    if aid not in token_to_market:
                                        continue
                                    mid, side = token_to_market[aid]
                                    if mid not in active_markets:
                                        continue
                                    if ch.get("price") is None:
                                        continue
                                    s = (ch.get("side") or "").upper()
                                    if s and s not in ("SELL", "ASK", "A"):
                                        continue
                                    book = (
                                        active_markets[mid]["up_asks"]
                                        if side == "UP"
                                        else active_markets[mid]["down_asks"]
                                    )
                                    update_book_level(book, ch["price"], ch.get("size"))
                                    touched.add(mid)
                                for mid in touched:
                                    evaluate_arbitrage(mid, signal_ts)
                finally:
                    sub.cancel()
        except Exception as e:
            logger.warning(f"Market WS lost: {e}. Retry 3s...")
            await asyncio.sleep(3)


# ==========================================
# MAIN
# ==========================================
async def main():
    global clob_client, balance

    mode = "LIVE" if LIVE_TRADING else ("DRY-RUN" if LIVE_DRY_RUN else "PAPER")
    print("=" * 75, flush=True)
    print(f"   POLYMARKET CRYPTO ARB BOT v6.6  [{mode}]  (fills + recon)", flush=True)
    print("=" * 75, flush=True)
    print(f" Balance ${INITIAL_BALANCE:.2f} | Max size ${LIVE_MAX_TRADE_SIZE if mode != 'PAPER' else MAX_TRADE_SIZE:.2f}", flush=True)
    print(f" Min edge {MIN_EDGE*100:.1f}% | Latency budget {MAX_LATENCY_MS} ms | Recon timeout {RECONCILE_TIMEOUT_S}s", flush=True)
    print(flush=True)

    tasks = [market_discovery_loop(), websocket_listener()]

    if LIVE_TRADING or LIVE_DRY_RUN:
        clob_client = init_clob_client()
        if clob_client is None and LIVE_TRADING:
            logger.error("Cannot live-trade without CLOB client")
            sys.exit(1)
        tasks.append(user_channel_listener())
        tasks.append(reconcile_timeout_watcher())

    logger.info("Initial discovery...")
    init_m, init_t = fetch_crypto_markets()
    active_markets.update(init_m)
    token_to_market.update(init_t)
    logger.info(f"Watching {len(active_markets)} markets")

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
