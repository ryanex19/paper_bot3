import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
import threading
from datetime import datetime, UTC
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
import websockets


# ============================================================
# CONFIGURATION
# ============================================================

# -------------------------
# PAPER MODE ONLY
# -------------------------
MODE = "PAPER"

INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "3000"))

MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "20"))

MIN_EDGE = float(os.getenv("MIN_EDGE", "0.025"))
MIN_DEPTH_USD = float(os.getenv("MIN_DEPTH_USD", "2.00"))
MIN_COMBINED_PRICE = float(os.getenv("MIN_COMBINED_PRICE", "0.92"))

# Paper execution assumptions
PAPER_SLIPPAGE_BPS = float(os.getenv("PAPER_SLIPPAGE_BPS", "0"))
PAPER_FILL_RATE = float(os.getenv("PAPER_FILL_RATE", "1.0"))

# Timing
MAX_SIGNAL_AGE_MS = float(os.getenv("MAX_SIGNAL_AGE_MS", "250"))
OPPORTUNITY_COOLDOWN_S = float(os.getenv("OPPORTUNITY_COOLDOWN_S", "3"))
EVALUATION_DEBOUNCE_S = float(os.getenv("EVALUATION_DEBOUNCE_S", "0.20"))

DISCOVERY_INTERVAL_S = float(os.getenv("DISCOVERY_INTERVAL_S", "60"))
HEALTH_INTERVAL_S = float(os.getenv("HEALTH_INTERVAL_S", "30"))

# REST protection
BOOK_CONCURRENCY = int(os.getenv("BOOK_CONCURRENCY", "4"))
HTTP_POOL_SIZE = int(os.getenv("HTTP_POOL_SIZE", "16"))
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5"))

# Prevent too many simultaneous paper opportunities
MAX_OPEN_PAPER_TRADES = int(os.getenv("MAX_OPEN_PAPER_TRADES", "3"))

# Safety
MAX_DAILY_PAPER_LOSS = float(os.getenv("MAX_DAILY_PAPER_LOSS", "100"))
MAX_PAPER_BALANCE = float(os.getenv("MAX_PAPER_BALANCE", "100000"))

# Fees
CRYPTO_TAKER_FEE_RATE = float(
    os.getenv("CRYPTO_TAKER_FEE_RATE", "0.07")
)

EST_GAS_FEE_USD = float(
    os.getenv("EST_GAS_FEE_USD", "0.01")
)

# Files
DATA_DIR = os.getenv("DATA_DIR", "/data")

CSV_FILENAME = os.getenv(
    "CSV_FILENAME",
    os.path.join(DATA_DIR, "paper_trades_v8.csv")
)

EVENT_CSV_FILENAME = os.getenv(
    "EVENT_CSV_FILENAME",
    os.path.join(DATA_DIR, "paper_events_v8.csv")
)

# APIs
GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

WS_CLOB_URL = (
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)

CRYPTO_KEYWORDS = [
    "btc",
    "eth",
    "sol",
    "bitcoin",
    "ethereum",
    "updown",
    "up/down",
    "5m",
    "15m",
    "crypto",
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger("polymarket-arb-v8")


# ============================================================
# GLOBAL STATE
# ============================================================

balance = INITIAL_BALANCE
reserved_capital = 0.0

active_markets: dict[str, dict] = {}
token_to_market: dict[str, tuple[str, str]] = {}

market_dirty_ts: dict[str, float] = {}
last_evaluation_ts: dict[str, float] = {}
last_opportunity_ts: dict[str, float] = {}

evaluation_queue: asyncio.Queue = asyncio.Queue()
queued_markets: set[str] = set()

shutdown_requested = False

discovery_generation = 0

paper_trades: list[dict] = []
paper_events: list[dict] = []

daily_paper_profit = 0.0
paper_trades_today = 0

stats = {
    "ws_messages": 0,
    "book_events": 0,
    "price_change_events": 0,
    "ws_errors": 0,
    "ws_reconnects": 0,

    "rest_book_requests": 0,
    "rest_book_success": 0,
    "rest_book_fail": 0,

    "evaluations": 0,
    "evaluation_skipped": 0,

    "no_book": 0,
    "edge_reject": 0,
    "depth_reject": 0,
    "latency_reject": 0,
    "cooldown_reject": 0,
    "phantom_reject": 0,

    "paper_trades": 0,
    "paper_profit": 0.0,
}

stats["ws_connected"] = False


# ============================================================
# HTTP CLIENT
# ============================================================

# One session per worker thread.
# This avoids unsafe concurrent access to one requests.Session
# while still giving each thread a persistent connection pool.

_thread_local = threading.local()


def create_http_session():
    session = requests.Session()

    adapter = HTTPAdapter(
        pool_connections=HTTP_POOL_SIZE,
        pool_maxsize=HTTP_POOL_SIZE,
        max_retries=0,
        pool_block=True,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": "PolymarketCryptoArbBot/8.0",
        "Accept": "application/json",
    })

    return session


def get_http_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = create_http_session()

    return _thread_local.session


# ============================================================
# ASYNC REST LIMITER
# ============================================================

book_semaphore = asyncio.Semaphore(BOOK_CONCURRENCY)


# ============================================================
# UTILITY
# ============================================================

def now_ts():
    return time.time()


def utc_string():
    return datetime.now(UTC).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def available_capital():
    return max(0.0, balance - reserved_capital)


def ensure_data_directory():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception as exc:
        logger.warning(
            "Could not create DATA_DIR=%s: %s",
            DATA_DIR,
            exc,
        )


def append_csv(record, filename):
    try:
        parent = os.path.dirname(filename)

        if parent:
            os.makedirs(parent, exist_ok=True)

        exists = os.path.isfile(filename)

        with open(
            filename,
            "a",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.DictWriter(
                f,
                fieldnames=record.keys(),
            )

            if not exists:
                writer.writeheader()

            writer.writerow(record)

    except Exception as exc:
        logger.warning(
            "CSV write failed %s: %s",
            filename,
            exc,
        )


def log_event(event, **kwargs):
    record = {
        "timestamp": utc_string(),
        "event": event,
        **kwargs,
    }

    paper_events.append(record)

    append_csv(
        record,
        EVENT_CSV_FILENAME,
    )


# ============================================================
# SIGNAL HANDLING
# ============================================================

def signal_handler(sig, frame):
    global shutdown_requested

    if shutdown_requested:
        return

    shutdown_requested = True

    logger.info(
        "Shutdown requested. Saving paper-trading data..."
    )

    flush_history()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================================
# MARKET PARSING
# ============================================================

def parse_tokens(market):
    tokens = market.get("clobTokenIds")

    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            return None

    if not isinstance(tokens, list):
        return None

    if len(tokens) < 2:
        return None

    return [
        str(tokens[0]),
        str(tokens[1]),
    ]


def is_crypto_market(market):
    title = (
        market.get("question")
        or market.get("title")
        or ""
    ).lower()

    return any(
        keyword in title
        for keyword in CRYPTO_KEYWORDS
    )


def normalize_market(market):
    if not market:
        return None

    tokens = parse_tokens(market)

    if not tokens:
        return None

    market_id = str(
        market.get("id")
        or market.get("conditionId")
        or ""
    )

    if not market_id:
        return None

    return {
        "id": market_id,

        "title": (
            market.get("question")
            or market.get("title")
            or "Unknown"
        ),

        "up_token": tokens[0],
        "down_token": tokens[1],

        "condition_id": (
            market.get("conditionId")
            or market.get("condition_id")
        ),

        "neg_risk": bool(
            market.get("negRisk")
            or market.get("neg_risk")
            or False
        ),

        "tick_size": str(
            market.get("minimum_tick_size")
            or market.get("orderPriceMinTickSize")
            or "0.01"
        ),
    }


# ============================================================
# MARKET DISCOVERY
# ============================================================

def get_btc_5m_slugs():
    timestamp = int(time.time())

    window = timestamp - (
        timestamp % 300
    )

    return [
        f"btc-updown-5m-{window}",
        f"btc-updown-5m-{window + 300}",
    ]


def fetch_market_by_slug(slug):
    session = get_http_session()

    try:
        response = session.get(
            f"{GAMMA_API_URL}/events",
            params={"slug": slug},
            timeout=HTTP_TIMEOUT_S,
        )

        if response.status_code != 200:
            return None

        data = response.json()

        if not isinstance(data, list):
            return None

        if not data:
            return None

        markets = data[0].get("markets") or []

        if not markets:
            return None

        return markets[0]

    except Exception as exc:
        logger.debug(
            "Slug lookup failed %s: %s",
            slug,
            exc,
        )

        return None


def fetch_crypto_markets():
    markets = {}
    tokens = {}

    # ----------------------------------------
    # BTC 5 minute markets
    # ----------------------------------------

    for slug in get_btc_5m_slugs():

        market = fetch_market_by_slug(slug)

        if not market:
            continue

        if not is_crypto_market(market):
            continue

        normalized = normalize_market(market)

        if not normalized:
            continue

        mid = normalized["id"]

        markets[mid] = normalized

        tokens[
            normalized["up_token"]
        ] = (mid, "UP")

        tokens[
            normalized["down_token"]
        ] = (mid, "DOWN")

    # ----------------------------------------
    # Crypto category
    # ----------------------------------------

    session = get_http_session()

    try:
        params = {
            "tag_id": 21,
            "limit": 25,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }

        response = session.get(
            f"{GAMMA_API_URL}/markets",
            params=params,
            timeout=HTTP_TIMEOUT_S,
        )

        if response.status_code == 200:

            data = response.json()

            if isinstance(data, list):

                for market in data:

                    if not is_crypto_market(market):
                        continue

                    normalized = normalize_market(market)

                    if not normalized:
                        continue

                    mid = normalized["id"]

                    markets[mid] = normalized

                    tokens[
                        normalized["up_token"]
                    ] = (mid, "UP")

                    tokens[
                        normalized["down_token"]
                    ] = (mid, "DOWN")

    except Exception as exc:
        logger.warning(
            "Crypto market discovery failed: %s",
            exc,
        )

    return markets, tokens


def update_discovery(new_markets, new_tokens):
    global active_markets
    global token_to_market
    global discovery_generation

    old_tokens = set(token_to_market)

    active_markets = dict(new_markets)
    token_to_market = dict(new_tokens)

    new_tokens_set = set(token_to_market)

    if old_tokens != new_tokens_set:
        discovery_generation += 1

        logger.info(
            "[DISCOVERY] Token set changed. "
            "WS generation=%d",
            discovery_generation,
        )


# ============================================================
# ORDER BOOK
# ============================================================

def fetch_order_book_sync(token_id):
    if not token_id:
        return None

    stats["rest_book_requests"] += 1

    session = get_http_session()

    started = time.perf_counter()

    try:
        response = session.get(
            f"{CLOB_API_URL}/book",
            params={"token_id": token_id},
            timeout=HTTP_TIMEOUT_S,
        )

        latency_ms = (
            time.perf_counter() - started
        ) * 1000

        if response.status_code != 200:
            stats["rest_book_fail"] += 1

            logger.debug(
                "Book HTTP %s token=%s latency=%.1fms",
                response.status_code,
                token_id[-8:],
                latency_ms,
            )

            return None

        data = response.json()

        if not isinstance(data, dict):
            stats["rest_book_fail"] += 1
            return None

        stats["rest_book_success"] += 1

        return {
            "book": data,
            "received_ts": now_ts(),
            "latency_ms": latency_ms,
        }

    except Exception as exc:
        stats["rest_book_fail"] += 1

        logger.debug(
            "Book fetch failed token=%s: %s",
            token_id[-8:],
            exc,
        )

        return None


async def fetch_order_book(token_id):
    async with book_semaphore:
        return await asyncio.to_thread(
            fetch_order_book_sync,
            token_id,
        )


# ============================================================
# BOOK CALCULATIONS
# ============================================================

def get_asks(book):
    levels = []

    for level in book.get("asks") or []:

        if not isinstance(level, dict):
            continue

        price = safe_float(
            level.get("price")
        )

        size = safe_float(
            level.get("size")
        )

        if (
            0.02 <= price <= 0.99
            and size > 0
        ):
            levels.append(
                (price, size)
            )

    levels.sort(
        key=lambda x: x[0]
    )

    return levels


def best_ask_with_depth(book):
    levels = get_asks(book)

    for price, size in levels:

        depth_usd = price * size

        if depth_usd >= MIN_DEPTH_USD:
            return (
                price,
                size,
                depth_usd,
            )

    return None, 0.0, 0.0


def price_for_shares(book, target_shares):
    if target_shares <= 0:
        return None

    levels = get_asks(book)

    remaining = target_shares
    cost = 0.0

    for price, size in levels:

        take = min(
            remaining,
            size,
        )

        cost += take * price
        remaining -= take

        if remaining <= 1e-9:
            return cost / target_shares

    return None


def calculate_vwap(book, target_usd):
    if target_usd <= 0:
        return None, 0.0, 0.0

    levels = get_asks(book)

    remaining_usd = target_usd
    shares = 0.0
    cost = 0.0

    for price, size in levels:

        level_usd = price * size

        take_usd = min(
            remaining_usd,
            level_usd,
        )

        take_shares = (
            take_usd / price
        )

        shares += take_shares
        cost += take_usd

        remaining_usd -= take_usd

        if remaining_usd <= 1e-9:
            return (
                cost / shares,
                shares,
                cost,
            )

    return None, shares, cost


def taker_fee(shares, price):
    return (
        shares
        * CRYPTO_TAKER_FEE_RATE
        * price
        * (1.0 - price)
    )


def evaluate_books(
    up_book,
    down_book,
    target_usd,
):
    up_best, _, up_depth = (
        best_ask_with_depth(up_book)
    )

    down_best, _, down_depth = (
        best_ask_with_depth(down_book)
    )

    if (
        up_best is None
        or down_best is None
    ):
        return None

    _, up_shares, _ = (
        calculate_vwap(
            up_book,
            target_usd,
        )
    )

    _, down_shares, _ = (
        calculate_vwap(
            down_book,
            target_usd,
        )
    )

    shares = min(
        up_shares,
        down_shares,
    )

    if shares <= 0:
        return None

    up_price = price_for_shares(
        up_book,
        shares,
    )

    down_price = price_for_shares(
        down_book,
        shares,
    )

    if (
        up_price is None
        or down_price is None
    ):
        return None

    combined = (
        up_price
        + down_price
    )

    gross_edge = (
        1.0 - combined
    )

    fees = (
        taker_fee(
            shares,
            up_price,
        )
        +
        taker_fee(
            shares,
            down_price,
        )
    )

    cost = (
        shares
        * combined
    )

    net = (
        shares
        - cost
        - fees
        - EST_GAS_FEE_USD
    )

    return {
        "up_best": up_best,
        "down_best": down_best,

        "up_depth": up_depth,
        "down_depth": down_depth,

        "up_price": up_price,
        "down_price": down_price,

        "shares": shares,

        "combined": combined,

        "gross_edge": gross_edge,

        "fees": fees,

        "cost": cost,

        "net": net,
    }


# ============================================================
# PAPER EXECUTION
# ============================================================

def execute_paper(
    market,
    result,
    signal_age_ms,
):
    global balance
    global reserved_capital
    global daily_paper_profit
    global paper_trades_today

    if result["net"] <= 0:
        return False

    if (
        signal_age_ms
        > MAX_SIGNAL_AGE_MS
    ):
        stats["latency_reject"] += 1
        return False

    if (
        daily_paper_profit
        <= -MAX_DAILY_PAPER_LOSS
    ):
        logger.warning(
            "[PAPER HALT] Daily loss limit reached."
        )
        return False

    if (
        len(paper_trades)
        >= MAX_OPEN_PAPER_TRADES
    ):
        stats["evaluation_skipped"] += 1
        return False

    cost = result["cost"]

    if (
        available_capital()
        < cost
    ):
        return False

    # ----------------------------------------
    # Simulated slippage
    # ----------------------------------------

    slippage_multiplier = (
        1.0
        +
        PAPER_SLIPPAGE_BPS
        / 10000.0
    )

    up_price = min(
        0.99,
        result["up_price"]
        * slippage_multiplier,
    )

    down_price = min(
        0.99,
        result["down_price"]
        * slippage_multiplier,
    )

    shares = (
        result["shares"]
        * max(
            0.0,
            min(
                1.0,
                PAPER_FILL_RATE,
            ),
        )
    )

    if shares <= 0:
        return False

    combined = (
        up_price
        + down_price
    )

    actual_cost = (
        shares
        * combined
    )

    fees = (
        taker_fee(
            shares,
            up_price,
        )
        +
        taker_fee(
            shares,
            down_price,
        )
    )

    net = (
        shares
        - actual_cost
        - fees
        - EST_GAS_FEE_USD
    )

    if net <= 0:
        return False

    # Reserve capital briefly so duplicate
    # evaluations cannot spend the same paper balance.
    reserved_capital += actual_cost

    try:
        balance += net

        daily_paper_profit += net
        paper_trades_today += 1

        stats["paper_trades"] += 1
        stats["paper_profit"] += net

        record = {
            "timestamp": utc_string(),

            "mode": "PAPER",

            "market_id": market["id"],
            "market_title": market["title"],

            "up_price": round(
                up_price,
                6,
            ),

            "down_price": round(
                down_price,
                6,
            ),

            "combined": round(
                combined,
                6,
            ),

            "shares": round(
                shares,
                6,
            ),

            "spend_usd": round(
                actual_cost,
                6,
            ),

            "fees_usd": round(
                fees,
                6,
            ),

            "gas_usd": round(
                EST_GAS_FEE_USD,
                6,
            ),

            "payout_usd": round(
                shares,
                6,
            ),

            "net_profit": round(
                net,
                6,
            ),

            "gross_edge_pct": round(
                (
                    1.0
                    - combined
                )
                * 100,
                3,
            ),

            "signal_age_ms": round(
                signal_age_ms,
                2,
            ),

            "daily_profit": round(
                daily_paper_profit,
                6,
            ),

            "new_balance": round(
                balance,
                6,
            ),
        }

        paper_trades.append(
            record
        )

        append_csv(
            record,
            CSV_FILENAME,
        )

        logger.info(
            "[PAPER TRADE] %s | "
            "UP %.4f + DOWN %.4f = %.4f | "
            "shares %.2f | "
            "edge %.2f%% | "
            "net +$%.4f | "
            "balance $%.2f",
            market["title"],
            up_price,
            down_price,
            combined,
            shares,
            (
                1.0
                - combined
            ) * 100,
            net,
            balance,
        )

        return True

    finally:
        reserved_capital = max(
            0.0,
            reserved_capital
            - actual_cost,
        )


# ============================================================
# MARKET EVALUATION
# ============================================================

async def evaluate_market(
    market_id,
    signal_ts,
):
    stats["evaluations"] += 1

    market = active_markets.get(
        market_id
    )

    if not market:
        return

    current_ts = now_ts()

    # Prevent excessive evaluations.
    if (
        current_ts
        - last_evaluation_ts.get(
            market_id,
            0,
        )
        < EVALUATION_DEBOUNCE_S
    ):
        stats["evaluation_skipped"] += 1
        return

    last_evaluation_ts[
        market_id
    ] = current_ts

    # Per-market opportunity cooldown.
    if (
        current_ts
        - last_opportunity_ts.get(
            market_id,
            0,
        )
        < OPPORTUNITY_COOLDOWN_S
    ):
        stats["cooldown_reject"] += 1
        return

    # ----------------------------------------
    # Fetch both books concurrently,
    # but through the global semaphore.
    # ----------------------------------------

    up_result, down_result = (
        await asyncio.gather(
            fetch_order_book(
                market["up_token"]
            ),
            fetch_order_book(
                market["down_token"]
            ),
        )
    )

    if (
        not up_result
        or not down_result
    ):
        stats["no_book"] += 1
        return

    up_book = up_result["book"]
    down_book = down_result["book"]

    # ----------------------------------------
    # Local fetch timestamps
    # ----------------------------------------

    newest_book_ts = max(
        up_result["received_ts"],
        down_result["received_ts"],
    )

    signal_age_ms = (
        now_ts()
        - signal_ts
    ) * 1000

    book_pair_age_ms = (
        abs(
            up_result["received_ts"]
            -
            down_result["received_ts"]
        )
        * 1000
    )

    if (
        signal_age_ms
        > MAX_SIGNAL_AGE_MS
    ):
        stats["latency_reject"] += 1
        return

    # If one REST request was substantially slower,
    # don't pretend the pair was atomic.
    if book_pair_age_ms > 1000:
        stats["latency_reject"] += 1
        return

    # ----------------------------------------
    # Evaluate
    # ----------------------------------------

    result = evaluate_books(
        up_book,
        down_book,
        MAX_TRADE_SIZE / 2,
    )

    if not result:
        stats["depth_reject"] += 1
        return

    # ----------------------------------------
    # Phantom protection
    # ----------------------------------------

    if (
        result["combined"]
        < MIN_COMBINED_PRICE
    ):
        stats["phantom_reject"] += 1
        return

    # ----------------------------------------
    # Edge
    # ----------------------------------------

    if (
        result["gross_edge"]
        < MIN_EDGE
        or result["net"] <= 0
    ):
        stats["edge_reject"] += 1
        return

    # ----------------------------------------
    # Capital
    # ----------------------------------------

    if (
        available_capital()
        < result["cost"]
    ):
        return

    # ----------------------------------------
    # Opportunity
    # ----------------------------------------

    last_opportunity_ts[
        market_id
    ] = now_ts()

    log_event(
        "OPPORTUNITY",
        market_id=market_id,
        market_title=market["title"],
        up_price=round(
            result["up_price"],
            6,
        ),
        down_price=round(
            result["down_price"],
            6,
        ),
        combined=round(
            result["combined"],
            6,
        ),
        gross_edge_pct=round(
            result["gross_edge"]
            * 100,
            3,
        ),
        net=round(
            result["net"],
            6,
        ),
        shares=round(
            result["shares"],
            6,
        ),
        signal_age_ms=round(
            signal_age_ms,
            2,
        ),
        book_pair_age_ms=round(
            book_pair_age_ms,
            2,
        ),
    )

    execute_paper(
        market,
        result,
        signal_age_ms,
    )


# ============================================================
# EVALUATION QUEUE
# ============================================================

def queue_market_evaluation(
    market_id,
    signal_ts=None,
):
    if market_id not in active_markets:
        return

    if market_id in queued_markets:
        return

    queued_markets.add(
        market_id
    )

    market_dirty_ts[
        market_id
    ] = signal_ts or now_ts()

    try:
        evaluation_queue.put_nowait(
            (
                market_id,
                market_dirty_ts[
                    market_id
                ],
            )
        )
    except asyncio.QueueFull:
        queued_markets.discard(
            market_id
        )


async def evaluation_worker():
    logger.info(
        "[EVALUATOR] Started"
    )

    while not shutdown_requested:

        market_id = None

        try:
            market_id, signal_ts = (
                await asyncio.wait_for(
                    evaluation_queue.get(),
                    timeout=1.0,
                )
            )

            queued_markets.discard(
                market_id
            )

            await evaluate_market(
                market_id,
                signal_ts,
            )

        except asyncio.TimeoutError:
            continue

        except Exception as exc:
            logger.exception(
                "[EVALUATOR] Error: %s",
                exc,
            )

        finally:
            if market_id is not None:
                evaluation_queue.task_done()


# ============================================================
# WEBSOCKET
# ============================================================

async def market_websocket_listener():

    while not shutdown_requested:

        generation_at_connect = (
            discovery_generation
        )

        assets = list(
            token_to_market.keys()
        )

        if not assets:
            await asyncio.sleep(2)
            continue

        try:

            stats["ws_reconnects"] += 1

            async with websockets.connect(
                WS_CLOB_URL,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=10,
                close_timeout=5,
                max_size=8 * 1024 * 1024,
            ) as ws:

                stats["ws_connected"] = True

                subscription = {
                    "type": "market",
                    "assets_ids": assets,
                    "custom_feature_enabled": True,
                }

                await ws.send(
                    json.dumps(
                        subscription
                    )
                )

                logger.info(
                    "[WS] Connected | "
                    "subscribed=%d tokens | "
                    "generation=%d",
                    len(assets),
                    generation_at_connect,
                )

                while not shutdown_requested:

                    # Reconnect if discovery changed.
                    if (
                        generation_at_connect
                        != discovery_generation
                    ):
                        logger.info(
                            "[WS] Token set changed. "
                            "Reconnecting..."
                        )
                        break

                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=5,
                        )

                    except asyncio.TimeoutError:
                        continue

                    stats["ws_messages"] += 1

                    if raw == "PONG":
                        continue

                    try:
                        message = json.loads(
                            raw
                        )
                    except Exception:
                        continue

                    events = (
                        message
                        if isinstance(
                            message,
                            list,
                        )
                        else [message]
                    )

                    for event in events:

                        if not isinstance(
                            event,
                            dict,
                        ):
                            continue

                        event_type = str(
                            event.get(
                                "event_type"
                            )
                            or ""
                        ).lower()

                        if (
                            event_type
                            == "book"
                        ):
                            stats[
                                "book_events"
                            ] += 1

                            asset_id = str(
                                event.get(
                                    "asset_id",
                                    "",
                                )
                            )

                            mapping = (
                                token_to_market.get(
                                    asset_id
                                )
                            )

                            if mapping:

                                market_id, _ = (
                                    mapping
                                )

                                queue_market_evaluation(
                                    market_id
                                )

                        elif (
                            event_type
                            == "price_change"
                        ):

                            stats[
                                "price_change_events"
                            ] += 1

                            changes = (
                                event.get(
                                    "price_changes"
                                )
                                or []
                            )

                            for change in changes:

                                asset_id = str(
                                    change.get(
                                        "asset_id",
                                        "",
                                    )
                                )

                                mapping = (
                                    token_to_market.get(
                                        asset_id
                                    )
                                )

                                if mapping:

                                    market_id, _ = (
                                        mapping
                                    )

                                    queue_market_evaluation(
                                        market_id
                                    )

        except Exception as exc:

            stats["ws_errors"] += 1

            logger.warning(
                "[WS] Disconnected: %s",
                exc,
            )

            await asyncio.sleep(3)

        finally:
            stats["ws_connected"] = False


# ============================================================
# FALLBACK SCANNER
# ============================================================

async def fallback_scan_loop():

    while not shutdown_requested:

        await asyncio.sleep(10)

        markets = list(
            active_markets.keys()
        )[:25]

        if not markets:
            continue

        logger.info(
            "[FALLBACK] queueing %d markets | WS=%s",
            len(markets),
            "UP"
            if stats["ws_connected"]
            else "DOWN",
        )

        for market_id in markets:
            queue_market_evaluation(
                market_id
            )


# ============================================================
# DISCOVERY
# ============================================================

async def discovery_loop():

    while not shutdown_requested:

        try:

            markets, tokens = (
                await asyncio.to_thread(
                    fetch_crypto_markets
                )
            )

            update_discovery(
                markets,
                tokens,
            )

            logger.info(
                "[DISCOVERY] markets=%d tokens=%d",
                len(active_markets),
                len(token_to_market),
            )

        except Exception as exc:

            logger.warning(
                "[DISCOVERY] Error: %s",
                exc,
            )

        await asyncio.sleep(
            DISCOVERY_INTERVAL_S
        )


# ============================================================
# HEALTH
# ============================================================

async def health_loop():

    while not shutdown_requested:

        await asyncio.sleep(
            HEALTH_INTERVAL_S
        )

        logger.info(
            "[HEALTH] "
            "mode=PAPER "
            "markets=%d "
            "tokens=%d "
            "ws=%s "
            "queue=%d "
            "books=%d/%d "
            "eval=%d "
            "no_book=%d "
            "depth=%d "
            "phantom=%d "
            "edge=%d "
            "paper=%d "
            "profit=$%.4f "
            "balance=$%.2f "
            "reserved=$%.2f "
            "available=$%.2f",
            len(active_markets),
            len(token_to_market),
            "UP"
            if stats["ws_connected"]
            else "DOWN",
            evaluation_queue.qsize(),
            stats["rest_book_success"],
            stats["rest_book_requests"],
            stats["evaluations"],
            stats["no_book"],
            stats["depth_reject"],
            stats["phantom_reject"],
            stats["edge_reject"],
            stats["paper_trades"],
            stats["paper_profit"],
            balance,
            reserved_capital,
            available_capital(),
        )


# ============================================================
# RAILWAY HEALTH SERVER
# ============================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return

        payload = {
            "status": "ok",
            "mode": "PAPER",
            "ws_connected": stats["ws_connected"],
            "markets": len(active_markets),
            "tokens": len(token_to_market),
            "queue": evaluation_queue.qsize(),
            "balance": round(
                balance,
                4,
            ),
            "paper_profit": round(
                stats["paper_profit"],
                4,
            ),
            "paper_trades": stats[
                "paper_trades"
            ],
        }

        body = json.dumps(
            payload
        ).encode("utf-8")

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "application/json",
        )

        self.send_header(
            "Content-Length",
            str(len(body)),
        )

        self.end_headers()

        self.wfile.write(body)

    def log_message(
        self,
        format,
        *args,
    ):
        return


class ThreadedHTTPServer(
    ThreadingMixIn,
    HTTPServer,
):
    daemon_threads = True


def start_health_server():

    port = int(
        os.getenv(
            "PORT",
            "8080",
        )
    )

    server = ThreadedHTTPServer(
        (
            "0.0.0.0",
            port,
        ),
        HealthHandler,
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )

    thread.start()

    logger.info(
        "[HEALTH] HTTP server listening on port %d",
        port,
    )

    return server


# ============================================================
# HISTORY
# ============================================================

def flush_history():

    for rows, filename in (
        (
            paper_trades,
            CSV_FILENAME,
        ),
        (
            paper_events,
            EVENT_CSV_FILENAME,
        ),
    ):

        if not rows:
            continue

        try:

            parent = os.path.dirname(
                filename
            )

            if parent:
                os.makedirs(
                    parent,
                    exist_ok=True,
                )

            with open(
                filename,
                "w",
                newline="",
                encoding="utf-8",
            ) as f:

                writer = csv.DictWriter(
                    f,
                    fieldnames=rows[0].keys(),
                )

                writer.writeheader()

                writer.writerows(
                    rows
                )

        except Exception as exc:

            logger.error(
                "History flush failed %s: %s",
                filename,
                exc,
            )


# ============================================================
# MAIN
# ============================================================

async def main():

    global shutdown_requested

    ensure_data_directory()

    logger.info(
        "=" * 78
    )

    logger.info(
        " POLYMARKET CRYPTO ARBITRAGE BOT v8.0"
    )

    logger.info(
        " PAPER MODE ONLY"
    )

    logger.info(
        "=" * 78
    )

    logger.info(
        "Initial balance: $%.2f",
        INITIAL_BALANCE,
    )

    logger.info(
        "Max paper trade: $%.2f",
        MAX_TRADE_SIZE,
    )

    logger.info(
        "Minimum edge: %.3f%%",
        MIN_EDGE * 100,
    )

    logger.info(
        "Book concurrency: %d",
        BOOK_CONCURRENCY,
    )

    logger.info(
        "HTTP pool size: %d",
        HTTP_POOL_SIZE,
    )

    logger.info(
        "Data directory: %s",
        DATA_DIR,
    )

    logger.info(
        "Health endpoint: /health"
    )

    # Railway health endpoint
    health_server = start_health_server()

    try:

        # ----------------------------------------
        # Initial discovery
        # ----------------------------------------

        markets, tokens = (
            await asyncio.to_thread(
                fetch_crypto_markets
            )
        )

        update_discovery(
            markets,
            tokens,
        )

        logger.info(
            "[STARTUP] Watching %d markets / %d tokens",
            len(active_markets),
            len(token_to_market),
        )

        # ----------------------------------------
        # Workers
        # ----------------------------------------

        tasks = [
            asyncio.create_task(
                discovery_loop()
            ),

            asyncio.create_task(
                market_websocket_listener()
            ),

            asyncio.create_task(
                evaluation_worker()
            ),

            asyncio.create_task(
                fallback_scan_loop()
            ),

            asyncio.create_task(
                health_loop()
            ),
        ]

        # ----------------------------------------
        # Main wait
        # ----------------------------------------

        while not shutdown_requested:
            await asyncio.sleep(1)

        logger.info(
            "Shutdown sequence started..."
        )

        for task in tasks:
            task.cancel()

        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    finally:

        shutdown_requested = True

        try:
            health_server.shutdown()
            health_server.server_close()
        except Exception:
            pass

        flush_history()

        logger.info(
            "[FINAL] Paper trades=%d profit=$%.4f balance=$%.2f",
            stats["paper_trades"],
            stats["paper_profit"],
            balance,
        )

        logger.info(
            "Bot stopped cleanly."


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        pass

    finally:
        flush_history()
