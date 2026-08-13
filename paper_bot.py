import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
import requests
import websockets

# ==========================================
# CONFIGURATION
# ==========================================
INITIAL_BALANCE = 3000.0  # Starting Paper Balance ($)
MAX_TRADE_SIZE = 45.0  # Max USD spent per arbitrage trade (target per side: $22.50)
MIN_EDGE = 0.025  # 2.5% minimum gross edge
EST_GAS_FEE_USD = 0.01  # Estimated Polygon Network Gas Fee per trade ($0.01)
CRYPTO_TAKER_FEE_RATE = 0.072  # Polymarket Crypto Taker Fee Rate (7.2%)
CSV_FILENAME = "paper_trades_ws_fees.csv"

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

# REST & WebSocket Endpoints
GAMMA_API_URL = "https://gamma-api.polymarket.com"
WS_CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Global State
balance = INITIAL_BALANCE
trades_history = []
active_markets = {}  # {market_id: {title, up_token, down_token, up_asks, down_asks}}
token_to_market = {}  # {token_id: (market_id, outcome_type)}

session = requests.Session()
session.headers.update({"User-Agent": "PolymarketCryptoWSBot/6.1"})


# ==========================================
# HELPER FUNCTIONS (SLIPPAGE & FEES)
# ==========================================
def calculate_vwap_and_slippage(asks, target_usd):
    """Walks the order book asks array to calculate VWAP price and slippage percentage for a target trade size in USD."""
    if not asks:
        return None, 0.0, 0.0

    # Ensure asks are sorted ascending by price
    sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 1.0)))
    best_ask = float(sorted_asks[0].get("price", 0))

    if best_ask <= 0:
        return None, 0.0, 0.0

    remaining_usd = target_usd
    total_shares = 0.0
    total_cost = 0.0

    for ask in sorted_asks:
        price = float(ask.get("price", 0))
        size = float(ask.get("size", 0))  # Available shares at level

        if price <= 0 or size <= 0:
            continue

        level_usd_available = price * size

        if remaining_usd <= level_usd_available:
            shares_bought = remaining_usd / price
            total_shares += shares_bought
            total_cost += remaining_usd
            remaining_usd = 0
            break
        else:
            total_shares += size
            total_cost += level_usd_available
            remaining_usd -= level_usd_available

    # Insufficient depth to fill target spend
    if remaining_usd > 0 or total_shares == 0:
        return None, 0.0, 0.0

    vwap_price = total_cost / total_shares
    slippage_pct = (
        ((vwap_price - best_ask) / best_ask) * 100 if best_ask > 0 else 0.0
    )

    return vwap_price, slippage_pct, best_ask


def calculate_polymarket_taker_fee(shares, price):
    """Calculates Polymarket Taker Fee for Crypto Markets:

    Fee = Shares * 0.072 * price * (1 - price)
    """
    return shares * CRYPTO_TAKER_FEE_RATE * price * (1.0 - price)


# ==========================================
# PERSISTENCE & SHUTDOWN
# ==========================================
def save_trades_to_csv():
    if not trades_history:
        logger.info("No trades recorded in this session.")
        return

    file_exists = os.path.isfile(CSV_FILENAME)
    try:
        with open(CSV_FILENAME, mode="a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=trades_history[0].keys())
            if not file_exists:
                writer.writeheader()
            writer.writerows(trades_history)
        logger.info(
            f"Successfully exported {len(trades_history)} trades to {CSV_FILENAME}"
        )
    except Exception as e:
        logger.error(f"Failed to save CSV file: {e}")


def signal_handler(sig, frame):
    logger.info("Termination signal received. Flushing logs...")
    save_trades_to_csv()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ==========================================
# REST MARKET DISCOVERY
# ==========================================
def is_crypto_market(market):
    title = (market.get("question") or market.get("title") or "").lower()
    return any(keyword in title for keyword in CRYPTO_KEYWORDS)


def fetch_crypto_markets():
    markets_dict = {}
    tokens_map = {}
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
            markets = resp.json()
            for m in markets:
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

                up_token = clob_tokens[0]
                down_token = clob_tokens[1]

                markets_dict[m_id] = {
                    "title": m.get("question") or m.get("title"),
                    "up_token": up_token,
                    "down_token": down_token,
                    "up_asks": [],
                    "down_asks": [],
                }

                tokens_map[up_token] = (m_id, "UP")
                tokens_map[down_token] = (m_id, "DOWN")
    except Exception as e:
        logger.warning(f"Failed to discover markets: {e}")

    return markets_dict, tokens_map


# ==========================================
# ARBITRAGE ENGINE
# ==========================================
def evaluate_arbitrage(market_id):
    global balance

    m_data = active_markets.get(market_id)
    if not m_data:
        return

    title = m_data.get("title")
    target_spend_per_side = MAX_TRADE_SIZE / 2.0  # $22.50 per side

    up_vwap, up_slippage, up_best = calculate_vwap_and_slippage(
        m_data.get("up_asks", []), target_spend_per_side
    )
    down_vwap, down_slippage, down_best = calculate_vwap_and_slippage(
        m_data.get("down_asks", []), target_spend_per_side
    )

    if up_vwap is None or down_vwap is None:
        return

    combined_cost = up_vwap + down_vwap
    gross_edge = 1.0 - combined_cost

    # Filter out corrupt/stale order books
    if combined_cost < 0.90:
        return

    if gross_edge >= MIN_EDGE:
        if balance < MAX_TRADE_SIZE:
            return

        up_shares = target_spend_per_side / up_vwap
        down_shares = target_spend_per_side / down_vwap

        executable_shares = min(up_shares, down_shares)
        guaranteed_payout = executable_shares * 1.0

        trade_cost = (executable_shares * up_vwap) + (
            executable_shares * down_vwap
        )

        up_fee = calculate_polymarket_taker_fee(executable_shares, up_vwap)
        down_fee = calculate_polymarket_taker_fee(
            executable_shares, down_vwap
        )
        total_taker_fee = up_fee + down_fee

        total_costs_with_fees = trade_cost + total_taker_fee + EST_GAS_FEE_USD
        net_profit = guaranteed_payout - total_costs_with_fees
        net_edge_pct = (net_profit / trade_cost) * 100

        # Don't execute if total fees consume net profit
        if net_profit <= 0:
            return

        balance += net_profit

        trade_record = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[
                :-3
            ],
            "market_title": title,
            "up_best_ask": round(up_best, 3),
            "up_vwap": round(up_vwap, 3),
            "up_slippage_pct": round(up_slippage, 2),
            "down_best_ask": round(down_best, 3),
            "down_vwap": round(down_vwap, 3),
            "down_slippage_pct": round(down_slippage, 2),
            "spend_usd": round(trade_cost, 2),
            "taker_fee_usd": round(total_taker_fee, 4),
            "gas_fee_usd": round(EST_GAS_FEE_USD, 4),
            "payout_usd": round(guaranteed_payout, 2),
            "net_profit": round(net_profit, 2),
            "net_edge_pct": round(net_edge_pct, 2),
            "new_balance": round(balance, 2),
        }

        trades_history.append(trade_record)

        print("\n" + "=" * 75, flush=True)
        print(f"⚡ [ARBITRAGE EXECUTED] {title}", flush=True)
        print(
            f"   UP  : Best ${up_best:.3f} | VWAP ${up_vwap:.3f} | Slippage: {up_slippage:.2f}%",
            flush=True,
        )
        print(
            f"   DOWN: Best ${down_best:.3f} | VWAP ${down_vwap:.3f} | Slippage: {down_slippage:.2f}%",
            flush=True,
        )
        print(
            f"   Spent: ${trade_cost:.2f} | Taker Fee: ${total_taker_fee:.4f} | Est Gas: ${EST_GAS_FEE_USD:.2f}",
            flush=True,
        )
        print(
            f"   Payout: ${guaranteed_payout:.2f} | Net Profit: +${net_profit:.2f} ({net_edge_pct:.2f}% Net Edge)",
            flush=True,
        )
        print(f"   New Balance: ${balance:.2f}", flush=True)
        print("=" * 75 + "\n", flush=True)


# ==========================================
# WEBSOCKET STREAM HANDLER
# ==========================================
async def market_discovery_loop():
    """Refreshes market list every 60s."""
    global active_markets, token_to_market
    while True:
        new_markets, new_tokens = fetch_crypto_markets()
        if new_markets:
            active_markets.update(new_markets)
            token_to_market.update(new_tokens)
            logger.info(
                f"[DISCOVERY] Monitoring {len(active_markets)} markets via WebSocket."
            )
        await asyncio.sleep(60)


async def websocket_listener():
    """Main WebSocket loop that processes real-time order book events."""
    global active_markets, token_to_market

    while True:
        try:
            async with websockets.connect(
                WS_CLOB_URL, ping_interval=20, ping_timeout=10
            ) as ws:
                logger.info("Connected to Polymarket CLOB WebSocket Stream.")

                asset_ids = list(token_to_market.keys())
                if asset_ids:
                    await ws.send(
                        json.dumps({"type": "market", "assets_ids": asset_ids})
                    )

                last_sub_time = time.time()

                async for message in ws:
                    # Periodically update subscriptions if new markets were discovered
                    if time.time() - last_sub_time > 30:
                        current_tokens = list(token_to_market.keys())
                        if len(current_tokens) > len(asset_ids):
                            asset_ids = current_tokens
                            await ws.send(
                                json.dumps(
                                    {
                                        "type": "market",
                                        "assets_ids": asset_ids,
                                    }
                                )
                            )
                        last_sub_time = time.time()

                    raw_data = json.loads(message)

                    # Handle single object vs list batch messages safely
                    events = (
                        raw_data if isinstance(raw_data, list) else [raw_data]
                    )

                    for data in events:
                        if not isinstance(data, dict):
                            continue

                        event_type = data.get("event_type")

                        # 1. Full Order Book Snapshots
                        if event_type == "book":
                            asset_id = data.get("asset_id")
                            asks = data.get("asks", [])

                            if asset_id in token_to_market and asks:
                                market_id, outcome_type = token_to_market[
                                    asset_id
                                ]

                                if outcome_type == "UP":
                                    active_markets[market_id]["up_asks"] = asks
                                else:
                                    active_markets[market_id][
                                        "down_asks"
                                    ] = asks

                                evaluate_arbitrage(market_id)

                        # 2. Incremental Price Changes
                        elif event_type == "price_change":
                            price_changes = data.get("price_changes", [])
                            for change in price_changes:
                                asset_id = change.get("asset_id")
                                if asset_id in token_to_market:
                                    market_id, outcome_type = token_to_market[
                                        asset_id
                                    ]

                                    best_ask = change.get("best_ask")
                                    if best_ask:
                                        new_ask = [
                                            {
                                                "price": float(best_ask),
                                                "size": float(
                                                    change.get("size", 100)
                                                ),
                                            }
                                        ]
                                        if outcome_type == "UP":
                                            active_markets[market_id][
                                                "up_asks"
                                            ] = new_ask
                                        else:
                                            active_markets[market_id][
                                                "down_asks"
                                            ] = new_ask

                                        evaluate_arbitrage(market_id)

        except (websockets.ConnectionClosed, Exception) as e:
            logger.warning(
                f"WebSocket connection lost: {e}. Reconnecting in 3s..."
            )
            await asyncio.sleep(3)


async def main():
    print("=" * 75, flush=True)
    print("   POLYMARKET WEBSOCKET BOT V6.1 (COMPLETE FIXES)")
    print("=" * 75, flush=True)
    print(f" Initial Balance : ${INITIAL_BALANCE:.2f}", flush=True)
    print(f" Est. Gas Fee    : ${EST_GAS_FEE_USD:.2f} / trade", flush=True)
    print(
        f" Taker Fee Rate  : {CRYPTO_TAKER_FEE_RATE*100:.1f}% (Crypto)",
        flush=True,
    )
    print(f" Minimum Gross   : {MIN_EDGE*100:.1f}%\n", flush=True)

    await asyncio.gather(market_discovery_loop(), websocket_listener())


if __name__ == "__main__":
    asyncio.run(main())
