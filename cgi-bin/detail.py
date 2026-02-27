#!/usr/bin/env python3
"""
ArbScanner — Deep-dive detail endpoint.
Given opportunity details, fetch full order book depth, compute optimal stakes,
and return detailed breakdown.
"""

import json
import os
import sys
import urllib.request
import urllib.parse
import time
from datetime import datetime, timezone

def fetch_json(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "ArbScanner/1.0",
            "Accept": "application/json"
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return {"_error": str(e)}

def american_to_decimal(american):
    american = float(american)
    if american > 0:
        return (american / 100.0) + 1.0
    return (100.0 / abs(american)) + 1.0

def american_to_implied_prob(american):
    d = american_to_decimal(american)
    return 1.0 / d if d > 0 else 0

def implied_prob_to_american(prob):
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return round(-100.0 * prob / (1.0 - prob))
    return round(100.0 * (1.0 - prob) / prob)

def fetch_polymarket_orderbook(token_id):
    """Fetch order book from Polymarket CLOB."""
    if not token_id:
        return None
    url = f"https://clob.polymarket.com/book?token_id={token_id}"
    data = fetch_json(url)
    if "_error" in data:
        return None
    return data

def fetch_kalshi_orderbook(ticker):
    """Fetch order book from Kalshi."""
    if not ticker:
        return None
    url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook"
    data = fetch_json(url)
    if "_error" in data:
        return None
    return data

def fetch_polymarket_price_history(token_id):
    """Fetch price history from Polymarket."""
    if not token_id:
        return None
    # Try the prices endpoint with fidelity parameter
    url = f"https://clob.polymarket.com/prices-history?market={token_id}&interval=all&fidelity=60"
    data = fetch_json(url)
    if "_error" in data:
        return None
    return data

def compute_optimal_stakes(prob_a, prob_b, bankroll, fee_a=0, fee_b=0):
    """
    Compute optimal stake allocation for arbitrage.
    Returns detailed breakdown.
    """
    if prob_a <= 0 or prob_b <= 0 or bankroll <= 0:
        return None

    cost = prob_a + prob_b
    if cost >= 1.0:
        return {"error": "No arbitrage exists (combined cost >= 1.0)"}

    # For equal payout strategy: stake proportional to probability
    # Target payout = bankroll / (prob_a + prob_b) per unit
    # stake_a = payout * prob_a
    # stake_b = payout * prob_b
    target_payout = bankroll
    stake_a = round(target_payout * prob_a, 2)
    stake_b = round(target_payout * prob_b, 2)
    total_staked = round(stake_a + stake_b, 2)

    # With fees
    win_a = round(stake_a / prob_a - stake_a, 2)  # winnings on side A
    win_b = round(stake_b / prob_b - stake_b, 2)  # winnings on side B
    fee_on_win_a = round(win_a * fee_a, 2)
    fee_on_win_b = round(win_b * fee_b, 2)

    # If A wins: payout from A - stake on B - fees
    profit_if_a = round(stake_a / prob_a - stake_a - stake_b - fee_on_win_a, 2)
    # If B wins: payout from B - stake on A - fees
    profit_if_b = round(stake_b / prob_b - stake_a - stake_b - fee_on_win_b, 2)

    gross_profit = round(target_payout - total_staked, 2)
    gross_roi = round((gross_profit / total_staked) * 100, 3) if total_staked > 0 else 0

    # Breakdown by bankroll amounts
    bankroll_scenarios = []
    for br in [100, 500, 1000, 5000, 10000]:
        scale = br / bankroll if bankroll > 0 else 1
        bankroll_scenarios.append({
            "bankroll": br,
            "stake_a": round(stake_a * scale, 2),
            "stake_b": round(stake_b * scale, 2),
            "total_staked": round(total_staked * scale, 2),
            "gross_profit": round(gross_profit * scale, 2),
            "profit_if_a": round(profit_if_a * scale, 2),
            "profit_if_b": round(profit_if_b * scale, 2),
        })

    return {
        "stake_a": stake_a,
        "stake_b": stake_b,
        "total_staked": total_staked,
        "payout": target_payout,
        "gross_profit": gross_profit,
        "gross_roi_pct": gross_roi,
        "fee_breakdown": {
            "platform_a_fee_pct": fee_a * 100,
            "platform_b_fee_pct": fee_b * 100,
            "fee_on_win_a": fee_on_win_a,
            "fee_on_win_b": fee_on_win_b,
        },
        "scenarios": {
            "if_a_wins": {
                "payout": round(stake_a / prob_a, 2),
                "minus_stake_b": stake_b,
                "minus_fees": fee_on_win_a,
                "net_profit": profit_if_a,
            },
            "if_b_wins": {
                "payout": round(stake_b / prob_b, 2),
                "minus_stake_a": stake_a,
                "minus_fees": fee_on_win_b,
                "net_profit": profit_if_b,
            },
        },
        "bankroll_table": bankroll_scenarios,
    }

def parse_orderbook(data, source):
    """Parse order book into normalized format."""
    if not data:
        return {"bids": [], "asks": [], "best_bid": 0, "best_ask": 0, "spread": 0, "depth": 0}

    bids = []
    asks = []

    if source == "polymarket":
        for entry in data.get("bids", []):
            bids.append({
                "price": float(entry.get("price", 0)),
                "size": float(entry.get("size", 0)),
            })
        for entry in data.get("asks", []):
            asks.append({
                "price": float(entry.get("price", 0)),
                "size": float(entry.get("size", 0)),
            })
    elif source == "kalshi":
        for entry in data.get("yes", []):
            price = float(entry[0]) / 100.0 if isinstance(entry, (list, tuple)) else float(entry.get("price", 0)) / 100.0
            size = float(entry[1]) if isinstance(entry, (list, tuple)) else float(entry.get("quantity", 0))
            bids.append({"price": price, "size": size})
        for entry in data.get("no", []):
            price = float(entry[0]) / 100.0 if isinstance(entry, (list, tuple)) else float(entry.get("price", 0)) / 100.0
            size = float(entry[1]) if isinstance(entry, (list, tuple)) else float(entry.get("quantity", 0))
            asks.append({"price": price, "size": size})

    bids.sort(key=lambda x: x["price"], reverse=True)
    asks.sort(key=lambda x: x["price"])

    best_bid = bids[0]["price"] if bids else 0
    best_ask = asks[0]["price"] if asks else 0
    spread = round(best_ask - best_bid, 4) if best_ask > 0 and best_bid > 0 else 0
    total_depth = sum(b["size"] for b in bids) + sum(a["size"] for a in asks)

    return {
        "bids": bids[:20],  # Top 20 levels
        "asks": asks[:20],
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "depth": round(total_depth, 2),
    }

def main():
    print("Content-Type: application/json")
    print()

    query_string = os.environ.get("QUERY_STRING", "")
    params = dict(urllib.parse.parse_qsl(query_string))

    # Get parameters
    platform_a = params.get("platform_a", "")
    platform_b = params.get("platform_b", "")
    market_id_a = params.get("market_id_a", "")
    market_id_b = params.get("market_id_b", "")
    prob_a = float(params.get("prob_a", "0"))
    prob_b = float(params.get("prob_b", "0"))
    bankroll = float(params.get("bankroll", "100"))
    fee_a = float(params.get("fee_a", "0.02"))
    fee_b = float(params.get("fee_b", "0"))

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "orderbook_a": None,
        "orderbook_b": None,
        "price_history": None,
        "stakes": None,
    }

    # Fetch order books
    if platform_a.lower() == "polymarket" and market_id_a:
        raw_book = fetch_polymarket_orderbook(market_id_a)
        result["orderbook_a"] = parse_orderbook(raw_book, "polymarket")
        # Price history
        history = fetch_polymarket_price_history(market_id_a)
        if history and not isinstance(history, dict):
            result["price_history"] = history
        elif history and "history" in history:
            result["price_history"] = history["history"]
    elif platform_a.lower() == "kalshi" and market_id_a:
        raw_book = fetch_kalshi_orderbook(market_id_a)
        result["orderbook_a"] = parse_orderbook(raw_book, "kalshi")

    if platform_b.lower() == "polymarket" and market_id_b:
        raw_book = fetch_polymarket_orderbook(market_id_b)
        result["orderbook_b"] = parse_orderbook(raw_book, "polymarket")
    elif platform_b.lower() == "kalshi" and market_id_b:
        raw_book = fetch_kalshi_orderbook(market_id_b)
        result["orderbook_b"] = parse_orderbook(raw_book, "kalshi")

    # Compute optimal stakes
    if prob_a > 0 and prob_b > 0:
        result["stakes"] = compute_optimal_stakes(prob_a, prob_b, bankroll, fee_a, fee_b)

    print(json.dumps(result))

if __name__ == "__main__":
    main()
