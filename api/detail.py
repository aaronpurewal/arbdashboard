"""Vercel serverless handler for /api/detail."""
import sys, os, json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cgi-bin'))
import detail as dtl  # noqa: E402


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        params = {k: v[0] for k, v in query.items()}

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

        if platform_a.lower() == "polymarket" and market_id_a:
            raw_book = dtl.fetch_polymarket_orderbook(market_id_a)
            result["orderbook_a"] = dtl.parse_orderbook(raw_book, "polymarket")
            history = dtl.fetch_polymarket_price_history(market_id_a)
            if history and not isinstance(history, dict):
                result["price_history"] = history
            elif history and "history" in history:
                result["price_history"] = history["history"]
        elif platform_a.lower() == "kalshi" and market_id_a:
            raw_book = dtl.fetch_kalshi_orderbook(market_id_a)
            result["orderbook_a"] = dtl.parse_orderbook(raw_book, "kalshi")

        if platform_b.lower() == "polymarket" and market_id_b:
            raw_book = dtl.fetch_polymarket_orderbook(market_id_b)
            result["orderbook_b"] = dtl.parse_orderbook(raw_book, "polymarket")
        elif platform_b.lower() == "kalshi" and market_id_b:
            raw_book = dtl.fetch_kalshi_orderbook(market_id_b)
            result["orderbook_b"] = dtl.parse_orderbook(raw_book, "kalshi")

        if prob_a > 0 and prob_b > 0:
            result["stakes"] = dtl.compute_optimal_stakes(prob_a, prob_b, bankroll, fee_a, fee_b)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def log_message(self, format, *args):
        pass
