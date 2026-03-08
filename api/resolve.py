"""Vercel serverless handler for /api/resolve — resolves pending tracked bets."""
import sys, os, json
from http.server import BaseHTTPRequestHandler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cgi-bin'))
import scan as scanner  # noqa: E402


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(content_length) if content_length > 0 else b'[]'
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self._respond(400, {"error": "Invalid JSON"})
            return

        api_key = self.headers.get("X-Odds-Api-Key", "")
        if not api_key:
            api_key = os.environ.get("ODDS_API_KEY", "")
        if not api_key:
            self._respond(400, {"error": "No API key"})
            return

        pending = data.get("pending", [])
        if not pending:
            self._respond(200, {"resolved": []})
            return

        results = scanner.resolve_tracked_bets(api_key, pending)
        self._respond(200, {"resolved": results})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Odds-Api-Key')
        self.end_headers()

    def _respond(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
