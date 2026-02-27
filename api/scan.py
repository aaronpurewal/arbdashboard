"""Vercel serverless handler for /api/scan."""
import sys, os, json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Add cgi-bin to path so we can import the scan module directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cgi-bin'))
import scan as scanner  # noqa: E402


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        params = {k: v[0] for k, v in query.items()}

        result = scanner.run_scan(params)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(result, default=scanner._json_default).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress logs
