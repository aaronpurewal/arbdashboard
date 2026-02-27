"""Vercel serverless handler for /api/config."""
import sys, os, json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cgi-bin'))
import config as cfg  # noqa: E402


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        db = cfg.get_db()
        config = cfg.get_all_config(db)
        for key, default in cfg.DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = default

        # Override with env vars if set
        env_key = os.environ.get("ODDS_API_KEY", "")
        if env_key:
            config["odds_api_key"] = env_key

        # Mask API keys for display
        masked = dict(config)
        for kname in ("odds_api_key", "oddspapi_key"):
            if masked.get(kname):
                k = masked[kname]
                masked[f"{kname}_masked"] = k[:4] + "****" + k[-4:] if len(k) > 8 else "****"

        body = json.dumps({"config": masked, "timestamp": datetime.now(timezone.utc).isoformat()})
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body.encode())

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(content_length) if content_length > 0 else b'{}'
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self._respond(400, {"error": "Invalid JSON"})
            return

        db = cfg.get_db()
        updated = []
        for key, value in data.items():
            if key in cfg.DEFAULT_CONFIG or key in ("odds_api_key", "oddspapi_key"):
                cfg.set_config(db, key, value)
                updated.append(key)

        self._respond(200, {"status": "ok", "updated": updated,
                            "timestamp": datetime.now(timezone.utc).isoformat()})

    def do_DELETE(self):
        query = parse_qs(urlparse(self.path).query)
        key = query.get("key", [""])[0]
        if key:
            db = cfg.get_db()
            db.execute("DELETE FROM config WHERE key=?", [key])
            db.commit()
            self._respond(200, {"status": "deleted", "key": key})
        else:
            self._respond(400, {"error": "No key specified"})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        pass
