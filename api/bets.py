"""Vercel serverless handler for /api/bets — bet journal CRUD."""
import sys, os, json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cgi-bin'))
import config as cfg  # noqa: E402

def _ensure_bets_table(db):
    db.execute("""CREATE TABLE IF NOT EXISTS bets (
        id TEXT PRIMARY KEY,
        opp_id TEXT,
        event TEXT,
        sport TEXT,
        platform TEXT,
        side TEXT,
        odds REAL,
        stake REAL,
        bet_type TEXT DEFAULT 'manual',
        status TEXT DEFAULT 'open',
        pnl REAL DEFAULT 0,
        notes TEXT DEFAULT '',
        created_at TEXT,
        resolved_at TEXT
    )""")
    db.commit()

def _ensure_scan_history_table(db):
    db.execute("""CREATE TABLE IF NOT EXISTS scan_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        opp_count INTEGER,
        arb_count INTEGER,
        ev_count INTEGER,
        best_edge REAL,
        avg_edge REAL,
        sports TEXT,
        hour INTEGER
    )""")
    db.commit()

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        endpoint = query.get("endpoint", ["bets"])[0]
        db = cfg.get_db()

        if endpoint == "scan_history":
            _ensure_scan_history_table(db)
            rows = db.execute(
                "SELECT * FROM scan_history ORDER BY id DESC LIMIT 500"
            ).fetchall()
            cols = ["id", "timestamp", "opp_count", "arb_count", "ev_count",
                    "best_edge", "avg_edge", "sports", "hour"]
            data = [dict(zip(cols, r)) for r in rows]
            self._respond(200, {"scan_history": data})
            return

        _ensure_bets_table(db)
        status_filter = query.get("status", ["all"])[0]
        if status_filter == "all":
            rows = db.execute(
                "SELECT * FROM bets ORDER BY created_at DESC"
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM bets WHERE status=? ORDER BY created_at DESC",
                [status_filter]
            ).fetchall()
        cols = ["id", "opp_id", "event", "sport", "platform", "side", "odds",
                "stake", "bet_type", "status", "pnl", "notes", "created_at",
                "resolved_at"]
        bets = [dict(zip(cols, r)) for r in rows]
        self._respond(200, {"bets": bets})

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(content_length) if content_length > 0 else b'{}'
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self._respond(400, {"error": "Invalid JSON"})
            return

        db = cfg.get_db()
        action = data.get("action", "create")

        if action == "create":
            _ensure_bets_table(db)
            import hashlib
            bet_id = data.get("id") or hashlib.md5(
                f"{data.get('event','')}-{data.get('side','')}-{datetime.now().isoformat()}".encode()
            ).hexdigest()[:12]
            now = datetime.now(timezone.utc).isoformat()
            db.execute("""INSERT OR REPLACE INTO bets
                (id, opp_id, event, sport, platform, side, odds, stake,
                 bet_type, status, pnl, notes, created_at, resolved_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [bet_id, data.get("opp_id", ""), data.get("event", ""),
                 data.get("sport", ""), data.get("platform", ""),
                 data.get("side", ""), data.get("odds", 0),
                 data.get("stake", 0), data.get("bet_type", "manual"),
                 "open", 0, data.get("notes", ""), now, None])
            db.commit()
            self._respond(200, {"status": "ok", "id": bet_id})

        elif action == "resolve":
            _ensure_bets_table(db)
            bet_id = data.get("id", "")
            outcome = data.get("outcome", "lost")  # won, lost, void
            now = datetime.now(timezone.utc).isoformat()

            row = db.execute("SELECT odds, stake FROM bets WHERE id=?", [bet_id]).fetchone()
            if not row:
                self._respond(404, {"error": "Bet not found"})
                return

            odds, stake = row
            if outcome == "won":
                if odds > 0:
                    pnl = stake * (odds / 100)
                else:
                    pnl = stake * (100 / abs(odds))
            elif outcome == "void":
                pnl = 0
            else:  # lost
                pnl = -stake

            db.execute(
                "UPDATE bets SET status=?, pnl=?, resolved_at=? WHERE id=?",
                [outcome, round(pnl, 2), now, bet_id])
            db.commit()
            self._respond(200, {"status": "ok", "pnl": round(pnl, 2)})

        elif action == "delete":
            _ensure_bets_table(db)
            bet_id = data.get("id", "")
            db.execute("DELETE FROM bets WHERE id=?", [bet_id])
            db.commit()
            self._respond(200, {"status": "deleted"})

        elif action == "log_scan":
            _ensure_scan_history_table(db)
            now = datetime.now(timezone.utc)
            db.execute("""INSERT INTO scan_history
                (timestamp, opp_count, arb_count, ev_count, best_edge, avg_edge, sports, hour)
                VALUES (?,?,?,?,?,?,?,?)""",
                [now.isoformat(), data.get("opp_count", 0),
                 data.get("arb_count", 0), data.get("ev_count", 0),
                 data.get("best_edge", 0), data.get("avg_edge", 0),
                 data.get("sports", ""), now.hour])
            db.commit()
            self._respond(200, {"status": "ok"})

        else:
            self._respond(400, {"error": f"Unknown action: {action}"})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
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
