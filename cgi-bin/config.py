#!/usr/bin/env python3
"""
ArbScanner — User configuration CRUD endpoint.
Stores and retrieves settings via SQLite.
"""

import json
import os
import sys
import sqlite3
import urllib.parse
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data.db")

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.execute("""CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    )""")
    db.commit()
    return db

def get_all_config(db):
    rows = db.execute("SELECT key, value FROM config").fetchall()
    config = {}
    for key, value in rows:
        try:
            config[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            config[key] = value
    return config

def set_config(db, key, value):
    if isinstance(value, (dict, list, bool)):
        value = json.dumps(value)
    db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", [key, str(value)])
    db.commit()

DEFAULT_CONFIG = {
    "odds_api_key": "",
    "oddspapi_key": "",
    "refresh_interval": 60,
    "min_arb_threshold": 0,
    "sports": ["NBA", "NFL", "MLB", "NHL", "Soccer", "MMA"],
    "platforms": ["polymarket", "kalshi", "draftkings", "fanduel", "betrivers", "pinnacle", "betmgm"],
    "notify_above_pct": 2.0,
    "sound_alerts": False,
    "include_live": True,
    "default_bankroll": 100,
}

def main():
    method = os.environ.get("REQUEST_METHOD", "GET")
    query_string = os.environ.get("QUERY_STRING", "")
    params = dict(urllib.parse.parse_qsl(query_string))

    print("Content-Type: application/json")
    print()

    db = get_db()

    if method == "GET":
        config = get_all_config(db)
        # Merge with defaults
        for key, default in DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = default
        # Mask API keys for display
        masked = dict(config)
        if masked.get("odds_api_key"):
            k = masked["odds_api_key"]
            masked["odds_api_key_masked"] = k[:4] + "****" + k[-4:] if len(k) > 8 else "****"
        if masked.get("oddspapi_key"):
            k = masked["oddspapi_key"]
            masked["oddspapi_key_masked"] = k[:4] + "****" + k[-4:] if len(k) > 8 else "****"
        print(json.dumps({"config": masked, "timestamp": datetime.now(timezone.utc).isoformat()}))

    elif method == "POST":
        content_length = int(os.environ.get("CONTENT_LENGTH", 0))
        body = sys.stdin.read(content_length) if content_length > 0 else "{}"
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            print(json.dumps({"error": "Invalid JSON"}))
            return

        updated = []
        for key, value in data.items():
            if key in DEFAULT_CONFIG or key in ("odds_api_key", "oddspapi_key"):
                set_config(db, key, value)
                updated.append(key)

        print(json.dumps({
            "status": "ok",
            "updated": updated,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))

    elif method == "DELETE":
        key = params.get("key", "")
        if key:
            db.execute("DELETE FROM config WHERE key=?", [key])
            db.commit()
            print(json.dumps({"status": "deleted", "key": key}))
        else:
            print(json.dumps({"error": "No key specified"}))

    else:
        print(json.dumps({"error": f"Unsupported method: {method}"}))

if __name__ == "__main__":
    main()
