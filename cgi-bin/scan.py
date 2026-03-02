#!/usr/bin/env python3
"""
ArbScanner — Main arbitrage scanner.
Fetches sports markets from Polymarket, Kalshi, and sportsbooks,
matches events across platforms, and computes arbitrage opportunities.
"""

import json
import os
import sys
import sqlite3
import subprocess
import shutil
import urllib.request
import urllib.parse
import urllib.error
import re
import time
import hashlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from functools import lru_cache

# ─── Configuration ────────────────────────────────────────────────────────────

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_PROJECT_ROOT, "data.db") if os.access(_PROJECT_ROOT, os.W_OK) else "/tmp/data.db"
CACHE_TTL = 60  # seconds

# ─── Database helpers ─────────────────────────────────────────────────────────

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS cache (
        cache_key TEXT PRIMARY KEY,
        data TEXT,
        ts REAL
    )""")
    db.commit()
    return db

def get_config(db, key, default=None):
    row = db.execute("SELECT value FROM config WHERE key=?", [key]).fetchone()
    return row[0] if row else default

def get_cached(db, cache_key, ttl=CACHE_TTL):
    row = db.execute("SELECT data, ts FROM cache WHERE cache_key=?", [cache_key]).fetchone()
    if row and (time.time() - row[1]) < ttl:
        return json.loads(row[0])
    return None

def _json_default(obj):
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def set_cached(db, cache_key, data):
    db.execute("INSERT OR REPLACE INTO cache (cache_key, data, ts) VALUES (?,?,?)",
               [cache_key, json.dumps(data, default=_json_default), time.time()])
    db.commit()

# ─── HTTP helper ──────────────────────────────────────────────────────────────

def fetch_json(url, timeout=12):
    """Fetch JSON from a URL with error handling."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "ArbScanner/1.0",
            "Accept": "application/json"
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except Exception as e:
        return {"_error": str(e)}

# ─── Odds conversion utilities ───────────────────────────────────────────────

def american_to_decimal(american):
    american = float(american)
    if american > 0:
        return (american / 100.0) + 1.0
    return (100.0 / abs(american)) + 1.0

def decimal_to_implied_prob(decimal_odds):
    if decimal_odds <= 0:
        return 0
    return 1.0 / decimal_odds

def american_to_implied_prob(american):
    return decimal_to_implied_prob(american_to_decimal(american))

def implied_prob_to_american(prob):
    if prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return round(-100.0 * prob / (1.0 - prob))
    return round(100.0 * (1.0 - prob) / prob)

def polymarket_price_to_prob(price):
    return float(price)

def kalshi_cents_to_prob(cents):
    return float(cents) / 100.0

# ─── Fuzzy matching ──────────────────────────────────────────────────────────

TEAM_ALIASES = {
    "lakers": "los angeles lakers", "celtics": "boston celtics",
    "warriors": "golden state warriors", "knicks": "new york knicks",
    "nets": "brooklyn nets", "76ers": "philadelphia 76ers",
    "sixers": "philadelphia 76ers", "heat": "miami heat",
    "bucks": "milwaukee bucks", "suns": "phoenix suns",
    "mavs": "dallas mavericks", "mavericks": "dallas mavericks",
    "nuggets": "denver nuggets", "clippers": "la clippers",
    "la clippers": "los angeles clippers",
    "thunder": "oklahoma city thunder", "okc": "oklahoma city thunder",
    "grizzlies": "memphis grizzlies", "cavs": "cleveland cavaliers",
    "cavaliers": "cleveland cavaliers", "wolves": "minnesota timberwolves",
    "timberwolves": "minnesota timberwolves", "kings": "sacramento kings",
    "pelicans": "new orleans pelicans", "hawks": "atlanta hawks",
    "bulls": "chicago bulls", "raptors": "toronto raptors",
    "magic": "orlando magic", "pacers": "indiana pacers",
    "hornets": "charlotte hornets", "wizards": "washington wizards",
    "pistons": "detroit pistons", "blazers": "portland trail blazers",
    "trail blazers": "portland trail blazers", "spurs": "san antonio spurs",
    "rockets": "houston rockets", "jazz": "utah jazz",
    # NFL
    "chiefs": "kansas city chiefs", "eagles": "philadelphia eagles",
    "bills": "buffalo bills", "ravens": "baltimore ravens",
    "lions": "detroit lions", "49ers": "san francisco 49ers",
    "niners": "san francisco 49ers", "cowboys": "dallas cowboys",
    "dolphins": "miami dolphins", "bengals": "cincinnati bengals",
    "steelers": "pittsburgh steelers", "packers": "green bay packers",
    "texans": "houston texans", "seahawks": "seattle seahawks",
    "rams": "los angeles rams", "chargers": "los angeles chargers",
    "jaguars": "jacksonville jaguars", "vikings": "minnesota vikings",
    "colts": "indianapolis colts", "saints": "new orleans saints",
    "bears": "chicago bears", "broncos": "denver broncos",
    "raiders": "las vegas raiders", "cardinals": "arizona cardinals",
    "falcons": "atlanta falcons", "commanders": "washington commanders",
    "panthers": "carolina panthers", "giants": "new york giants",
    "jets": "new york jets", "browns": "cleveland browns",
    "patriots": "new england patriots", "titans": "tennessee titans",
    # MLB
    "yankees": "new york yankees", "dodgers": "los angeles dodgers",
    "astros": "houston astros", "braves": "atlanta braves",
    "mets": "new york mets", "phillies": "philadelphia phillies",
    "padres": "san diego padres", "cubs": "chicago cubs",
    "red sox": "boston red sox", "blue jays": "toronto blue jays",
    "guardians": "cleveland guardians", "orioles": "baltimore orioles",
    "twins": "minnesota twins", "mariners": "seattle mariners",
    "rangers": "texas rangers", "rays": "tampa bay rays",
    "brewers": "milwaukee brewers", "diamondbacks": "arizona diamondbacks",
    "d-backs": "arizona diamondbacks", "pirates": "pittsburgh pirates",
    "reds": "cincinnati reds", "white sox": "chicago white sox",
    "royals": "kansas city royals", "rockies": "colorado rockies",
    "angels": "los angeles angels", "tigers": "detroit tigers",
    "nationals": "washington nationals", "marlins": "miami marlins",
    "athletics": "oakland athletics",
    # NHL
    "bruins": "boston bruins", "maple leafs": "toronto maple leafs",
    "oilers": "edmonton oilers", "avalanche": "colorado avalanche",
    "hurricanes": "carolina hurricanes", "wild": "minnesota wild",
    "canucks": "vancouver canucks", "stars": "dallas stars",
    "penguins": "pittsburgh penguins", "lightning": "tampa bay lightning",
    "blackhawks": "chicago blackhawks", "red wings": "detroit red wings",
    "flames": "calgary flames", "predators": "nashville predators",
    "capitals": "washington capitals", "senators": "ottawa senators",
    "sabres": "buffalo sabres", "islanders": "new york islanders",
    "flyers": "philadelphia flyers", "coyotes": "utah hockey club",
    "kraken": "seattle kraken", "blue jackets": "columbus blue jackets",
    "ducks": "anaheim ducks", "sharks": "san jose sharks",
    "devils": "new jersey devils",
    # City-name aliases (for Kalshi-style titles like "Denver at Oklahoma City")
    # NBA
    "los angeles l": "los angeles lakers", "los angeles c": "la clippers",
    "boston": "boston celtics", "golden state": "golden state warriors",
    "new york": "new york knicks", "brooklyn": "brooklyn nets",
    "philadelphia": "philadelphia 76ers", "miami": "miami heat",
    "milwaukee": "milwaukee bucks", "phoenix": "phoenix suns",
    "dallas": "dallas mavericks", "denver": "denver nuggets",
    "oklahoma city": "oklahoma city thunder", "memphis": "memphis grizzlies",
    "cleveland": "cleveland cavaliers", "minnesota": "minnesota timberwolves",
    "sacramento": "sacramento kings", "new orleans": "new orleans pelicans",
    "atlanta": "atlanta hawks", "chicago": "chicago bulls",
    "toronto": "toronto raptors", "orlando": "orlando magic",
    "indiana": "indiana pacers", "charlotte": "charlotte hornets",
    "washington": "washington wizards", "detroit": "detroit pistons",
    "portland": "portland trail blazers", "san antonio": "san antonio spurs",
    "houston": "houston rockets", "utah": "utah jazz",
    # NFL (city names that don't collide with NBA above)
    "kansas city": "kansas city chiefs", "buffalo": "buffalo bills",
    "baltimore": "baltimore ravens", "san francisco": "san francisco 49ers",
    "cincinnati": "cincinnati bengals", "pittsburgh": "pittsburgh steelers",
    "green bay": "green bay packers", "seattle": "seattle seahawks",
    "jacksonville": "jacksonville jaguars", "las vegas": "las vegas raiders",
    "carolina": "carolina panthers", "tennessee": "tennessee titans",
    "new england": "new england patriots",
    # NHL (city names that don't collide above)
    "edmonton": "edmonton oilers", "colorado": "colorado avalanche",
    "vancouver": "vancouver canucks", "tampa bay": "tampa bay lightning",
    "calgary": "calgary flames", "nashville": "nashville predators",
    "ottawa": "ottawa senators", "columbus": "columbus blue jackets",
    "anaheim": "anaheim ducks", "san jose": "san jose sharks",
    "new jersey": "new jersey devils", "winnipeg": "winnipeg jets",
    "vegas": "vegas golden knights",
    # EPL
    "liverpool": "liverpool", "manchester city": "manchester city",
    "manchester united": "manchester united", "arsenal": "arsenal",
    "chelsea": "chelsea", "tottenham": "tottenham",
    "aston villa": "aston villa", "nottingham": "nottingham forest",
    "fulham": "fulham", "brentford": "brentford",
    "brighton": "brighton", "crystal palace": "crystal palace",
    "wolves": "wolverhampton", "everton": "everton",
    "west ham": "west ham", "bournemouth": "bournemouth",
    "leicester": "leicester city", "southampton": "southampton",
    "ipswich": "ipswich town",
}

@lru_cache(maxsize=4096)
def normalize_name(name):
    """Normalize team/player name for matching."""
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name)
    # Check aliases
    if name in TEAM_ALIASES:
        return TEAM_ALIASES[name]
    # Check if any alias is contained
    for alias, full in TEAM_ALIASES.items():
        if alias in name:
            name = name.replace(alias, full)
    return name.strip()

def extract_teams_from_text(text):
    """Extract potential team names from market text."""
    text = text.lower()
    found = []
    for alias, full in TEAM_ALIASES.items():
        if alias in text:
            if full not in found:
                found.append(full)
    return found

def similarity_score(a, b):
    """Simple token overlap similarity."""
    if not a or not b:
        return 0
    tokens_a = set(normalize_name(a).split())
    tokens_b = set(normalize_name(b).split())
    if not tokens_a or not tokens_b:
        return 0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)

def similarity_score_from_tokens(tokens_a, tokens_b):
    """Token overlap similarity from pre-computed token sets (or lists from cache)."""
    if not tokens_a or not tokens_b:
        return 0
    if not isinstance(tokens_a, set):
        tokens_a = set(tokens_a)
    if not isinstance(tokens_b, set):
        tokens_b = set(tokens_b)
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)

# ─── Sport category helpers ──────────────────────────────────────────────────

SPORT_KEY_TO_CATEGORY = {
    "basketball_nba": "nba",
    "americanfootball_nfl": "nfl",
    "baseball_mlb": "mlb",
    "icehockey_nhl": "nhl",
    "soccer_usa_mls": "soccer",
    "soccer_epl": "soccer",
    "mma_mixed_martial_arts": "mma",
}

SPORT_CATEGORY_KEYWORDS = {
    "nba": ["nba", "basketball"],
    "nfl": ["nfl", "football", "touchdowns", "yards"],
    "mlb": ["mlb", "baseball", "runs"],
    "nhl": ["nhl", "hockey", "stanley cup"],
    "soccer": ["soccer", "epl", "mls", "premier league"],
    "mma": ["mma", "ufc"],
}

# Map full team names to sport categories
TEAM_TO_SPORT = {}
_nba_teams = {"los angeles lakers", "boston celtics", "golden state warriors", "new york knicks",
    "brooklyn nets", "philadelphia 76ers", "miami heat", "milwaukee bucks", "phoenix suns",
    "dallas mavericks", "denver nuggets", "la clippers", "los angeles clippers",
    "oklahoma city thunder", "memphis grizzlies", "cleveland cavaliers", "minnesota timberwolves",
    "sacramento kings", "new orleans pelicans", "atlanta hawks", "chicago bulls", "toronto raptors",
    "orlando magic", "indiana pacers", "charlotte hornets", "washington wizards", "detroit pistons",
    "portland trail blazers", "san antonio spurs", "houston rockets", "utah jazz"}
_nfl_teams = {"kansas city chiefs", "philadelphia eagles", "buffalo bills", "baltimore ravens",
    "detroit lions", "san francisco 49ers", "dallas cowboys", "miami dolphins", "cincinnati bengals",
    "pittsburgh steelers", "green bay packers", "houston texans", "seattle seahawks",
    "los angeles rams", "los angeles chargers", "jacksonville jaguars", "minnesota vikings",
    "indianapolis colts", "new orleans saints", "chicago bears", "denver broncos",
    "las vegas raiders", "arizona cardinals", "atlanta falcons", "washington commanders",
    "carolina panthers", "new york giants", "new york jets", "cleveland browns",
    "new england patriots", "tennessee titans"}
_mlb_teams = {"new york yankees", "los angeles dodgers", "houston astros", "atlanta braves",
    "new york mets", "philadelphia phillies", "san diego padres", "chicago cubs", "boston red sox",
    "toronto blue jays", "cleveland guardians", "baltimore orioles", "minnesota twins",
    "seattle mariners", "texas rangers", "tampa bay rays", "milwaukee brewers",
    "arizona diamondbacks", "pittsburgh pirates", "cincinnati reds", "chicago white sox",
    "kansas city royals", "colorado rockies", "los angeles angels", "detroit tigers",
    "washington nationals", "miami marlins", "oakland athletics"}
_nhl_teams = {"boston bruins", "toronto maple leafs", "edmonton oilers", "colorado avalanche",
    "carolina hurricanes", "minnesota wild", "vancouver canucks", "dallas stars",
    "pittsburgh penguins", "tampa bay lightning", "chicago blackhawks", "detroit red wings",
    "calgary flames", "nashville predators", "washington capitals", "ottawa senators",
    "buffalo sabres", "new york islanders", "philadelphia flyers", "utah hockey club",
    "seattle kraken", "columbus blue jackets", "anaheim ducks", "san jose sharks",
    "new jersey devils"}
for t in _nba_teams: TEAM_TO_SPORT[t] = "nba"
for t in _nfl_teams: TEAM_TO_SPORT[t] = "nfl"
for t in _mlb_teams: TEAM_TO_SPORT[t] = "mlb"
for t in _nhl_teams: TEAM_TO_SPORT[t] = "nhl"

def _detect_sport_category(text):
    """Detect sport category from text keywords or team names."""
    text_lower = text.lower()
    for category, keywords in SPORT_CATEGORY_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return category
    # Fall back to team name detection
    teams = extract_teams_from_text(text)
    for team in teams:
        if team in TEAM_TO_SPORT:
            return TEAM_TO_SPORT[team]
    return None

# ─── Market subtype classification ───────────────────────────────────────────

# Map Kalshi series tickers to market subtypes
SERIES_MARKET_SUBTYPE = {
    "KXNBAGAME": "h2h", "KXNFLGAME": "h2h", "KXMLBGAME": "h2h",
    "KXNHLGAME": "h2h", "KXUFCFIGHT": "h2h", "KXMMAGAME": "h2h",
    "KXEPLGAME": "h2h", "KXMLSGAME": "h2h",
    "KXNBASPREAD": "winning_margin", "KXNFLSPREAD": "winning_margin",
    "KXMLBSPREAD": "winning_margin", "KXNHLSPREAD": "winning_margin",
    "KXNBATOTAL": "totals", "KXNFLTOTAL": "totals", "KXEPLTOTAL": "totals",
    "KXNBA1HTOTAL": "1h_totals",
    "KXNBAPTS": "player_props",
}

# Which sportsbook market_type values each prediction subtype can match
_MARKET_TYPE_COMPAT = {
    "h2h": {"h2h"},
    "spreads": {"spreads"},
    "totals": {"totals"},
    "winning_margin": set(),    # Kalshi "wins by over X" ≠ standard sportsbook spreads
    "1h_totals": set(),         # No sportsbook 1st-half data available
    "player_props": {"player_points", "player_rebounds", "player_assists", "player_threes"},
    "futures": set(),           # Season/championship markets don't match single-game odds
    "unknown": {"h2h", "spreads", "totals", "player_points", "player_rebounds",
                "player_assists", "player_threes"},
}

_FUTURES_RE = re.compile(
    r"championship|stanley cup|world series|super bowl|"
    r"mvp|most valuable|make.*playoffs|win.*20\d\d|"
    r"nba finals|win.*title|win.*division|win.*conference",
    re.IGNORECASE,
)

_POINT_LINE_RE = re.compile(
    r'(?:over|under|spread|cover|[+-])\s*(\d+(?:\.\d+)?)',
    re.IGNORECASE,
)


def _infer_market_subtype(question):
    """Infer market subtype from prediction market question text."""
    q = question.lower()
    has_over = bool(re.search(r'\bover\b', q))
    has_under = bool(re.search(r'\bunder\b', q))
    # Futures / championship — check first (may also contain "win")
    if _FUTURES_RE.search(question):
        return "futures"
    # Spreads — check before totals (avoid "cover" containing "over")
    if "spread" in q or "cover" in q:
        return "spreads"
    # Game totals: "total" keyword + over/under
    if "total" in q and (has_over or has_under):
        return "totals"
    # Player props: stat keyword + over/under (without "total")
    if any(kw in q for kw in ("points", "rebounds", "assists", "threes", "strikeouts")):
        if has_over or has_under:
            return "player_props"
    # Totals: over/under with a point number (no "total" keyword)
    if (has_over or has_under) and _POINT_LINE_RE.search(question):
        return "totals"
    # Game winner / moneyline
    if any(kw in q for kw in ("win", "winner", "beat", "defeat")):
        return "h2h"
    return "unknown"


def _extract_point_line(text):
    """Extract point line (e.g., 215.5) from question text."""
    m = _POINT_LINE_RE.search(text)
    return float(m.group(1)) if m else None


# ─── Polymarket CLI helpers ───────────────────────────────────────────────────

def _polymarket_cli_available():
    """Check if the polymarket CLI tool is installed."""
    return shutil.which("polymarket") is not None

# Strong keywords: league/sport names — one match is enough
_STRONG_SPORT_KW = frozenset([
    "nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball",
    "baseball", "hockey", "mma", "ufc", "tennis",
])
# Weak keywords: appear in non-sports contexts — require a strong match too
_WEAK_SPORT_KW = frozenset([
    "points", "rebounds", "assists", "touchdowns", "goals", "runs", "yards",
    "over", "under", "spread", "moneyline",
])
SPORT_KEYWORDS = _STRONG_SPORT_KW | _WEAK_SPORT_KW

def _fetch_polymarket_via_cli():
    """
    Fetch all active Polymarket markets via CLI in a single call,
    then filter for sports client-side.

    Returns a list of raw market dicts (same shape as Gamma API),
    or None if the CLI call fails.
    """
    try:
        result = subprocess.run(
            ["polymarket", "-o", "json", "markets", "list",
             "--active", "true", "--closed", "false", "--limit", "500"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            return None
        all_markets = json.loads(result.stdout)
        if not isinstance(all_markets, list):
            return None
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None

    sports_markets = _filter_sports_markets(all_markets)
    return sports_markets

def _filter_sports_markets(markets):
    """Filter a list of raw market dicts to only sports-related ones."""
    filtered = []
    for m in markets:
        title = (m.get("question", "") + " " + m.get("description", "")
                 + " " + " ".join(m.get("tags") or [])).lower()
        has_strong = any(kw in title for kw in _STRONG_SPORT_KW)
        has_team = bool(extract_teams_from_text(title))
        if has_strong or has_team:
            filtered.append(m)
    return filtered

# ─── Data fetchers ────────────────────────────────────────────────────────────

def fetch_polymarket_sports(db=None):
    """Fetch sports markets from Polymarket Gamma API."""
    if db is None:
        db = get_db()
    cache_key = "polymarket_sports"
    cached = get_cached(db, cache_key)
    if cached is not None:
        return cached

    # Try CLI first (single call), fall back to sequential HTTP
    markets = None
    if _polymarket_cli_available():
        markets = _fetch_polymarket_via_cli()

    if markets is None:
        # HTTP fallback: parallel fetch across all sport tags
        markets = []
        sport_tags = ["sports", "nba", "nfl", "mlb", "nhl", "soccer", "football",
                      "basketball", "baseball", "hockey", "mma", "ufc"]

        def _fetch_tag(tag):
            url = f"https://gamma-api.polymarket.com/markets?tag={tag}&closed=false&limit=100"
            data = fetch_json(url)
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and not data.get("_error") and "markets" in data:
                return data["markets"]
            return []

        def _fetch_untagged():
            url = "https://gamma-api.polymarket.com/markets?closed=false&limit=200&active=true"
            data = fetch_json(url)
            if isinstance(data, list):
                return [m for m in data
                        if any(kw in (m.get("question", "") + " " + m.get("description", "")).lower()
                               for kw in _STRONG_SPORT_KW)
                        or extract_teams_from_text(
                            (m.get("question", "") + " " + m.get("description", "")).lower())]
            return []

        with ThreadPoolExecutor(max_workers=14) as pool:
            tag_futures = [pool.submit(_fetch_tag, tag) for tag in sport_tags]
            untagged_future = pool.submit(_fetch_untagged)
            for f in as_completed(tag_futures + [untagged_future]):
                try:
                    markets.extend(f.result(timeout=12))
                except Exception:
                    continue

    # Deduplicate by condition_id
    seen = set()
    unique = []
    for m in markets:
        cid = m.get("conditionId") or m.get("condition_id") or m.get("id", "")
        if cid and cid not in seen:
            seen.add(cid)
            unique.append(m)

    # Parse into normalized format
    results = []
    for m in unique:
        try:
            question = m.get("question", "") or m.get("title", "")
            outcomes = m.get("outcomes", [])
            outcome_prices = m.get("outcomePrices", [])
            tokens = m.get("clobTokenIds", [])

            # Parse JSON-encoded strings from Gamma API
            if isinstance(outcomes, str):
                try: outcomes = json.loads(outcomes)
                except (json.JSONDecodeError, ValueError): outcomes = []
            if isinstance(outcome_prices, str):
                try: outcome_prices = json.loads(outcome_prices)
                except (json.JSONDecodeError, ValueError): outcome_prices = []
            if isinstance(tokens, str):
                try: tokens = json.loads(tokens)
                except (json.JSONDecodeError, ValueError): tokens = []

            if not outcomes or not outcome_prices:
                continue

            prices = []
            for p in outcome_prices:
                try:
                    prices.append(float(p))
                except (ValueError, TypeError):
                    prices.append(0)

            entry = {
                "source": "polymarket",
                "id": m.get("conditionId") or m.get("condition_id") or m.get("id", ""),
                "question": question,
                "description": m.get("description", ""),
                "outcomes": outcomes,
                "prices": prices,
                "tokens": tokens,
                "end_date": m.get("endDate") or m.get("end_date_iso", ""),
                "volume": m.get("volume", 0),
                "liquidity": m.get("liquidity", 0),
                "slug": m.get("slug", ""),
                "teams": extract_teams_from_text(question),
                "_tokens": set(normalize_name(question + " " + (m.get("description", "") or "")).split()),
                "_sport_category": _detect_sport_category(question),
                "_market_subtype": _infer_market_subtype(question),
                "url": f"https://polymarket.com/event/{m.get('slug', '')}" if m.get('slug') else "",
            }
            results.append(entry)
        except Exception:
            continue

    set_cached(db, cache_key, results)
    return results

KALSHI_SPORTS_SERIES = {
    # (series_ticker, sport_category)
    "nba": [
        ("KXNBAGAME", "nba"),       # Game winners
        ("KXNBASPREAD", "nba"),      # Spreads
        ("KXNBATOTAL", "nba"),       # Totals
        ("KXNBAPTS", "nba"),         # Player points
        ("KXNBA1HTOTAL", "nba"),     # 1st half totals
    ],
    "nfl": [
        ("KXNFLGAME", "nfl"),
        ("KXNFLSPREAD", "nfl"),
        ("KXNFLTOTAL", "nfl"),
    ],
    "mlb": [
        ("KXMLBGAME", "mlb"),
        ("KXMLBSPREAD", "mlb"),
    ],
    "nhl": [
        ("KXNHLGAME", "nhl"),
        ("KXNHLSPREAD", "nhl"),
    ],
    "mma": [
        ("KXUFCFIGHT", "mma"),
        ("KXMMAGAME", "mma"),
    ],
    "soccer": [
        ("KXEPLGAME", "soccer"),
        ("KXEPLTOTAL", "soccer"),
        ("KXMLSGAME", "soccer"),
    ],
}


def _kalshi_parse_price(m):
    """Parse Kalshi market prices (in cents 0-100) to probabilities (0-1)."""
    yes_price = m.get("yes_bid", 0) or m.get("last_price", 0) or 0
    no_price = m.get("no_bid", 0) or 0
    if yes_price == 0 and no_price == 0:
        yes_price = m.get("yes_ask", 0) or 0
        no_price = m.get("no_ask", 0) or 0
    yes_prob = yes_price / 100.0 if yes_price > 1 else float(yes_price)
    no_prob = no_price / 100.0 if no_price > 1 else float(no_price)
    if yes_prob == 0 and no_prob > 0:
        yes_prob = 1.0 - no_prob
    elif no_prob == 0 and yes_prob > 0:
        no_prob = 1.0 - yes_prob
    return yes_prob, no_prob


def fetch_kalshi_sports(db=None):
    """Fetch sports markets from Kalshi via series → markets API."""
    if db is None:
        db = get_db()
    cache_key = "kalshi_sports"
    cached = get_cached(db, cache_key)
    if cached is not None:
        return cached

    # Collect all series tickers with their sport categories
    all_series = []
    for sport, series_list in KALSHI_SPORTS_SERIES.items():
        for ticker, category in series_list:
            all_series.append((ticker, category))

    # Fetch markets for all series in parallel (skip events step)
    raw_markets = []  # (market_dict, sport_category)

    def _fetch_series_markets(series_ticker, category):
        url = (f"https://api.elections.kalshi.com/trade-api/v2/markets"
               f"?series_ticker={series_ticker}&status=open&limit=200")
        data = fetch_json(url)
        mkts = []
        if isinstance(data, dict) and "markets" in data:
            for m in data["markets"]:
                mkts.append((m, category, series_ticker))
        return mkts

    # Kalshi Basic tier: 20 reads/sec — 5 workers avoids throttling
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(_fetch_series_markets, t, c) for t, c in all_series]
        for future in as_completed(futures):
            try:
                raw_markets.extend(future.result(timeout=10))
            except Exception:
                continue

    # Normalize into standard format
    results = []
    for m, category, series_ticker in raw_markets:
        try:
            title = m.get("title", "")
            yes_prob, no_prob = _kalshi_parse_price(m)

            # Use floor_strike for point line (totals/spreads/props)
            floor_strike = m.get("floor_strike")
            no_sub = m.get("no_sub_title", "") or ""

            entry = {
                "source": "kalshi",
                "id": m.get("ticker", ""),
                "question": title,
                "description": no_sub or m.get("subtitle", "") or title,
                "outcomes": ["Yes", "No"],
                "prices": [yes_prob, no_prob],
                "end_date": m.get("expiration_time", "") or m.get("close_time", ""),
                "volume": m.get("volume", 0),
                "liquidity": m.get("open_interest", 0),
                "ticker": m.get("ticker", ""),
                "event_ticker": m.get("event_ticker", ""),
                "teams": extract_teams_from_text(title),
                "_tokens": set(normalize_name(title + " " + no_sub).split()),
                "_sport_category": category,
                "_market_subtype": SERIES_MARKET_SUBTYPE.get(series_ticker, "unknown"),
                "_floor_strike": float(floor_strike) if floor_strike is not None else None,
                "_no_sub_title": no_sub,
                "url": f"https://kalshi.com/markets/{m.get('ticker', '').lower()}" if m.get('ticker') else "",
            }
            results.append(entry)
        except Exception:
            continue

    set_cached(db, cache_key, results)
    return results

def fetch_sportsbook_odds(db=None, api_key=""):
    """Fetch odds from The Odds API for major sports."""
    if db is None:
        db = get_db()
    cache_key = "sportsbook_odds"
    cached = get_cached(db, cache_key)
    if cached is not None:
        return cached

    if not api_key:
        return []

    sports_to_fetch = [
        "basketball_nba",
        "americanfootball_nfl",
        "baseball_mlb",
        "icehockey_nhl",
        "soccer_usa_mls",
        "soccer_epl",
        "mma_mixed_martial_arts",
    ]

    bookmakers = "draftkings,fanduel,betrivers,betmgm,pinnacle,williamhill_us,bovada"
    all_events = []

    api_errors = []

    def _fetch_sport(sport, is_prop=False):
        """Fetch a single sport from The Odds API. Thread-safe (no shared state)."""
        if is_prop:
            markets_param = "player_points,player_rebounds,player_assists,player_threes"
        else:
            markets_param = "h2h,spreads,totals"
        url = (f"https://api.the-odds-api.com/v4/sports/{sport}/odds?"
               f"apiKey={api_key}&regions=us&markets={markets_param}"
               f"&bookmakers={bookmakers}&oddsFormat=american")
        data = fetch_json(url)
        if isinstance(data, dict) and "_error" in data:
            err = data["_error"]
            if "401" in err or "403" in err:
                raise RuntimeError("INVALID_KEY")
            if "429" in err or "quota" in err.lower() or "limit" in err.lower():
                raise RuntimeError("QUOTA_EXCEEDED")
            raise RuntimeError(err)
        events = []
        if isinstance(data, list):
            for event in data:
                event["_sport_key"] = sport
                if is_prop:
                    event["_is_prop"] = True
                events.append(event)
        return events

    # Fire all 8 requests in parallel (7 sports + 1 NBA props)
    fetch_tasks = [(sport, False) for sport in sports_to_fetch]
    fetch_tasks.append(("basketball_nba", True))  # NBA player props

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_fetch_sport, sport, is_prop)
                   for sport, is_prop in fetch_tasks]
        for future in as_completed(futures):
            try:
                all_events.extend(future.result(timeout=12))
            except RuntimeError as e:
                api_errors.append(str(e))
            except Exception:
                continue

    # If all requests failed with the same API error, propagate it
    if not all_events and api_errors:
        if any(e == "QUOTA_EXCEEDED" for e in api_errors):
            raise RuntimeError("QUOTA_EXCEEDED: Odds API usage limit reached. Check your plan at https://the-odds-api.com")
        if any(e == "INVALID_KEY" for e in api_errors):
            raise RuntimeError("INVALID_KEY: Odds API key is invalid or expired. Update it in Settings.")

    # Parse into normalized format
    results = []
    for event in all_events:
        try:
            home = event.get("home_team", "")
            away = event.get("away_team", "")
            commence = event.get("commence_time", "")
            sport_key = event.get("_sport_key", "")
            is_prop = event.get("_is_prop", False)

            for bookmaker in event.get("bookmakers", []):
                bk_name = bookmaker.get("key", "")
                bk_title = bookmaker.get("title", "")

                for market in bookmaker.get("markets", []):
                    market_key = market.get("key", "")

                    for outcome in market.get("outcomes", []):
                        price = outcome.get("price", 0)
                        name = outcome.get("name", "")
                        point = outcome.get("point")
                        description = outcome.get("description", "")

                        imp_prob = american_to_implied_prob(price) if price != 0 else 0

                        entry = {
                            "source": "sportsbook",
                            "bookmaker": bk_name,
                            "bookmaker_title": bk_title,
                            "sport": sport_key,
                            "home_team": home,
                            "away_team": away,
                            "commence_time": commence,
                            "market_type": market_key,
                            "outcome_name": name,
                            "outcome_point": point,
                            "description": description,
                            "american_odds": price,
                            "implied_prob": imp_prob,
                            "decimal_odds": american_to_decimal(price) if price != 0 else 0,
                            "is_prop": is_prop,
                            "teams": extract_teams_from_text(home + " " + away),
                            "_tokens": set(normalize_name(away + " " + home + " " + name).split()),
                            "_sport_category": SPORT_KEY_TO_CATEGORY.get(sport_key, "other"),
                            "event_name": f"{away} @ {home}",
                        }
                        results.append(entry)
        except Exception:
            continue

    set_cached(db, cache_key, results)
    return results

# ─── Matching engine ──────────────────────────────────────────────────────────

def try_match_prediction_to_sportsbook(pred_market, sportsbook_entries):
    """
    Try to match a prediction market to sportsbook entries.
    Returns list of potential matches with confidence scores.
    """
    question = pred_market.get("question", "").lower()
    description = pred_market.get("description", "").lower()
    pred_teams = pred_market.get("teams", [])
    full_text = question + " " + description
    pred_tokens = pred_market.get("_tokens", None)

    matches = []

    for sb in sportsbook_entries:
        score = 0
        sb_teams = sb.get("teams", [])

        # Team matching — count how many prediction teams appear in sportsbook teams
        team_matches = 0
        for pt in pred_teams:
            for st in sb_teams:
                if pt and st and (pt in st or st in pt):
                    team_matches += 1
                    break  # count each pred team at most once

        # For game-specific markets (h2h, totals, spreads), BOTH teams must match
        # This prevents "Brighton at Sunderland" matching "Arsenal at Brighton"
        pred_subtype = pred_market.get("_market_subtype", "unknown")
        is_game_market = pred_subtype in ("h2h", "totals", "spreads", "winning_margin") or (
            "winner" in question or "win" in question
            or sb.get("market_type") in ("h2h", "totals", "spreads"))
        if is_game_market and len(pred_teams) >= 2:
            if team_matches < 2:
                continue  # skip — wrong game

        if team_matches >= 2:
            score += 0.6
        elif team_matches == 1:
            score += 0.3
        else:
            continue  # no team overlap at all — skip

        # Text similarity (use pre-computed tokens when available)
        sb_tokens = sb.get("_tokens", None)
        if pred_tokens is not None and sb_tokens is not None:
            text_sim = similarity_score_from_tokens(pred_tokens, sb_tokens)
        else:
            sb_event = sb.get("event_name", "").lower()
            sb_outcome = sb.get("outcome_name", "").lower()
            text_sim = similarity_score(full_text, sb_event + " " + sb_outcome)
        score += text_sim * 0.3

        # Player name matching for props
        if sb.get("description"):  # Player props have description field
            player_name = sb["description"].lower()
            if player_name in full_text:
                score += 0.4

        # Point/line matching
        if sb.get("outcome_point") is not None:
            point_str = str(sb["outcome_point"])
            if point_str in full_text:
                score += 0.2

        # Market type matching
        if "over" in question and sb["outcome_name"].lower() == "over":
            score += 0.15
        elif "under" in question and sb["outcome_name"].lower() == "under":
            score += 0.15
        elif "win" in question or "winner" in question:
            if sb["market_type"] == "h2h":
                score += 0.1

        if score >= 0.4:
            matches.append({
                "sportsbook_entry": sb,
                "confidence": min(score, 1.0)
            })

    # Sort by confidence
    matches.sort(key=lambda x: x["confidence"], reverse=True)
    return matches[:5]

# ─── Arbitrage computation ────────────────────────────────────────────────────

def compute_arb_binary(prob_a, prob_b, fee_a=0, fee_b=0):
    """
    Compute arbitrage for a binary market.
    prob_a: probability for outcome A on platform A
    prob_b: probability for opposing outcome on platform B
    fee_a, fee_b: effective fees as decimal (e.g., 0.02 for 2%)
    """
    if prob_a <= 0 or prob_b <= 0:
        return None

    # Gross cost to cover both sides
    cost = prob_a + prob_b
    if cost >= 1.0:
        return None  # No arb

    gross_profit_pct = (1.0 - cost) * 100

    # Net after fees
    # Fee reduces winnings: effective cost increases
    adj_prob_a = prob_a + (1 - prob_a) * fee_a  # cost + fee on winnings
    adj_prob_b = prob_b + (1 - prob_b) * fee_b
    net_cost = adj_prob_a + adj_prob_b

    if net_cost >= 1.0:
        net_profit_pct = (1.0 - net_cost) * 100  # Will be negative
    else:
        net_profit_pct = (1.0 - net_cost) * 100

    return {
        "gross_arb_pct": round(gross_profit_pct, 3),
        "net_arb_pct": round(net_profit_pct, 3),
        "cost": round(cost, 5),
        "net_cost": round(net_cost, 5),
    }

def compute_stake_allocation(prob_a, prob_b, bankroll):
    """Calculate optimal stakes for equal payout."""
    if prob_a <= 0 or prob_b <= 0:
        return None
    total = prob_a + prob_b
    if total >= 1.0:
        return None
    stake_a = bankroll * prob_a / total * (1 / (1 - total + total))
    stake_b = bankroll * prob_b / total * (1 / (1 - total + total))
    # Simpler: for binary arb, stake proportional to probability
    stake_a = round(bankroll * prob_a, 2)
    stake_b = round(bankroll * prob_b, 2)
    total_staked = stake_a + stake_b
    payout_a = round(stake_a / prob_a, 2) if prob_a > 0 else 0
    payout_b = round(stake_b / prob_b, 2) if prob_b > 0 else 0
    # Equalize payouts
    target_payout = bankroll  # aim for equal payouts
    stake_a = round(target_payout * prob_a, 2)
    stake_b = round(target_payout * prob_b, 2)
    total_staked = stake_a + stake_b
    guaranteed_profit = round(target_payout - total_staked, 2)
    return {
        "stake_a": stake_a,
        "stake_b": stake_b,
        "total_staked": total_staked,
        "payout": round(target_payout, 2),
        "guaranteed_profit": guaranteed_profit,
    }


def find_all_arb_opportunities(prediction_markets, sportsbook_entries, min_net_pct=-999):
    """
    Find all arbitrage opportunities across prediction markets and sportsbooks.
    """
    opportunities = []

    # Fees
    KALSHI_FEE = 0.012  # ~1.2% effective
    POLYMARKET_FEE = 0.02  # 2% taker fee on winnings
    SPORTSBOOK_FEE = 0.0  # Built into odds

    # Build team-to-entries index for candidate narrowing
    team_index = defaultdict(set)
    for i, sb in enumerate(sportsbook_entries):
        for team in sb.get("teams", []):
            if team:
                team_index[team].add(i)

    for pred in prediction_markets:
        source = pred.get("source", "")
        prices = pred.get("prices", [])
        outcomes = pred.get("outcomes", [])

        if len(prices) < 2 or len(outcomes) < 2:
            continue

        yes_price = prices[0]  # YES / first outcome
        no_price = prices[1]   # NO / second outcome

        if yes_price <= 0 or yes_price >= 1:
            continue

        # Skip illiquid markets — wide bid-ask spreads create phantom arbs
        if yes_price + no_price < 0.90:
            continue

        pred_fee = POLYMARKET_FEE if source == "polymarket" else KALSHI_FEE

        # Narrow candidates by team index — skip markets with no teams
        pred_teams = pred.get("teams", [])
        if not pred_teams:
            continue  # can't match to sportsbook without team info

        candidate_indices = set()
        for team in pred_teams:
            candidate_indices.update(team_index.get(team, set()))
        if not candidate_indices:
            continue  # no sportsbook entries share a team
        candidates = [sportsbook_entries[i] for i in candidate_indices]

        # Further narrow by sport category
        pred_sport = pred.get("_sport_category")
        if pred_sport:
            candidates = [c for c in candidates
                          if not c.get("_sport_category") or c["_sport_category"] == pred_sport]

        # Narrow by market type compatibility
        pred_subtype = pred.get("_market_subtype", "unknown")
        allowed_sb_types = _MARKET_TYPE_COMPAT.get(pred_subtype, _MARKET_TYPE_COMPAT["unknown"])
        if not allowed_sb_types:
            continue  # futures and 1h_totals can't match sportsbooks
        candidates = [c for c in candidates if c.get("market_type") in allowed_sb_types]

        # Skip soccer h2h — 3-way market (win/draw/lose) can't arb against binary
        if pred_subtype == "h2h" and pred.get("_sport_category") == "soccer":
            continue

        # For totals/spreads/props, require matching point line
        if pred_subtype in ("totals", "spreads", "player_props"):
            # Use floor_strike (Kalshi API) first, fall back to text extraction
            pred_line = pred.get("_floor_strike") or _extract_point_line(pred.get("question", ""))
            if pred_line is not None:
                candidates = [c for c in candidates
                              if c.get("outcome_point") is not None
                              and abs(c["outcome_point"] - pred_line) < 0.01]
            else:
                # No point line extractable — too ambiguous to match reliably
                continue

        if not candidates:
            continue

        # Find matching sportsbook entries
        matches = try_match_prediction_to_sportsbook(pred, candidates)

        for match in matches:
            sb = match["sportsbook_entry"]
            confidence = match["confidence"]
            sb_prob = sb.get("implied_prob", 0)

            if sb_prob <= 0 or sb_prob >= 1:
                continue

            # Determine side alignment using market-type-aware logic
            sb_market_type = sb.get("market_type", "")

            if pred_subtype in ("totals", "player_props") and sb_market_type in ("totals", "player_points", "player_rebounds", "player_assists", "player_threes"):
                # Explicit over/under alignment — price proximity fails for totals
                # Check question, description, and _no_sub_title for over/under
                pred_text = (pred.get("question", "") + " " + pred.get("description", "") + " "
                             + pred.get("_no_sub_title", "")).lower()
                sb_outcome_lower = sb.get("outcome_name", "").lower()
                has_over = bool(re.search(r'\bover\b', pred_text))
                has_under = bool(re.search(r'\bunder\b', pred_text))
                if has_over or has_under:
                    pred_is_over = has_over and not has_under
                    sb_is_over = sb_outcome_lower == "over"
                    sb_same_as_yes = (pred_is_over == sb_is_over)
                else:
                    # No over/under found anywhere — fall back to price proximity
                    diff_yes = abs(yes_price - sb_prob)
                    diff_no = abs(no_price - sb_prob)
                    sb_same_as_yes = (diff_yes <= diff_no)
            else:
                # h2h and spreads: price proximity heuristic
                diff_yes = abs(yes_price - sb_prob)
                diff_no = abs(no_price - sb_prob)
                sb_same_as_yes = (diff_yes <= diff_no)

            if sb_same_as_yes:
                # sb same side as pred YES → arb: pred NO + sb
                arb = compute_arb_binary(no_price, sb_prob, pred_fee, SPORTSBOOK_FEE)
                pred_side_raw = "No"
                pred_price = no_price
            else:
                # sb opposite side from pred YES → arb: pred YES + sb
                arb = compute_arb_binary(yes_price, sb_prob, pred_fee, SPORTSBOOK_FEE)
                pred_side_raw = "Yes"
                pred_price = yes_price

            sb_side = sb.get("outcome_name", "")

            # Translate Yes/No into meaningful labels
            pred_line = pred.get("_floor_strike")
            no_sub = pred.get("_no_sub_title", "")
            if pred_subtype == "totals" and pred_line is not None:
                pred_side = f"Over {pred_line}" if pred_side_raw == "Yes" else f"Under {pred_line}"
            elif pred_subtype == "h2h" and no_sub:
                # no_sub_title has the YES team name (e.g., "Sacramento", "Phoenix")
                yes_team = no_sub.strip()
                if pred_side_raw == "Yes":
                    pred_side = yes_team
                else:
                    # NO = the other team — find it from teams list
                    pred_teams_list = pred.get("teams", [])
                    other = [t for t in pred_teams_list
                             if yes_team.lower() not in t]
                    pred_side = other[0].title() if other else f"Not {yes_team}"
            else:
                pred_side = pred_side_raw
            sb_price_display = sb.get("american_odds", 0)

            if arb is None or arb["gross_arb_pct"] <= 0:
                continue
            if arb["gross_arb_pct"] > 15:
                continue  # >15% gross is certainly stale/non-executable pricing
            if arb["net_arb_pct"] < min_net_pct:
                continue

            # Determine sport
            sport = sb.get("sport", "").replace("_", " ").upper()
            if "nba" in sport.lower() or "basketball" in sport.lower():
                sport_display = "NBA"
            elif "nfl" in sport.lower() or "football" in sport.lower():
                sport_display = "NFL"
            elif "mlb" in sport.lower() or "baseball" in sport.lower():
                sport_display = "MLB"
            elif "nhl" in sport.lower() or "hockey" in sport.lower():
                sport_display = "NHL"
            elif "soccer" in sport.lower() or "mls" in sport.lower() or "epl" in sport.lower():
                sport_display = "Soccer"
            elif "mma" in sport.lower() or "ufc" in sport.lower():
                sport_display = "MMA"
            else:
                sport_display = sport[:10] if sport else "Sports"

            stakes = compute_stake_allocation(pred_price, sb_prob, 100)

            # Time sensitivity
            commence = sb.get("commence_time", "")
            is_live = False
            time_display = ""
            if commence:
                try:
                    event_time = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    if event_time < now:
                        is_live = True
                        time_display = "LIVE"
                    else:
                        delta = event_time - now
                        if delta.days > 0:
                            time_display = f"{delta.days}d"
                        elif delta.seconds > 3600:
                            time_display = f"{delta.seconds // 3600}h"
                        else:
                            time_display = f"{delta.seconds // 60}m"
                except Exception:
                    time_display = ""

            # Resolution risk
            resolution_risk = "low"
            risk_note = ""
            gross_pct = arb["gross_arb_pct"]
            if gross_pct > 10:
                resolution_risk = "high"
                risk_note = "Likely stale pricing — arb this large (>10%) usually means one side has outdated odds"
            elif confidence < 0.6:
                resolution_risk = "high"
                risk_note = "Low match confidence — verify markets reference the same event and conditions"
            elif confidence < 0.8:
                resolution_risk = "medium"
                risk_note = "Moderate match confidence — check resolution criteria on both platforms"
            elif source != "sportsbook":
                resolution_risk = "low"
                risk_note = "Different platforms may use different data sources for settlement"

            # Build descriptive event string
            base_event = sb.get("event_name", pred.get("question", "")[:60])
            if pred_subtype == "totals" and pred_line is not None:
                event_display = f"{base_event} — O/U {pred_line}"
            elif pred_subtype == "h2h":
                event_display = f"{base_event} — ML"
            elif pred_subtype == "spreads":
                event_display = f"{base_event} — Spread"
            elif pred_subtype == "player_props":
                event_display = f"{base_event} — Props"
            else:
                event_display = base_event

            opp = {
                "id": hashlib.md5(f"{pred.get('id','')}-{sb.get('bookmaker','')}-{sb.get('outcome_name','')}-{pred_side}".encode()).hexdigest()[:12],
                "sport": sport_display,
                "event": event_display,
                "event_detail": pred.get("question", ""),
                "commence_time": commence,
                "time_display": time_display,
                "is_live": is_live,
                "platform_a": {
                    "name": source.capitalize(),
                    "side": pred_side,
                    "price": round(pred_price, 4),
                    "implied_prob": round(pred_price, 4),
                    "american_odds": implied_prob_to_american(pred_price),
                    "fee_pct": pred_fee * 100,
                    "url": pred.get("url", ""),
                    "market_id": pred.get("id", ""),
                },
                "platform_b": {
                    "name": sb.get("bookmaker_title", sb.get("bookmaker", "")),
                    "side": sb_side,
                    "price": sb_price_display,
                    "implied_prob": round(sb_prob, 4),
                    "american_odds": sb.get("american_odds", 0),
                    "fee_pct": 0,
                    "url": "",
                    "market_id": "",
                },
                "market_type": sb.get("market_type", "h2h"),
                "gross_arb_pct": arb["gross_arb_pct"],
                "net_arb_pct": arb["net_arb_pct"],
                "stakes": stakes,
                "match_confidence": round(confidence, 2),
                "resolution_risk": resolution_risk,
                "risk_note": risk_note,
                "is_prop": sb.get("is_prop", False),
                "liquidity": pred.get("liquidity", 0),
                "volume": pred.get("volume", 0),
            }
            opportunities.append(opp)

    # Deduplicate: keep best arb per unique event+platforms pair
    seen = {}
    for opp in opportunities:
        key = f"{opp['event']}-{opp['platform_a']['name']}-{opp['platform_b']['name']}-{opp['market_type']}"
        if key not in seen or opp['net_arb_pct'] > seen[key]['net_arb_pct']:
            seen[key] = opp

    deduped = sorted(seen.values(), key=lambda x: x['net_arb_pct'], reverse=True)
    return deduped

# ─── Also check cross-prediction-market arbs ─────────────────────────────────

def find_cross_prediction_arbs(poly_markets, kalshi_markets, min_net_pct=-999):
    """Find arbs between Polymarket and Kalshi on the same event."""
    opportunities = []
    KALSHI_FEE = 0.012
    POLYMARKET_FEE = 0.02

    # Build team index for Kalshi markets
    kalshi_team_index = defaultdict(set)
    for i, km in enumerate(kalshi_markets):
        for team in km.get("teams", []):
            if team:
                kalshi_team_index[team].add(i)

    for pm in poly_markets:
        pm_question = pm.get("question", "").lower()
        pm_teams = pm.get("teams", [])
        pm_prices = pm.get("prices", [])
        pm_tokens = pm.get("_tokens", None)

        if len(pm_prices) < 2:
            continue
        if pm_prices[0] + pm_prices[1] < 0.90:
            continue  # illiquid — wide bid-ask creates phantom arbs

        # Narrow Kalshi candidates by team overlap
        if pm_teams:
            candidate_indices = set()
            for team in pm_teams:
                candidate_indices.update(kalshi_team_index.get(team, set()))
            candidates = [kalshi_markets[i] for i in candidate_indices]
        else:
            candidates = kalshi_markets

        for km in candidates:
            km_question = km.get("question", "").lower()
            km_teams = km.get("teams", [])
            km_prices = km.get("prices", [])

            if len(km_prices) < 2:
                continue
            if km_prices[0] + km_prices[1] < 0.90:
                continue  # illiquid — wide bid-ask creates phantom arbs

            # Match by teams and text
            team_overlap = len(set(pm_teams) & set(km_teams))

            # Market subtype must be compatible (don't match h2h vs totals)
            pm_subtype = pm.get("_market_subtype", "unknown")
            km_subtype = km.get("_market_subtype", "unknown")
            if pm_subtype != "unknown" and km_subtype != "unknown":
                if pm_subtype != km_subtype:
                    continue  # different market types

            # For totals, require matching point line
            if pm_subtype == "totals" and km_subtype == "totals":
                pm_line = _extract_point_line(pm.get("question", ""))
                km_line = _extract_point_line(km.get("question", ""))
                if pm_line is not None and km_line is not None and abs(pm_line - km_line) >= 0.01:
                    continue

            # For game winner markets, require both teams to match
            is_game = ("winner" in pm_question or "win" in pm_question
                       or "winner" in km_question or "win" in km_question)
            if is_game and len(pm_teams) >= 2 and len(km_teams) >= 2:
                if team_overlap < 2:
                    continue

            km_tokens = km.get("_tokens", None)
            if pm_tokens is not None and km_tokens is not None:
                text_sim = similarity_score_from_tokens(pm_tokens, km_tokens)
            else:
                text_sim = similarity_score(pm_question, km_question)
            score = team_overlap * 0.3 + text_sim * 0.4

            if score < 0.35:
                continue

            # Determine if Poly YES and Kalshi YES are the same outcome
            pm_yes, pm_no = pm_prices[0], pm_prices[1]
            km_yes, km_no = km_prices[0], km_prices[1]

            if pm_subtype in ("totals", "player_props"):
                # Explicit over/under alignment for totals
                pm_has_over = bool(re.search(r'\bover\b', pm_question))
                pm_has_under = bool(re.search(r'\bunder\b', pm_question))
                km_has_over = bool(re.search(r'\bover\b', km_question))
                km_has_under = bool(re.search(r'\bunder\b', km_question))
                if (pm_has_over or pm_has_under) and (km_has_over or km_has_under):
                    pm_is_over = pm_has_over and not pm_has_under
                    km_is_over = km_has_over and not km_has_under
                    aligned = (pm_is_over == km_is_over)
                else:
                    # Fall back to price proximity
                    diff_aligned = abs(pm_yes - km_yes)
                    diff_misaligned = abs(pm_yes - km_no)
                    aligned = (diff_aligned <= diff_misaligned)
            else:
                # Price proximity for h2h / unknown
                diff_aligned = abs(pm_yes - km_yes)
                diff_misaligned = abs(pm_yes - km_no)
                aligned = (diff_aligned <= diff_misaligned)

            if aligned:
                # Aligned: PM YES ≈ KM YES → arb: PM YES + KM NO
                arb = compute_arb_binary(pm_yes, km_no, POLYMARKET_FEE, KALSHI_FEE)
                pa_side = pm.get("outcomes", ["Yes"])[0]
                pa_price = pm_yes
                pb_side = km.get("outcomes", ["", "No"])[1]
                pb_price = km_no
            else:
                # Misaligned: PM YES ≈ KM NO → arb: PM YES + KM YES
                arb = compute_arb_binary(pm_yes, km_yes, POLYMARKET_FEE, KALSHI_FEE)
                pa_side = pm.get("outcomes", ["Yes"])[0]
                pa_price = pm_yes
                pb_side = km.get("outcomes", ["Yes"])[0]
                pb_price = km_yes

            if arb is None or arb["gross_arb_pct"] <= 0:
                continue
            if arb["gross_arb_pct"] > 15:
                continue  # >15% gross is certainly stale/non-executable pricing
            if arb["net_arb_pct"] < min_net_pct:
                continue

            stakes = compute_stake_allocation(pa_price, pb_price, 100)

            opp = {
                "id": hashlib.md5(f"cross-{pm.get('id','')}-{km.get('id','')}-{pa_side}".encode()).hexdigest()[:12],
                "sport": "Sports",
                "event": pm.get("question", "")[:60],
                "event_detail": pm.get("question", ""),
                "commence_time": "",
                "time_display": "",
                "is_live": False,
                "platform_a": {
                    "name": "Polymarket",
                    "side": pa_side,
                    "price": round(pa_price, 4),
                    "implied_prob": round(pa_price, 4),
                    "american_odds": implied_prob_to_american(pa_price),
                    "fee_pct": POLYMARKET_FEE * 100,
                    "url": pm.get("url", ""),
                    "market_id": pm.get("id", ""),
                },
                "platform_b": {
                    "name": "Kalshi",
                    "side": pb_side,
                    "price": round(pb_price, 4),
                    "implied_prob": round(pb_price, 4),
                    "american_odds": implied_prob_to_american(pb_price),
                    "fee_pct": KALSHI_FEE * 100,
                    "url": km.get("url", ""),
                    "market_id": km.get("id", ""),
                },
                "market_type": "binary",
                "gross_arb_pct": arb["gross_arb_pct"],
                "net_arb_pct": arb["net_arb_pct"],
                "stakes": stakes,
                "match_confidence": round(score, 2),
                "resolution_risk": "high" if arb["gross_arb_pct"] > 10 else ("medium" if score < 0.6 else "low"),
                "risk_note": ("Likely stale pricing — arb this large (>10%) usually means one side has outdated odds"
                              if arb["gross_arb_pct"] > 10
                              else "Cross-platform prediction market arb — verify both markets resolve on the same criteria"),
                "is_prop": False,
                "liquidity": pm.get("liquidity", 0),
                "volume": pm.get("volume", 0),
            }
            opportunities.append(opp)

    return sorted(opportunities, key=lambda x: x['net_arb_pct'], reverse=True)


# ─── Core scan logic (callable from CGI or Vercel handler) ───────────────────

def run_scan(params):
    """Run the full scan and return the response dict."""
    db = get_db()

    min_net_pct = float(params.get("min_pct", "-999"))
    sports_filter = params.get("sports", "").split(",") if params.get("sports") else []

    api_key = (params.get("api_key", "")
               or os.environ.get("ODDS_API_KEY", "")
               or get_config(db, "odds_api_key", ""))

    scan_start = time.time()
    errors = []
    sources_status = {
        "polymarket": "pending",
        "kalshi": "pending",
        "sportsbook": "pending" if api_key else "no_key",
    }

    all_opportunities = []
    poly_markets = []
    kalshi_markets = []
    sportsbook_entries = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_poly = executor.submit(fetch_polymarket_sports, None)
        future_kalshi = executor.submit(fetch_kalshi_sports, None)
        future_sb = executor.submit(fetch_sportsbook_odds, None, api_key) if api_key else None

        try:
            poly_markets = future_poly.result(timeout=15)
            sources_status["polymarket"] = "ok" if poly_markets else "empty"
        except Exception as e:
            sources_status["polymarket"] = "error"
            errors.append(f"Polymarket: {str(e)}")

        try:
            kalshi_markets = future_kalshi.result(timeout=15)
            sources_status["kalshi"] = "ok" if kalshi_markets else "empty"
        except Exception as e:
            sources_status["kalshi"] = "error"
            errors.append(f"Kalshi: {str(e)}")

        if future_sb is not None:
            try:
                sportsbook_entries = future_sb.result(timeout=15)
                sources_status["sportsbook"] = "ok" if sportsbook_entries else "empty"
            except RuntimeError as e:
                err_msg = str(e)
                if "QUOTA_EXCEEDED" in err_msg:
                    sources_status["sportsbook"] = "quota_exceeded"
                elif "INVALID_KEY" in err_msg:
                    sources_status["sportsbook"] = "invalid_key"
                else:
                    sources_status["sportsbook"] = "error"
                errors.append(f"Sportsbook: {err_msg}")
            except Exception as e:
                sources_status["sportsbook"] = "error"
                errors.append(f"Sportsbook: {str(e)}")

    # Find arbs: prediction markets vs sportsbooks
    if sportsbook_entries:
        if poly_markets:
            arbs1 = find_all_arb_opportunities(poly_markets, sportsbook_entries, min_net_pct)
            all_opportunities.extend(arbs1)
        if kalshi_markets:
            arbs2 = find_all_arb_opportunities(kalshi_markets, sportsbook_entries, min_net_pct)
            all_opportunities.extend(arbs2)

    # Find cross-prediction-market arbs
    if poly_markets and kalshi_markets:
        cross_arbs = find_cross_prediction_arbs(poly_markets, kalshi_markets, min_net_pct)
        all_opportunities.extend(cross_arbs)

    # Apply sports filter
    if sports_filter and sports_filter[0]:
        sports_set = set(s.upper() for s in sports_filter)
        all_opportunities = [o for o in all_opportunities if o["sport"].upper() in sports_set]

    # Sort by net arb %
    all_opportunities.sort(key=lambda x: x["net_arb_pct"], reverse=True)

    scan_duration = round(time.time() - scan_start, 2)

    return {
        "opportunities": all_opportunities,
        "meta": {
            "scan_time": scan_duration,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_opportunities": len(all_opportunities),
            "sources": sources_status,
            "errors": errors,
            "is_demo": False,
            "poly_count": len(poly_markets),
            "kalshi_count": len(kalshi_markets),
            "sportsbook_count": len(sportsbook_entries),
        }
    }

# ─── CGI entry point (for local development) ─────────────────────────────────

def main():
    print("Content-Type: application/json")
    print()
    query_string = os.environ.get("QUERY_STRING", "")
    params = dict(urllib.parse.parse_qsl(query_string))
    print(json.dumps(run_scan(params), default=_json_default))

if __name__ == "__main__":
    main()
