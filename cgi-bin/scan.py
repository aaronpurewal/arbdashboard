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
# Cache TTLs calibrated to each API's actual update frequency / rate limits:
# - Polymarket: real-time CLOB, 300 req/10s limit — no cache needed
# - Kalshi: real-time CLOB, but only 20 req/s (Basic tier) — needs light cache
# - Odds API: server updates every 60s (pre-match) / 40s (in-play) — match their refresh
POLYMARKET_CACHE_TTL = 0
KALSHI_CACHE_TTL = 15   # 20 req/s limit is tight with ~15 series per scan
SPORTSBOOK_CACHE_TTL = 45  # their server only refreshes every 60s anyway

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

def _safe_int(val, default=None):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default

def get_cached(db, cache_key, ttl=0):
    row = db.execute("SELECT data, ts FROM cache WHERE cache_key=?", [cache_key]).fetchone()
    if row and (time.time() - row[1]) < ttl:
        return json.loads(row[0])
    return None

def get_stale_cached(db, cache_key):
    """Return cached data even if expired — used as fallback when API fails."""
    row = db.execute("SELECT data, ts FROM cache WHERE cache_key=?", [cache_key]).fetchone()
    if row:
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

def fetch_json_with_headers(url, timeout=12):
    """Like fetch_json but also returns response headers (for API quota tracking)."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "ArbScanner/1.0",
            "Accept": "application/json"
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            headers = dict(resp.headers)
            raw = resp.read().decode("utf-8")
            return json.loads(raw), headers
    except urllib.error.HTTPError as e:
        return {"_error": f"HTTP {e.code}: {e.reason}"}, {}
    except Exception as e:
        return {"_error": str(e)}, {}

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
    "wolverhampton wanderers": "wolverhampton", "everton": "everton",
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

def extract_teams_from_text(text, sport_category=None):
    """Extract potential team names from market text.

    When sport_category is provided, filters results to only include teams
    from that sport.  Teams whose sport is unknown (not in TEAM_TO_SPORT)
    are kept regardless so that newer leagues aren't silently dropped.
    Falls back to the unfiltered list if filtering removes everything.
    """
    text = text.lower()
    found = []
    for alias, full in TEAM_ALIASES.items():
        if alias in text:
            if full not in found:
                found.append(full)
    if sport_category and found:
        sport_filtered = [t for t in found
                          if TEAM_TO_SPORT.get(t) is None
                          or TEAM_TO_SPORT.get(t) == sport_category]
        if sport_filtered:
            return sport_filtered
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

def _parse_event_date(dt_str):
    """Extract date from an ISO datetime string. Returns datetime.date or None."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return None

def _dates_compatible(date_a, date_b, max_days=2):
    """Check if two dates are within max_days of each other."""
    if date_a is None or date_b is None:
        return True  # can't determine — don't filter
    return abs((date_a - date_b).days) <= max_days

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
    "boxing": ["boxing", "bout"],
}

# Sports with 3-way h2h markets (win/draw/lose) — can't arb against binary predictions.
# MMA is intentionally excluded: draws exist (~1-2%) but no book offers draw lines,
# so 3-way treatment blocks ALL MMA processing with zero benefit.
THREE_WAY_SPORTS = {"soccer", "boxing"}

# Sports where draws are possible but unpriced — flag in risk notes
UNPRICED_DRAW_SPORTS = {"mma"}

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
    "new jersey devils", "winnipeg jets", "vegas golden knights",
    "florida panthers", "new york rangers", "montreal canadiens",
    "los angeles kings", "st louis blues"}
_soccer_teams = {"liverpool", "manchester city", "manchester united", "arsenal",
    "chelsea", "tottenham", "aston villa", "nottingham forest", "fulham", "brentford",
    "brighton", "crystal palace", "wolverhampton", "everton", "west ham", "bournemouth",
    "leicester city", "southampton", "ipswich town"}
for t in _nba_teams: TEAM_TO_SPORT[t] = "nba"
for t in _nfl_teams: TEAM_TO_SPORT[t] = "nfl"
for t in _mlb_teams: TEAM_TO_SPORT[t] = "mlb"
for t in _nhl_teams: TEAM_TO_SPORT[t] = "nhl"
for t in _soccer_teams: TEAM_TO_SPORT[t] = "soccer"

def _event_date_bucket(commence_time_str):
    """Return a date bucket string from a commence_time ISO string.

    Books sometimes list the same event with slightly different times (e.g.
    7:00 PM vs 7:30 PM). We bucket by ISO date so those merge.  But events
    months apart (like TBD placeholder dates) stay separate.

    Returns '' if no valid date could be parsed (entries will still group
    by team names alone, same as before).
    """
    if not commence_time_str:
        return ""
    try:
        dt = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


# Common placeholder dates used by books when the real date is TBD
_PLACEHOLDER_DATES = {"12-31", "01-01", "12-30"}


def _is_placeholder_date(date_bucket):
    """Return True if this date bucket looks like a TBD placeholder."""
    if not date_bucket:
        return False
    return date_bucket[5:] in _PLACEHOLDER_DATES


def _make_event_key(away, home, commence_time_str=""):
    """Build an event key that includes a date bucket.

    Entries with the same teams on the same calendar day merge.
    Entries months apart stay separate (prevents TBD date pollution).
    Placeholder dates (Dec 31, Jan 1) are excluded from the key so they
    don't form their own isolated group — instead they get dropped during
    date-conflict detection downstream.
    """
    bucket = _event_date_bucket(commence_time_str)
    if _is_placeholder_date(bucket):
        bucket = ""  # don't include placeholder in key — let conflict detection handle it
    base = f"{away}@{home}"
    return f"{base}|{bucket}" if bucket else base


def _display_event_key(event_key):
    """Strip date bucket from event key for display purposes.
    'Ciryl Gane@Tom Aspinall|2026-06-28' → 'Ciryl Gane @ Tom Aspinall'
    """
    base = event_key.split("|")[0] if "|" in event_key else event_key
    return base.replace("@", " @ ")


def _sport_display_from_entry(entry):
    """Return a human-friendly sport label from a sportsbook entry."""
    sport = entry.get("sport", "").replace("_", " ").lower()
    cat = entry.get("_sport_category", "")
    if cat == "nba" or "nba" in sport or "basketball" in sport:
        return "NBA"
    if cat == "nfl" or "nfl" in sport or "football" in sport:
        return "NFL"
    if cat == "mlb" or "mlb" in sport or "baseball" in sport:
        return "MLB"
    if cat == "nhl" or "nhl" in sport or "hockey" in sport:
        return "NHL"
    if cat == "mma" or "mma" in sport or "mixed martial" in sport:
        return "MMA"
    if cat == "boxing" or "boxing" in sport:
        return "Boxing"
    if cat == "soccer" or "soccer" in sport or "mls" in sport or "epl" in sport:
        return "Soccer"
    return sport[:10].title() if sport else "Sports"


def _detect_sport_from_keywords(text):
    """Detect sport category from keywords only (no team-name fallback)."""
    text_lower = text.lower()
    for category, keywords in SPORT_CATEGORY_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return category
    return None


def _detect_sport_category(text):
    """Detect sport category from text keywords or team names.

    Uses keyword matching first (reliable), then falls back to majority-vote
    across extracted team names to avoid single-alias misclassification
    (e.g. 'Denver' alone defaulting to NBA when the context is NFL).
    """
    kw_sport = _detect_sport_from_keywords(text)
    if kw_sport:
        return kw_sport
    # Fall back to team name detection with majority vote
    teams = extract_teams_from_text(text)
    if not teams:
        return None
    sport_votes = {}
    for team in teams:
        s = TEAM_TO_SPORT.get(team)
        if s:
            sport_votes[s] = sport_votes.get(s, 0) + 1
    if not sport_votes:
        return None
    return max(sport_votes, key=sport_votes.get)

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
    "baseball", "hockey", "mma", "ufc", "tennis", "boxing",
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
    cached = get_cached(db, cache_key, ttl=POLYMARKET_CACHE_TTL)
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

            # Detect sport from keywords first, then extract sport-filtered teams
            sport_cat = _detect_sport_category(question)
            teams = extract_teams_from_text(question, sport_category=sport_cat)

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
                "teams": teams,
                "_tokens": set(normalize_name(question + " " + (m.get("description", "") or "")).split()),
                "_sport_category": sport_cat,
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

# Series ticker → URL slug (from Kalshi series titles)
KALSHI_SERIES_SLUG = {
    "KXNBAGAME": "professional-basketball-game",
    "KXNBASPREAD": "pro-basketball-spread",
    "KXNBATOTAL": "pro-basketball-total-points",
    "KXNBAPTS": "pro-basketball-player-points",
    "KXNBA1HTOTAL": "nba-1st-half-total-points",
    "KXNFLGAME": "professional-football-game",
    "KXNFLSPREAD": "pro-football-spread",
    "KXNFLTOTAL": "pro-football-total-points",
    "KXMLBGAME": "professional-baseball-game",
    "KXMLBSPREAD": "pro-baseball-spread",
    "KXNHLGAME": "nhl-game",
    "KXNHLSPREAD": "nhl-spread",
    "KXUFCFIGHT": "ufc-fight",
    "KXMMAGAME": "mma-fight",
    "KXEPLGAME": "english-premier-league-game",
    "KXEPLTOTAL": "english-premier-league-total-goals",
    "KXMLSGAME": "major-league-soccer-game",
}


def _kalshi_build_url(ticker, series_ticker, event_ticker=""):
    """Build correct Kalshi market URL: /markets/{series}/{slug}/{event_ticker}
    Uses event_ticker (the event page showing all outcomes) rather than
    individual market ticker (which has outcome suffixes like -WOL, -TIE)."""
    # Prefer event_ticker — it's the page Kalshi shows for the event
    page_id = (event_ticker or ticker or "").lower()
    if not page_id:
        return ""
    slug = KALSHI_SERIES_SLUG.get(series_ticker, "")
    s = series_ticker.lower() if series_ticker else ""
    if slug and s:
        return f"https://kalshi.com/markets/{s}/{slug}/{page_id}"
    elif s:
        return f"https://kalshi.com/markets/{s}/{page_id}"
    return f"https://kalshi.com/markets/{page_id}"


def _kalshi_parse_price(m):
    """
    Parse Kalshi market prices (in cents 0-100) to probabilities (0-1).
    Uses last_price (what Kalshi UI shows) as primary, yes_ask (cost to buy)
    as fallback. Avoids yes_bid which can be far below the actual market price
    due to wide bid/ask spreads.
    """
    # Primary: last traded price (matches what Kalshi UI displays)
    yes_price = m.get("last_price", 0) or 0

    # Fallback: yes_ask (the actual cost to buy YES right now)
    if yes_price == 0:
        yes_price = m.get("yes_ask", 0) or m.get("yes_bid", 0) or 0

    yes_prob = yes_price / 100.0 if yes_price > 1 else float(yes_price)

    # NO price is complement (binary market: YES + NO = 100 cents)
    no_prob = 1.0 - yes_prob if yes_prob > 0 else 0
    return yes_prob, no_prob


def fetch_kalshi_sports(db=None):
    """Fetch sports markets from Kalshi via series → markets API."""
    if db is None:
        db = get_db()
    cache_key = "kalshi_sports"
    cached = get_cached(db, cache_key, ttl=KALSHI_CACHE_TTL)
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
            yes_sub = m.get("yes_sub_title", "") or ""

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
                "teams": extract_teams_from_text(title, sport_category=category),
                "_tokens": set(normalize_name(title + " " + no_sub).split()),
                "_sport_category": category,
                "_market_subtype": SERIES_MARKET_SUBTYPE.get(series_ticker, "unknown"),
                "_floor_strike": float(floor_strike) if floor_strike is not None else None,
                "_no_sub_title": no_sub,
                "_yes_sub_title": yes_sub,
                "url": _kalshi_build_url(m.get("ticker", ""), series_ticker, m.get("event_ticker", "")),
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
    cached = get_cached(db, cache_key, ttl=SPORTSBOOK_CACHE_TTL)
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

    # No bookmakers filter — the API returns ALL available bookmakers at no
    # extra credit cost.  More books = better consensus for the +EV engine.
    all_events = []
    api_errors = []
    api_quota = {"remaining": None, "used": None}  # from response headers

    def _fetch_sport(sport, is_prop=False):
        """Fetch a single sport from The Odds API. Returns (events, headers).
        Retries once on transient errors (401/403/timeout)."""
        if is_prop:
            markets_param = "player_points,player_rebounds,player_assists,player_threes"
        else:
            markets_param = "h2h,spreads,totals"
        url = (f"https://api.the-odds-api.com/v4/sports/{sport}/odds?"
               f"apiKey={api_key}&regions=us&markets={markets_param}"
               f"&oddsFormat=american")

        for attempt in range(2):  # 1 retry on transient errors
            data, headers = fetch_json_with_headers(url)
            if isinstance(data, dict) and "_error" in data:
                err = data["_error"]
                if "429" in err or "quota" in err.lower() or "limit" in err.lower():
                    raise RuntimeError("QUOTA_EXCEEDED")
                if ("401" in err or "403" in err) and attempt == 0:
                    time.sleep(0.5)  # retry once — often transient
                    continue
                if "401" in err or "403" in err:
                    raise RuntimeError("INVALID_KEY")
                if attempt == 0:
                    time.sleep(0.3)
                    continue
                raise RuntimeError(err)
            break

        events = []
        if isinstance(data, list):
            for event in data:
                event["_sport_key"] = sport
                if is_prop:
                    event["_is_prop"] = True
                events.append(event)
        return events, headers

    # Stagger requests: 3 workers max to avoid per-second rate limits.
    # Even on higher tiers, 8 simultaneous hits can trigger throttling.
    fetch_tasks = [(sport, False) for sport in sports_to_fetch]
    fetch_tasks.append(("basketball_nba", True))  # NBA player props

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(_fetch_sport, sport, is_prop)
                   for sport, is_prop in fetch_tasks]
        for future in as_completed(futures):
            try:
                events, headers = future.result(timeout=12)
                all_events.extend(events)
                # Track API quota from response headers
                remaining = headers.get("x-requests-remaining") or headers.get("X-Requests-Remaining")
                used = headers.get("x-requests-used") or headers.get("X-Requests-Used")
                if remaining is not None:
                    api_quota["remaining"] = int(remaining)
                if used is not None:
                    api_quota["used"] = int(used)
            except RuntimeError as e:
                api_errors.append(str(e))
            except Exception:
                continue

    # If all requests failed, try stale cache before giving up
    if not all_events and api_errors:
        # Always try stale cache first — even for "invalid key" which can be transient
        stale = get_stale_cached(db, cache_key)
        if stale is not None:
            return stale
        # No stale cache — propagate the error
        if any(e == "INVALID_KEY" for e in api_errors):
            raise RuntimeError("INVALID_KEY: Odds API key is invalid or expired. Update it in Settings.")
        if any(e == "QUOTA_EXCEEDED" for e in api_errors):
            raise RuntimeError("QUOTA_EXCEEDED: Odds API usage limit reached. Check your plan at https://the-odds-api.com")

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
                bk_last_update = bookmaker.get("last_update", "")

                for market in bookmaker.get("markets", []):
                    market_key = market.get("key", "")
                    # Market-level last_update is more granular than bookmaker-level
                    mkt_last_update = market.get("last_update", "") or bk_last_update

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
                            "teams": extract_teams_from_text(home + " " + away, sport_category=SPORT_KEY_TO_CATEGORY.get(sport_key)),
                            "_tokens": set(normalize_name(away + " " + home + " " + name).split()),
                            "_sport_category": SPORT_KEY_TO_CATEGORY.get(sport_key, "other"),
                            "event_name": f"{away} @ {home}",
                            "last_update": mkt_last_update,
                        }
                        results.append(entry)
        except Exception:
            continue

    # Save API quota info for frontend display
    if api_quota["remaining"] is not None or api_quota["used"] is not None:
        db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                   ["_odds_api_remaining", str(api_quota["remaining"] or 0)])
        db.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                   ["_odds_api_used", str(api_quota["used"] or 0)])
        db.commit()

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
    pred_date = _parse_event_date(pred_market.get("end_date", ""))

    matches = []

    for sb in sportsbook_entries:
        score = 0
        sb_teams = sb.get("teams", [])

        # Date check — same teams can play on different dates
        sb_date = _parse_event_date(sb.get("commence_time", ""))
        if not _dates_compatible(pred_date, sb_date, max_days=2):
            continue  # wrong date — skip

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
    # Kalshi: dynamic fee = 0.07 * price * (1-price) per contract
    #   → as fraction of winnings: 0.07 * price
    KALSHI_FEE_COEFF = 0.07  # fee_on_winnings = 0.07 * contract_price
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

        is_kalshi = source != "polymarket"

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

        # Skip 3-way h2h sports (win/draw/lose) — can't arb against binary
        if pred_subtype == "h2h" and pred.get("_sport_category") in THREE_WAY_SPORTS:
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
            elif pred_subtype == "h2h":
                # For h2h, use team name matching instead of price proximity.
                # Price proximity fails near 50/50 and creates phantom arbs
                # where both legs bet the same outcome.
                #
                # Prefer _yes_sub_title (the YES team) when available.
                # Fall back to _no_sub_title (the NO team) with inverted logic.
                yes_team_label = pred.get("_yes_sub_title", "").strip()
                no_team_label = pred.get("_no_sub_title", "").strip()
                sb_outcome_name = sb.get("outcome_name", "").strip()
                if yes_team_label and sb_outcome_name:
                    # Direct YES-team comparison
                    yes_tokens = set(normalize_name(yes_team_label).split())
                    sb_tokens = set(normalize_name(sb_outcome_name).split())
                    overlap = yes_tokens & sb_tokens
                    overlap -= {"fc", "city", "united", "the", "de", "la"}
                    sb_same_as_yes = len(overlap) > 0
                elif no_team_label and sb_outcome_name:
                    # Use NO-team label (inverted): if sb matches the NO team,
                    # then sb is on the NO side → sb_same_as_yes = False
                    no_tokens = set(normalize_name(no_team_label).split())
                    sb_tokens = set(normalize_name(sb_outcome_name).split())
                    overlap = no_tokens & sb_tokens
                    overlap -= {"fc", "city", "united", "the", "de", "la"}
                    sb_same_as_yes = len(overlap) == 0
                else:
                    # No team label — fall back to price proximity
                    diff_yes = abs(yes_price - sb_prob)
                    diff_no = abs(no_price - sb_prob)
                    sb_same_as_yes = (diff_yes <= diff_no)
            else:
                # spreads and other types: price proximity heuristic
                diff_yes = abs(yes_price - sb_prob)
                diff_no = abs(no_price - sb_prob)
                sb_same_as_yes = (diff_yes <= diff_no)

            if sb_same_as_yes:
                # sb same side as pred YES → arb: pred NO + sb
                pred_price = no_price
                pred_fee = (KALSHI_FEE_COEFF * pred_price) if is_kalshi else POLYMARKET_FEE
                arb = compute_arb_binary(pred_price, sb_prob, pred_fee, SPORTSBOOK_FEE)
                pred_side_raw = "No"
            else:
                # sb opposite side from pred YES → arb: pred YES + sb
                pred_price = yes_price
                pred_fee = (KALSHI_FEE_COEFF * pred_price) if is_kalshi else POLYMARKET_FEE
                arb = compute_arb_binary(pred_price, sb_prob, pred_fee, SPORTSBOOK_FEE)
                pred_side_raw = "Yes"

            # Build descriptive sportsbook side label
            sb_outcome = sb.get("outcome_name", "")
            sb_point = sb.get("outcome_point")
            if sb_point is not None and sb_outcome.lower() in ("over", "under"):
                sb_side = f"{sb_outcome} {sb_point}"
            elif sb_point is not None and sb_outcome.lower() not in ("over", "under"):
                # Spreads: team name + point line (e.g., "Thunder -5.5")
                sign = "+" if sb_point > 0 else ""
                sb_side = f"{sb_outcome} {sign}{sb_point}"
            else:
                sb_side = sb_outcome

            # Translate Yes/No into meaningful labels
            pred_line = pred.get("_floor_strike")
            yes_sub = pred.get("_yes_sub_title", "")
            no_sub = pred.get("_no_sub_title", "")
            if pred_subtype == "totals" and pred_line is not None:
                pred_side = f"Over {pred_line}" if pred_side_raw == "Yes" else f"Under {pred_line}"
            elif pred_subtype == "h2h" and (yes_sub or no_sub):
                # Use yes_sub_title / no_sub_title for human-readable side labels
                if pred_side_raw == "Yes" and yes_sub:
                    pred_side = yes_sub.strip()
                elif pred_side_raw == "No" and no_sub:
                    pred_side = no_sub.strip()
                elif pred_side_raw == "Yes" and no_sub:
                    # YES = the team NOT in no_sub_title
                    pred_teams_list = pred.get("teams", [])
                    no_team = no_sub.strip().lower()
                    other = [t for t in pred_teams_list if no_team not in t]
                    pred_side = other[0].title() if other else "Yes"
                elif pred_side_raw == "No" and yes_sub:
                    # NO = the team NOT in yes_sub_title
                    pred_teams_list = pred.get("teams", [])
                    yes_team = yes_sub.strip().lower()
                    other = [t for t in pred_teams_list if yes_team not in t]
                    pred_side = other[0].title() if other else "No"
                else:
                    pred_side = pred_side_raw
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
            sport_display = _sport_display_from_entry(sb)

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
                "type": "arb",
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
    KALSHI_FEE_COEFF = 0.07  # fee_on_winnings = 0.07 * contract_price
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

        pm_date = _parse_event_date(pm.get("end_date", ""))

        for km in candidates:
            km_question = km.get("question", "").lower()
            km_teams = km.get("teams", [])
            km_prices = km.get("prices", [])

            if len(km_prices) < 2:
                continue
            if km_prices[0] + km_prices[1] < 0.90:
                continue  # illiquid — wide bid-ask creates phantom arbs

            # Date check — same teams can play on different dates
            km_date = _parse_event_date(km.get("end_date", ""))
            if not _dates_compatible(pm_date, km_date, max_days=2):
                continue  # wrong date — skip

            # Sport category check — prevent cross-sport matching
            pm_sport = pm.get("_sport_category")
            km_sport = km.get("_sport_category")
            if pm_sport and km_sport and pm_sport != km_sport:
                continue  # different sports — skip

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

            # For game markets, require both teams to match
            # Check by market subtype (h2h) AND keywords — EPL titles often lack "win"
            is_game = (pm.get("_market_subtype") == "h2h"
                       or km.get("_market_subtype") == "h2h"
                       or "winner" in pm_question or "win" in pm_question
                       or "winner" in km_question or "win" in km_question
                       or " vs " in pm_question or " vs " in km_question
                       or " at " in pm_question or " at " in km_question)
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
            elif pm_subtype == "h2h":
                # For h2h, use team name matching to determine side alignment.
                # Price proximity fails near 50/50 and can put both legs on the
                # same outcome, creating phantom arbs.
                #
                # Prefer _yes_sub_title (the YES team) when available.
                # Fall back to _no_sub_title (the NO team) with inverted logic.
                km_yes_team = km.get("_yes_sub_title", "").strip()
                km_no_team = km.get("_no_sub_title", "").strip()
                pm_text_norm = normalize_name(pm.get("question", ""))
                pm_text_tokens = set(pm_text_norm.split())
                generic = {"fc", "city", "united", "the", "de", "la"}
                if km_yes_team and pm_teams:
                    km_yes_tokens = set(normalize_name(km_yes_team).split()) - generic
                    # aligned = PM YES and KM YES refer to the same team
                    aligned = bool(km_yes_tokens & pm_text_tokens)
                elif km_no_team and pm_teams:
                    km_no_tokens = set(normalize_name(km_no_team).split()) - generic
                    # If KM NO team appears in PM question text, PM YES is
                    # likely about that team too → but KM NO is the OTHER side,
                    # so they are NOT aligned (PM YES ≈ KM NO)
                    aligned = not bool(km_no_tokens & pm_text_tokens)
                else:
                    # No team label — fall back to price proximity
                    diff_aligned = abs(pm_yes - km_yes)
                    diff_misaligned = abs(pm_yes - km_no)
                    aligned = (diff_aligned <= diff_misaligned)
            else:
                # Spreads and other types: price proximity heuristic
                diff_aligned = abs(pm_yes - km_yes)
                diff_misaligned = abs(pm_yes - km_no)
                aligned = (diff_aligned <= diff_misaligned)

            if aligned:
                # Aligned: PM YES ≈ KM YES → arb: PM YES + KM NO
                pa_price = pm_yes
                pb_price = km_no
                kalshi_fee = KALSHI_FEE_COEFF * pb_price
                arb = compute_arb_binary(pa_price, pb_price, POLYMARKET_FEE, kalshi_fee)
                pa_side = pm.get("outcomes", ["Yes"])[0]
                pb_side = km.get("outcomes", ["", "No"])[1]
            else:
                # Misaligned: PM YES ≈ KM NO → arb: PM YES + KM YES
                pa_price = pm_yes
                pb_price = km_yes
                kalshi_fee = KALSHI_FEE_COEFF * pb_price
                arb = compute_arb_binary(pa_price, pb_price, POLYMARKET_FEE, kalshi_fee)
                pa_side = pm.get("outcomes", ["Yes"])[0]
                pb_side = km.get("outcomes", ["Yes"])[0]

            if arb is None or arb["gross_arb_pct"] <= 0:
                continue
            if arb["gross_arb_pct"] > 15:
                continue  # >15% gross is certainly stale/non-executable pricing
            if arb["net_arb_pct"] < min_net_pct:
                continue

            stakes = compute_stake_allocation(pa_price, pb_price, 100)

            opp = {
                "id": hashlib.md5(f"cross-{pm.get('id','')}-{km.get('id','')}-{pa_side}".encode()).hexdigest()[:12],
                "type": "arb",
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
                    "fee_pct": round(kalshi_fee * 100, 2),
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


# ─── Devigging & +EV engine ──────────────────────────────────────────────────

# Sportsbooks considered "sharp" (lowest vig, sharpest lines) — used preferentially
SHARP_BOOKS = {"pinnacle", "lowvig", "novig"}

# Sharpness weights for weighted consensus (Hubáček 2019 — decorrelation)
BOOK_SHARPNESS = {
    "pinnacle": 1.0,
    "lowvig": 0.9,
    "novig": 0.9,
    "draftkings": 0.5,
    "fanduel": 0.5,
    "betmgm": 0.4,
    "betrivers": 0.4,
    "espnbet": 0.4,
    "fanatics": 0.4,
    "hardrock": 0.35,
    "betonline": 0.35,
    "mybookie": 0.3,
    "betus": 0.3,
    "ballybet": 0.3,
    "betparx": 0.3,
}
DEFAULT_BOOK_WEIGHT = 0.35

# Staleness decay: odds older than this (in seconds) get heavily downweighted.
# Full weight within 2 min, linear decay to 10% at 10 min, 10% floor after.
STALENESS_FULL_WEIGHT_SECS = 120     # 2 min — full weight
STALENESS_FLOOR_SECS = 600           # 10 min — minimum weight
STALENESS_FLOOR_WEIGHT = 0.10        # floor: 10% of base weight

def _staleness_factor(last_update_str, now):
    """Return a 0.1–1.0 factor based on how old the odds are."""
    if not last_update_str:
        return 0.5  # unknown age — half weight
    try:
        lu = datetime.fromisoformat(last_update_str.replace("Z", "+00:00"))
        age = (now - lu).total_seconds()
    except (ValueError, TypeError):
        return 0.5
    if age <= STALENESS_FULL_WEIGHT_SECS:
        return 1.0
    if age >= STALENESS_FLOOR_SECS:
        return STALENESS_FLOOR_WEIGHT
    # Linear decay between full and floor
    frac = (age - STALENESS_FULL_WEIGHT_SECS) / (STALENESS_FLOOR_SECS - STALENESS_FULL_WEIGHT_SECS)
    return 1.0 - frac * (1.0 - STALENESS_FLOOR_WEIGHT)


def _power_devig(probs):
    """
    Power method devigging — industry standard correction for favorite-longshot bias.
    Finds exponent k such that sum(p_i^k) = 1, then fair_prob_i = p_i^k.
    Uses bisection (no scipy needed). Falls back to multiplicative if any prob <= 0.
    """
    if len(probs) < 2:
        return list(probs)
    if any(p <= 0 for p in probs):
        # Fallback: multiplicative
        total = sum(probs)
        return [p / total for p in probs] if total > 0 else list(probs)

    lo, hi = 0.5, 20.0
    for _ in range(80):
        mid = (lo + hi) / 2.0
        s = sum(p ** mid for p in probs)
        if s > 1.0:
            lo = mid
        else:
            hi = mid
        if abs(s - 1.0) < 1e-12:
            break

    k = (lo + hi) / 2.0
    return [p ** k for p in probs]


def _shin_devig(probs):
    """
    Shin method devigging (Shin 1993, Wheatcroft 2024).
    Models bookmaker vig as arising from an insider-trading fraction z.
    Better calibrated for longshot markets.

    Solves: Σ sqrt(z² + 4(1-z)(pᵢ/S)) = 2  for z ∈ (0, 1)
    Then: fair_i = (sqrt(z² + 4(1-z)(pᵢ/S)) - z) / (2(1-z))
    """
    import math

    if len(probs) < 2:
        return list(probs)
    if any(p <= 0 for p in probs):
        total = sum(probs)
        return [p / total for p in probs] if total > 0 else list(probs)

    S = sum(probs)
    if S <= 1.0:
        # No overround — already fair
        return list(probs)

    # Bisect for insider fraction z
    lo, hi = 0.0, 1.0
    for _ in range(100):
        z = (lo + hi) / 2.0
        one_minus_z = 1.0 - z
        if one_minus_z <= 0:
            hi = z
            continue
        total = sum(math.sqrt(z * z + 4.0 * one_minus_z * (p / S)) for p in probs)
        if total > 2.0:
            lo = z
        else:
            hi = z
        if abs(total - 2.0) < 1e-12:
            break

    z = (lo + hi) / 2.0
    one_minus_z = 1.0 - z
    if one_minus_z <= 0:
        # Fallback to multiplicative
        return [p / S for p in probs]

    fair = []
    for p in probs:
        disc = z * z + 4.0 * one_minus_z * (p / S)
        f = (math.sqrt(disc) - z) / (2.0 * one_minus_z)
        fair.append(max(0.0, f))

    # Renormalize (should be very close to 1.0 already)
    total = sum(fair)
    if total > 0 and abs(total - 1.0) > 1e-9:
        fair = [f / total for f in fair]

    return fair


def build_fair_odds_index(sportsbook_entries, devig_method="power"):
    """
    Build a fair-odds index using weighted multi-book consensus (Hubáček 2019).
    Devigs each bookmaker's line independently, then computes weighted average
    by sharpness weight. Returns richer structure with metadata.

    Returns dict keyed by (event_key, market_type) → {
        outcome_key → fair_prob,          # backward-compatible simple access
        "_meta": {
            outcome_key → {fair_prob, spread, stdev, n_books, source_books, overround}
        }
    }
    """
    devig_fn = _shin_devig if devig_method == "shin" else _power_devig

    # Group: (event_key, mtype) → { bookmaker → { outcome_key → implied_prob } }
    market_groups = defaultdict(lambda: defaultdict(lambda: {}))
    # Track last_update per (market_key, bookmaker) for staleness weighting
    book_last_update = {}
    # Also track which outcomes exist per market
    market_outcomes = defaultdict(set)

    now = datetime.now(timezone.utc)

    # Track commence dates per book for date-conflict detection
    book_commence_dates = {}  # (market_key, bk) → date_bucket

    for sb in sportsbook_entries:
        home = sb.get("home_team", "")
        away = sb.get("away_team", "")
        mtype = sb.get("market_type", "")
        outcome = sb.get("outcome_name", "")
        point = sb.get("outcome_point")
        prob = sb.get("implied_prob", 0)
        bk = sb.get("bookmaker", "")
        commence = sb.get("commence_time", "")

        if prob <= 0 or prob >= 1:
            continue

        event_key = _make_event_key(away, home, commence)
        outcome_key = f"{outcome}|{point}" if point is not None else outcome
        market_key = (event_key, mtype)

        market_groups[market_key][bk][outcome_key] = prob
        market_outcomes[market_key].add(outcome_key)

        # Track commence date per book for conflict detection
        date_bucket = _event_date_bucket(commence)
        if date_bucket:
            book_commence_dates[(market_key, bk)] = date_bucket

        # Track the most recent last_update for this (market, book)
        lu = sb.get("last_update", "")
        if lu:
            lu_key = (market_key, bk)
            if lu_key not in book_last_update:
                book_last_update[lu_key] = lu

    fair_index = {}

    for market_key, book_lines in market_groups.items():
        all_okeys = sorted(market_outcomes[market_key])
        if len(all_okeys) < 2:
            continue

        # Drop books with conflicting or placeholder dates before devigging.
        # If books disagree on the event date by >7 days, keep only the majority
        # date cluster and discard outliers (prevents TBD date pollution).
        date_counts = defaultdict(list)
        for bk in book_lines:
            d = book_commence_dates.get((market_key, bk), "")
            date_counts[d].append(bk)
        # Find the largest date cluster (ignoring empty/unknown dates)
        real_dates = {d: bks for d, bks in date_counts.items() if d and not _is_placeholder_date(d)}
        if real_dates:
            majority_date = max(real_dates, key=lambda d: len(real_dates[d]))
            try:
                majority_dt = datetime.strptime(majority_date, "%Y-%m-%d")
            except ValueError:
                majority_dt = None
            if majority_dt:
                excluded_books = set()
                for d, bks in date_counts.items():
                    if not d or _is_placeholder_date(d):
                        excluded_books.update(bks)
                        continue
                    try:
                        d_dt = datetime.strptime(d, "%Y-%m-%d")
                        if abs((d_dt - majority_dt).days) > 7:
                            excluded_books.update(bks)
                    except ValueError:
                        pass
                if excluded_books:
                    book_lines = {bk: v for bk, v in book_lines.items() if bk not in excluded_books}
                    if not book_lines:
                        continue

        # Devig each book independently, then compute weighted average
        # Only use books that have prices for ALL outcomes in this market
        devigged_by_book = {}
        for bk, omap in book_lines.items():
            if not all(ok in omap for ok in all_okeys):
                continue  # incomplete book — skip
            raw = [omap[ok] for ok in all_okeys]
            overround = sum(raw)
            if overround <= 0:
                continue
            fair = devig_fn(raw)
            devigged_by_book[bk] = {
                "fair": dict(zip(all_okeys, fair)),
                "overround": overround,
            }

        if not devigged_by_book:
            continue

        # Weighted average across books
        fair_probs = {}
        meta = {}
        for ok in all_okeys:
            weighted_sum = 0.0
            weight_sum = 0.0
            values = []
            source_books = []

            for bk, bdata in devigged_by_book.items():
                fp = bdata["fair"].get(ok, 0)
                if fp <= 0:
                    continue
                base_w = BOOK_SHARPNESS.get(bk, DEFAULT_BOOK_WEIGHT)
                # Downweight stale odds — if a book hasn't updated in 10 min,
                # its weight drops to 10% of base.
                lu_str = book_last_update.get((market_key, bk), "")
                sf = _staleness_factor(lu_str, now)
                w = base_w * sf
                weighted_sum += w * fp
                weight_sum += w
                values.append(fp)
                source_books.append(bk)

            if weight_sum > 0 and values:
                fair_p = weighted_sum / weight_sum
                n_books = len(values)
                spread = max(values) - min(values) if n_books > 1 else 0
                mean = sum(values) / n_books
                stdev = (sum((v - mean) ** 2 for v in values) / n_books) ** 0.5 if n_books > 1 else 0
                avg_overround = sum(d["overround"] for d in devigged_by_book.values()) / len(devigged_by_book)

                fair_probs[ok] = fair_p
                meta[ok] = {
                    "fair_prob": fair_p,
                    "spread": round(spread, 4),
                    "stdev": round(stdev, 4),
                    "n_books": n_books,
                    "source_books": source_books,
                    "overround": round(avg_overround, 4),
                }

        if not fair_probs:
            continue

        # Renormalize so fair probs sum to 1.0
        total = sum(fair_probs.values())
        if total > 0 and abs(total - 1.0) > 1e-9:
            for ok in fair_probs:
                fair_probs[ok] /= total
                if ok in meta:
                    meta[ok]["fair_prob"] = fair_probs[ok]

        fair_probs["_meta"] = meta
        fair_index[market_key] = fair_probs

    return fair_index


def compute_arb_3way(prob_a, prob_b, prob_c, bankroll=100):
    """
    Compute stake allocation for a 3-way arbitrage (e.g. soccer h2h).
    prob_a/b/c are implied probabilities from the best price on each outcome.
    Returns dict with stakes, profit, and roi — or None if no arb exists.
    """
    cost = prob_a + prob_b + prob_c
    if cost >= 1.0:
        return None  # no arb

    # Stakes proportional to probability so each outcome pays the same total
    target_payout = bankroll
    stake_a = round(target_payout * prob_a, 2)
    stake_b = round(target_payout * prob_b, 2)
    stake_c = round(target_payout * prob_c, 2)
    total_staked = round(stake_a + stake_b + stake_c, 2)
    profit = round(target_payout - total_staked, 2)
    roi = round((profit / total_staked) * 100, 3) if total_staked > 0 else 0

    return {
        "stake_a": stake_a,
        "stake_b": stake_b,
        "stake_c": stake_c,
        "total_staked": total_staked,
        "profit": profit,
        "roi": roi,
    }


def compute_adaptive_kelly(fair_prob, b, ev_pct, match_confidence=1.0,
                           n_books=1, is_live=False, alpha_base=0.5):
    """
    Adaptive fractional Kelly (Uhrín/Hubáček 2021).
    Scales Kelly fraction by confidence: f_adaptive = f_full × alpha_base × C
    where C = min(1, C_match × C_books × C_edge × C_time).

    Returns dict with kelly_adaptive, kelly_confidence, and component breakdown.
    """
    import math

    if b <= 0 or fair_prob <= 0 or fair_prob >= 1:
        return {"kelly_adaptive": 0, "kelly_confidence": 0, "full_kelly": 0}

    q = 1.0 - fair_prob
    full_kelly = max(0, (b * fair_prob - q) / b)

    # Confidence components
    c_match = max(0.1, min(1.0, match_confidence))
    c_books = min(1.0, n_books / 4.0)
    c_edge = 1.0 if ev_pct <= 15 else max(0.2, 1.0 - (ev_pct - 15.0) / 30.0)
    c_time = 0.7 if is_live else 1.0

    C = min(1.0, c_match * c_books * c_edge * c_time)
    kelly_adaptive = full_kelly * alpha_base * C

    return {
        "kelly_adaptive": round(kelly_adaptive, 6),
        "kelly_confidence": round(C, 4),
        "full_kelly": round(full_kelly, 6),
        "c_match": round(c_match, 4),
        "c_books": round(c_books, 4),
        "c_edge": round(c_edge, 4),
        "c_time": round(c_time, 4),
    }


def compute_edge_quality_score(fair_prob, b, kelly_f, confidence, liquidity=0):
    """
    Edge Quality Score (Hubáček 2019 + Uhrín 2021).
    G = p×ln(1+b×f) + (1−p)×ln(1−f)  (Kelly growth rate per bet)
    EQS = G × C × liquidity_factor

    Returns dict with eqs, growth_rate, and bets_to_double.
    """
    import math

    if b <= 0 or fair_prob <= 0 or fair_prob >= 1 or kelly_f <= 0:
        return {"eqs": 0, "growth_rate": 0, "bets_to_double": 0}

    p = fair_prob
    q = 1.0 - p
    f = kelly_f

    # Clamp f to avoid log(0) or log(negative)
    f = min(f, 0.99)
    bf = b * f
    if bf <= -1 or f >= 1:
        return {"eqs": 0, "growth_rate": 0, "bets_to_double": 0}

    growth_rate = p * math.log(1.0 + bf) + q * math.log(1.0 - f)

    # Liquidity factor: log-scaled, maxes at ~$10,000
    liq_factor = min(1.0, math.log10(max(liquidity, 1)) / 4.0) if liquidity > 0 else 0.5

    eqs = growth_rate * confidence * liq_factor
    bets_to_double = math.log(2) / growth_rate if growth_rate > 0 else 0

    return {
        "eqs": round(eqs, 6),
        "growth_rate": round(growth_rate, 6),
        "bets_to_double": round(bets_to_double, 1),
    }


def compute_risk_score(ev_pct, n_books=1, consensus_spread=0, match_confidence=1.0,
                       is_live=False):
    """
    Multi-factor risk score (0–100), replacing simple low/medium/high.
    Lower = less risky. Factors: edge size, book count, disagreement, confidence, live status.
    """
    score = 30  # base
    if ev_pct > 15:
        score += min(20, (ev_pct - 15) * 2)
    if ev_pct > 25:
        score += 15
    score -= min(15, n_books * 3)
    score += min(15, consensus_spread * 100)
    if match_confidence < 0.8:
        score += 10
    if is_live:
        score += 10
    if n_books <= 1:
        score += 10
    return max(0, min(100, round(score)))


def risk_score_label(score):
    if score <= 25:
        return "low"
    elif score <= 50:
        return "medium"
    elif score <= 75:
        return "high"
    return "very_high"


def compute_ev(price, fair_prob, fee_rate=0.0):
    """
    Compute expected value of a bet.
    price: implied probability (cost) of the bet
    fair_prob: estimated true probability of winning
    fee_rate: fee as fraction of winnings
    Returns EV as percentage (e.g., 5.0 means +5% EV).
    """
    if price <= 0 or price >= 1 or fair_prob <= 0:
        return None
    payout = 1.0 / price
    effective_payout = payout - (payout - 1.0) * fee_rate
    ev = effective_payout * fair_prob - 1.0
    return ev * 100


def find_ev_opportunities(prediction_markets, sportsbook_entries, fair_index, min_ev_pct=1.0):
    """
    Find +EV opportunities where prediction market prices beat fair odds.
    Reuses the existing matching engine to pair prediction markets with sportsbook events.
    """
    opportunities = []
    KALSHI_FEE_COEFF = 0.07
    POLYMARKET_FEE = 0.02

    # Build team index
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

        yes_price = prices[0]
        no_price = prices[1]
        if yes_price <= 0 or yes_price >= 1:
            continue
        if yes_price + no_price < 0.90:
            continue  # illiquid

        is_kalshi = source != "polymarket"

        pred_teams = pred.get("teams", [])
        if not pred_teams:
            continue

        # Get candidate sportsbook entries by team
        candidate_indices = set()
        for team in pred_teams:
            candidate_indices.update(team_index.get(team, set()))
        if not candidate_indices:
            continue
        candidates = [sportsbook_entries[i] for i in candidate_indices]

        # Filter by sport
        pred_sport = pred.get("_sport_category")
        if pred_sport:
            candidates = [c for c in candidates if not c.get("_sport_category") or c["_sport_category"] == pred_sport]

        # Filter by market type
        pred_subtype = pred.get("_market_subtype", "unknown")
        allowed_sb_types = _MARKET_TYPE_COMPAT.get(pred_subtype, _MARKET_TYPE_COMPAT["unknown"])
        if not allowed_sb_types:
            continue
        candidates = [c for c in candidates if c.get("market_type") in allowed_sb_types]

        # Note: 3-way sports (soccer/boxing/mma) have draw outcomes but EV detection
        # still works because Kalshi's "Will X win?" maps to a single sportsbook outcome.
        # Only arb detection needs the 3-way skip (can't cover 3 sides with 2 bets).

        # For totals/spreads, require matching point line
        if pred_subtype in ("totals", "spreads", "player_props"):
            pred_line = pred.get("_floor_strike") or _extract_point_line(pred.get("question", ""))
            if pred_line is not None:
                candidates = [c for c in candidates
                              if c.get("outcome_point") is not None
                              and abs(c["outcome_point"] - pred_line) < 0.01]
            else:
                continue

        if not candidates:
            continue

        # Match prediction to sportsbook
        matches = try_match_prediction_to_sportsbook(pred, candidates)
        if not matches:
            continue

        best_match = matches[0]
        sb = best_match["sportsbook_entry"]
        confidence = best_match["confidence"]

        # Find the fair odds for this event/market
        home = sb.get("home_team", "")
        away = sb.get("away_team", "")
        mtype = sb.get("market_type", "")
        commence = sb.get("commence_time", "")
        event_key = _make_event_key(away, home, commence)

        fair_probs = fair_index.get((event_key, mtype))
        if not fair_probs:
            # Fallback: try without date bucket (covers cases where the
            # fair_index entry has a different date format)
            event_key_nodate = f"{away}@{home}"
            fair_probs = fair_index.get((event_key_nodate, mtype))
        if not fair_probs:
            continue

        # Determine side alignment (same logic as arb engine)
        sb_prob = sb.get("implied_prob", 0)
        if sb_prob <= 0 or sb_prob >= 1:
            continue

        sb_market_type = sb.get("market_type", "")

        if pred_subtype in ("totals", "player_props") and sb_market_type in ("totals", "player_points", "player_rebounds", "player_assists", "player_threes"):
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
                diff_yes = abs(yes_price - sb_prob)
                diff_no = abs(no_price - sb_prob)
                sb_same_as_yes = (diff_yes <= diff_no)
        elif pred_subtype == "h2h":
            # Prefer _yes_sub_title (YES team), fall back to _no_sub_title (NO team)
            yes_team_label = pred.get("_yes_sub_title", "").strip()
            no_team_label = pred.get("_no_sub_title", "").strip()
            sb_outcome_name = sb.get("outcome_name", "").strip()
            if yes_team_label and sb_outcome_name:
                yes_tokens = set(normalize_name(yes_team_label).split())
                sb_tokens = set(normalize_name(sb_outcome_name).split())
                overlap = yes_tokens & sb_tokens
                overlap -= {"fc", "city", "united", "the", "de", "la"}
                sb_same_as_yes = len(overlap) > 0
            elif no_team_label and sb_outcome_name:
                no_tokens = set(normalize_name(no_team_label).split())
                sb_tokens = set(normalize_name(sb_outcome_name).split())
                overlap = no_tokens & sb_tokens
                overlap -= {"fc", "city", "united", "the", "de", "la"}
                # sb matches NO team → sb is NOT same as YES
                sb_same_as_yes = len(overlap) == 0
            else:
                diff_yes = abs(yes_price - sb_prob)
                diff_no = abs(no_price - sb_prob)
                sb_same_as_yes = (diff_yes <= diff_no)
        else:
            diff_yes = abs(yes_price - sb_prob)
            diff_no = abs(no_price - sb_prob)
            sb_same_as_yes = (diff_yes <= diff_no)

        # Determine which fair prob to compare against
        sb_outcome = sb.get("outcome_name", "")
        sb_point = sb.get("outcome_point")
        outcome_key = f"{sb_outcome}|{sb_point}" if sb_point is not None else sb_outcome

        # For the prediction side, we want fair_prob of the OPPOSING outcome
        # If sb is same as YES, we're betting pred NO → fair_prob of losing side for sb
        if sb_same_as_yes:
            pred_price = no_price
            pred_side_raw = "No"
            # We need fair prob of the "other" outcome (NOT the sb outcome)
            other_keys = [k for k in fair_probs if k != outcome_key and not k.startswith("_")]
            if not other_keys:
                continue
            fair_prob = fair_probs.get(other_keys[0], 0)
        else:
            pred_price = yes_price
            pred_side_raw = "Yes"
            fair_prob = fair_probs.get(outcome_key, 0)

        if fair_prob <= 0:
            continue

        # Extract consensus metadata from weighted multi-book devig
        fair_meta = fair_probs.get("_meta", {})
        outcome_meta = fair_meta.get(outcome_key, {}) if fair_meta else {}
        n_books = outcome_meta.get("n_books", 1)
        consensus_spread = outcome_meta.get("spread", 0)
        consensus_stdev = outcome_meta.get("stdev", 0)
        source_books = outcome_meta.get("source_books", [])
        overround = outcome_meta.get("overround", 0)

        pred_fee = (KALSHI_FEE_COEFF * pred_price) if is_kalshi else POLYMARKET_FEE
        ev = compute_ev(pred_price, fair_prob, pred_fee)
        if ev is None or ev < min_ev_pct:
            continue
        if ev > 30:
            continue  # almost certainly stale data

        # Determine live status early (needed for adaptive Kelly)
        commence = sb.get("commence_time", "")
        is_live = False
        if commence:
            try:
                event_time = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                is_live = event_time < datetime.now(timezone.utc)
            except Exception:
                pass

        # Compute Kelly fractions
        gross_payout = 1.0 / pred_price if pred_price > 0 else 0
        b = (gross_payout - 1.0) * (1.0 - pred_fee) if gross_payout > 1 else 0
        kelly_f = max(0, (b * fair_prob - (1.0 - fair_prob)) / b) / 2.0 if b > 0 else 0

        # Adaptive Kelly (confidence-weighted)
        adaptive = compute_adaptive_kelly(
            fair_prob, b, ev, match_confidence=confidence,
            n_books=n_books, is_live=is_live
        )

        # Edge Quality Score
        eqs_data = compute_edge_quality_score(
            fair_prob, b, adaptive.get("kelly_adaptive", kelly_f),
            adaptive.get("kelly_confidence", 0.5),
            liquidity=pred.get("liquidity", 0)
        )

        # Build side labels
        pred_line = pred.get("_floor_strike")
        yes_sub = pred.get("_yes_sub_title", "")
        no_sub = pred.get("_no_sub_title", "")
        if pred_subtype == "totals" and pred_line is not None:
            pred_side = f"Over {pred_line}" if pred_side_raw == "Yes" else f"Under {pred_line}"
        elif pred_subtype == "h2h" and (yes_sub or no_sub):
            # Use yes/no sub_titles for human-readable side labels
            if pred_side_raw == "Yes" and yes_sub:
                pred_side = yes_sub.strip()
            elif pred_side_raw == "No" and no_sub:
                pred_side = no_sub.strip()
            elif pred_side_raw == "Yes" and no_sub:
                pred_teams_list = pred.get("teams", [])
                no_team = no_sub.strip().lower()
                other = [t for t in pred_teams_list if no_team not in t]
                pred_side = other[0].title() if other else "Yes"
            elif pred_side_raw == "No" and yes_sub:
                pred_teams_list = pred.get("teams", [])
                yes_team = yes_sub.strip().lower()
                other = [t for t in pred_teams_list if yes_team not in t]
                pred_side = other[0].title() if other else "No"
            else:
                pred_side = pred_side_raw
        else:
            pred_side = pred_side_raw

        if sb_point is not None and sb_outcome.lower() in ("over", "under"):
            sb_side = f"{sb_outcome} {sb_point}"
        elif sb_point is not None:
            sign = "+" if sb_point > 0 else ""
            sb_side = f"{sb_outcome} {sign}{sb_point}"
        else:
            sb_side = sb_outcome

        # Sport display
        sport_display = _sport_display_from_entry(sb)

        # Time display (is_live and commence already set above)
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
                    if delta.days > 0: time_display = f"{delta.days}d"
                    elif delta.seconds > 3600: time_display = f"{delta.seconds // 3600}h"
                    else: time_display = f"{delta.seconds // 60}m"
            except Exception:
                time_display = ""

        base_event = sb.get("event_name", pred.get("question", "")[:60])
        if pred_subtype == "totals" and pred_line is not None:
            event_display = f"{base_event} — O/U {pred_line}"
        elif pred_subtype == "h2h":
            event_display = f"{base_event} — ML"
        else:
            event_display = base_event

        opp = {
            "id": hashlib.md5(f"ev-{pred.get('id','')}-{sb.get('bookmaker','')}-{pred_side}".encode()).hexdigest()[:12],
            "type": "ev",
            "sport": sport_display,
            "event": event_display,
            "event_detail": pred.get("question", ""),
            "commence_time": commence,
            "time_display": time_display,
            "is_live": is_live,
            "platform_a": {
                "name": source.capitalize(),
                "side": pred_side,
                "price": round(pred_price, 6),
                "implied_prob": round(pred_price, 6),
                "american_odds": implied_prob_to_american(pred_price),
                "fee_pct": round(pred_fee * 100, 2),
                "url": pred.get("url", ""),
                "market_id": pred.get("id", ""),
            },
            "platform_b": {
                "name": sb.get("bookmaker_title", sb.get("bookmaker", "")),
                "side": sb_side + " (ref)",
                "price": sb.get("american_odds", 0),
                "implied_prob": round(sb_prob, 6),
                "american_odds": sb.get("american_odds", 0),
                "fee_pct": 0,
                "url": "",
                "market_id": "",
            },
            "market_type": sb.get("market_type", "h2h"),
            "gross_arb_pct": 0,
            "net_arb_pct": round(ev, 3),
            "ev_pct": round(ev, 3),
            "kelly_fraction": round(kelly_f, 6),
            "kelly_adaptive": adaptive.get("kelly_adaptive", kelly_f),
            "kelly_confidence": adaptive.get("kelly_confidence", 0.5),
            "edge_quality_score": eqs_data.get("eqs", 0),
            "growth_rate": eqs_data.get("growth_rate", 0),
            "bets_to_double": eqs_data.get("bets_to_double", 0),
            "consensus_prob": round(fair_prob, 6),
            "match_confidence": round(confidence, 2),
            "n_books": n_books,
            "consensus_spread": round(consensus_spread, 4),
            "consensus_stdev": round(consensus_stdev, 4),
            "source_books": source_books,
            "overround": round(overround, 4),
            "risk_score": compute_risk_score(ev, n_books, consensus_spread, confidence, is_live),
            "resolution_risk": risk_score_label(compute_risk_score(ev, n_books, consensus_spread, confidence, is_live)),
            "risk_note": f"+EV bet: {round(ev, 1)}% edge vs consensus fair odds ({n_books} books). Not a guaranteed arb — variance applies.",
            "is_prop": sb.get("is_prop", False),
            "liquidity": pred.get("liquidity", 0),
            "volume": pred.get("volume", 0),
            "sb_last_update": sb.get("last_update", ""),
        }
        opportunities.append(opp)

    # Deduplicate: keep best EV per event+platform
    seen = {}
    for opp in opportunities:
        key = f"{opp['event']}-{opp['platform_a']['name']}-{opp['market_type']}"
        if key not in seen or opp['ev_pct'] > seen[key]['ev_pct']:
            seen[key] = opp

    return sorted(seen.values(), key=lambda x: x['ev_pct'], reverse=True)


def find_cross_sportsbook_opportunities(sportsbook_entries, fair_index, min_ev_pct=1.0):
    """
    Find cross-sportsbook arbs and +EV bets.
    Groups sportsbook entries by event, finds best prices on opposing sides.
    """
    opportunities = []

    # Group by (event_key, market_type, outcome_point) to find opposing sides
    event_groups = defaultdict(lambda: defaultdict(list))
    for sb in sportsbook_entries:
        home = sb.get("home_team", "")
        away = sb.get("away_team", "")
        mtype = sb.get("market_type", "")
        outcome = sb.get("outcome_name", "")
        point = sb.get("outcome_point")
        commence = sb.get("commence_time", "")
        event_key = _make_event_key(away, home, commence)

        group_key = (event_key, mtype, point)
        event_groups[group_key][outcome].append(sb)

    for (event_key, mtype, point), outcome_map in event_groups.items():
        outcomes = list(outcome_map.keys())
        if len(outcomes) < 2:
            continue

        # ── Date-conflict filtering ──
        # Within this group, drop entries from books whose dates conflict
        # with the majority (>7 days off or placeholder dates).
        all_group_entries = [ent for ent_list in outcome_map.values() for ent in ent_list]
        date_to_books = defaultdict(set)
        for e in all_group_entries:
            db_ = _event_date_bucket(e.get("commence_time", ""))
            if db_:
                date_to_books[db_].add(e.get("bookmaker", ""))
        real_dates = {d: bks for d, bks in date_to_books.items() if not _is_placeholder_date(d)}
        if real_dates:
            majority_date = max(real_dates, key=lambda d: len(real_dates[d]))
            try:
                majority_dt = datetime.strptime(majority_date, "%Y-%m-%d")
                bad_books = set()
                for d, bks in date_to_books.items():
                    if _is_placeholder_date(d):
                        bad_books.update(bks)
                        continue
                    try:
                        d_dt = datetime.strptime(d, "%Y-%m-%d")
                        if abs((d_dt - majority_dt).days) > 7:
                            bad_books.update(bks)
                    except ValueError:
                        pass
                if bad_books:
                    for oname in list(outcome_map.keys()):
                        outcome_map[oname] = [e for e in outcome_map[oname]
                                               if e.get("bookmaker", "") not in bad_books]
                    outcome_map = {k: v for k, v in outcome_map.items() if v}
                    outcomes = list(outcome_map.keys())
                    if len(outcomes) < 2:
                        continue
            except ValueError:
                pass

        # ── 3-way market detection ──
        # Some sports (soccer, boxing, MMA) have draw outcomes even when the API
        # only returns 2 sides.  Treat h2h as 3-way for any THREE_WAY_SPORT so we
        # never build a 2-way arb that ignores the uncovered draw.
        sample_entry = next(iter(outcome_map.values()))[0]
        sport_cat = sample_entry.get("_sport_category", "other")
        is_3way = mtype == "h2h" and (len(outcomes) >= 3 or sport_cat in THREE_WAY_SPORTS)

        if is_3way:
            # 3-way arb: find best price for each outcome, check if sum < 1.0
            best_per_outcome = []
            for oname in outcomes[:3]:  # only first 3 outcomes
                entries = outcome_map[oname]
                best = min(entries, key=lambda x: x.get("implied_prob", 1))
                prob = best.get("implied_prob", 0)
                if prob <= 0 or prob >= 1:
                    break
                best_per_outcome.append((oname, best, prob))

            if len(best_per_outcome) == 3:
                prob_a = best_per_outcome[0][2]
                prob_b = best_per_outcome[1][2]
                prob_c = best_per_outcome[2][2]
                cost = prob_a + prob_b + prob_c

                # Check that not all from same book
                books = {best_per_outcome[k][1].get("bookmaker") for k in range(3)}

                if cost < 1.0 and len(books) >= 2:
                    gross_pct = (1.0 - cost) * 100
                    if gross_pct <= 15:  # filter stale data
                        best_a_entry = best_per_outcome[0][1]
                        best_b_entry = best_per_outcome[1][1]
                        best_c_entry = best_per_outcome[2][1]

                        commence = best_a_entry.get("commence_time", "")
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
                                    if delta.days > 0: time_display = f"{delta.days}d"
                                    elif delta.seconds > 3600: time_display = f"{delta.seconds // 3600}h"
                                    else: time_display = f"{delta.seconds // 60}m"
                            except Exception:
                                pass

                        sport_display = _sport_display_from_entry(best_a_entry)

                        opp = {
                            "id": hashlib.md5(f"xsb3-{event_key}-{mtype}-{'|'.join(outcomes[:3])}".encode()).hexdigest()[:12],
                            "type": "arb",
                            "n_sides": 3,
                            "sport": sport_display,
                            "event": f"{_display_event_key(event_key)} — ML (3-way)",
                            "event_detail": f"3-way sportsbook arb: {best_a_entry.get('bookmaker_title', '')} / {best_b_entry.get('bookmaker_title', '')} / {best_c_entry.get('bookmaker_title', '')}",
                            "commence_time": commence,
                            "time_display": time_display,
                            "is_live": is_live,
                            "platform_a": {
                                "name": best_a_entry.get("bookmaker_title", best_a_entry.get("bookmaker", "")),
                                "side": best_per_outcome[0][0],
                                "price": best_a_entry.get("american_odds", 0),
                                "implied_prob": round(prob_a, 4),
                                "american_odds": best_a_entry.get("american_odds", 0),
                                "fee_pct": 0,
                                "url": "",
                                "market_id": "",
                            },
                            "platform_b": {
                                "name": best_b_entry.get("bookmaker_title", best_b_entry.get("bookmaker", "")),
                                "side": best_per_outcome[1][0],
                                "price": best_b_entry.get("american_odds", 0),
                                "implied_prob": round(prob_b, 4),
                                "american_odds": best_b_entry.get("american_odds", 0),
                                "fee_pct": 0,
                                "url": "",
                                "market_id": "",
                            },
                            "platform_c": {
                                "name": best_c_entry.get("bookmaker_title", best_c_entry.get("bookmaker", "")),
                                "side": best_per_outcome[2][0],
                                "price": best_c_entry.get("american_odds", 0),
                                "implied_prob": round(prob_c, 4),
                                "american_odds": best_c_entry.get("american_odds", 0),
                                "fee_pct": 0,
                                "url": "",
                                "market_id": "",
                            },
                            "market_type": mtype,
                            "gross_arb_pct": round(gross_pct, 3),
                            "net_arb_pct": round(gross_pct, 3),
                            "ev_pct": 0,
                            "consensus_prob": 0,
                            "match_confidence": 1.0,
                            "resolution_risk": "low",
                            "risk_note": "3-way sportsbook arb — all outcomes covered across bookmakers.",
                            "is_prop": best_a_entry.get("is_prop", False),
                            "liquidity": 0,
                            "volume": 0,
                        }
                        opportunities.append(opp)

            # Skip pairwise arb checks for 3-way markets — they are never true 2-way arbs
            # Still check for +EV on individual outcomes below
            fair_probs = fair_index.get((event_key, mtype))
            if fair_probs:
                for oname in outcomes:
                    entries = outcome_map[oname]
                    best = min(entries, key=lambda x: x.get("implied_prob", 1))
                    prob = best.get("implied_prob", 0)
                    if prob <= 0 or prob >= 1:
                        continue
                    outcome_key = f"{oname}|{point}" if point is not None else oname
                    fair_p = fair_probs.get(outcome_key, 0)
                    if fair_p <= 0:
                        continue
                    ev = compute_ev(prob, fair_p)
                    if ev is None or ev < min_ev_pct or ev > 30:
                        continue

                    xsb_payout = 1.0 / prob if prob > 0 else 0
                    xsb_b = (xsb_payout - 1.0) if xsb_payout > 1 else 0
                    xsb_kelly = max(0, (xsb_b * fair_p - (1.0 - fair_p)) / xsb_b) / 2.0 if xsb_b > 0 else 0

                    commence = best.get("commence_time", "")
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
                                if delta.days > 0: time_display = f"{delta.days}d"
                                elif delta.seconds > 3600: time_display = f"{delta.seconds // 3600}h"
                                else: time_display = f"{delta.seconds // 60}m"
                        except Exception:
                            pass

                    sport_display = _sport_display_from_entry(best)
                    side_label = oname

                    opp = {
                        "id": hashlib.md5(f"xev3-{event_key}-{mtype}-{oname}-{best.get('bookmaker','')}".encode()).hexdigest()[:12],
                        "type": "ev",
                        "sport": sport_display,
                        "event": f"{_display_event_key(event_key)} — ML (3-way)",
                        "event_detail": f"+EV: {best.get('bookmaker_title', '')} {side_label} vs consensus fair odds",
                        "commence_time": commence,
                        "time_display": time_display,
                        "is_live": is_live,
                        "platform_a": {
                            "name": best.get("bookmaker_title", best.get("bookmaker", "")),
                            "side": side_label,
                            "price": best.get("american_odds", 0),
                            "implied_prob": round(prob, 4),
                            "american_odds": best.get("american_odds", 0),
                            "fee_pct": 0,
                            "url": "",
                            "market_id": "",
                        },
                        "platform_b": {
                            "name": "Consensus",
                            "side": f"Fair: {round(fair_p * 100, 1)}%",
                            "price": 0,
                            "implied_prob": round(fair_p, 4),
                            "american_odds": implied_prob_to_american(fair_p),
                            "fee_pct": 0,
                            "url": "",
                            "market_id": "",
                        },
                        "market_type": mtype,
                        "gross_arb_pct": 0,
                        "net_arb_pct": round(ev, 3),
                        "ev_pct": round(ev, 3),
                        "kelly_fraction": round(xsb_kelly, 6),
                        "consensus_prob": round(fair_p, 4),
                        "match_confidence": 1.0,
                        "resolution_risk": "medium",
                        "risk_note": f"+EV bet (3-way market): {round(ev, 1)}% edge vs devigged consensus.",
                        "is_prop": best.get("is_prop", False),
                        "liquidity": 0,
                        "volume": 0,
                    }
                    opportunities.append(opp)

            continue  # skip pairwise loop for 3-way markets

        # ── 2-way market arb checks (h2h binary, totals, spreads) ──
        for i in range(len(outcomes)):
            for j in range(i + 1, len(outcomes)):
                side_a_entries = outcome_map[outcomes[i]]
                side_b_entries = outcome_map[outcomes[j]]

                # Find best price (lowest implied prob = best odds) for each side
                best_a = min(side_a_entries, key=lambda x: x.get("implied_prob", 1))
                best_b = min(side_b_entries, key=lambda x: x.get("implied_prob", 1))

                # Skip if same bookmaker
                if best_a.get("bookmaker") == best_b.get("bookmaker"):
                    continue

                prob_a = best_a.get("implied_prob", 0)
                prob_b = best_b.get("implied_prob", 0)
                if prob_a <= 0 or prob_b <= 0 or prob_a >= 1 or prob_b >= 1:
                    continue

                cost = prob_a + prob_b
                if cost < 1.0:
                    # Full arb between sportsbooks
                    gross_pct = (1.0 - cost) * 100
                    if gross_pct > 15:
                        continue  # stale data

                    # Sanity: for spreads, detect flipped sides (some books
                    # swap home/away spread signs).  If both best prices are
                    # on the same side of even (both underdogs), it's bogus.
                    if mtype == "spreads" and point is not None:
                        odds_a = best_a.get("american_odds", 0)
                        odds_b = best_b.get("american_odds", 0)
                        if odds_a > 0 and odds_b > 0 and gross_pct > 5:
                            continue  # both underdogs — flipped spread data

                    commence = best_a.get("commence_time", "")
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
                                if delta.days > 0: time_display = f"{delta.days}d"
                                elif delta.seconds > 3600: time_display = f"{delta.seconds // 3600}h"
                                else: time_display = f"{delta.seconds // 60}m"
                        except Exception:
                            pass

                    sport_display = _sport_display_from_entry(best_a)

                    side_a = outcomes[i]
                    side_b = outcomes[j]
                    if point is not None:
                        if side_a.lower() in ("over", "under"):
                            side_a = f"{side_a} {point}"
                            side_b = f"{side_b} {point}"
                        else:
                            sign_a = "+" if point > 0 else ""
                            sign_b = "+" if (-point if point else 0) > 0 else ""
                            side_a = f"{side_a} {sign_a}{point}"
                            side_b = f"{side_b} {sign_b}{-point if point else ''}"

                    opp = {
                        "id": hashlib.md5(f"xsb-{event_key}-{mtype}-{outcomes[i]}-{outcomes[j]}".encode()).hexdigest()[:12],
                        "type": "arb",
                        "sport": sport_display,
                        "event": f"{_display_event_key(event_key)} — {'ML' if mtype == 'h2h' else mtype.replace('_', ' ').title()}",
                        "event_detail": f"Sportsbook arb: {best_a.get('bookmaker_title', '')} vs {best_b.get('bookmaker_title', '')}",
                        "commence_time": commence,
                        "time_display": time_display,
                        "is_live": is_live,
                        "platform_a": {
                            "name": best_a.get("bookmaker_title", best_a.get("bookmaker", "")),
                            "side": side_a,
                            "price": best_a.get("american_odds", 0),
                            "implied_prob": round(prob_a, 4),
                            "american_odds": best_a.get("american_odds", 0),
                            "fee_pct": 0,
                            "url": "",
                            "market_id": "",
                        },
                        "platform_b": {
                            "name": best_b.get("bookmaker_title", best_b.get("bookmaker", "")),
                            "side": side_b,
                            "price": best_b.get("american_odds", 0),
                            "implied_prob": round(prob_b, 4),
                            "american_odds": best_b.get("american_odds", 0),
                            "fee_pct": 0,
                            "url": "",
                            "market_id": "",
                        },
                        "market_type": mtype,
                        "gross_arb_pct": round(gross_pct, 3),
                        "net_arb_pct": round(gross_pct, 3),
                        "ev_pct": 0,
                        "consensus_prob": 0,
                        "match_confidence": 1.0,
                        "resolution_risk": "medium" if sport_cat in UNPRICED_DRAW_SPORTS else "low",
                        "risk_note": ("Cross-sportsbook arb (MMA) — draw outcome is possible (~1-2%) but unpriced by books."
                                      if sport_cat in UNPRICED_DRAW_SPORTS
                                      else "Cross-sportsbook arb — same event, different bookmakers. Low risk."),
                        "is_prop": best_a.get("is_prop", False),
                        "liquidity": 0,
                        "volume": 0,
                    }
                    opportunities.append(opp)
                else:
                    # No full arb — check for +EV on the best-priced side
                    fair_probs = fair_index.get((event_key, mtype))
                    if not fair_probs:
                        continue

                    # Extract consensus metadata
                    xsb_fair_meta = fair_probs.get("_meta", {})

                    # Check each side for +EV
                    for side_idx, (best, outcome_name) in enumerate([(best_a, outcomes[i]), (best_b, outcomes[j])]):
                        prob = best.get("implied_prob", 0)
                        outcome_key = f"{outcome_name}|{point}" if point is not None else outcome_name
                        fair_p = fair_probs.get(outcome_key, 0)
                        if fair_p <= 0:
                            continue

                        # Get per-outcome metadata
                        o_meta = xsb_fair_meta.get(outcome_key, {})
                        xsb_n_books = o_meta.get("n_books", 1)
                        xsb_spread = o_meta.get("spread", 0)
                        xsb_stdev = o_meta.get("stdev", 0)
                        xsb_source_books = o_meta.get("source_books", [])
                        xsb_overround = o_meta.get("overround", 0)

                        ev = compute_ev(prob, fair_p)
                        if ev is None or ev < min_ev_pct or ev > 30:
                            continue

                        commence = best.get("commence_time", "")
                        is_live = False

                        # Compute Kelly fractions
                        xsb_payout = 1.0 / prob if prob > 0 else 0
                        xsb_b = (xsb_payout - 1.0) if xsb_payout > 1 else 0
                        xsb_kelly = max(0, (xsb_b * fair_p - (1.0 - fair_p)) / xsb_b) / 2.0 if xsb_b > 0 else 0

                        # Adaptive Kelly
                        xsb_adaptive = compute_adaptive_kelly(
                            fair_p, xsb_b, ev, match_confidence=1.0,
                            n_books=xsb_n_books, is_live=is_live
                        )

                        # Edge Quality Score
                        xsb_eqs = compute_edge_quality_score(
                            fair_p, xsb_b, xsb_adaptive.get("kelly_adaptive", xsb_kelly),
                            xsb_adaptive.get("kelly_confidence", 0.5)
                        )
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
                                    if delta.days > 0: time_display = f"{delta.days}d"
                                    elif delta.seconds > 3600: time_display = f"{delta.seconds // 3600}h"
                                    else: time_display = f"{delta.seconds // 60}m"
                            except Exception:
                                pass

                        sport = best.get("sport", "").replace("_", " ").upper()
                        if "nba" in sport.lower(): sport_display = "NBA"
                        elif "nfl" in sport.lower(): sport_display = "NFL"
                        elif "mlb" in sport.lower(): sport_display = "MLB"
                        elif "nhl" in sport.lower(): sport_display = "NHL"
                        elif "soccer" in sport.lower() or "mls" in sport.lower() or "epl" in sport.lower(): sport_display = "Soccer"
                        elif "mma" in sport.lower(): sport_display = "MMA"
                        else: sport_display = sport[:10] if sport else "Sports"

                        side_label = outcome_name
                        if point is not None:
                            if outcome_name.lower() in ("over", "under"):
                                side_label = f"{outcome_name} {point}"
                            else:
                                sign = "+" if point > 0 else ""
                                side_label = f"{outcome_name} {sign}{point}"

                        # Reference side (consensus)
                        other_outcome = outcomes[j] if side_idx == 0 else outcomes[i]
                        other_best = best_b if side_idx == 0 else best_a

                        opp = {
                            "id": hashlib.md5(f"xev-{event_key}-{mtype}-{outcome_name}-{best.get('bookmaker','')}".encode()).hexdigest()[:12],
                            "type": "ev",
                            "sport": sport_display,
                            "event": f"{_display_event_key(event_key)} — {'ML' if mtype == 'h2h' else mtype.replace('_', ' ').title()}",
                            "event_detail": f"+EV: {best.get('bookmaker_title', '')} {side_label} vs consensus fair odds",
                            "commence_time": commence,
                            "time_display": time_display,
                            "is_live": is_live,
                            "platform_a": {
                                "name": best.get("bookmaker_title", best.get("bookmaker", "")),
                                "side": side_label,
                                "price": best.get("american_odds", 0),
                                "implied_prob": round(prob, 4),
                                "american_odds": best.get("american_odds", 0),
                                "fee_pct": 0,
                                "url": "",
                                "market_id": "",
                            },
                            "platform_b": {
                                "name": "Consensus",
                                "side": f"Fair: {round(fair_p * 100, 1)}%",
                                "price": 0,
                                "implied_prob": round(fair_p, 4),
                                "american_odds": implied_prob_to_american(fair_p),
                                "fee_pct": 0,
                                "url": "",
                                "market_id": "",
                            },
                            "market_type": mtype,
                            "gross_arb_pct": 0,
                            "net_arb_pct": round(ev, 3),
                            "ev_pct": round(ev, 3),
                            "kelly_fraction": round(xsb_kelly, 6),
                            "kelly_adaptive": xsb_adaptive.get("kelly_adaptive", xsb_kelly),
                            "kelly_confidence": xsb_adaptive.get("kelly_confidence", 0.5),
                            "edge_quality_score": xsb_eqs.get("eqs", 0),
                            "growth_rate": xsb_eqs.get("growth_rate", 0),
                            "bets_to_double": xsb_eqs.get("bets_to_double", 0),
                            "consensus_prob": round(fair_p, 4),
                            "match_confidence": 1.0,
                            "n_books": xsb_n_books,
                            "consensus_spread": round(xsb_spread, 4),
                            "consensus_stdev": round(xsb_stdev, 4),
                            "source_books": xsb_source_books,
                            "overround": round(xsb_overround, 4),
                            "risk_score": compute_risk_score(ev, xsb_n_books, xsb_spread, 1.0, is_live),
                            "resolution_risk": risk_score_label(compute_risk_score(ev, xsb_n_books, xsb_spread, 1.0, is_live)),
                            "risk_note": (f"+EV bet (MMA — draw possible but unpriced): {round(ev, 1)}% edge vs consensus ({xsb_n_books} books)."
                                          if sport_cat in UNPRICED_DRAW_SPORTS
                                          else f"+EV bet: {round(ev, 1)}% edge vs devigged consensus ({xsb_n_books} books)."),
                            "is_prop": best.get("is_prop", False),
                            "liquidity": 0,
                            "volume": 0,
                        }
                        opportunities.append(opp)

    # Deduplicate
    seen = {}
    for opp in opportunities:
        key = f"{opp['event']}-{opp['platform_a']['name']}-{opp['platform_a']['side']}"
        if key not in seen or (opp.get('ev_pct', 0) + opp.get('gross_arb_pct', 0)) > (seen[key].get('ev_pct', 0) + seen[key].get('gross_arb_pct', 0)):
            seen[key] = opp

    return sorted(seen.values(), key=lambda x: x.get('ev_pct', 0) + x.get('net_arb_pct', 0), reverse=True)


# ─── Alert Delivery ──────────────────────────────────────────────────────────

def _send_alerts(db, opportunities):
    """Send Discord/Telegram alerts for high-edge opportunities."""
    discord_url = get_config(db, "discord_webhook", "")
    tg_token = get_config(db, "telegram_bot_token", "")
    tg_chat = get_config(db, "telegram_chat_id", "")
    min_edge = float(get_config(db, "alert_min_edge", 2))

    if not discord_url and not tg_token:
        return

    for opp in opportunities:
        edge = opp.get("ev_pct", 0) or opp.get("net_arb_pct", 0)
        if edge < min_edge:
            continue

        opp_type = "ARB" if opp.get("type") == "arb" else "+EV"
        msg = (
            f"**{opp_type} {edge:.1f}%** — {opp.get('event', '?')}\n"
            f"{opp.get('platform_a', {}).get('name', '?')}: "
            f"{opp.get('platform_a', {}).get('side', '?')} @ "
            f"{opp.get('platform_a', {}).get('implied_prob', 0)*100:.0f}%\n"
            f"{opp.get('platform_b', {}).get('name', '?')}: "
            f"{opp.get('platform_b', {}).get('side', '?')} @ "
            f"{opp.get('platform_b', {}).get('implied_prob', 0)*100:.0f}%"
        )

        if discord_url:
            try:
                req = urllib.request.Request(
                    discord_url,
                    data=json.dumps({"content": msg}).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=5)
            except Exception:
                pass

        if tg_token and tg_chat:
            try:
                tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
                req = urllib.request.Request(
                    tg_url,
                    data=json.dumps({
                        "chat_id": tg_chat,
                        "text": msg.replace("**", "*"),
                        "parse_mode": "Markdown",
                    }).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=5)
            except Exception:
                pass


# ─── Scanner Auto-Tracking & Resolution ──────────────────────────────────────

# Map display sport back to Odds API sport key for scores lookup
_SPORT_DISPLAY_TO_KEY = {
    "NBA": "basketball_nba",
    "NFL": "americanfootball_nfl",
    "MLB": "baseball_mlb",
    "NHL": "icehockey_nhl",
    "Soccer": "soccer_usa_mls",
    "MMA": "mma_mixed_martial_arts",
}

# Estimated game durations (hours) for knowing when to check scores
_SPORT_DURATION_HOURS = {
    "NBA": 3, "NFL": 4, "MLB": 4, "NHL": 3, "Soccer": 2, "MMA": 5,
}

def _ensure_scanner_track_table(db):
    db.execute("""CREATE TABLE IF NOT EXISTS scanner_track (
        id TEXT PRIMARY KEY,
        opp_type TEXT,
        sport TEXT,
        sport_key TEXT,
        event TEXT,
        event_detail TEXT,
        commence_time TEXT,
        platform_a TEXT,
        side_a TEXT,
        prob_a REAL,
        odds_a INTEGER,
        platform_b TEXT,
        side_b TEXT,
        prob_b REAL,
        odds_b INTEGER,
        market_type TEXT,
        edge_pct REAL,
        hypothetical_stake REAL DEFAULT 100,
        status TEXT DEFAULT 'pending',
        pnl REAL DEFAULT 0,
        created_at TEXT,
        resolve_after TEXT,
        resolved_at TEXT
    )""")
    db.commit()


def _auto_track_opportunities(db, opportunities):
    """Store new scanner opportunities for automatic tracking."""
    _ensure_scanner_track_table(db)
    now = datetime.now(timezone.utc).isoformat()

    for opp in opportunities:
        opp_id = opp.get("id", "")
        if not opp_id:
            continue

        # Skip if already tracked
        existing = db.execute("SELECT id FROM scanner_track WHERE id=?", [opp_id]).fetchone()
        if existing:
            continue

        opp_type = opp.get("type", "arb")
        sport = opp.get("sport", "")
        sport_key = _SPORT_DISPLAY_TO_KEY.get(sport, "")
        commence = opp.get("commence_time", "")

        # Calculate when to check for results
        duration_h = _SPORT_DURATION_HOURS.get(sport, 4)
        resolve_after = ""
        if commence:
            try:
                ct = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                resolve_after = (ct + timedelta(hours=duration_h)).isoformat()
            except (ValueError, TypeError):
                pass

        pa = opp.get("platform_a", {})
        pb = opp.get("platform_b", {})
        edge = opp.get("ev_pct", 0) or opp.get("net_arb_pct", 0)

        # For arbs: auto-resolve immediately — profit is guaranteed (assuming execution)
        status = "pending"
        pnl = 0
        resolved_at = None
        if opp_type == "arb":
            status = "won"
            pnl = round(edge, 2)  # edge_pct on $100 hypothetical = dollar profit
            resolved_at = now

        db.execute("""INSERT OR IGNORE INTO scanner_track
            (id, opp_type, sport, sport_key, event, event_detail,
             commence_time, platform_a, side_a, prob_a, odds_a,
             platform_b, side_b, prob_b, odds_b, market_type,
             edge_pct, hypothetical_stake, status, pnl, created_at,
             resolve_after, resolved_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [opp_id, opp_type, sport, sport_key,
             opp.get("event", ""), opp.get("event_detail", ""),
             commence, pa.get("name", ""), pa.get("side", ""),
             pa.get("implied_prob", 0), pa.get("american_odds", 0),
             pb.get("name", ""), pb.get("side", ""),
             pb.get("implied_prob", 0), pb.get("american_odds", 0),
             opp.get("market_type", ""), edge,
             100, status, pnl, now, resolve_after, resolved_at])

    db.commit()


def _resolve_pending_bets(db, api_key):
    """Check completed events and resolve pending EV bets."""
    _ensure_scanner_track_table(db)
    now = datetime.now(timezone.utc)

    # Find bets that should be resolvable (resolve_after has passed)
    pending = db.execute("""
        SELECT id, sport_key, event, side_a, commence_time, platform_a
        FROM scanner_track
        WHERE status='pending' AND resolve_after != '' AND resolve_after < ?
        LIMIT 50
    """, [now.isoformat()]).fetchall()

    if not pending or not api_key:
        return

    # Group by sport_key to minimize API calls (1 call per sport)
    by_sport = defaultdict(list)
    for row in pending:
        bet_id, sport_key, event, side_a, commence, platform_a = row
        if sport_key:
            by_sport[sport_key].append({
                "id": bet_id, "event": event, "side_a": side_a,
                "commence": commence, "platform_a": platform_a,
            })

    for sport_key, bets in by_sport.items():
        # Fetch completed scores from Odds API
        scores_url = (
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/scores?"
            f"apiKey={api_key}&daysFrom=3"
        )
        scores_data = fetch_json(scores_url)
        if not isinstance(scores_data, list):
            continue

        # Build lookup: completed events by team names
        completed = {}
        for game in scores_data:
            if not game.get("completed"):
                continue
            home = game.get("home_team", "").lower()
            away = game.get("away_team", "").lower()
            scores = game.get("scores")
            if not scores:
                continue
            # Determine winner
            home_score = 0
            away_score = 0
            for s in scores:
                if s.get("name", "").lower() == home:
                    home_score = int(s.get("score", 0))
                elif s.get("name", "").lower() == away:
                    away_score = int(s.get("score", 0))
            winner = home if home_score > away_score else away if away_score > home_score else "draw"
            key = f"{away}@{home}"
            completed[key] = {"winner": winner, "home": home, "away": away,
                              "home_score": home_score, "away_score": away_score}

        # Match our bets against completed games
        for bet in bets:
            event_lower = bet["event"].lower()
            side_lower = bet["side_a"].lower().replace(" yes", "").strip()

            matched_result = None
            for key, result in completed.items():
                # Check if event text contains both team names
                if result["home"] in event_lower and result["away"] in event_lower:
                    matched_result = result
                    break
                # Also check individual words overlap
                event_words = set(event_lower.split())
                home_words = set(result["home"].split())
                away_words = set(result["away"].split())
                if len(event_words & home_words) > 0 and len(event_words & away_words) > 0:
                    matched_result = result
                    break

            if matched_result is None:
                # Event not in scores yet — might be delayed. Mark stale after 7 days.
                try:
                    ct = datetime.fromisoformat(bet["commence"].replace("Z", "+00:00"))
                    if (now - ct).days > 7:
                        db.execute(
                            "UPDATE scanner_track SET status='void', resolved_at=? WHERE id=?",
                            [now.isoformat(), bet["id"]])
                except (ValueError, TypeError):
                    pass
                continue

            # Determine if our side won
            winner = matched_result["winner"]
            won = False
            if winner != "draw":
                # Check if our side matches the winner
                winner_words = set(winner.split())
                side_words = set(side_lower.split())
                won = len(winner_words & side_words) > 0

            # For prediction market bets: "Yes" on team X = that team winning
            if "yes" in bet["side_a"].lower():
                # side_a has team name embedded, check if that team won
                pass  # already handled above

            status = "won" if won else "lost"
            # Calculate P&L: $100 hypothetical stake
            stake = 100
            if won:
                # Get the stored odds
                row = db.execute("SELECT odds_a FROM scanner_track WHERE id=?",
                                 [bet["id"]]).fetchone()
                odds = row[0] if row else 0
                if odds > 0:
                    pnl = round(stake * (odds / 100), 2)
                elif odds < 0:
                    pnl = round(stake * (100 / abs(odds)), 2)
                else:
                    pnl = 0
            else:
                pnl = -stake

            db.execute(
                "UPDATE scanner_track SET status=?, pnl=?, resolved_at=? WHERE id=?",
                [status, pnl, now.isoformat(), bet["id"]])

    db.commit()


def _get_scanner_track_stats(db):
    """Return summary stats for the auto-tracker."""
    _ensure_scanner_track_table(db)
    rows = db.execute("""
        SELECT opp_type, status, COUNT(*), SUM(pnl), SUM(hypothetical_stake)
        FROM scanner_track
        GROUP BY opp_type, status
    """).fetchall()

    stats = {
        "arb": {"total": 0, "won": 0, "lost": 0, "pending": 0, "void": 0, "pnl": 0, "staked": 0},
        "ev":  {"total": 0, "won": 0, "lost": 0, "pending": 0, "void": 0, "pnl": 0, "staked": 0},
    }
    for opp_type, status, count, pnl, staked in rows:
        bucket = stats.get(opp_type, stats["ev"])
        bucket["total"] += count
        bucket[status] = bucket.get(status, 0) + count
        bucket["pnl"] += pnl or 0
        if status != "pending":
            bucket["staked"] += staked or 0

    # Recent tracked bets for charting (last 200)
    recent = db.execute("""
        SELECT id, opp_type, sport, event, edge_pct, status, pnl,
               created_at, resolved_at
        FROM scanner_track
        WHERE status != 'pending'
        ORDER BY resolved_at DESC
        LIMIT 200
    """).fetchall()
    cols = ["id", "opp_type", "sport", "event", "edge_pct", "status", "pnl",
            "created_at", "resolved_at"]
    recent_list = [dict(zip(cols, r)) for r in recent]

    return {"stats": stats, "recent": recent_list}


# ─── Core scan logic (callable from CGI or Vercel handler) ───────────────────

def run_scan(params):
    """Run the full scan and return the response dict.
    Supports mode='quick' for fast prediction-market-only scans that
    reuse cached sportsbook data, and mode='full' (default) for a
    complete refresh of all sources."""
    db = get_db()
    scan_mode = params.get("mode", "full")  # "quick" or "full"

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

    # In quick mode, skip the sportsbook API and reuse cached data.
    # This makes scans ~3× faster and costs zero Odds API quota.
    if scan_mode == "quick":
        sportsbook_entries = get_stale_cached(db, "sportsbook_odds") or []
        sources_status["sportsbook"] = "cached" if sportsbook_entries else "no_data"

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_poly = executor.submit(fetch_polymarket_sports, None)
        future_kalshi = executor.submit(fetch_kalshi_sports, None)
        future_sb = (executor.submit(fetch_sportsbook_odds, None, api_key)
                     if api_key and scan_mode != "quick" else None)

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

    # +EV detection: build fair odds index, find +EV opportunities
    devig_method = get_config(db, "devig_method", "power")
    fair_index = {}
    if sportsbook_entries:
        fair_index = build_fair_odds_index(sportsbook_entries, devig_method=devig_method)

        # +EV: prediction markets vs fair odds
        if poly_markets:
            ev1 = find_ev_opportunities(poly_markets, sportsbook_entries, fair_index)
            all_opportunities.extend(ev1)
        if kalshi_markets:
            ev2 = find_ev_opportunities(kalshi_markets, sportsbook_entries, fair_index)
            all_opportunities.extend(ev2)

        # Cross-sportsbook arbs & +EV
        xsb = find_cross_sportsbook_opportunities(sportsbook_entries, fair_index)
        all_opportunities.extend(xsb)

    # Filter out live games — odds change every few seconds, stale before
    # a manual scanner can act.  Keep only upcoming/pre-match events.
    include_live = get_config(db, "include_live", True)
    if not include_live:
        all_opportunities = [o for o in all_opportunities if not o.get("is_live")]

    # Apply sports filter
    if sports_filter and sports_filter[0]:
        sports_set = set(s.upper() for s in sports_filter)
        all_opportunities = [o for o in all_opportunities if o["sport"].upper() in sports_set]

    # Deduplicate across all sources by id
    seen_ids = {}
    for opp in all_opportunities:
        oid = opp["id"]
        if oid not in seen_ids:
            seen_ids[oid] = opp
        else:
            # Keep the better one
            existing = seen_ids[oid]
            if opp.get("ev_pct", 0) + opp.get("net_arb_pct", 0) > existing.get("ev_pct", 0) + existing.get("net_arb_pct", 0):
                seen_ids[oid] = opp
    all_opportunities = list(seen_ids.values())

    # Sort: arbs by net_arb_pct, +EV by ev_pct, arbs first
    all_opportunities.sort(
        key=lambda x: (0 if x.get("type") == "arb" else 1, -(x.get("net_arb_pct", 0) + x.get("ev_pct", 0))),
    )

    scan_duration = round(time.time() - scan_start, 2)

    arb_count = sum(1 for o in all_opportunities if o.get("type") == "arb")
    ev_count = sum(1 for o in all_opportunities if o.get("type") == "ev")

    # Send alerts (non-blocking — failures are silently ignored)
    if all_opportunities and scan_mode == "full":
        try:
            _send_alerts(db, all_opportunities)
        except Exception:
            pass

    # Auto-track all opportunities for performance tracking
    if all_opportunities:
        try:
            _auto_track_opportunities(db, all_opportunities)
        except Exception:
            pass

    # Resolve pending EV bets using scores API (only on full scans)
    if scan_mode == "full" and api_key:
        try:
            _resolve_pending_bets(db, api_key)
        except Exception:
            pass

    # Get tracker stats for frontend
    tracker_stats = {}
    try:
        tracker_stats = _get_scanner_track_stats(db)
    except Exception:
        pass

    return {
        "opportunities": all_opportunities,
        "meta": {
            "scan_time": scan_duration,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_opportunities": len(all_opportunities),
            "arb_count": arb_count,
            "ev_count": ev_count,
            "sources": sources_status,
            "errors": errors,
            "is_demo": False,
            "scan_mode": scan_mode,
            "poly_count": len(poly_markets),
            "kalshi_count": len(kalshi_markets),
            "sportsbook_count": len(sportsbook_entries),
            "odds_api_remaining": _safe_int(get_config(db, "_odds_api_remaining")),
            "odds_api_used": _safe_int(get_config(db, "_odds_api_used")),
            "tracker": tracker_stats,
        }
    }

# ─── CGI entry point (for local development) ─────────────────────────────────

def main():
    print("Content-Type: application/json")
    print()
    query_string = os.environ.get("QUERY_STRING", "")
    params = dict(urllib.parse.parse_qsl(query_string))
    try:
        print(json.dumps(run_scan(params), default=_json_default))
    except Exception as e:
        err_msg = str(e)
        sources = {"polymarket": "error", "kalshi": "error", "sportsbook": "error"}
        if "QUOTA_EXCEEDED" in err_msg:
            sources["sportsbook"] = "quota_exceeded"
            sources["polymarket"] = "ok"
            sources["kalshi"] = "ok"
        elif "INVALID_KEY" in err_msg:
            sources["sportsbook"] = "invalid_key"
        print(json.dumps({
            "opportunities": [],
            "meta": {
                "scan_time": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_opportunities": 0,
                "arb_count": 0,
                "ev_count": 0,
                "sources": sources,
                "errors": [err_msg],
                "is_demo": False,
            }
        }))

if __name__ == "__main__":
    main()
