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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ─── Configuration ────────────────────────────────────────────────────────────

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data.db")
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

def set_cached(db, cache_key, data):
    db.execute("INSERT OR REPLACE INTO cache (cache_key, data, ts) VALUES (?,?,?)",
               [cache_key, json.dumps(data), time.time()])
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
}

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

# ─── Polymarket CLI helpers ───────────────────────────────────────────────────

def _polymarket_cli_available():
    """Check if the polymarket CLI tool is installed."""
    return shutil.which("polymarket") is not None

SPORT_KEYWORDS = frozenset([
    "nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball",
    "baseball", "hockey", "mma", "ufc", "tennis", "points", "rebounds",
    "assists", "touchdowns", "goals", "runs", "yards",
    "over", "under", "spread", "moneyline",
])

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
        if any(kw in title for kw in SPORT_KEYWORDS):
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
        # HTTP fallback: original 13-call approach
        markets = []
        sport_tags = ["sports", "nba", "nfl", "mlb", "nhl", "soccer", "football",
                      "basketball", "baseball", "hockey", "mma", "ufc"]

        for tag in sport_tags:
            url = f"https://gamma-api.polymarket.com/markets?tag={tag}&closed=false&limit=100"
            data = fetch_json(url)
            if isinstance(data, list):
                markets.extend(data)
            elif isinstance(data, dict) and not data.get("_error"):
                if "markets" in data:
                    markets.extend(data["markets"])

        # Also try without tag filter and search for sports keywords
        url = "https://gamma-api.polymarket.com/markets?closed=false&limit=200&active=true"
        data = fetch_json(url)
        if isinstance(data, list):
            for m in data:
                title = (m.get("question", "") + " " + m.get("description", "")).lower()
                if any(kw in title for kw in SPORT_KEYWORDS):
                    markets.append(m)

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
                "url": f"https://polymarket.com/event/{m.get('slug', '')}" if m.get('slug') else "",
            }
            results.append(entry)
        except Exception:
            continue

    set_cached(db, cache_key, results)
    return results

def fetch_kalshi_sports(db=None):
    """Fetch sports/event markets from Kalshi."""
    if db is None:
        db = get_db()
    cache_key = "kalshi_sports"
    cached = get_cached(db, cache_key)
    if cached is not None:
        return cached

    markets = []
    # Kalshi uses event tickers — fetch open markets
    url = "https://api.elections.kalshi.com/trade-api/v2/markets?status=open&limit=200"
    data = fetch_json(url)

    if isinstance(data, dict) and "markets" in data:
        raw_markets = data["markets"]
    elif isinstance(data, list):
        raw_markets = data
    else:
        raw_markets = []

    sport_keywords = ["nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball",
                      "baseball", "hockey", "mma", "ufc", "tennis", "points", "rebounds",
                      "assists", "touchdowns", "goals", "runs", "yards", "super bowl",
                      "world series", "stanley cup", "march madness", "over", "under"]

    results = []
    for m in raw_markets:
        try:
            title = (m.get("title", "") + " " + m.get("subtitle", "") + " " +
                     m.get("event_ticker", "") + " " + m.get("category", "")).lower()

            if not any(kw in title for kw in sport_keywords):
                continue

            yes_price = m.get("yes_bid", 0) or m.get("last_price", 0) or 0
            no_price = m.get("no_bid", 0) or 0
            if yes_price == 0 and no_price == 0:
                yes_price = m.get("yes_ask", 0) or 0
                no_price = m.get("no_ask", 0) or 0

            # Kalshi prices are in cents (0-100) or decimals (0-1)
            if isinstance(yes_price, (int, float)) and yes_price > 1:
                yes_prob = yes_price / 100.0
            else:
                yes_prob = float(yes_price)

            if isinstance(no_price, (int, float)) and no_price > 1:
                no_prob = no_price / 100.0
            else:
                no_prob = float(no_price)

            if yes_prob == 0 and no_prob > 0:
                yes_prob = 1.0 - no_prob
            elif no_prob == 0 and yes_prob > 0:
                no_prob = 1.0 - yes_prob

            entry = {
                "source": "kalshi",
                "id": m.get("ticker", ""),
                "question": m.get("title", "") or m.get("subtitle", ""),
                "description": m.get("subtitle", "") or m.get("title", ""),
                "outcomes": ["Yes", "No"],
                "prices": [yes_prob, no_prob],
                "end_date": m.get("expiration_time", "") or m.get("close_time", ""),
                "volume": m.get("volume", 0),
                "liquidity": m.get("open_interest", 0),
                "ticker": m.get("ticker", ""),
                "event_ticker": m.get("event_ticker", ""),
                "teams": extract_teams_from_text(m.get("title", "")),
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
            except Exception:
                continue

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
                            "teams": [normalize_name(home), normalize_name(away)],
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

    matches = []

    for sb in sportsbook_entries:
        score = 0
        sb_teams = sb.get("teams", [])
        sb_event = sb.get("event_name", "").lower()
        sb_outcome = sb.get("outcome_name", "").lower()

        # Team matching
        team_matches = 0
        for pt in pred_teams:
            for st in sb_teams:
                if pt and st and (pt in st or st in pt):
                    team_matches += 1
        if team_matches >= 2:
            score += 0.6
        elif team_matches == 1:
            score += 0.3

        # Text similarity
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

        pred_fee = POLYMARKET_FEE if source == "polymarket" else KALSHI_FEE

        # Find matching sportsbook entries
        matches = try_match_prediction_to_sportsbook(pred, sportsbook_entries)

        for match in matches:
            sb = match["sportsbook_entry"]
            confidence = match["confidence"]
            sb_prob = sb.get("implied_prob", 0)

            if sb_prob <= 0 or sb_prob >= 1:
                continue

            # Scenario 1: Prediction YES + Sportsbook opposing side
            # If prediction market has YES at yes_price, and sportsbook has
            # the opposing side at sb_prob
            arb1 = compute_arb_binary(yes_price, sb_prob, pred_fee, SPORTSBOOK_FEE)

            # Scenario 2: Prediction NO + Sportsbook same side
            arb2 = compute_arb_binary(no_price, 1 - sb_prob, pred_fee, SPORTSBOOK_FEE)

            for arb, scenario in [(arb1, "pred_yes_sb_no"), (arb2, "pred_no_sb_yes")]:
                if arb is None:
                    continue
                if arb["gross_arb_pct"] <= 0:
                    continue
                if arb["net_arb_pct"] < min_net_pct:
                    continue

                if scenario == "pred_yes_sb_no":
                    pred_side = outcomes[0] if outcomes else "Yes"
                    pred_price = yes_price
                    sb_side = sb.get("outcome_name", "")
                    sb_price_display = sb.get("american_odds", 0)
                else:
                    pred_side = outcomes[1] if len(outcomes) > 1 else "No"
                    pred_price = no_price
                    sb_side = sb.get("outcome_name", "")
                    sb_price_display = sb.get("american_odds", 0)

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

                stakes = compute_stake_allocation(
                    pred_price if scenario == "pred_yes_sb_no" else no_price,
                    sb_prob if scenario == "pred_yes_sb_no" else (1 - sb_prob),
                    100  # $100 default
                )

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
                if confidence < 0.6:
                    resolution_risk = "high"
                    risk_note = "Low match confidence — verify markets reference the same event and conditions"
                elif confidence < 0.8:
                    resolution_risk = "medium"
                    risk_note = "Moderate match confidence — check resolution criteria on both platforms"
                elif source != "sportsbook":
                    resolution_risk = "low"
                    risk_note = "Different platforms may use different data sources for settlement"

                opp = {
                    "id": hashlib.md5(f"{pred.get('id','')}-{sb.get('bookmaker','')}-{sb.get('outcome_name','')}-{scenario}".encode()).hexdigest()[:12],
                    "sport": sport_display,
                    "event": sb.get("event_name", pred.get("question", "")[:60]),
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
        key = f"{opp['event']}-{opp['platform_a']['name']}-{opp['platform_b']['name']}"
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

    for pm in poly_markets:
        pm_question = pm.get("question", "").lower()
        pm_teams = pm.get("teams", [])
        pm_prices = pm.get("prices", [])

        if len(pm_prices) < 2:
            continue

        for km in kalshi_markets:
            km_question = km.get("question", "").lower()
            km_teams = km.get("teams", [])
            km_prices = km.get("prices", [])

            if len(km_prices) < 2:
                continue

            # Match by teams and text
            team_overlap = len(set(pm_teams) & set(km_teams))
            text_sim = similarity_score(pm_question, km_question)
            score = team_overlap * 0.3 + text_sim * 0.4

            if score < 0.35:
                continue

            # Check: Poly YES + Kalshi NO
            arb1 = compute_arb_binary(pm_prices[0], km_prices[1], POLYMARKET_FEE, KALSHI_FEE)
            # Check: Poly NO + Kalshi YES
            arb2 = compute_arb_binary(pm_prices[1], km_prices[0], POLYMARKET_FEE, KALSHI_FEE)

            for arb, scenario in [(arb1, "poly_yes_kalshi_no"), (arb2, "poly_no_kalshi_yes")]:
                if arb is None or arb["gross_arb_pct"] <= 0:
                    continue
                if arb["net_arb_pct"] < min_net_pct:
                    continue

                if scenario == "poly_yes_kalshi_no":
                    pa_side = pm.get("outcomes", ["Yes"])[0]
                    pa_price = pm_prices[0]
                    pb_side = km.get("outcomes", ["", "No"])[1]
                    pb_price = km_prices[1]
                else:
                    pa_side = pm.get("outcomes", ["", "No"])[1]
                    pa_price = pm_prices[1]
                    pb_side = km.get("outcomes", ["Yes"])[0]
                    pb_price = km_prices[0]

                stakes = compute_stake_allocation(pa_price, pb_price, 100)

                opp = {
                    "id": hashlib.md5(f"cross-{pm.get('id','')}-{km.get('id','')}-{scenario}".encode()).hexdigest()[:12],
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
                    "resolution_risk": "medium" if score < 0.6 else "low",
                    "risk_note": "Cross-platform prediction market arb — verify both markets resolve on the same criteria",
                    "is_prop": False,
                    "liquidity": pm.get("liquidity", 0),
                    "volume": pm.get("volume", 0),
                }
                opportunities.append(opp)

    return sorted(opportunities, key=lambda x: x['net_arb_pct'], reverse=True)

# ─── Generate demo data for testing ──────────────────────────────────────────

def generate_demo_opportunities():
    """Generate realistic demo data when APIs return no results or no API key."""
    import random
    random.seed(int(time.time()) // 60)  # Changes every minute

    events = [
        {
            "sport": "NBA", "event": "Celtics @ Lakers",
            "detail": "Boston Celtics vs Los Angeles Lakers - Moneyline",
            "type": "h2h", "commence": (datetime.now(timezone.utc) + timedelta(hours=random.randint(1, 48))).isoformat(),
        },
        {
            "sport": "NBA", "event": "Nuggets @ Warriors",
            "detail": "Denver Nuggets vs Golden State Warriors - Total Points Over/Under 224.5",
            "type": "totals", "commence": (datetime.now(timezone.utc) + timedelta(hours=random.randint(2, 72))).isoformat(),
        },
        {
            "sport": "NBA", "event": "Mavericks @ 76ers",
            "detail": "Luka Doncic Over 28.5 Points",
            "type": "player_points", "commence": (datetime.now(timezone.utc) + timedelta(hours=random.randint(1, 24))).isoformat(),
        },
        {
            "sport": "NFL", "event": "Chiefs @ Bills",
            "detail": "Kansas City Chiefs vs Buffalo Bills - Spread -3.5",
            "type": "spreads", "commence": (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 7))).isoformat(),
        },
        {
            "sport": "NHL", "event": "Bruins @ Rangers",
            "detail": "Boston Bruins vs New York Rangers - Moneyline",
            "type": "h2h", "commence": (datetime.now(timezone.utc) + timedelta(hours=random.randint(3, 48))).isoformat(),
        },
        {
            "sport": "MLB", "event": "Yankees @ Dodgers",
            "detail": "New York Yankees vs Los Angeles Dodgers - Total Runs Over/Under 8.5",
            "type": "totals", "commence": (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 5))).isoformat(),
        },
        {
            "sport": "NBA", "event": "Bucks @ Knicks",
            "detail": "Giannis Antetokounmpo Over 31.5 Points",
            "type": "player_points", "commence": (datetime.now(timezone.utc) + timedelta(hours=random.randint(4, 36))).isoformat(),
        },
        {
            "sport": "Soccer", "event": "Arsenal @ Liverpool",
            "detail": "Arsenal vs Liverpool - Match Result",
            "type": "h2h", "commence": (datetime.now(timezone.utc) + timedelta(days=random.randint(1, 10))).isoformat(),
        },
        {
            "sport": "MMA", "event": "UFC 310 Main Event",
            "detail": "Fighter A vs Fighter B - Winner",
            "type": "h2h", "commence": (datetime.now(timezone.utc) + timedelta(days=random.randint(5, 20))).isoformat(),
        },
        {
            "sport": "NBA", "event": "Thunder @ Cavaliers",
            "detail": "Shai Gilgeous-Alexander Over 30.5 Points",
            "type": "player_points", "commence": (datetime.now(timezone.utc) + timedelta(hours=random.randint(2, 48))).isoformat(),
        },
        {
            "sport": "NFL", "event": "Eagles @ Cowboys",
            "detail": "Philadelphia Eagles vs Dallas Cowboys - Moneyline",
            "type": "h2h", "commence": (datetime.now(timezone.utc) + timedelta(days=random.randint(2, 8))).isoformat(),
        },
        {
            "sport": "NBA", "event": "Heat @ Suns",
            "detail": "Miami Heat vs Phoenix Suns - Spread +4.5",
            "type": "spreads", "commence": (datetime.now(timezone.utc) + timedelta(hours=random.randint(6, 48))).isoformat(),
        },
    ]

    platforms_a = [
        ("Polymarket", 0.02), ("Kalshi", 0.012),
    ]
    platforms_b = [
        ("DraftKings", 0), ("FanDuel", 0), ("BetRivers", 0),
        ("Pinnacle", 0), ("BetMGM", 0),
    ]

    opportunities = []
    for i, ev in enumerate(events):
        pa = platforms_a[random.randint(0, len(platforms_a) - 1)]
        pb = platforms_b[random.randint(0, len(platforms_b) - 1)]

        # Generate realistic prices that create arb opportunities
        # For an arb: prob_a + prob_b < 1.0
        base_prob = random.uniform(0.30, 0.70)
        arb_margin = random.uniform(0.01, 0.08)  # 1-8% gross arb
        prob_a = round(base_prob, 4)
        prob_b = round(1.0 - base_prob - arb_margin, 4)

        if prob_b <= 0.05 or prob_b >= 0.95:
            continue

        gross_arb = round((1.0 - prob_a - prob_b) * 100, 3)
        net_cost = (prob_a + (1 - prob_a) * pa[1]) + (prob_b + (1 - prob_b) * pb[1])
        net_arb = round((1.0 - net_cost) * 100, 3)

        commence = ev["commence"]
        try:
            event_time = datetime.fromisoformat(commence.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            delta = event_time - now
            if delta.days > 0:
                time_display = f"{delta.days}d"
            elif delta.seconds > 3600:
                time_display = f"{delta.seconds // 3600}h"
            else:
                time_display = f"{delta.seconds // 60}m"
        except:
            time_display = ""

        sides = ["Yes", "No"] if random.random() > 0.5 else ["Over", "Under"]

        stakes_a = round(100 * prob_a, 2)
        stakes_b = round(100 * prob_b, 2)

        opp = {
            "id": hashlib.md5(f"demo-{i}-{int(time.time()) // 60}".encode()).hexdigest()[:12],
            "sport": ev["sport"],
            "event": ev["event"],
            "event_detail": ev["detail"],
            "commence_time": commence,
            "time_display": time_display,
            "is_live": random.random() < 0.15,
            "platform_a": {
                "name": pa[0],
                "side": sides[0],
                "price": prob_a,
                "implied_prob": prob_a,
                "american_odds": implied_prob_to_american(prob_a),
                "fee_pct": pa[1] * 100,
                "url": f"https://{'polymarket.com' if pa[0] == 'Polymarket' else 'kalshi.com'}/markets/demo",
                "market_id": f"demo_{i}_a",
            },
            "platform_b": {
                "name": pb[0],
                "side": sides[1],
                "price": implied_prob_to_american(prob_b),
                "implied_prob": prob_b,
                "american_odds": implied_prob_to_american(prob_b),
                "fee_pct": 0,
                "url": "",
                "market_id": f"demo_{i}_b",
            },
            "market_type": ev["type"],
            "gross_arb_pct": gross_arb,
            "net_arb_pct": net_arb,
            "stakes": {
                "stake_a": stakes_a,
                "stake_b": stakes_b,
                "total_staked": round(stakes_a + stakes_b, 2),
                "payout": 100.0,
                "guaranteed_profit": round(100 - stakes_a - stakes_b, 2),
            },
            "match_confidence": round(random.uniform(0.6, 0.98), 2),
            "resolution_risk": random.choice(["low", "low", "medium"]),
            "risk_note": "Demo data — connect your API keys for live market scanning",
            "is_prop": ev["type"].startswith("player"),
            "liquidity": random.randint(5000, 500000),
            "volume": random.randint(10000, 2000000),
        }
        if opp["is_live"]:
            opp["time_display"] = "LIVE"
        opportunities.append(opp)

    return sorted(opportunities, key=lambda x: x["net_arb_pct"], reverse=True)


# ─── Main handler ─────────────────────────────────────────────────────────────

def main():
    print("Content-Type: application/json")
    print()

    db = get_db()
    query_string = os.environ.get("QUERY_STRING", "")
    params = dict(urllib.parse.parse_qsl(query_string))

    min_net_pct = float(params.get("min_pct", "-999"))
    sports_filter = params.get("sports", "").split(",") if params.get("sports") else []
    demo_mode = params.get("demo", "false").lower() == "true"

    api_key = params.get("api_key", "") or get_config(db, "odds_api_key", "")

    scan_start = time.time()
    errors = []
    sources_status = {
        "polymarket": "pending",
        "kalshi": "pending",
        "sportsbook": "pending" if api_key else "no_key",
    }

    all_opportunities = []

    if demo_mode or not api_key:
        # Use demo data
        all_opportunities = generate_demo_opportunities()
        sources_status = {
            "polymarket": "demo",
            "kalshi": "demo",
            "sportsbook": "demo" if not api_key else "no_key",
        }
    else:
        # Parallel fetch from all sources
        poly_markets = []
        kalshi_markets = []
        sportsbook_entries = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_poly = executor.submit(fetch_polymarket_sports, None)
            future_kalshi = executor.submit(fetch_kalshi_sports, None)
            future_sb = executor.submit(fetch_sportsbook_odds, None, api_key)

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

            try:
                sportsbook_entries = future_sb.result(timeout=15)
                sources_status["sportsbook"] = "ok" if sportsbook_entries else "empty"
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

        # If no live results, fall back to demo
        if not all_opportunities:
            all_opportunities = generate_demo_opportunities()
            for s in sources_status:
                if sources_status[s] in ("ok", "empty"):
                    sources_status[s] = "ok_no_arbs"

    # Apply sports filter
    if sports_filter and sports_filter[0]:
        sports_set = set(s.upper() for s in sports_filter)
        all_opportunities = [o for o in all_opportunities if o["sport"].upper() in sports_set]

    # Sort by net arb %
    all_opportunities.sort(key=lambda x: x["net_arb_pct"], reverse=True)

    scan_duration = round(time.time() - scan_start, 2)

    response = {
        "opportunities": all_opportunities,
        "meta": {
            "scan_time": scan_duration,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_opportunities": len(all_opportunities),
            "sources": sources_status,
            "errors": errors,
            "is_demo": demo_mode or not api_key or (not any(s == "ok" for s in sources_status.values())),
            "poly_count": len(poly_markets) if 'poly_markets' in dir() else 0,
            "kalshi_count": len(kalshi_markets) if 'kalshi_markets' in dir() else 0,
            "sportsbook_count": len(sportsbook_entries) if 'sportsbook_entries' in dir() else 0,
        }
    }

    print(json.dumps(response))

if __name__ == "__main__":
    main()
