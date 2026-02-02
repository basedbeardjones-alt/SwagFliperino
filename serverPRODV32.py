# server.py
# SwagFlip Brain — local copilot server for RuneLite plugin
# - Suggestion engine + SQLite ledger (lots/trades/recs)
# - Auto-migrates SQLite schema in-place (keeps history)
# - Implements /prices and profit-tracking endpoints with the binary/msgpack formats the plugin expects

import os
import time
import math
import json
import threading
import sqlite3
import uuid
import struct
import zlib
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Set, Tuple

from html import escape as html_escape

import requests
import msgpack
from flask import Flask, request, jsonify, Response, g

def _now() -> int:
    return int(time.time())


# ============================================================
# CONFIG
# ============================================================
HOST = os.getenv("SWAGFLIP_BIND_HOST", "127.0.0.1")
PORT = int(os.getenv("SWAGFLIP_PORT", "5000"))

PRICES_BASE = "https://prices.runescape.wiki/api/v1/osrs"
USER_AGENT = os.getenv("SWAGFLIP_USER_AGENT", "SwagFlipBrain/final")
REFRESH_SECONDS = int(os.getenv("SWAGFLIP_REFRESH_SECONDS", "60"))

DEBUG_REJECTIONS = os.getenv("SWAGFLIP_DEBUG_REJECTIONS", "true").lower() == "true"
USE_OSRSBOX = os.getenv("SWAGFLIP_USE_OSRSBOX", "true").lower() == "true"

# Trading constraints
MAX_CASH_FRACTION = float(os.getenv("SWAGFLIP_MAX_CASH_FRACTION", "0.90"))
BUY_BUDGET_CAP = int(os.getenv("SWAGFLIP_BUY_BUDGET_CAP", "10000000"))
TARGET_FILL_MINUTES = float(os.getenv("SWAGFLIP_TARGET_FILL_MINUTES", "5.0"))

# Trend-assisted scoring (used for 30m/2h/8h horizons)
ENABLE_TRENDS = os.getenv("SWAGFLIP_ENABLE_TRENDS", "1") not in ("0", "false", "False")
TREND_CACHE_TTL_SECONDS = int(os.getenv("SWAGFLIP_TREND_CACHE_TTL", "180"))
TREND_RECHECK_TOP_N = int(os.getenv("SWAGFLIP_TREND_TOP_N", "20"))

MIN_BUY_PRICE = int(os.getenv("SWAGFLIP_MIN_BUY_PRICE", "1"))
MIN_MARGIN_GP = int(os.getenv("SWAGFLIP_MIN_MARGIN_GP", "1"))
MIN_ROI = float(os.getenv("SWAGFLIP_MIN_ROI", "0.0005"))
MAX_ROI = float(os.getenv("SWAGFLIP_MAX_ROI", "0.40"))

MIN_DAILY_VOLUME = int(os.getenv("SWAGFLIP_MIN_DAILY_VOLUME", "10000"))
MAX_DAILY_VOLUME = int(os.getenv("SWAGFLIP_MAX_DAILY_VOLUME", "1000000000"))

# Tax (server-side profit estimates for crash guard + lots PnL)
SELLER_TAX_RATE = float(os.getenv("SWAGFLIP_SELLER_TAX_RATE", "0.02"))


# GE tax logic (match plugin util.GeTax)
MAX_PRICE_FOR_GE_TAX = 250000000
GE_TAX_CAP = 5000000
# Items exempt list copied from plugin util.GeTax (IDs)
GE_TAX_EXEMPT_ITEMS: Set[int] = {
    8011, 365, 2309, 882, 806, 1891, 8010, 1755, 28824, 2140, 2142, 8009, 5325, 1785, 2347, 347,
    884, 807, 28790, 379, 8008, 355, 2327, 558, 1733, 13190, 233, 351, 5341, 2552, 329, 8794, 5329,
    5343, 1735, 315, 952, 886, 808, 8013, 361, 8007, 5331
}

def ge_post_tax_price(item_id: int, price: int) -> int:
    """Post-tax price per item (matches the plugin's GeTax)."""
    if price <= 0:
        return 0
    iid = int(item_id)
    p = int(price)
    if iid in GE_TAX_EXEMPT_ITEMS:
        return p
    if p >= MAX_PRICE_FOR_GE_TAX:
        return max(p - GE_TAX_CAP, 0)
    tax = int(math.floor(p * SELLER_TAX_RATE))
    return max(p - tax, 0)

def ge_tax_per_unit(item_id: int, price: int) -> int:
    return max(int(price) - ge_post_tax_price(item_id, price), 0)

# Buy-limit window (GE reset): typically 4h
BUY_LIMIT_RESET_SECONDS = int(os.getenv("SWAGFLIP_BUY_LIMIT_RESET_SECONDS", str(4 * 60 * 60)))

# If item has NO recorded cost basis, sell fast using low price and require est under this
FAST_SELL_TARGET_MINUTES = float(os.getenv("SWAGFLIP_FAST_SELL_TARGET_MINUTES", "2.0"))

# Crash-guard aggressiveness
CRASH_PROFIT_PER_ITEM_MIN = int(os.getenv("SWAGFLIP_CRASH_PROFIT_PER_ITEM_MIN", "3"))
REPRICE_OVER_MARKET_PCT = float(os.getenv("SWAGFLIP_REPRICE_OVER_MARKET_PCT", "0.02"))

# Recommendation "failed buy" timeout (seconds)
BUY_REC_TIMEOUT_SECONDS = int(os.getenv("SWAGFLIP_BUY_REC_TIMEOUT_SECONDS", str(20 * 60)))  # 20 min

# Abort feature knobs
ABORT_COOLDOWN_SECONDS = int(os.getenv("SWAGFLIP_ABORT_COOLDOWN_SECONDS", "120"))
STUCK_BUY_ABORT_SECONDS = int(os.getenv("SWAGFLIP_STUCK_BUY_ABORT_SECONDS", str(20 * 60)))

# NEW: Stale offer inactivity window
STALE_OFFER_SECONDS = int(os.getenv("SWAGFLIP_STALE_OFFER_SECONDS", "120"))  # 2 minutes

# Queue file (only queue; all trade history is in SQLite)
LEDGER_PATH = os.getenv("SWAGFLIP_LEDGER_PATH", "ledger.json")

# SQLite DB (durable dataset)
DB_PATH = os.getenv("SWAGFLIP_DB_PATH", "swagflip.db")

# Optional token if you expose beyond localhost
DASH_TOKEN = os.getenv("SWAGFLIP_DASH_TOKEN", "").strip()

# Logging
LOG_PATH = os.getenv("SWAGFLIP_LOG_PATH", "swagflip.log")
LOG_LEVEL = os.getenv("SWAGFLIP_LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = int(os.getenv("SWAGFLIP_LOG_MAX_BYTES", str(5 * 1024 * 1024)))
LOG_BACKUPS = int(os.getenv("SWAGFLIP_LOG_BACKUPS", "5"))

# ============================================================
# FLASK + HTTP
# ============================================================
app = Flask(__name__)

# Logging setup
logger = logging.getLogger("swagflip")

def configure_logging(flask_app: Flask) -> None:
    lvl = getattr(logging, LOG_LEVEL, logging.INFO)
    logger.setLevel(lvl)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")

    fh = RotatingFileHandler(LOG_PATH, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(lvl)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(lvl)

    # Avoid duplicates if reloaded
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logger.addHandler(fh)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(sh)

    flask_app.logger.handlers = logger.handlers
    flask_app.logger.setLevel(lvl)

    werk = logging.getLogger("werkzeug")
    werk.handlers = logger.handlers
    werk.setLevel(lvl)

configure_logging(app)

@app.before_request
def _req_start():
    g._t0 = time.time()
    g.request_id = uuid.uuid4().hex[:10]

@app.after_request
def _req_done(resp):
    try:
        dt_ms = int((time.time() - getattr(g, "_t0", time.time())) * 1000)
        rid = getattr(g, "request_id", "-")
        resp.headers["X-Request-ID"] = rid
        logger.info(f"{rid} {request.method} {request.path} -> {resp.status_code} ({dt_ms}ms)")
    except Exception:
        pass
    return resp

# HTTP session
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

# ============================================================
# LAST STATUS SNAPSHOT (dashboard "current offers")
# ============================================================
_last_status_lock = threading.Lock()
LAST_STATUS: Dict[str, Any] = {
    "updated_unix": 0,
    "offers": [],
    "items": [],
    "coins": 0,
    "inv": [],
}

def update_last_status(status: Dict[str, Any]) -> None:
    offers = status.get("offers") or []
    items = status.get("items") or []
    coins = 0
    inv: List[Tuple[int, int]] = []
    for it in items:
        try:
            iid = int(it.get("item_id", 0))
            amt = int(it.get("amount", 0))
            if iid == 995:
                coins += amt
            elif iid > 0 and amt > 0:
                inv.append((iid, amt))
        except Exception:
            continue
    with _last_status_lock:
        LAST_STATUS["updated_unix"] = int(time.time())
        LAST_STATUS["offers"] = offers
        LAST_STATUS["items"] = items
        LAST_STATUS["coins"] = coins
        LAST_STATUS["inv"] = inv

# ============================================================
# MODELS + PRICE CACHE
# ============================================================
@dataclass
class ItemMeta:
    name: str
    limit: Optional[int]

class PriceCache:
    def __init__(self):
        self._lock = threading.Lock()
        self.mapping: Dict[int, ItemMeta] = {}
        self.latest: Dict[int, Dict[str, Any]] = {}
        self.volumes: Dict[int, int] = {}
        self.last_refresh: float = 0.0

    def refresh_forever(self):
        while True:
            try:
                if not self.mapping:
                    self._fetch_mapping()
                self._fetch_latest()
                self._fetch_volumes()
                with self._lock:
                    self.last_refresh = time.time()
            except Exception:
                logger.exception("[PRICES] refresh error")
            time.sleep(REFRESH_SECONDS)

    def _fetch_mapping(self):
        if USE_OSRSBOX:
            try:
                from osrsbox import items_api
                all_items = items_api.load()
                m: Dict[int, ItemMeta] = {}
                for it in all_items:
                    try:
                        iid = int(getattr(it, "id", 0))
                        if iid <= 0:
                            continue
                        name = getattr(it, "name", None) or str(iid)
                        lim = getattr(it, "buy_limit", None)
                        m[iid] = ItemMeta(name=name, limit=int(lim) if lim is not None else None)
                    except Exception:
                        continue
                with self._lock:
                    self.mapping = m
                logger.info(f"[PRICES] OSRSBox mapping loaded: {len(m)} items")
                return
            except Exception as e:
                logger.warning(f"[PRICES] OSRSBox mapping failed ({e}), falling back to wiki mapping")

        r = session.get(f"{PRICES_BASE}/mapping", timeout=15)
        r.raise_for_status()
        payload = r.json()
        if isinstance(payload, dict):
            items = payload.get("data", []) or []
        elif isinstance(payload, list):
            items = payload
        else:
            items = []

        m: Dict[int, ItemMeta] = {}
        for it in items:
            try:
                iid = int(it.get("id"))
                name = it.get("name") or str(iid)
                lim = it.get("limit")
                m[iid] = ItemMeta(name=name, limit=int(lim) if lim is not None else None)
            except Exception:
                continue

        with self._lock:
            self.mapping = m
        logger.info(f"[PRICES] wiki mapping loaded: {len(m)} items")

    def _fetch_latest(self):
        r = session.get(f"{PRICES_BASE}/latest", timeout=15)
        r.raise_for_status()
        data = r.json().get("data", {}) or {}
        latest = {int(k): v for k, v in data.items() if str(k).isdigit()}
        with self._lock:
            self.latest = latest
        logger.info(f"[PRICES] latest refreshed: {len(latest)} items")

    def _fetch_volumes(self):
        r = session.get(f"{PRICES_BASE}/volumes", timeout=15)
        r.raise_for_status()
        data = r.json().get("data", {}) or {}
        vols: Dict[int, int] = {}
        for k, v in data.items():
            try:
                vols[int(k)] = int(v)
            except Exception:
                continue
        with self._lock:
            self.volumes = vols
        logger.info(f"[PRICES] volumes refreshed: {len(vols)} items")

    def snapshot(self):
        with self._lock:
            return dict(self.mapping), dict(self.latest), dict(self.volumes), self.last_refresh

prices = PriceCache()

# ============================================================
# SQLITE (durable dataset) + AUTO-MIGRATION
# ============================================================
_db_lock = threading.Lock()


class TrendCache:
    """Small TTL cache for trend estimates from the prices.wiki /timeseries endpoint.

    We only use it for the top N candidates when timeframe > 5 minutes, so it stays lightweight.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: Dict[Tuple[int, int], Tuple[float, int]] = {}  # (item_id, horizon_min) -> (trend, ts)

    def get(self, item_id: int, horizon_minutes: int) -> float:
        if item_id <= 0 or horizon_minutes <= 0:
            return 0.0

        key = (item_id, horizon_minutes)
        now = _now()

        with self._lock:
            hit = self._cache.get(key)
            if hit and (now - hit[1] < TREND_CACHE_TTL_SECONDS):
                return float(hit[0])

        trend = 0.0
        try:
            # 5-minute timestep gives enough resolution for 30m/2h/8h horizons.
            r = requests.get(
                f"{PRICES_BASE}/timeseries",
                params={"timestep": "5m", "id": str(item_id)},
                headers={"User-Agent": USER_AGENT},
                timeout=10,
            )
            if r.status_code == 200:
                js = r.json() or {}
                data = js.get("data") or []
                if isinstance(data, list) and len(data) >= 2:
                    points_needed = max(2, int(horizon_minutes / 5) + 1)
                    window = data[-points_needed:] if len(data) > points_needed else data

                    def mid(p: Dict[str, Any]) -> float:
                        # Known keys: avgHighPrice / avgLowPrice (prices.wiki), with some wrappers using snake_case.
                        ah = p.get("avgHighPrice", p.get("avg_high_price", 0)) or 0
                        al = p.get("avgLowPrice", p.get("avg_low_price", 0)) or 0
                        try:
                            ah = float(ah)
                            al = float(al)
                        except Exception:
                            return 0.0
                        if ah <= 0 and al <= 0:
                            return 0.0
                        if ah <= 0:
                            return al
                        if al <= 0:
                            return ah
                        return (ah + al) / 2.0

                    m0 = mid(window[0])
                    m1 = mid(window[-1])
                    if m0 > 0 and m1 > 0:
                        trend = (m1 - m0) / m0
        except Exception:
            trend = 0.0

        # clamp to avoid insane influence from bad data
        if trend > 0.25:
            trend = 0.25
        elif trend < -0.25:
            trend = -0.25

        with self._lock:
            self._cache[key] = (float(trend), now)

        return float(trend)


TREND_CACHE = TrendCache()

def db_connect() -> sqlite3.Connection:
    # check_same_thread=False keeps sqlite happy if used across flask threads
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception as e:
        logger.warning(f"[DB] WAL not available, falling back to DELETE mode: {e}")
        conn.execute("PRAGMA journal_mode=DELETE;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn

def _columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        return {str(r[1]) for r in cur.fetchall()}  # r[1] = column name
    except Exception:
        return set()

def _ensure_table(conn: sqlite3.Connection, create_sql: str) -> None:
    conn.execute(create_sql)

def _ensure_column(conn: sqlite3.Connection, table: str, col: str, col_def: str) -> None:
    cols = _columns(conn, table)
    if col in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
    logger.info(f"[DB] migrated: added column {table}.{col} {col_def}")

def db_init() -> None:
    """
    Creates any missing tables, and migrates older swagflip.db schemas in-place
    by adding newly-required columns. Keeps all history.
    """
    with _db_lock:
        conn = db_connect()
        try:
            cur = conn.cursor()

            # --- Create tables if missing (safe even if they exist) ---
            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS lots (
                    tx_id TEXT PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    buy_price INTEGER NOT NULL,
                    qty_total INTEGER NOT NULL,
                    qty_remaining INTEGER NOT NULL,
                    buy_ts INTEGER NOT NULL,
                    buy_offer_id INTEGER,
                    buy_rec_id TEXT
                )
            """)

            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS offer_instances (
                    offer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    box_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    amount_total INTEGER NOT NULL,
                    start_ts INTEGER NOT NULL,
                    first_fill_ts INTEGER,
                    done_ts INTEGER,
                    last_seen_ts INTEGER NOT NULL,
                    last_traded INTEGER NOT NULL,
                    last_trade_ts INTEGER,
                    active INTEGER NOT NULL,
                    rec_id TEXT
                )
            """)

            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS buy_fills (
                    fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    buy_price INTEGER NOT NULL,
                    fill_ts INTEGER NOT NULL,
                    offer_id INTEGER,
                    rec_id TEXT
                )
            """)

            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS realized_trades (
                    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    buy_price INTEGER NOT NULL,
                    sell_price INTEGER NOT NULL,
                    buy_ts INTEGER NOT NULL,
                    sell_ts INTEGER NOT NULL,
                    profit INTEGER NOT NULL,
                    sell_offer_id INTEGER,
                    sell_rec_id TEXT,
                    buy_rec_id TEXT
                )
            """)

            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS recommendations (
                    rec_id TEXT PRIMARY KEY,
                    issued_ts INTEGER NOT NULL,
                    rec_type TEXT NOT NULL,
                    box_id INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    expected_profit REAL NOT NULL,
                    expected_duration REAL NOT NULL,
                    note TEXT,
                    linked_offer_id INTEGER,
                    outcome_status TEXT NOT NULL,
                    buy_first_fill_ts INTEGER,
                    buy_done_ts INTEGER,
                    buy_phase_seconds INTEGER,
                    first_sell_ts INTEGER,
                    last_sell_ts INTEGER,
                    sell_phase_seconds INTEGER,
                    realized_profit INTEGER,
                    realized_cost INTEGER,
                    realized_roi REAL,
                    realized_vs_expected REAL,
                    closed_ts INTEGER
                )
            """)

            # --- Profit tracking tables (durable) ---
            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS pt_accounts (
                    display_name TEXT PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    created_ts INTEGER NOT NULL
                )
            """)

            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS pt_flips (
                    flip_uuid TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    account_id INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    opened_time INTEGER NOT NULL,
                    opened_qty INTEGER NOT NULL,
                    spent INTEGER NOT NULL,
                    closed_time INTEGER NOT NULL,
                    closed_qty INTEGER NOT NULL,
                    received_post_tax INTEGER NOT NULL,
                    profit INTEGER NOT NULL,
                    tax_paid INTEGER NOT NULL,
                    status_ord INTEGER NOT NULL,
                    updated_time INTEGER NOT NULL,
                    deleted INTEGER NOT NULL
                )
            """)

            _ensure_table(conn, """
                CREATE TABLE IF NOT EXISTS pt_transactions (
                    tx_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    account_id INTEGER NOT NULL,
                    flip_uuid TEXT NOT NULL,
                    time INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    box_id INTEGER NOT NULL,
                    amount_spent INTEGER NOT NULL,
                    was_copilot_suggestion INTEGER NOT NULL,
                    copilot_price_used INTEGER NOT NULL,
                    login INTEGER NOT NULL,
                    raw_json TEXT
                )
            """)

            try:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pt_flips_account_updated ON pt_flips(account_id, updated_time)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pt_tx_display_time ON pt_transactions(display_name, time)")
                conn.commit()
            except Exception:
                pass

            # --- Migrate older DBs (add missing columns) ---
            _ensure_column(conn, "lots", "buy_offer_id", "INTEGER")
            _ensure_column(conn, "lots", "buy_rec_id", "TEXT")

            _ensure_column(conn, "buy_fills", "offer_id", "INTEGER")
            _ensure_column(conn, "buy_fills", "rec_id", "TEXT")

            _ensure_column(conn, "realized_trades", "sell_offer_id", "INTEGER")
            _ensure_column(conn, "realized_trades", "sell_rec_id", "TEXT")
            _ensure_column(conn, "realized_trades", "buy_rec_id", "TEXT")

            _ensure_column(conn, "offer_instances", "rec_id", "TEXT")
            _ensure_column(conn, "offer_instances", "last_trade_ts", "INTEGER")

            _ensure_column(conn, "recommendations", "linked_offer_id", "INTEGER")
            _ensure_column(conn, "recommendations", "buy_first_fill_ts", "INTEGER")
            _ensure_column(conn, "recommendations", "buy_done_ts", "INTEGER")
            _ensure_column(conn, "recommendations", "buy_phase_seconds", "INTEGER")
            _ensure_column(conn, "recommendations", "first_sell_ts", "INTEGER")
            _ensure_column(conn, "recommendations", "last_sell_ts", "INTEGER")
            _ensure_column(conn, "recommendations", "sell_phase_seconds", "INTEGER")
            _ensure_column(conn, "recommendations", "realized_profit", "INTEGER")
            _ensure_column(conn, "recommendations", "realized_cost", "INTEGER")
            _ensure_column(conn, "recommendations", "realized_roi", "REAL")
            _ensure_column(conn, "recommendations", "realized_vs_expected", "REAL")
            _ensure_column(conn, "recommendations", "closed_ts", "INTEGER")
            _ensure_column(conn, "recommendations", "note", "TEXT")

            # Profit tracking schema migrations
            _ensure_column(conn, "pt_transactions", "tx_id", "TEXT")
            _ensure_column(conn, "pt_transactions", "display_name", "TEXT")
            _ensure_column(conn, "pt_transactions", "account_id", "INTEGER")
            _ensure_column(conn, "pt_transactions", "flip_uuid", "TEXT")
            _ensure_column(conn, "pt_transactions", "time", "INTEGER")
            _ensure_column(conn, "pt_transactions", "item_id", "INTEGER")
            _ensure_column(conn, "pt_transactions", "quantity", "INTEGER")
            _ensure_column(conn, "pt_transactions", "price", "INTEGER")
            _ensure_column(conn, "pt_transactions", "box_id", "INTEGER")
            _ensure_column(conn, "pt_transactions", "amount_spent", "INTEGER")
            _ensure_column(conn, "pt_transactions", "was_copilot_suggestion", "INTEGER")
            _ensure_column(conn, "pt_transactions", "copilot_price_used", "INTEGER")
            _ensure_column(conn, "pt_transactions", "login", "INTEGER")
            _ensure_column(conn, "pt_transactions", "raw_json", "TEXT")


            # --- Indexes (safe) ---
            cur.execute("CREATE INDEX IF NOT EXISTS idx_offer_open ON offer_instances(box_id, done_ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_offer_item ON offer_instances(item_id, done_ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_buyfills_item_ts ON buy_fills(item_id, fill_ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_sellts ON realized_trades(sell_ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_recs_item_ts ON recommendations(item_id, issued_ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_recs_type_box_ts ON recommendations(rec_type, box_id, issued_ts)")

            conn.commit()
        finally:
            conn.close()

# ============================================================
# MATH / HELPERS
# ============================================================
def seller_tax(price: int, item_id: Optional[int] = None) -> int:
    """Estimate GE seller tax for profit displays.
    Uses SELLER_TAX_RATE and optional cap (SWAGFLIP_SELLER_TAX_CAP, default 5000000).
    item_id is accepted for compatibility (ignored).
    """
    if price <= 0:
        return 0
    tax = int(price * SELLER_TAX_RATE)
    cap = int(os.getenv("SWAGFLIP_SELLER_TAX_CAP", "5000000"))
    if cap > 0 and tax > cap:
        tax = cap
    return tax

def estimate_minutes_from_daily(qty: int, daily_vol: Optional[int]) -> float:
    if daily_vol is None or daily_vol <= 0:
        return 999999.0
    per_min = max(daily_vol / 1440.0, 1e-6)
    return qty / per_min

def min_profitable_sell_price(avg_buy: int) -> int:
    if avg_buy <= 0:
        return 1
    guess = int(math.ceil((avg_buy + 1) / 0.98))
    start = max(1, guess - 30)
    for p in range(start, guess + 500):
        if p - avg_buy - seller_tax(p) >= 1:
            return p
    return max(guess, 1)

def offer_is_empty(o: Dict[str, Any]) -> bool:
    return str(o.get("status", "")).lower() == "empty"

def offer_is_done(o: Dict[str, Any]) -> bool:
    return (str(o.get("status", "")).lower() != "empty") and (not bool(o.get("active", False)))

def offer_is_buy(o: Dict[str, Any]) -> bool:
    return str(o.get("status", "")).lower() == "buy"

def offer_is_sell(o: Dict[str, Any]) -> bool:
    return str(o.get("status", "")).lower() == "sell"

def first_empty_slot_id(offers: List[Dict[str, Any]]) -> Optional[int]:
    for o in offers:
        if offer_is_empty(o):
            try:
                return int(o.get("box_id", 0))
            except Exception:
                return 0
    return None

def active_offer_item_ids(offers: List[Dict[str, Any]]) -> Set[int]:
    s: Set[int] = set()
    for o in offers:
        if bool(o.get("active", False)):
            try:
                iid = int(o.get("item_id", 0))
                if iid > 0:
                    s.add(iid)
            except Exception:
                pass
    return s

def _requested_types(status: Dict[str, Any]) -> Set[str]:
    try:
        arr = status.get("requested_suggestion_types") or []
        return {str(x).lower() for x in arr if x}
    except Exception:
        return set()

def _type_allowed(requested: Set[str], t: str) -> bool:
    return (not requested) or (t in requested)


def _status_timeframe_minutes(status: Dict[str, Any]) -> int:
    """How often the client wants to adjust offers (minutes).
    RuneLite UI presets: 5, 30, 120, 480 (5m, 30m, 2h, 8h).
    """
    tf = None
    try:
        tf = status.get("timeframe")
    except Exception:
        tf = None

    if tf is None:
        return max(1, int(round(TARGET_FILL_MINUTES)))

    try:
        if isinstance(tf, str):
            s = tf.strip().lower()
            if s.endswith("m"):
                tfm = int(float(s[:-1]))
            elif s.endswith("h"):
                tfm = int(float(s[:-1]) * 60)
            else:
                tfm = int(float(s))
        else:
            tfm = int(tf)
    except Exception:
        tfm = max(1, int(round(TARGET_FILL_MINUTES)))

    if tfm <= 0:
        tfm = max(1, int(round(TARGET_FILL_MINUTES)))

    # clamp to something sane (up to 24h)
    return max(1, min(tfm, 24 * 60))


# ============================================================
# LEDGER (queue only)
# ============================================================
def load_ledger() -> Dict[str, Any]:
    if not os.path.exists(LEDGER_PATH):
        return {"buy_queue": []}
    try:
        with open(LEDGER_PATH, "r", encoding="utf-8") as f:
            led = json.load(f)
        led.setdefault("buy_queue", [])
        return led
    except Exception:
        return {"buy_queue": []}

def save_ledger(ledger: Dict[str, Any]) -> None:
    tmp = LEDGER_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ledger, f, indent=2)
    os.replace(tmp, LEDGER_PATH)

# ============================================================
# COMMAND BUILDERS (internal JSON shape)
# ============================================================
def build_wait(msg: str) -> Dict[str, Any]:
    return {
        "type": "wait",
        "box_id": 0,
        "item_id": -1,
        "price": 0,
        "quantity": 0,
        "name": "",
        "command_id": 0,
        "message": msg,
        "expectedProfit": 0.0,
        "expectedDuration": 0.0,
    }

def build_buy(box_id: int, item_id: int, name: str, price: int, qty: int,
              exp_profit: float, exp_min: float, note: str = "") -> Dict[str, Any]:
    rec_id = uuid.uuid4().hex
    msg = f"Buy {qty} {name} @ {price}"
    if note:
        msg += f" — {note}"
    return {
        "type": "buy",
        "rec_id": rec_id,
        "issued_unix": int(time.time()),
        "box_id": int(box_id),
        "item_id": int(item_id),
        "price": int(price),
        "quantity": int(qty),
        "name": name,
        "command_id": 1,
        "message": msg,
        "expectedProfit": float(exp_profit),
        "expectedDuration": float(exp_min),
        "note": note,
    }

def build_sell(box_id: int, item_id: int, name: str, price: int, qty: int,
                exp_profit: float, exp_min: float, note: str = "") -> Dict[str, Any]:
    rec_id = uuid.uuid4().hex
    msg = f"Sell {qty} {name} @ {price}"
    if note:
        msg += f" — {note}"
    return {
        "type": "sell",
        "rec_id": rec_id,
        "issued_unix": int(time.time()),
        "box_id": int(box_id),
        "item_id": int(item_id),
        "price": int(price),
        "quantity": int(qty),
        "name": name,
        "command_id": 2,
        "message": msg,
        "expectedProfit": float(exp_profit),
        "expectedDuration": float(exp_min),
        "note": note,
    }

def build_abort(box_id: int, item_id: int, name: str, reason: str) -> Dict[str, Any]:
    rec_id = uuid.uuid4().hex
    msg = f"ABORT slot {box_id}: {reason}"
    return {
        "type": "abort",
        "rec_id": rec_id,
        "issued_unix": int(time.time()),
        "box_id": int(box_id),
        "item_id": int(item_id),
        "price": 0,
        "quantity": 0,
        "name": name,
        "command_id": 3,
        "message": msg,
        "expectedProfit": 0.0,
        "expectedDuration": 0.0,
        "note": reason,
    }

# ============================================================
# MSGPACK ENCODERS FOR THE JAVA CLIENT
# ============================================================
def _wants_msgpack() -> bool:
    accept = (request.headers.get("Accept") or "").lower()
    return "application/x-msgpack" in accept

def _suggestion_to_msgpack(action: Dict[str, Any]) -> bytes:
    """
    Java expects keys: t,b,i,p,q,n,id,m,ed,ep,(gd optional)
    """
    payload = {
        "t": str(action.get("type", "")),
        "b": int(action.get("box_id", 0)),
        "i": int(action.get("item_id", 0)),
        "p": int(action.get("price", 0)),
        "q": int(action.get("quantity", 0)),
        "n": str(action.get("name", "")),
        "id": int(action.get("command_id", 0)),
        "m": str(action.get("message", "")),
        "ed": float(action.get("expectedDuration", 0.0)),
        "ep": float(action.get("expectedProfit", 0.0)),
    }
    return msgpack.packb(payload, use_bin_type=True)

def _item_price_to_msgpack(buy_price: int, sell_price: int, message: str = "") -> bytes:
    """
    Java expects keys: bp, sp, m, (gd optional)
    """
    payload = {
        "bp": int(buy_price),
        "sp": int(sell_price),
        "m": str(message or ""),
        # omit "gd" (graph data) for now; plugin handles missing graph
    }
    return msgpack.packb(payload, use_bin_type=True)

def _visualize_flip_response_to_msgpack() -> bytes:
    """
    Java expects keys: bt,bv,bp,st,sv,sp,(gd optional)
    We'll return empty series so the UI doesn't error.
    """
    payload = {"bt": [], "bv": [], "bp": [], "st": [], "sv": [], "sp": []}
    return msgpack.packb(payload, use_bin_type=True)

# ============================================================
# RECOMMENDATION RECORDING (for ML)
# ============================================================
def record_recommendation(action: Dict[str, Any]) -> None:
    t = str(action.get("type", "")).lower()
    if t not in ("buy", "sell", "abort"):
        return
    rec_id = str(action.get("rec_id", "")).strip()
    if not rec_id:
        return

    issued_ts = int(action.get("issued_unix") or time.time())
    box_id = int(action.get("box_id") or 0)
    item_id = int(action.get("item_id") or 0)
    item_name = str(action.get("name") or item_id)
    price = int(action.get("price") or 0)
    qty = int(action.get("quantity") or 0)
    exp_profit = float(action.get("expectedProfit") or 0.0)
    exp_dur = float(action.get("expectedDuration") or 0.0)
    note = str(action.get("note") or "")

    with _db_lock:
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT OR IGNORE INTO recommendations (
                    rec_id, issued_ts, rec_type, box_id, item_id, item_name, price, qty,
                    expected_profit, expected_duration, note,
                    linked_offer_id, outcome_status,
                    buy_first_fill_ts, buy_done_ts, buy_phase_seconds,
                    first_sell_ts, last_sell_ts, sell_phase_seconds,
                    realized_profit, realized_cost, realized_roi, realized_vs_expected,
                    closed_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'issued',
                          NULL, NULL, NULL, NULL, NULL, NULL,
                          NULL, NULL, NULL, NULL, NULL)
            """, (
                rec_id, issued_ts, t, box_id, item_id, item_name, price, qty,
                exp_profit, exp_dur, note
            ))
            conn.commit()
        finally:
            conn.close()

def should_throttle_abort(conn: sqlite3.Connection, box_id: int, now: int) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT issued_ts
        FROM recommendations
        WHERE rec_type = 'abort' AND box_id = ?
        ORDER BY issued_ts DESC
        LIMIT 1
    """, (int(box_id),))
    row = cur.fetchone()
    if not row:
        return False
    last_ts = int(row["issued_ts"])
    return (now - last_ts) < ABORT_COOLDOWN_SECONDS

# ============================================================
# OFFER INSTANCE + FILL SYNC (tracks actual fills + phases)
# ============================================================
def get_open_offer_instance(conn: sqlite3.Connection, box_id: int) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM offer_instances
        WHERE box_id = ? AND done_ts IS NULL
        ORDER BY offer_id DESC
        LIMIT 1
    """, (int(box_id),))
    return cur.fetchone()

def close_offer_instance(conn: sqlite3.Connection, offer_id: int, now: int) -> None:
    cur = conn.cursor()
    cur.execute("""
        UPDATE offer_instances
        SET done_ts = COALESCE(done_ts, ?), last_seen_ts = ?
        WHERE offer_id = ?
    """, (int(now), int(now), int(offer_id)))

def create_offer_instance(conn: sqlite3.Connection, box_id: int, status: str, item_id: int,
                          price: int, amount_total: int, traded: int, active: bool, now: int) -> int:
    cur = conn.cursor()
    first_fill_ts = int(now) if int(traded) > 0 else None
    last_trade_ts = int(now) if int(traded) > 0 else None
    cur.execute("""
        INSERT INTO offer_instances (
            box_id, status, item_id, price, amount_total,
            start_ts, first_fill_ts, done_ts, last_seen_ts,
            last_traded, last_trade_ts, active, rec_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL)
    """, (
        int(box_id), str(status), int(item_id), int(price), int(amount_total),
        int(now), first_fill_ts,
        int(now), int(traded), last_trade_ts, 1 if active else 0
    ))
    return int(cur.lastrowid)

def maybe_link_offer_to_recent_rec(conn: sqlite3.Connection, status: str,
                                   box_id: int, item_id: int, now: int) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("""
        SELECT rec_id
        FROM recommendations
        WHERE rec_type = ?
          AND box_id = ?
          AND item_id = ?
          AND linked_offer_id IS NULL
          AND outcome_status IN ('issued')
          AND issued_ts >= ?
        ORDER BY issued_ts DESC
        LIMIT 1
    """, (status, int(box_id), int(item_id), int(now) - 15 * 60))
    row = cur.fetchone()
    if not row:
        return None
    return str(row["rec_id"])

def db_insert_lot(conn: sqlite3.Connection, item_id: int, item_name: str, buy_price: int,
                  qty: int, buy_ts: int, buy_offer_id: int, buy_rec_id: Optional[str]) -> None:
    tx = uuid.uuid4().hex
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO lots (tx_id, item_id, item_name, buy_price, qty_total, qty_remaining, buy_ts, buy_offer_id, buy_rec_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (tx, int(item_id), str(item_name), int(buy_price), int(qty), int(qty), int(buy_ts), int(buy_offer_id), buy_rec_id))

def db_insert_buy_fill(conn: sqlite3.Connection, item_id: int, item_name: str, buy_price: int,
                        qty: int, fill_ts: int, offer_id: int, rec_id: Optional[str]) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO buy_fills (item_id, item_name, qty, buy_price, fill_ts, offer_id, rec_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (int(item_id), str(item_name), int(qty), int(buy_price), int(fill_ts), int(offer_id), rec_id))

def db_open_qty_and_avg_cost(conn: sqlite3.Connection, item_id: int) -> Tuple[int, int, int]:
    cur = conn.cursor()
    cur.execute("""
        SELECT
          COALESCE(SUM(qty_remaining), 0) AS q,
          COALESCE(SUM(qty_remaining * buy_price), 0) AS v
        FROM lots
        WHERE item_id = ? AND qty_remaining > 0
    """, (int(item_id),))
    row = cur.fetchone()
    q = int(row["q"]) if row else 0
    v = int(row["v"]) if row else 0
    avg = int(v / q) if q > 0 else 0
    return q, avg, v

def db_consume_lots_fifo_for_sell(conn: sqlite3.Connection, item_id: int, sell_price: int,
                                  sell_qty: int, sell_ts: int, sell_offer_id: int,
                                  sell_rec_id: Optional[str]) -> int:
    cur = conn.cursor()
    qty_left = int(sell_qty)
    consumed = 0

    while qty_left > 0:
        cur.execute("""
            SELECT tx_id, item_name, buy_price, qty_remaining, buy_ts, buy_rec_id
            FROM lots
            WHERE item_id = ? AND qty_remaining > 0
            ORDER BY buy_ts ASC
            LIMIT 1
        """, (int(item_id),))
        lot = cur.fetchone()
        if not lot:
            break

        take = min(qty_left, int(lot["qty_remaining"]))
        if take <= 0:
            break

        buy_price = int(lot["buy_price"])
        buy_ts = int(lot["buy_ts"])
        buy_rec_id = lot["buy_rec_id"]

        tax = seller_tax(sell_price)
        profit_per = sell_price - buy_price - tax
        profit = profit_per * take

        cur.execute("""
            INSERT INTO realized_trades (
                item_id, item_name, qty, buy_price, sell_price, buy_ts, sell_ts, profit,
                sell_offer_id, sell_rec_id, buy_rec_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            int(item_id), str(lot["item_name"]), int(take), int(buy_price),
            int(sell_price), int(buy_ts), int(sell_ts), int(profit),
            int(sell_offer_id), sell_rec_id, buy_rec_id
        ))

        new_rem = int(lot["qty_remaining"]) - take
        if new_rem <= 0:
            cur.execute("DELETE FROM lots WHERE tx_id = ?", (str(lot["tx_id"]),))
        else:
            cur.execute("UPDATE lots SET qty_remaining = ? WHERE tx_id = ?", (int(new_rem), str(lot["tx_id"])))

        qty_left -= take
        consumed += take

    return consumed

def db_bought_qty_in_window(conn: sqlite3.Connection, item_id: int, cutoff_ts: int) -> int:
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(qty), 0) AS s
        FROM buy_fills
        WHERE item_id = ? AND fill_ts >= ?
    """, (int(item_id), int(cutoff_ts)))
    row = cur.fetchone()
    return int(row["s"]) if row else 0

def update_recommendation_outcomes(conn: sqlite3.Connection, now: int) -> None:
    cur = conn.cursor()

    # timeout failures (buy recs with no fill)
    cur.execute("""
        SELECT rec_id, issued_ts
        FROM recommendations
        WHERE rec_type = 'buy'
          AND outcome_status IN ('issued','linked')
          AND (buy_first_fill_ts IS NULL)
    """)
    for row in cur.fetchall():
        rec_id = str(row["rec_id"])
        issued_ts = int(row["issued_ts"])
        if now - issued_ts >= BUY_REC_TIMEOUT_SECONDS:
            cur.execute("""
                UPDATE recommendations
                SET outcome_status = 'failed_no_fill',
                    closed_ts = ?
                WHERE rec_id = ? AND outcome_status IN ('issued','linked')
            """, (int(now), rec_id))

    # rolling realized metrics for buy recs
    cur.execute("""
        SELECT rec_id, expected_profit
        FROM recommendations
        WHERE rec_type = 'buy'
          AND outcome_status NOT LIKE 'failed_%'
          AND outcome_status != 'completed'
    """)
    for r in cur.fetchall():
        rec_id = str(r["rec_id"])
        expected_profit = float(r["expected_profit"] or 0.0)

        cur.execute("SELECT COALESCE(SUM(qty),0) AS q FROM buy_fills WHERE rec_id = ?", (rec_id,))
        bought_qty = int(cur.fetchone()["q"])
        if bought_qty <= 0:
            continue

        cur.execute("SELECT COALESCE(SUM(qty_remaining),0) AS q FROM lots WHERE buy_rec_id = ?", (rec_id,))
        remaining_qty = int(cur.fetchone()["q"])

        cur.execute("""
            SELECT
              COALESCE(SUM(profit), 0) AS p,
              COALESCE(SUM(qty * buy_price), 0) AS c,
              MIN(sell_ts) AS fst,
              MAX(sell_ts) AS lst
            FROM realized_trades
            WHERE buy_rec_id = ?
        """, (rec_id,))
        s = cur.fetchone()
        realized_profit = int(s["p"] or 0)
        realized_cost = int(s["c"] or 0)
        first_sell_ts = int(s["fst"]) if s["fst"] is not None else None
        last_sell_ts = int(s["lst"]) if s["lst"] is not None else None

        realized_roi = (realized_profit / realized_cost) if realized_cost > 0 else None
        realized_vs_expected = (realized_profit / expected_profit) if expected_profit > 0 else None
        sell_phase_seconds = (last_sell_ts - first_sell_ts) if (first_sell_ts and last_sell_ts) else None

        cur.execute("""
            UPDATE recommendations
            SET realized_profit = ?,
                realized_cost = ?,
                realized_roi = ?,
                realized_vs_expected = ?,
                first_sell_ts = ?,
                last_sell_ts = ?,
                sell_phase_seconds = ?
            WHERE rec_id = ?
        """, (
            realized_profit, realized_cost, realized_roi, realized_vs_expected,
            first_sell_ts, last_sell_ts, sell_phase_seconds, rec_id
        ))

        if remaining_qty <= 0 and last_sell_ts is not None:
            cur.execute("""
                UPDATE recommendations
                SET outcome_status = 'completed',
                    closed_ts = COALESCE(closed_ts, ?)
                WHERE rec_id = ? AND outcome_status NOT LIKE 'failed_%'
            """, (int(last_sell_ts), rec_id))

    # buy phase from linked offer_instances (if present)
    cur.execute("""
        SELECT r.rec_id, r.linked_offer_id
        FROM recommendations r
        WHERE r.rec_type = 'buy'
          AND r.linked_offer_id IS NOT NULL
          AND (r.buy_phase_seconds IS NULL)
          AND r.outcome_status NOT LIKE 'failed_%'
    """)
    for row in cur.fetchall():
        rec_id = str(row["rec_id"])
        offer_id = int(row["linked_offer_id"])
        cur.execute("SELECT first_fill_ts, done_ts FROM offer_instances WHERE offer_id = ?", (offer_id,))
        o = cur.fetchone()
        if not o:
            continue
        ff = int(o["first_fill_ts"]) if o["first_fill_ts"] is not None else None
        dn = int(o["done_ts"]) if o["done_ts"] is not None else None
        if ff and dn and dn >= ff:
            cur.execute("""
                UPDATE recommendations
                SET buy_first_fill_ts = COALESCE(buy_first_fill_ts, ?),
                    buy_done_ts = COALESCE(buy_done_ts, ?),
                    buy_phase_seconds = COALESCE(buy_phase_seconds, ?),
                    outcome_status = CASE
                        WHEN outcome_status IN ('linked','issued','buy_started') THEN 'buy_done'
                        ELSE outcome_status
                    END
                WHERE rec_id = ?
            """, (ff, dn, dn - ff, rec_id))
        elif ff and not dn:
            cur.execute("""
                UPDATE recommendations
                SET buy_first_fill_ts = COALESCE(buy_first_fill_ts, ?),
                    outcome_status = CASE
                        WHEN outcome_status IN ('linked','issued') THEN 'buy_started'
                        ELSE outcome_status
                    END
                WHERE rec_id = ?
            """, (ff, rec_id))

def sync_offers_and_fills(mapping: Dict[int, ItemMeta], offers: List[Dict[str, Any]]) -> None:
    now = int(time.time())

    with _db_lock:
        conn = db_connect()
        try:
            cur = conn.cursor()

            for o in offers:
                try:
                    box_id = int(o.get("box_id", 0))
                except Exception:
                    box_id = 0

                status = str(o.get("status", "")).lower()
                active = bool(o.get("active", False))

                if status == "empty":
                    open_inst = get_open_offer_instance(conn, box_id)
                    if open_inst is not None:
                        # If a linked buy offer is cancelled with 0 fills, mark as failed_cancelled
                        rec = open_inst["rec_id"]
                        if rec and str(open_inst["status"]) == "buy" and int(open_inst["last_traded"]) == 0:
                            cur.execute("""
                                UPDATE recommendations
                                SET outcome_status = 'failed_cancelled',
                                    closed_ts = ?
                                WHERE rec_id = ? AND outcome_status NOT LIKE 'failed_%' AND outcome_status != 'completed'
                            """, (int(now), str(rec)))
                        close_offer_instance(conn, int(open_inst["offer_id"]), now)
                    continue

                if status not in ("buy", "sell"):
                    continue

                try:
                    item_id = int(o.get("item_id", 0))
                    price = int(o.get("price", 0))
                    amount_total = int(o.get("amount_total", 0))
                    traded = int(o.get("amount_traded", 0))
                except Exception:
                    continue

                if item_id <= 0 or price <= 0 or amount_total < 0 or traded < 0:
                    continue

                open_inst = get_open_offer_instance(conn, box_id)
                if open_inst is None:
                    offer_id = create_offer_instance(conn, box_id, status, item_id, price, amount_total, traded, active, now)
                else:
                    old_status = str(open_inst["status"])
                    old_item = int(open_inst["item_id"])
                    old_total = int(open_inst["amount_total"])
                    if old_status != status or old_item != item_id or old_total != amount_total:
                        close_offer_instance(conn, int(open_inst["offer_id"]), now)
                        offer_id = create_offer_instance(conn, box_id, status, item_id, price, amount_total, traded, active, now)
                    else:
                        offer_id = int(open_inst["offer_id"])

                cur.execute("SELECT rec_id, last_traded, first_fill_ts FROM offer_instances WHERE offer_id = ?", (offer_id,))
                inst_row = cur.fetchone()
                inst_rec = inst_row["rec_id"] if inst_row else None
                prev_traded = int(inst_row["last_traded"]) if inst_row else 0
                prev_first_fill = inst_row["first_fill_ts"] if inst_row else None

                if inst_rec is None:
                    inst_rec = maybe_link_offer_to_recent_rec(conn, status, box_id, item_id, now)
                    if inst_rec:
                        cur.execute("UPDATE offer_instances SET rec_id = ? WHERE offer_id = ? AND rec_id IS NULL", (inst_rec, offer_id))
                        cur.execute("""
                            UPDATE recommendations
                            SET linked_offer_id = ?, outcome_status = 'linked'
                            WHERE rec_id = ? AND linked_offer_id IS NULL
                        """, (int(offer_id), inst_rec))

                delta = traded - prev_traded
                new_last_trade_ts = now if delta > 0 else None

                if delta > 0:
                    name = mapping[item_id].name if item_id in mapping else str(item_id)
                    if status == "buy":
                        db_insert_lot(conn, item_id, name, price, delta, now, offer_id, inst_rec)
                        db_insert_buy_fill(conn, item_id, name, price, delta, now, offer_id, inst_rec)
                    else:
                        db_consume_lots_fifo_for_sell(conn, item_id, price, delta, now, offer_id, inst_rec)

                first_fill_ts = None
                if prev_first_fill is not None:
                    first_fill_ts = int(prev_first_fill)
                elif traded > 0:
                    first_fill_ts = now

                done_ts = None
                if (not active) or (amount_total > 0 and traded >= amount_total):
                    done_ts = now

                cur.execute("""
                    UPDATE offer_instances
                    SET price = ?,
                        amount_total = ?,
                        first_fill_ts = COALESCE(first_fill_ts, ?),
                        done_ts = COALESCE(done_ts, ?),
                        last_seen_ts = ?,
                        last_traded = ?,
                        last_trade_ts = COALESCE(?, last_trade_ts),
                        active = ?,
                        rec_id = COALESCE(rec_id, ?)
                    WHERE offer_id = ?
                """, (
                    int(price), int(amount_total), first_fill_ts, done_ts,
                    int(now), int(traded),
                    new_last_trade_ts,
                    1 if active else 0, inst_rec, int(offer_id)
                ))

            update_recommendation_outcomes(conn, now)
            conn.commit()
        finally:
            conn.close()

# ============================================================
# CRASH GUARD: reprice active sell offers while still profitable
# ============================================================
def maybe_reprice_active_sell(mapping: Dict[int, ItemMeta], latest: Dict[int, Dict[str, Any]],
                              volumes: Dict[int, int], offers: List[Dict[str, Any]], stale_seconds: int) -> Optional[Dict[str, Any]]:
    """Crash-guard active sells.

    If a SELL offer is priced meaningfully above the current market and it has not traded for the
    user's selected "Adjust offers" timeframe, suggest an abort/relist at a more realistic price
    *as long as profit stays positive*.
    """
    now = _now()

    for o in offers:
        try:
            if not bool(o.get("active", False)):
                continue
            if not offer_is_sell(o):
                continue

            iid = int(o.get("item_id") or 0)
            box_id = int(o.get("box_id") or 0)
            offer_price = int(o.get("price") or 0)
            total = int(o.get("amount_total") or 0)
            traded = int(o.get("amount_traded") or 0)
        except Exception:
            continue

        if iid <= 0 or offer_price <= 0 or box_id <= 0:
            continue

        remaining = max(total - traded, 0)
        if remaining <= 0:
            continue

        # Respect adjust-offers timeframe: don't reprice too often.
        if stale_seconds:
            no_trade_for = 0
            try:
                with _db_lock:
                    conn = db_connect()
                    try:
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT COALESCE(last_trade_ts, start_ts) AS last_ts "
                            "FROM offer_instances WHERE box_id = ? AND done_ts IS NULL "
                            "ORDER BY start_ts DESC LIMIT 1",
                            (box_id,),
                        )
                        row = cur.fetchone()
                        if row and row["last_ts"]:
                            no_trade_for = max(0, now - int(row["last_ts"]))
                    finally:
                        conn.close()
            except Exception:
                no_trade_for = 0

            if no_trade_for < stale_seconds:
                continue

        lp = latest.get(iid) or {}
        try:
            high = int(lp.get("high") or 0)
            low = int(lp.get("low") or 0)
        except Exception:
            continue

        if high <= 0 or low <= 0:
            continue

        name = mapping[iid].name if iid in mapping else str(iid)

        with _db_lock:
            conn = db_connect()
            try:
                open_qty, avg_buy, _ = db_open_qty_and_avg_cost(conn, iid)
            finally:
                conn.close()

        avg_buy = avg_buy or low

        # If we're only slightly above market, don't touch it.
        target_market = max(1, high - 1)
        if offer_price <= target_market + 2:
            continue

        # Nudge down: either near market, or a 1% cut, whichever is higher.
        desired = max(target_market, int(offer_price * 0.99))
        desired = min(desired, offer_price - 1)

        profit_per = desired - avg_buy - seller_tax(desired)
        if profit_per <= 0:
            continue

        return build_abort(
            name=name,
            item_id=iid,
            box_id=box_id,
            note=f"reprice sell -> {desired}gp (crash-guard)",
        )

    return None

def maybe_handle_stale_offers(status_payload: Dict[str, Any],
                              mapping: Dict[int, ItemMeta],
                              latest: Dict[int, Dict[str, Any]],
                              volumes: Dict[int, int],
                              offers: List[Dict[str, Any]],
                              coins: int,
                              inv: List[Tuple[int, int]],
                              inv_full: bool) -> Optional[Dict[str, Any]]:
    now = int(time.time())
    requested = _requested_types(status_payload)
    tf_min = _status_timeframe_minutes(status_payload)
    stale_seconds = max(STALE_OFFER_SECONDS, tf_min * 60)

    for o in offers:
        try:
            if not bool(o.get("active", False)):
                continue
            st = str(o.get("status", "")).lower()
            if st not in ("buy", "sell"):
                continue
            box_id = int(o.get("box_id", 0))
            item_id = int(o.get("item_id", 0))
            price = int(o.get("price", 0))
            total = int(o.get("amount_total", 0))
            traded = int(o.get("amount_traded", 0))
        except Exception:
            continue

        if item_id <= 0 or price <= 0:
            continue

        with _db_lock:
            conn = db_connect()
            try:
                inst = get_open_offer_instance(conn, box_id)
                if not inst:
                    continue

                start_ts = int(inst["start_ts"] or now)
                last_trade_ts = inst["last_trade_ts"]
                last_ts = int(last_trade_ts) if last_trade_ts is not None else start_ts
                no_trade_for = now - last_ts

                if no_trade_for < stale_seconds:
                    continue

                name = mapping[item_id].name if item_id in mapping else str(item_id)

                # Stale SELL: fast-dump to low while still profitable (if we have tracked cost basis)
                if st == "sell":
                    # If sells aren't allowed right now, consider abort if allowed
                    if not _type_allowed(requested, "sell"):
                        if _type_allowed(requested, "abort"):
                            inv_has_item = any(i == item_id for i, _ in inv)
                            can_abort_sell = (not inv_full) or inv_has_item
                            if can_abort_sell and (not should_throttle_abort(conn, box_id, now)):
                                return build_abort(box_id, item_id, name, f"stale sell > {stale_seconds}s")
                        continue

                    remaining = max(total - traded, 0)
                    if remaining <= 0:
                        continue

                    lp = latest.get(item_id) or {}
                    try:
                        low = int(lp.get("low", 0))
                        high = int(lp.get("high", 0))
                    except Exception:
                        continue
                    if low <= 0 or high <= 0:
                        continue

                    open_qty, avg_buy, _ = db_open_qty_and_avg_cost(conn, item_id)
                    if open_qty > 0 and avg_buy > 0:
                        min_profit_price = min_profitable_sell_price(avg_buy)
                        desired = max(low, min_profit_price)

                        if desired < price:
                            mins = estimate_minutes_from_daily(remaining, volumes.get(item_id))
                            profit_per = desired - avg_buy - seller_tax(desired)
                            exp_profit = float(max(profit_per, 0) * remaining)
                            return build_sell(box_id, item_id, name, desired, remaining, exp_profit, mins, note=f"stale>{stale_seconds}s fast-dump")

                    # If we can't reprice down (or no basis), abort if allowed and safe
                    if _type_allowed(requested, "abort"):
                        inv_has_item = any(i == item_id for i, _ in inv)
                        can_abort_sell = (not inv_full) or inv_has_item
                        if can_abort_sell and (not should_throttle_abort(conn, box_id, now)):
                            return build_abort(box_id, item_id, name, f"stale sell > {stale_seconds}s (no trades)")
                    continue

                # Stale BUY: abort to free cash/slot
                if st == "buy":
                    if not _type_allowed(requested, "abort"):
                        continue

                    can_abort_buy = (not inv_full) or (coins > 0)
                    if not can_abort_buy:
                        continue

                    if should_throttle_abort(conn, box_id, now):
                        continue

                    return build_abort(box_id, item_id, name, f"stale buy > {stale_seconds}s (no trades)")

            finally:
                conn.close()

    return None

# ============================================================
# PROFIT TRACKING (durable SQLite + binary formats the client expects)
# ============================================================
_profit_lock = threading.Lock()

def _stable_account_id(display_name: str) -> int:
    # Python's built-in hash() is randomized per-process; use a stable hash instead.
    dn = (display_name or "").strip().lower()
    if not dn:
        dn = "default"
    aid = (zlib.crc32(dn.encode("utf-8")) & 0x7fffffff)
    return aid if aid != 0 else 1

def _pt_get_or_create_account(conn: sqlite3.Connection, display_name: str) -> int:
    dn = (display_name or "").strip() or "default"
    cur = conn.cursor()
    cur.execute("SELECT account_id FROM pt_accounts WHERE display_name = ?", (dn,))
    row = cur.fetchone()
    if row:
        return int(row["account_id"])
    aid = _stable_account_id(dn)
    cur.execute(
        "INSERT OR REPLACE INTO pt_accounts(display_name, account_id, created_ts) VALUES(?,?,?)",
        (dn, int(aid), int(time.time())),
    )
    conn.commit()
    return int(aid)

def _i64(x: int) -> int:
    # clamp into signed 64-bit for Java long
    if x >= (1 << 63):
        x -= (1 << 64)
    if x < -(1 << 63):
        x = -(1 << 63)
    return int(x)

def _pack_uuid(u: uuid.UUID) -> Tuple[int, int]:
    # Java UUID is two signed longs (msb, lsb)
    msb = (u.int >> 64) & ((1 << 64) - 1)
    lsb = u.int & ((1 << 64) - 1)
    if msb >= (1 << 63):
        msb -= (1 << 64)
    if lsb >= (1 << 63):
        lsb -= (1 << 64)
    return int(msb), int(lsb)

def _flip_to_raw(f: Dict[str, Any]) -> bytes:
    """FlipV2 RAW record (BIG_ENDIAN), 84 bytes (matches client FlipV2.RAW_SIZE)."""
    msb, lsb = _pack_uuid(f["uuid"])
    return struct.pack(
        ">qqiiiiqiiqqqiii",
        msb, lsb,
        int(f["account_id"]), int(f["item_id"]),
        int(f["opened_time"]), int(f["opened_qty"]),
        _i64(int(f["spent"])),
        int(f.get("closed_time", 0)), int(f.get("closed_qty", 0)),
        _i64(int(f.get("received_post_tax", 0))),
        _i64(int(f.get("profit", 0))),
        _i64(int(f.get("tax_paid", 0))),
        int(f.get("status_ord", 0)),
        int(f.get("updated_time", int(time.time()))),
        int(f.get("deleted", 0)),
    )

def _acked_tx_to_raw(tx: Dict[str, Any]) -> bytes:
    """AckedTransaction RAW record (BIG_ENDIAN), 56 bytes (matches client AckedTransaction.RAW_SIZE)."""
    tx_uuid = uuid.UUID(tx["tx_id"])
    flip_uuid = uuid.UUID(tx["flip_uuid"]) if tx.get("flip_uuid") else tx_uuid
    msb1, lsb1 = _pack_uuid(tx_uuid)
    msb2, lsb2 = _pack_uuid(flip_uuid)
    return struct.pack(
        ">qqqqiiiiii",
        msb1, lsb1,
        msb2, lsb2,
        int(tx["account_id"]),
        int(tx["time"]),
        int(tx["item_id"]),
        int(tx["quantity"]),
        int(tx["price"]),
        int(tx["amount_spent"]),
    )

def _pt_flip_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "uuid": uuid.UUID(row["flip_uuid"]),
        "account_id": int(row["account_id"]),
        "item_id": int(row["item_id"]),
        "opened_time": int(row["opened_time"]),
        "opened_qty": int(row["opened_qty"]),
        "spent": int(row["spent"]),
        "closed_time": int(row["closed_time"]),
        "closed_qty": int(row["closed_qty"]),
        "received_post_tax": int(row["received_post_tax"]),
        "profit": int(row["profit"]),
        "tax_paid": int(row["tax_paid"]),
        "status_ord": int(row["status_ord"]),
        "updated_time": int(row["updated_time"]),
        "deleted": int(row["deleted"]),
    }

def _pt_get_open_flip(conn: sqlite3.Connection, display_name: str, item_id: int) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM pt_flips
        WHERE display_name = ? AND item_id = ? AND deleted = 0 AND status_ord != 2
        ORDER BY updated_time DESC
        LIMIT 1
        """,
        ((display_name or "default"), int(item_id)),
    )
    return cur.fetchone()

def _pt_create_flip(conn: sqlite3.Connection, display_name: str, account_id: int, item_id: int, opened_time: int, status_ord: int) -> sqlite3.Row:
    flip_uuid = str(uuid.uuid4())
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO pt_flips(
            flip_uuid, display_name, account_id, item_id,
            opened_time, opened_qty, spent,
            closed_time, closed_qty, received_post_tax, profit, tax_paid,
            status_ord, updated_time, deleted
        ) VALUES (?,?,?,?, ?,?,?, ?,?,?, ?,?, ?,?,?)
        """,
        (
            flip_uuid, (display_name or "default"), int(account_id), int(item_id),
            int(opened_time), 0, 0,
            0, 0, 0, 0, 0,
            int(status_ord), int(now), 0
        ),
    )
    conn.commit()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pt_flips WHERE flip_uuid = ?", (flip_uuid,))
    return cur.fetchone()

def _pt_estimate_cost_basis(conn: sqlite3.Connection, item_id: int, latest_low: int = 0, latest_high: int = 0) -> int:
    # Prefer open-lot average cost (tracked), then last buy fill, then latest low/high.
    try:
        open_qty, avg_buy, _ = db_open_qty_and_avg_cost(conn, int(item_id))
        if open_qty > 0 and avg_buy > 0:
            return int(avg_buy)
    except Exception:
        pass

    cur = conn.cursor()
    cur.execute("SELECT buy_price FROM buy_fills WHERE item_id = ? ORDER BY fill_ts DESC LIMIT 1", (int(item_id),))
    row = cur.fetchone()
    if row and int(row[0]) > 0:
        return int(row[0])

    if latest_low > 0:
        return int(latest_low)
    if latest_high > 0:
        return int(latest_high)
    return 0

def _pt_apply_buy(conn: sqlite3.Connection, flip: sqlite3.Row, qty: int, price: int, ts: int) -> sqlite3.Row:
    opened_qty = int(flip["opened_qty"]) + int(qty)
    spent = int(flip["spent"]) + int(price) * int(qty)
    status_ord = 0 if int(flip["closed_qty"]) == 0 else 1
    updated_time = int(ts)

    conn.execute(
        "UPDATE pt_flips SET opened_qty=?, spent=?, status_ord=?, updated_time=? WHERE flip_uuid=?",
        (opened_qty, spent, status_ord, updated_time, flip["flip_uuid"]),
    )
    conn.commit()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pt_flips WHERE flip_uuid = ?", (flip["flip_uuid"],))
    return cur.fetchone()

def _pt_apply_sell(conn: sqlite3.Connection, flip: sqlite3.Row, sell_qty: int, price: int, ts: int, latest_low: int, latest_high: int) -> sqlite3.Row:
    # Ensure we have a basis. If the flip has 0 opened qty, synthesize a buy so profit can be shown.
    opened_qty = int(flip["opened_qty"])
    spent = int(flip["spent"])

    if opened_qty <= 0:
        basis = _pt_estimate_cost_basis(conn, int(flip["item_id"]), latest_low=latest_low, latest_high=latest_high)
        if basis <= 0:
            basis = int(price)
        opened_qty = int(sell_qty)
        spent = int(basis) * int(sell_qty)
    elif int(flip["closed_qty"]) + int(sell_qty) > opened_qty:
        # Selling more than we tracked as bought (extra inventory). Extend basis with an estimate.
        extra = (int(flip["closed_qty"]) + int(sell_qty)) - opened_qty
        basis = _pt_estimate_cost_basis(conn, int(flip["item_id"]), latest_low=latest_low, latest_high=latest_high)
        if basis <= 0:
            basis = int(price)
        opened_qty += int(extra)
        spent += int(basis) * int(extra)

    post_tax_price = ge_post_tax_price(int(flip["item_id"]), int(price))
    per_tax = int(price) - int(post_tax_price)
    received_post_tax = int(flip["received_post_tax"]) + int(post_tax_price) * int(sell_qty)
    tax_paid = int(flip["tax_paid"]) + int(per_tax) * int(sell_qty)

    closed_qty = int(flip["closed_qty"]) + int(sell_qty)
    closed_time = int(ts)
    updated_time = int(ts)

    status_ord = 1  # SELLING
    profit = int(received_post_tax - spent)
    if closed_qty >= opened_qty:
        status_ord = 2  # FINISHED

    conn.execute(
        """UPDATE pt_flips
           SET opened_qty=?, spent=?,
               closed_time=?, closed_qty=?,
               received_post_tax=?, tax_paid=?, profit=?,
               status_ord=?, updated_time=?
           WHERE flip_uuid=?""",
        (opened_qty, spent, closed_time, closed_qty, received_post_tax, tax_paid, profit, status_ord, updated_time, flip["flip_uuid"]),
    )
    conn.commit()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pt_flips WHERE flip_uuid = ?", (flip["flip_uuid"],))
    return cur.fetchone()

def _pt_ingest_transactions(display_name: str, txs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Store transactions durably, update flips, and return changed flips for the client merge."""
    dn = (display_name or "").strip() or "default"
    now = int(time.time())
    mapping, latest, _, _ = prices.snapshot()  # snapshot once for cost basis fallback

    changed_flip_uuids: Set[str] = set()

    with _db_lock:
        conn = db_connect()
        try:
            account_id = _pt_get_or_create_account(conn, dn)

            def _ts_key(t: Dict[str, Any]) -> int:
                try:
                    return int(t.get("time") or now)
                except Exception:
                    return now

            for t in sorted(txs or [], key=_ts_key):
                try:
                    tx_id = str(t.get("id") or "").strip()
                    if not tx_id:
                        continue

                    # dedupe
                    cur = conn.cursor()
                    cur.execute("SELECT 1 FROM pt_transactions WHERE tx_id = ?", (tx_id,))
                    if cur.fetchone():
                        continue

                    item_id = int(t.get("item_id") or 0)
                    price = int(t.get("price") or 0)
                    qty = int(t.get("quantity") or 0)
                    box_id = int(t.get("box_id") or 0)
                    amount_spent = int(t.get("amount_spent") or (abs(qty) * price))
                    ts = int(t.get("time") or now)

                    if item_id <= 0 or price <= 0 or qty == 0:
                        continue

                    flip = _pt_get_open_flip(conn, dn, item_id)
                    if flip is None:
                        flip = _pt_create_flip(conn, dn, account_id, item_id, opened_time=ts, status_ord=(0 if qty > 0 else 1))

                    lp = latest.get(item_id) or {}
                    try:
                        low = int(lp.get("low", 0))
                        high = int(lp.get("high", 0))
                    except Exception:
                        low, high = 0, 0

                    if qty > 0:
                        flip = _pt_apply_buy(conn, flip, qty=qty, price=price, ts=ts)
                    else:
                        flip = _pt_apply_sell(conn, flip, sell_qty=abs(qty), price=price, ts=ts, latest_low=low, latest_high=high)

                    conn.execute(
                        """INSERT INTO pt_transactions(
                               tx_id, display_name, account_id, flip_uuid,
                               time, item_id, quantity, price, box_id, amount_spent,
                               was_copilot_suggestion, copilot_price_used, login,
                               raw_json
                           ) VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?)""",
                        (
                            tx_id, dn, int(account_id), str(flip["flip_uuid"]),
                            int(ts), int(item_id), int(qty), int(price), int(box_id), int(amount_spent),
                            int(bool(t.get("was_copilot_suggestion", False))),
                            int(bool(t.get("copilot_price_used", False))),
                            int(bool(t.get("login", False))),
                            json.dumps(t, separators=(",", ":"), ensure_ascii=False),
                        ),
                    )
                    conn.commit()
                    changed_flip_uuids.add(str(flip["flip_uuid"]))
                except Exception:
                    logger.exception("[PROFIT] failed to ingest transaction")
                    continue

            flips_out: List[Dict[str, Any]] = []
            if changed_flip_uuids:
                cur = conn.cursor()
                qmarks = ",".join(["?"] * len(changed_flip_uuids))
                cur.execute(f"SELECT * FROM pt_flips WHERE flip_uuid IN ({qmarks})", tuple(changed_flip_uuids))
                rows = cur.fetchall() or []
                for r in rows:
                    flips_out.append(_pt_flip_row_to_dict(r))
            return flips_out
        finally:
            conn.close()

def _pt_fetch_flips_delta(account_id_time: Dict[int, int]) -> Tuple[int, List[Dict[str, Any]]]:
    now = int(time.time())
    flips: List[Dict[str, Any]] = []
    max_time = now
    if not account_id_time:
        return max_time, flips

    with _db_lock:
        conn = db_connect()
        try:
            cur = conn.cursor()
            for aid, last_t in account_id_time.items():
                try:
                    aid_i = int(aid)
                    last_i = int(last_t)
                except Exception:
                    continue
                cur.execute(
                    "SELECT * FROM pt_flips WHERE account_id = ? AND updated_time > ?",
                    (aid_i, last_i),
                )
                rows = cur.fetchall() or []
                for r in rows:
                    flips.append(_pt_flip_row_to_dict(r))
                    max_time = max(max_time, int(r["updated_time"]))
            return int(max_time), flips
        finally:
            conn.close()

@app.get("/profit-tracking/rs-account-names")
def rs_account_names():
    # Client expects JSON object Map<String,Integer>
    with _db_lock:
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT display_name, account_id FROM pt_accounts ORDER BY display_name ASC")
            rows = cur.fetchall() or []
            out = {str(r["display_name"]): int(r["account_id"]) for r in rows}
        finally:
            conn.close()
    return jsonify(out), 200

@app.route("/profit-tracking/account-client-transactions", methods=["POST"])
def account_client_transactions():
    # Client expects AckedTransaction.listFromRaw (recordCount + records)
    display_name = request.args.get("display_name", "").strip() or "default"
    payload = request.get_json(silent=True) or {}
    try:
        limit = int(payload.get("limit", 30))
    except Exception:
        limit = 30
    try:
        end = int(payload.get("end", int(time.time())))
    except Exception:
        end = int(time.time())
    limit = max(1, min(limit, 200))

    with _db_lock:
        conn = db_connect()
        try:
            _ = _pt_get_or_create_account(conn, display_name)
            cur = conn.cursor()
            cur.execute(
                """SELECT tx_id, flip_uuid, account_id, time, item_id, quantity, price, amount_spent
                   FROM pt_transactions
                   WHERE display_name = ? AND time <= ?
                   ORDER BY time DESC
                   LIMIT ?""",
                (display_name, int(end), int(limit)),
            )
            rows = cur.fetchall() or []
        finally:
            conn.close()

    out = bytearray()
    out += struct.pack(">i", int(len(rows)))
    for r in rows:
        out += _acked_tx_to_raw({
            "tx_id": r["tx_id"],
            "flip_uuid": r["flip_uuid"],
            "account_id": r["account_id"],
            "time": r["time"],
            "item_id": r["item_id"],
            "quantity": r["quantity"],
            "price": r["price"],
            "amount_spent": r["amount_spent"],
        })

    return Response(bytes(out), status=200, content_type="application/x-bytes")

@app.route("/profit-tracking/client-transactions", methods=["POST","GET"])
def client_transactions():
    # Client sends JSON array of transactions; expects FlipV2 list bytes in response.
    display_name = request.args.get("display_name", "").strip() or "default"
    if request.method == "GET":
        # Return all transactions as raw records, wrapped with recordCount prefix + suffix
        with _db_lock:
            conn = db_connect()
            try:
                cur = conn.cursor()
                cur.execute("""SELECT tx_id, flip_uuid, account_id, time, item_id, quantity, price, amount_spent
                               FROM pt_transactions
                               ORDER BY time ASC""")
                rows = cur.fetchall() or []
            finally:
                conn.close()

        recs = bytearray()
        for r in rows:
            recs += _acked_tx_to_raw({
                "tx_id": r["tx_id"],
                "flip_uuid": r["flip_uuid"],
                "account_id": r["account_id"],
                "time": r["time"],
                "item_id": r["item_id"],
                "quantity": r["quantity"],
                "price": r["price"],
                "amount_spent": r["amount_spent"],
            })
        out = struct.pack(">i", int(len(rows))) + bytes(recs) + struct.pack(">i", int(len(rows)))
        return Response(out, status=200, content_type="application/x-bytes")

    payload = request.get_json(silent=True)
    txs = payload if isinstance(payload, list) else []

    flips = _pt_ingest_transactions(display_name, txs)

    out = bytearray()
    out += struct.pack(">i", int(len(flips)))
    for f in flips:
        out += _flip_to_raw(f)

    resp = Response(bytes(out), status=200, content_type="application/x-bytes")
    resp.headers["X-USER-ID"] = "0"
    return resp

@app.route("/profit-tracking/client-flips-delta", methods=["POST"], strict_slashes=False)
def client_flips_delta():
    # Client sends DataDeltaRequest (JSON) and expects FlipsDeltaResult raw bytes:
    # 4-byte time + FlipV2.listFromRaw bytes (recordCount + records)
    payload = request.get_json(silent=True) or {}
    account_id_time = payload.get("account_id_time") or payload.get("accountIdTime") or {}
    parsed: Dict[int, int] = {}
    if isinstance(account_id_time, dict):
        for k, v in account_id_time.items():
            try:
                parsed[int(k)] = int(v)
            except Exception:
                continue

    new_time, flips = _pt_fetch_flips_delta(parsed)

    flips_bytes = bytearray()
    flips_bytes += struct.pack(">i", int(len(flips)))
    for f in flips:
        flips_bytes += _flip_to_raw(f)

    out = struct.pack(">i", int(new_time)) + bytes(flips_bytes)
    return Response(out, status=200, content_type="application/x-bytes")

@app.route("/profit-tracking/orphan-transaction", methods=["POST"])
def orphan_transaction():
    # Minimal local implementation: detach a transaction into its own flip.
    payload = request.get_json(silent=True) or {}
    tx_id = str(payload.get("transaction_id") or "").strip()
    if not tx_id:
        return Response(struct.pack(">i", 0), status=200, content_type="application/x-bytes")

    with _db_lock:
        conn = db_connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM pt_transactions WHERE tx_id = ?", (tx_id,))
            tx = cur.fetchone()
            if not tx:
                return Response(struct.pack(">i", 0), status=200, content_type="application/x-bytes")

            new_flip = _pt_create_flip(conn, tx["display_name"], int(tx["account_id"]), int(tx["item_id"]), opened_time=int(tx["time"]), status_ord=(0 if int(tx["quantity"]) > 0 else 1))
            conn.execute("UPDATE pt_transactions SET flip_uuid = ? WHERE tx_id = ?", (new_flip["flip_uuid"], tx_id))
            conn.commit()

            mapping, latest, _, _ = prices.snapshot()
            lp = latest.get(int(tx["item_id"])) or {}
            try:
                low = int(lp.get("low", 0))
                high = int(lp.get("high", 0))
            except Exception:
                low, high = 0, 0

            if int(tx["quantity"]) > 0:
                new_flip = _pt_apply_buy(conn, new_flip, qty=int(tx["quantity"]), price=int(tx["price"]), ts=int(tx["time"]))
            else:
                new_flip = _pt_apply_sell(conn, new_flip, sell_qty=abs(int(tx["quantity"])), price=int(tx["price"]), ts=int(tx["time"]), latest_low=low, latest_high=high)

            flips = [_pt_flip_row_to_dict(new_flip)]
        finally:
            conn.close()

    out = bytearray()
    out += struct.pack(">i", int(len(flips)))
    for f in flips:
        out += _flip_to_raw(f)
    return Response(bytes(out), status=200, content_type="application/x-bytes")

@app.route("/profit-tracking/delete-transaction", methods=["POST"])
def delete_transaction():
    # Minimal local implementation: delete transaction row (does not fully rebuild historical flips).
    payload = request.get_json(silent=True) or {}
    tx_id = str(payload.get("transaction_id") or "").strip()
    if not tx_id:
        return Response(struct.pack(">i", 0), status=200, content_type="application/x-bytes")

    with _db_lock:
        conn = db_connect()
        try:
            conn.execute("DELETE FROM pt_transactions WHERE tx_id = ?", (tx_id,))
            conn.commit()
        finally:
            conn.close()

    return Response(struct.pack(">i", 0), status=200, content_type="application/x-bytes")

@app.route("/profit-tracking/visualize-flip", methods=["POST"])
def visualize_flip():
    # Client expects msgpack VisualizeFlipResponse.
    data = _visualize_flip_response_to_msgpack()
    return Response(data, status=200, content_type="application/x-msgpack")


# ============================================================
# ITEM PRICES ENDPOINT (msgpack) — supports GET + POST
# ============================================================
@app.route("/prices", methods=["GET", "POST"])
def prices_endpoint():
    # Plugin may POST JSON {"item_id": ...} and expect msgpack ItemPrice
    item_id = 0
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        try:
            item_id = int(payload.get("item_id", 0))
        except Exception:
            item_id = 0
    else:
        try:
            item_id = int(request.args.get("item_id", "0"))
        except Exception:
            item_id = 0

    mapping, latest, _, _ = prices.snapshot()
    lp = latest.get(item_id) or {}
    try:
        low = int(lp.get("low", 0))
        high = int(lp.get("high", 0))
    except Exception:
        low, high = 0, 0

    if low <= 0 and high <= 0:
        packed = _item_price_to_msgpack(0, 0, "No price data")
        resp = Response(packed, status=200, content_type="application/x-msgpack")
        resp.headers["Content-Length"] = str(len(packed))
        return resp

    buy_price = max(low, 1) if low > 0 else max(high, 1)
    sell_price = max(high, 1) if high > 0 else max(low, 1)

    packed = _item_price_to_msgpack(buy_price, sell_price, "")
    resp = Response(packed, status=200, content_type="application/x-msgpack")
    resp.headers["Content-Length"] = str(len(packed))
    return resp

# ============================================================
# DASH AUTH + DASHBOARD (dark mode tables)
# ============================================================
def dashboard_allowed() -> bool:
    if not DASH_TOKEN:
        return True
    token = request.args.get("token", "")
    return token == DASH_TOKEN

def format_ts(ts: int) -> str:
    if not ts:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
    except Exception:
        return str(ts)

@app.get("/")
@app.get("/dashboard")
def dashboard():
    if not dashboard_allowed():
        return "Forbidden", 403

    try:
        mapping, latest, volumes, last_refresh = prices.snapshot()

        with _db_lock:
            conn = db_connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT COALESCE(SUM(profit), 0) AS p FROM realized_trades")
                realized_profit = int(cur.fetchone()["p"])

                cur.execute("SELECT COALESCE(SUM(qty * buy_price), 0) AS c FROM realized_trades")
                realized_cost = int(cur.fetchone()["c"])

                roi = (realized_profit / realized_cost) if realized_cost > 0 else 0.0

                cur.execute("""SELECT COALESCE(SUM(qty_remaining * buy_price), 0) AS open_cost
                               FROM lots WHERE qty_remaining > 0""")
                open_cost_total = int(cur.fetchone()["open_cost"])

                cur.execute("""
                    SELECT item_id, item_name,
                           SUM(qty_remaining) AS open_qty,
                           SUM(qty_remaining * buy_price) AS open_cost
                    FROM lots
                    WHERE qty_remaining > 0
                    GROUP BY item_id, item_name
                    ORDER BY open_cost DESC
                    LIMIT 200
                """)
                open_rows = [dict(x) for x in cur.fetchall()]

                cur.execute("""
                    SELECT trade_id, item_id, item_name, qty, buy_price, sell_price, sell_ts, profit
                    FROM realized_trades
                    ORDER BY sell_ts DESC
                    LIMIT 100
                """)
                hist_rows = [dict(x) for x in cur.fetchall()]
            finally:
                conn.close()

        with _last_status_lock:
            st = dict(LAST_STATUS)
        last_status_ts = int(st.get("updated_unix", 0))
        coins = int(st.get("coins", 0))
        offers = st.get("offers", []) or []

        offer_rows_html = ""
        for o in offers[:50]:
            try:
                status = str(o.get("status", "")).lower()
                active = bool(o.get("active", False))
                iid = int(o.get("item_id", 0))
                box_id = int(o.get("box_id", 0))
                price = int(o.get("price", 0))
                total = int(o.get("amount_total", 0))
                traded = int(o.get("amount_traded", 0))
                gp_to_collect = int(o.get("gp_to_collect", 0))
            except Exception:
                continue
            name = mapping[iid].name if iid in mapping else str(iid)
            offer_rows_html += (
                "<tr>"
                f"<td>{box_id}</td>"
                f"<td>{html_escape(status)}</td>"
                f"<td>{'yes' if active else 'no'}</td>"
                f"<td>{iid}</td>"
                f"<td>{html_escape(name)}</td>"
                f"<td>{price:,}</td>"
                f"<td>{traded:,}/{total:,}</td>"
                f"<td>{gp_to_collect:,}</td>"
                "</tr>"
            )
        if not offer_rows_html:
            offer_rows_html = "<tr><td colspan='8' style='opacity:0.8;'>No offers received yet (open GE / let plugin POST /suggestion)</td></tr>"

        open_pos_html = ""
        for r in open_rows:
            iid = int(r["item_id"])
            name = str(r["item_name"])
            open_qty = int(r["open_qty"])
            open_cost = int(r["open_cost"])
            avg_buy = int(open_cost / open_qty) if open_qty > 0 else 0

            lp = latest.get(iid) or {}
            high = int(lp.get("high", 0)) if isinstance(lp, dict) else 0
            sell_px = max(high - 1, 1) if high > 0 else 0
            tax = seller_tax(sell_px) if sell_px > 0 else 0
            unreal_per = (sell_px - avg_buy - tax) if sell_px > 0 else 0
            unreal = unreal_per * open_qty if sell_px > 0 else 0

            open_pos_html += (
                "<tr>"
                f"<td>{iid}</td>"
                f"<td>{html_escape(name)}</td>"
                f"<td>{open_qty:,}</td>"
                f"<td>{avg_buy:,}</td>"
                f"<td>{open_cost:,}</td>"
                f"<td>{sell_px:,}</td>"
                f"<td>{unreal:,}</td>"
                "</tr>"
            )
        if not open_pos_html:
            open_pos_html = "<tr><td colspan='7' style='opacity:0.8;'>No open lots (nothing currently held)</td></tr>"

        hist_html = ""
        for t in hist_rows:
            hist_html += (
                "<tr>"
                f"<td>{int(t['trade_id'])}</td>"
                f"<td>{int(t['item_id'])}</td>"
                f"<td>{html_escape(str(t['item_name']))}</td>"
                f"<td>{int(t['qty']):,}</td>"
                f"<td>{int(t['buy_price']):,}</td>"
                f"<td>{int(t['sell_price']):,}</td>"
                f"<td>{int(t['profit']):,}</td>"
                f"<td>{html_escape(format_ts(int(t['sell_ts'])))} </td>"
                "</tr>"
            )
        if not hist_html:
            hist_html = "<tr><td colspan='8' style='opacity:0.8;'>No realized trades yet</td></tr>"

        html = f"""
        <html>
        <head>
          <title>SwagFlip Dashboard</title>
          <style>
            body {{
              font-family: system-ui, sans-serif;
              padding: 22px;
              background: #0f1115;
              color: #f2f2f2;
            }}
            a {{ color: #8ab4ff; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
            .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }}
            .card {{ background: #161a22; border: 1px solid #2a3243; border-radius: 10px; padding: 12px; }}
            .k {{ opacity: 0.78; font-size: 12px; }}
            .v {{ font-size: 20px; font-weight: 800; margin-top: 4px; }}
            .row {{ display:flex; gap: 10px; flex-wrap: wrap; margin: 10px 0 18px; }}
            .pill {{ background:#161a22; border:1px solid #2a3243; border-radius:999px; padding:8px 12px; font-size:13px; }}
            .wrap {{ max-height: 340px; overflow:auto; border-radius: 10px; border: 1px solid #2a3243; }}

            table {{
              width: 100%;
              border-collapse: collapse;
              background: #11151d;
              color: #f2f2f2;
            }}
            th, td {{
              border: 1px solid #2a3243;
              padding: 8px;
              text-align: left;
              color: #f2f2f2;
            }}
            th {{
              background: #141a26;
              position: sticky;
              top: 0;
            }}
            h2 {{ margin: 18px 0 10px; }}
          </style>
        </head>
        <body>
          <h1 style="margin:0 0 8px;">SwagFlip Dashboard</h1>

          <div class="row">
            <div class="pill">DB: <span style="opacity:0.85">{html_escape(DB_PATH)}</span></div>
            <div class="pill">Prices refreshed: <span style="opacity:0.85">{html_escape(format_ts(int(last_refresh)))}</span></div>
            <div class="pill">Last plugin status: <span style="opacity:0.85">{html_escape(format_ts(int(last_status_ts)))}</span></div>
            <div class="pill">Coins (last status): <span style="opacity:0.85">{coins:,}</span></div>
            <div class="pill">Buy-limit window: <span style="opacity:0.85">{BUY_LIMIT_RESET_SECONDS}s</span></div>
            <div class="pill">Logs: <a href="/debug/logs" style="opacity:0.95">/debug/logs</a></div>
          </div>

          <div class="grid">
            <div class="card"><div class="k">Realized Profit</div><div class="v">{realized_profit:,} gp</div></div>
            <div class="card"><div class="k">Realized Cost</div><div class="v">{realized_cost:,} gp</div></div>
            <div class="card"><div class="k">Realized ROI</div><div class="v">{roi*100:.2f}%</div></div>
            <div class="card"><div class="k">Open Cost Basis</div><div class="v">{open_cost_total:,} gp</div></div>
          </div>

          <h2>Current Offers (from last /suggestion payload)</h2>
          <div class="wrap">
            <table>
              <tr>
                <th>Box</th><th>Status</th><th>Active</th><th>Item ID</th><th>Name</th><th>Price</th><th>Traded</th><th>GP to Collect</th>
              </tr>
              {offer_rows_html}
            </table>
          </div>

          <h2>Open Positions (lots aggregated)</h2>
          <div class="wrap">
            <table>
              <tr>
                <th>Item ID</th><th>Name</th><th>Open Qty</th><th>Avg Buy</th><th>Open Cost</th><th>Est Sell (high-1)</th><th>Unrealized (est)</th>
              </tr>
              {open_pos_html}
            </table>
          </div>

          <h2>Recent Realized Trades</h2>
          <div class="wrap">
            <table>
              <tr>
                <th>ID</th><th>Item ID</th><th>Name</th><th>Qty</th><th>Buy</th><th>Sell</th><th>Profit</th><th>Sold</th>
              </tr>
              {hist_html}
            </table>
          </div>
        </body>
        </html>
        """
        return html, 200

    except Exception:
        rid = getattr(g, "request_id", uuid.uuid4().hex[:10])
        logger.exception(f"[DASHBOARD] crash rid={rid}")
        return f"<h1>Dashboard error</h1><p>Request ID: {rid}</p><pre>Check {html_escape(LOG_PATH)} or /debug/logs</pre>", 500

# ============================================================
# DEBUG
# ============================================================
@app.get("/health")
def health():
    _, _, _, refreshed = prices.snapshot()
    return jsonify({"ok": True, "last_price_refresh_unix": refreshed, "db": DB_PATH, "log": LOG_PATH}), 200

def _tail_file(path: str, lines: int = 250) -> str:
    try:
        if not os.path.exists(path):
            return f"(no log file yet) {path}\n"
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            data = f.readlines()
        return "".join(data[-max(1, lines):])
    except Exception as e:
        return f"(failed reading log) {e}\n"

@app.get("/debug/logs")
def debug_logs():
    if not dashboard_allowed():
        return "Forbidden", 403
    try:
        n = int(request.args.get("n", "250"))
    except Exception:
        n = 250
    return Response(_tail_file(LOG_PATH, n), status=200, content_type="text/plain; charset=utf-8")

# ============================================================
# MAIN SUGGESTION ENDPOINT
# ============================================================
@app.post("/suggestion")
def suggestion():
    try:
        status = request.get_json(silent=True) or {}
        update_last_status(status)

        offers: List[Dict[str, Any]] = status.get("offers") or []
        items: List[Dict[str, Any]] = status.get("items") or []

        mapping, latest, volumes, _ = prices.snapshot()

        # parse coins + inventory (needed for stale logic + safety checks)
        coins = 0
        inv: List[Tuple[int, int]] = []
        for it in items:
            try:
                iid = int(it.get("item_id", 0))
                amt = int(it.get("amount", 0))
                if iid == 995:
                    coins += amt
                elif iid > 0 and amt > 0:
                    inv.append((iid, amt))
            except Exception:
                continue

        # Check for full inventory (28 slots)
        used_inv_slots = len(inv) + (1 if coins > 0 else 0)
        inv_full = (used_inv_slots >= 28)

        # Client-configurable offer adjust window (minutes). Presets: 5, 30, 120, 480.
        tf_minutes = _status_timeframe_minutes(status)
        stale_seconds = max(STALE_OFFER_SECONDS, tf_minutes * 60)

        # Use the adjust-offers window as a rough "flip horizon":
        # longer windows => accept slower items, but demand higher margins/ROI.
        if tf_minutes <= 5:
            min_roi_eff = MIN_ROI
            min_margin_eff = max(1, MIN_MARGIN_GP)
            max_buy_mins = float(TARGET_FILL_MINUTES) * 3.0
        elif tf_minutes <= 30:
            min_roi_eff = max(MIN_ROI, 0.003)   # 0.3%
            min_margin_eff = max(MIN_MARGIN_GP, 25)
            max_buy_mins = 60.0
        elif tf_minutes <= 120:
            min_roi_eff = max(MIN_ROI, 0.006)   # 0.6%
            min_margin_eff = max(MIN_MARGIN_GP, 50)
            max_buy_mins = 240.0
        else:
            min_roi_eff = max(MIN_ROI, 0.010)   # 1.0%
            min_margin_eff = max(MIN_MARGIN_GP, 100)
            max_buy_mins = 720.0



        # 1) sync offers -> DB (fills, lots, trades, rec metrics)
        try:
            sync_offers_and_fills(mapping, offers)
        except Exception:
            logger.exception("[SYNC] error")

        # 1.5) stale offers (abort stale buys, fast-dump stale sells)
        stale_action = maybe_handle_stale_offers(status, mapping, latest, volumes, offers, coins, inv, inv_full)
        if stale_action is not None:
            record_recommendation(stale_action)
            return _reply_suggestion(stale_action)

        # 2) crash-guard: reprice active sells while still profitable (no free slot required)
        crash_action = maybe_reprice_active_sell(mapping, latest, volumes, offers, stale_seconds)
        if crash_action is not None:
            record_recommendation(crash_action)
            return _reply_suggestion(crash_action)

        blocked: Set[int] = set()
        try:
            for b in (status.get("blocked_items") or []):
                blocked.add(int(b))
        except Exception:
            pass

        skip_id = status.get("skip_suggestion", -1)
        try:
            skip_id = int(skip_id)
        except Exception:
            skip_id = -1
        if skip_id and skip_id != -1:
            blocked.add(skip_id)

        active_ids = active_offer_item_ids(offers)
        empty_slots = [o for o in offers if offer_is_empty(o)]
        done_exists = any(offer_is_done(o) for o in offers)

        empty_slot_id = first_empty_slot_id(offers)
        slots_open = len(empty_slots)

        led = load_ledger()

        # ====================================================
        # PRIORITY 0: CLEAR GE (Collect or Abort)
        # ====================================================

        # Case A: GE is full, and at least one offer is DONE.
        # NOTE: The plugin does NOT have a 'collect' action type. For fast-dump / clear-slot,
        # we return an ABORT suggestion on the completed slot (user will click the slot and collect).
        if slots_open <= 0 and done_exists:
            now = int(time.time())

            # Find a completed (inactive, non-empty) offer to target
            done_offer = None
            for o in offers:
                try:
                    if offer_is_done(o):
                        done_offer = o
                        break
                except Exception:
                    continue

            if not done_offer:
                return _reply_suggestion(build_wait("Wait (slots full; done_exists true but no done offer found)"))

            try:
                box_id = int(done_offer.get("box_id", 0))
                item_id = int(done_offer.get("item_id", 0))
                st = str(done_offer.get("status", "")).lower()
            except Exception:
                box_id, item_id, st = 0, 0, ""

            name = mapping[item_id].name if item_id in mapping else str(item_id)

            # Safety: collecting coins requires either an existing coin stack OR a free inv slot.
            # Collecting items can stack if we already have that item in inventory.
            inv_has_item = any(i == item_id for i, _ in inv)

            if st == "sell":
                if inv_full and coins <= 0:
                    return _reply_suggestion(build_wait("Inventory FULL (no coins) - Cannot collect sale gp safely."))
            elif st == "buy":
                if inv_full and (not inv_has_item):
                    return _reply_suggestion(build_wait("Inventory FULL - Cannot collect bought items safely (no stack space)."))

            # Throttle abort spam
            with _db_lock:
                conn = db_connect()
                try:
                    if should_throttle_abort(conn, box_id, now):
                        return _reply_suggestion(build_wait("Wait (slots full; completed offer; abort throttled)"))
                finally:
                    conn.close()

            abort_action = build_abort(box_id, item_id, name, "done offer (clear slot for fast dump)")
            record_recommendation(abort_action)
            return _reply_suggestion(abort_action)

        # Case B: GE is full, nothing to collect -> Abort stuck buys
        if slots_open <= 0 and (not done_exists):
            now = int(time.time())
            with _db_lock:
                conn = db_connect()
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT offer_id, box_id, item_id, price, amount_total, start_ts, last_traded, rec_id
                        FROM offer_instances
                        WHERE done_ts IS NULL
                          AND active = 1
                          AND status = 'buy'
                          AND last_traded = 0
                          AND start_ts <= ?
                        ORDER BY start_ts ASC
                        LIMIT 1
                    """, (int(now - STUCK_BUY_ABORT_SECONDS),))
                    row = cur.fetchone()
                    if row:
                        box_id = int(row["box_id"])
                        item_id = int(row["item_id"])
                        name = mapping[item_id].name if item_id in mapping else str(item_id)

                        # Logic: We can only abort a buy if we have space for the coins,
                        # OR if we already have a coin stack (coins > 0).
                        can_abort_buy = (not inv_full) or (coins > 0)

                        if not can_abort_buy:
                             return _reply_suggestion(build_wait("Inventory FULL (No coins) - Cannot abort buy safely."))

                        if not should_throttle_abort(conn, box_id, now):
                            reason = f"stuck buy (0 fills) > {STUCK_BUY_ABORT_SECONDS//60}m"
                            abort_action = build_abort(box_id, item_id, name, reason)
                            record_recommendation(abort_action)
                            return _reply_suggestion(abort_action)
                finally:
                    conn.close()

            return _reply_suggestion(build_wait("Wait (slots full; no collectable offers; no stuck-buy abort candidate)"))

        # ====================================================
        # PRIORITY 1: SELL INVENTORY (unless item already active in GE)
        # ====================================================
        if slots_open > 0 and empty_slot_id is not None:
            for iid, amt in inv:
                if iid in blocked or iid in active_ids:
                    continue
                if iid not in latest or iid not in mapping:
                    continue

                lp = latest.get(iid) or {}
                high = int(lp.get("high", 0))
                low = int(lp.get("low", 0))
                if high <= 0 or low <= 0:
                    continue

                name = mapping[iid].name
                mins = estimate_minutes_from_daily(amt, volumes.get(iid))

                with _db_lock:
                    conn = db_connect()
                    try:
                        open_qty, avg_buy, _ = db_open_qty_and_avg_cost(conn, iid)
                    finally:
                        conn.close()

                if open_qty > 0 and avg_buy > 0:
                    sell_price = max(high - 1, 1)
                    profit_per = sell_price - avg_buy - seller_tax(sell_price)
                    if profit_per <= 0:
                        continue
                    exp_profit = float(profit_per * amt)
                    action = build_sell(int(empty_slot_id), iid, name, sell_price, amt, exp_profit, mins, note="inventory (tracked)")
                    record_recommendation(action)
                    return _reply_suggestion(action)

                if mins <= FAST_SELL_TARGET_MINUTES:
                    fast_price = max(low, 1)
                    # Estimate cost basis for untracked inventory so the UI can show +/- profit
                    basis = 0
                    with _db_lock:
                        conn2 = db_connect()
                        try:
                            # Prefer last known buy from our history if any
                            cur2 = conn2.cursor()
                            cur2.execute("SELECT buy_price FROM buy_fills WHERE item_id = ? ORDER BY fill_ts DESC LIMIT 1", (int(iid),))
                            r2 = cur2.fetchone()
                            if r2 and int(r2[0]) > 0:
                                basis = int(r2[0])
                        finally:
                            conn2.close()
                    if basis <= 0:
                        # Fall back to current low (proxy for acquisition)
                        basis = max(low, 1)

                    # Profit estimate is post-tax proceeds minus estimated basis
                    post_tax = ge_post_tax_price(iid, fast_price)
                    profit_per = int(post_tax - basis)
                    exp_profit = float(profit_per * amt)

                    action = build_sell(int(empty_slot_id), iid, name, fast_price, amt, exp_profit, mins, note="inventory fast-sell (untracked)")
                    record_recommendation(action)
                    return _reply_suggestion(action)

        # ====================================================
        # PRIORITY 2: QUEUED BUYS
        # ====================================================
        if slots_open > 0 and led.get("buy_queue"):
            if skip_id and skip_id != -1:
                led["buy_queue"] = [q for q in led["buy_queue"] if int(q.get("item_id", -1)) != skip_id]
                save_ledger(led)

            if led["buy_queue"]:
                q = led["buy_queue"].pop(0)
                save_ledger(led)
                q["box_id"] = int(empty_slot_id or 0)
                record_recommendation(q)
                return _reply_suggestion(q)

        # ====================================================
        # PRIORITY 3: NEW BUYS
        # ====================================================
        sell_only = bool(status.get("sell_only", False))
        if slots_open > 0 and (not sell_only) and empty_slot_id is not None:
            budget_total = min(int(coins * MAX_CASH_FRACTION), BUY_BUDGET_CAP)
            if budget_total <= 0:
                return _reply_suggestion(build_wait("Wait (no cash)"))

            per_slot_budget = max(int(budget_total / max(slots_open, 1)), 1)

            reject_counts: Dict[str, int] = {}
            def rej(reason: str):
                if DEBUG_REJECTIONS:
                    reject_counts[reason] = reject_counts.get(reason, 0) + 1

            now = int(time.time())
            cutoff = now - BUY_LIMIT_RESET_SECONDS

            candidates: List[Dict[str, Any]] = []
            for item_id, lp in latest.items():
                if item_id in blocked:
                    rej("blocked"); continue
                if item_id in active_ids:
                    rej("already_active"); continue
                if item_id not in mapping:
                    rej("no_mapping"); continue

                daily_vol = volumes.get(item_id)
                if daily_vol is None:
                    rej("no_daily_volume"); continue
                if not (MIN_DAILY_VOLUME <= daily_vol <= MAX_DAILY_VOLUME):
                    rej("daily_volume_out_of_range"); continue

                try:
                    low = int(lp.get("low", 0))
                    high = int(lp.get("high", 0))
                    if low <= 0 or high <= 0:
                        rej("bad_prices"); continue
                    if low < MIN_BUY_PRICE:
                        rej("below_min_buy_price"); continue

                    buy_price = low
                    sell_price = max(high - 1, 1)

                    margin = sell_price - buy_price
                    if margin < MIN_MARGIN_GP:
                        rej("margin_too_small"); continue

                    profit_per = sell_price - buy_price - seller_tax(sell_price)
                    if profit_per < max(1, int(min_margin_eff)):
                        rej("profit_after_tax_too_small"); continue

                    roi = profit_per / float(buy_price)
                    if roi < min_roi_eff:
                        rej("roi_too_low"); continue
                    if roi > MAX_ROI:
                        rej("roi_too_high"); continue

                    qty = per_slot_budget // buy_price
                    if qty <= 0:
                        rej("qty_zero_budget"); continue

                    meta = mapping[item_id]
                    note = ""

                    if meta.limit is not None:
                        lim = int(meta.limit)
                        with _db_lock:
                            conn = db_connect()
                            try:
                                bought_in_window = db_bought_qty_in_window(conn, item_id, cutoff)
                            finally:
                                conn.close()
                        remaining = lim - bought_in_window
                        if remaining <= 0:
                            rej("buy_limit_reached_window"); continue
                        if qty > remaining:
                            qty = remaining
                            note = f"limit remaining {remaining}/{lim} (4h)"

                    if qty <= 0:
                        rej("qty_zero_limit"); continue

                    mins = estimate_minutes_from_daily(qty, daily_vol)
                    if mins > max_buy_mins:
                        rej("too_slow"); continue

                    expected_profit = profit_per * qty
                    speed_weight = 1.7 / math.sqrt(max(mins, 0.25))
                    score = (expected_profit / max(mins, 0.25)) * speed_weight

                    candidates.append({
                        "item_id": item_id,
                        "name": meta.name,
                        "price": buy_price,
                        "quantity": qty,
                        "expectedProfit": float(expected_profit),
                        "expectedDuration": float(mins),
                        "score": float(score),
                        "note": note,
                    })
                except Exception:
                    rej("exception"); continue

            if not candidates:
                if DEBUG_REJECTIONS and reject_counts:
                    top = sorted(reject_counts.items(), key=lambda x: x[1], reverse=True)[:12]
                    logger.info("[REJECTS] top reasons: " + ", ".join([f"{k}={v}" for k, v in top]))
                return _reply_suggestion(build_wait("Wait (no buy candidates)"))

            candidates.sort(key=lambda x: x["score"], reverse=True)

            # Optional trend assist: for longer horizons, re-score top candidates using recent timeseries direction.
            if ENABLE_TRENDS and tf_minutes > 5 and candidates:
                top_n = min(len(candidates), max(1, TREND_RECHECK_TOP_N))
                influence = 2.0 if tf_minutes <= 30 else (3.5 if tf_minutes <= 120 else 5.0)
                for c in candidates[:top_n]:
                    try:
                        trend = TREND_CACHE.get(int(c.get("item_id") or 0), tf_minutes)
                    except Exception:
                        trend = 0.0

                    # store for debugging/UI
                    c["trend"] = float(trend)

                    # clamp and adjust score gently
                    t = max(-0.05, min(0.05, float(trend)))
                    c["score"] = float(c["score"]) * (1.0 + (t * influence))

                    # for 2h/8h horizons, strongly negative trends are usually bad holds
                    if tf_minutes >= 120 and float(trend) < -0.03:
                        c["score"] *= 0.5

                candidates.sort(key=lambda x: x["score"], reverse=True)


            buy_queue: List[Dict[str, Any]] = []
            for c in candidates[:max(slots_open, 1)]:
                buy_queue.append(
                    build_buy(
                        int(empty_slot_id),
                        int(c["item_id"]),
                        str(c["name"]),
                        int(c["price"]),
                        int(c["quantity"]),
                        float(c["expectedProfit"]),
                        float(c["expectedDuration"]),
                        note=str(c.get("note", "")),
                    )
                )

            led["buy_queue"] = buy_queue[1:]
            save_ledger(led)

            first = buy_queue[0]
            record_recommendation(first)
            return _reply_suggestion(first)

        if slots_open <= 0:
            return _reply_suggestion(build_wait("Wait (all 8 GE slots full)"))

        return _reply_suggestion(build_wait("Wait (no actionable move)"))

    except Exception:
        logger.exception("[SUGGESTION] fatal error")
        return _reply_suggestion(build_wait("Wait (server issue — check swagflip.log)"))

def _reply_suggestion(action: Dict[str, Any]):
    """
    If client Accepts msgpack, reply in msgpack format and include the split-length headers
    the Java client uses for graph data (we send 0 graph bytes for now).
    Otherwise, reply as JSON.
    """
    if _wants_msgpack():
        suggestion_bytes = _suggestion_to_msgpack(action)
        graph_bytes = b""
        resp = Response(suggestion_bytes + graph_bytes, status=200, content_type="application/x-msgpack")
        resp.headers["X-SUGGESTION-CONTENT-LENGTH"] = str(len(suggestion_bytes))
        resp.headers["X-GRAPH-DATA-CONTENT-LENGTH"] = "0"
        return resp
    return jsonify(action), 200

# ============================================================
# BOOT
# ============================================================
def main():
    db_init()
    t = threading.Thread(target=prices.refresh_forever, daemon=True)
    t.start()

    logger.info("--------------------------------------------------")
    logger.info("   SWAGFLIP BRAIN — ONLINE")
    logger.info("   POST /suggestion")
    logger.info(f"   Dashboard: http://{HOST}:{PORT}/")
    if DASH_TOKEN:
        logger.info("   Dashboard token enabled: add ?token=YOUR_TOKEN")
    logger.info(f"   DB (durable): {DB_PATH}")
    logger.info(f"   Log file: {LOG_PATH}")
    logger.info(f"   Abort cooldown: {ABORT_COOLDOWN_SECONDS}s | Stuck buy abort: {STUCK_BUY_ABORT_SECONDS}s | Stale offer: {STALE_OFFER_SECONDS}s")
    logger.info("   Profit tracking endpoints enabled: /profit-tracking/*")
    logger.info("   Item prices endpoint enabled: /prices (GET/POST)")
    logger.info("--------------------------------------------------")

    app.run(host=HOST, port=PORT, debug=False)

if __name__ == "__main__":
    main()
