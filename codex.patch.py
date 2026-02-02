diff --git a/server_swagflip_relaxed_learning_fixed_v13.py b/ServerV1.py
similarity index 94%
rename from server_swagflip_relaxed_learning_fixed_v13.py
rename to ServerV1.py
index abcaf4598b67989065f7c328e77c37bb8bae5782..db77d4b2992863ada0465b6e2e9e36d275214f3b 100644
--- a/server_swagflip_relaxed_learning_fixed_v13.py
+++ b/ServerV1.py
@@ -1,115 +1,121 @@
-# server.py
+# ServerV1.py (rename to ServerV2.py/ServerV3.py as iterations land)
 # SwagFlip Brain — local copilot server for RuneLite plugin
 # - Suggestion engine + SQLite ledger (lots/trades/recs)
 # - Auto-migrates SQLite schema in-place (keeps history)
 # - Implements /prices and profit-tracking endpoints with the binary/msgpack formats the plugin expects
 
 import os
 import time
 import math
 import json
 import re
 import csv
 import io
 import threading
 import sqlite3
 import uuid
 import struct
 import datetime
 import statistics
 import zlib
 import logging
 from logging.handlers import RotatingFileHandler
 from contextlib import contextmanager
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
+# QUICK FLIP KNOBS (buy/sell filters + scoring)
+# - Volume filter: MIN_DAILY_VOLUME, MAX_DAILY_VOLUME
+# - ROI after tax band: MIN_ROI, MAX_ROI
+# - Per-item profit guards: MIN_MARGIN_GP, MIN_TOTAL_PROFIT_GP
+# - Speed/price nudges: TARGET_FILL_MINUTES, FILL_FRACTION, BUY_OVERBID_GP, SELL_UNDERCUT_GP
+# - Safety: MAX_INVENTORY_LOSS_GP
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
 
 # Aggressive flip knobs (used in buy/sell model)
 BUY_OVERBID_GP = int(os.getenv("SWAGFLIP_BUY_OVERBID_GP", "2"))
 SELL_UNDERCUT_GP = int(os.getenv("SWAGFLIP_SELL_UNDERCUT_GP", "2"))
 FILL_FRACTION = float(os.getenv("SWAGFLIP_FILL_FRACTION", "0.25"))
 
 # Trend-assisted scoring (used for 30m/2h/8h horizons)
 ENABLE_TRENDS = os.getenv("SWAGFLIP_ENABLE_TRENDS", "1") not in ("0", "false", "False")
 TREND_CACHE_TTL_SECONDS = int(os.getenv("SWAGFLIP_TREND_CACHE_TTL", "180"))
 TREND_RECHECK_TOP_N = int(os.getenv("SWAGFLIP_TREND_TOP_N", "20"))
 
 MIN_BUY_PRICE = int(os.getenv("SWAGFLIP_MIN_BUY_PRICE", "1"))
 MIN_MARGIN_GP = int(os.getenv("SWAGFLIP_MIN_MARGIN_GP", "1"))
-MIN_TOTAL_PROFIT_GP = int(os.getenv("SWAGFLIP_MIN_TOTAL_PROFIT_GP", "5000"))
+MIN_TOTAL_PROFIT_GP = int(os.getenv("SWAGFLIP_MIN_TOTAL_PROFIT_GP", "1000"))
 MAX_INVENTORY_LOSS_GP = int(os.getenv("SWAGFLIP_MAX_INVENTORY_LOSS_GP", "50000"))
 
 # ============================================================
 # "Good flips" CSV prior (optional)
 # If enabled, item_ids found in this CSV receive a small score boost.
 # This is NOT a hard filter unless SWAGFLIP_ONLY_WINNING_ITEMS is enabled.
 # ============================================================
 GOOD_CSV_ENABLED = os.getenv("SWAGFLIP_GOOD_CSV_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
 GOOD_CSV_PATH = os.getenv("SWAGFLIP_GOOD_CSV_PATH", os.getenv("SWAGFLIP_WINNING_FLIPS_CSV", "DaityasPls.csv"))
 WINNING_FLIPS_CSV = GOOD_CSV_PATH  # legacy alias
 GOOD_CSV_SCORE_BOOST = float(os.getenv("SWAGFLIP_GOOD_CSV_SCORE_BOOST", "1.15"))
 
 # Legacy knobs (kept for compatibility)
 ONLY_WINNING_ITEMS = os.getenv("SWAGFLIP_ONLY_WINNING_ITEMS", "0").strip().lower() not in ("0", "false", "no", "off")
 WINNER_SCORE_MULT = float(os.getenv("SWAGFLIP_WINNER_SCORE_MULT", str(GOOD_CSV_SCORE_BOOST)))
 WINNER_MIN_PROFIT_FRACTION = float(os.getenv("SWAGFLIP_WINNER_MIN_PROFIT_FRACTION", "0.35"))
 WINNER_MAX_MINS_MULT = float(os.getenv("SWAGFLIP_WINNER_MAX_MINS_MULT", "1.5"))
 WINNER_EXTRA_BUY_BUMP = float(os.getenv("SWAGFLIP_WINNER_EXTRA_BUY_BUMP", "0.002"))
 
 # Aggressive fast-fill bump on buys (capped later so we don't pay away the whole spread)
 BUY_FAST_BUMP_PCT = float(os.getenv("SWAGFLIP_BUY_FAST_BUMP_PCT", "0.006"))
-MIN_ROI = float(os.getenv("SWAGFLIP_MIN_ROI", "0.0005"))
-MAX_ROI = float(os.getenv("SWAGFLIP_MAX_ROI", "0.40"))
+MIN_ROI = float(os.getenv("SWAGFLIP_MIN_ROI", "0.008"))
+MAX_ROI = float(os.getenv("SWAGFLIP_MAX_ROI", "0.02"))
 
 # Hard volume filter (avg daily volume)
 MIN_DAILY_VOLUME = int(os.getenv("SWAGFLIP_MIN_DAILY_VOLUME", "100000"))
 MAX_DAILY_VOLUME = int(os.getenv("SWAGFLIP_MAX_DAILY_VOLUME", "1000000000"))
 
 # Tax rule: items under this sell price are tax-free (default 50 gp)
 GE_TAX_FREE_UNDER_PRICE = int(os.getenv("SWAGFLIP_GE_TAX_FREE_UNDER_PRICE", "50"))
 
 # Inventory safety: do not auto-sell if we don't know cost basis unless enabled
 AUTO_SELL_UNKNOWN_BASIS = os.getenv("SWAGFLIP_AUTO_SELL_UNKNOWN_BASIS", "true").strip().lower() not in ("0", "false", "no", "off")
 
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
 
@@ -3047,259 +3053,74 @@ def prices_endpoint():
 
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
-        mapping, latest, volumes, last_refresh = prices.snapshot()
-
-        with _db_lock:
-            conn = db_connect()
-            try:
-                cur = conn.cursor()
-                cur.execute("SELECT COALESCE(SUM(profit), 0) AS p FROM realized_trades")
-                realized_profit = int(cur.fetchone()["p"])
-
-                cur.execute("SELECT COALESCE(SUM(qty * buy_price), 0) AS c FROM realized_trades")
-                realized_cost = int(cur.fetchone()["c"])
-
-                roi = (realized_profit / realized_cost) if realized_cost > 0 else 0.0
-
-                cur.execute("""SELECT COALESCE(SUM(qty_remaining * buy_price), 0) AS open_cost
-                               FROM lots WHERE qty_remaining > 0""")
-                open_cost_total = int(cur.fetchone()["open_cost"])
-
-                cur.execute("""
-                    SELECT item_id, item_name,
-                           SUM(qty_remaining) AS open_qty,
-                           SUM(qty_remaining * buy_price) AS open_cost
-                    FROM lots
-                    WHERE qty_remaining > 0
-                    GROUP BY item_id, item_name
-                    ORDER BY open_cost DESC
-                    LIMIT 200
-                """)
-                open_rows = [dict(x) for x in cur.fetchall()]
-
-                cur.execute("""
-                    SELECT trade_id, item_id, item_name, qty, buy_price, sell_price, sell_ts, profit
-                    FROM realized_trades
-                    ORDER BY sell_ts DESC
-                    LIMIT 100
-                """)
-                hist_rows = [dict(x) for x in cur.fetchall()]
-            finally:
-                conn.close()
-
-        with _last_status_lock:
-            st = dict(LAST_STATUS)
-        last_status_ts = int(st.get("updated_unix", 0))
-        coins = int(st.get("coins", 0))
-        offers = st.get("offers", []) or []
-
-        offer_rows_html = ""
-        for o in offers[:50]:
-            try:
-                status = str(o.get("status", "")).lower()
-                active = bool(o.get("active", False))
-                iid = int(o.get("item_id", 0))
-                box_id = int(o.get("box_id", 0))
-                price = int(o.get("price", 0))
-                total = int(o.get("amount_total", 0))
-                traded = int(o.get("amount_traded", 0))
-                gp_to_collect = int(o.get("gp_to_collect", 0))
-            except Exception:
-                continue
-            name = mapping[iid].name if iid in mapping else str(iid)
-            offer_rows_html += (
-                "<tr>"
-                f"<td>{box_id}</td>"
-                f"<td>{html_escape(status)}</td>"
-                f"<td>{'yes' if active else 'no'}</td>"
-                f"<td>{iid}</td>"
-                f"<td>{html_escape(name)}</td>"
-                f"<td>{price:,}</td>"
-                f"<td>{traded:,}/{total:,}</td>"
-                f"<td>{gp_to_collect:,}</td>"
-                "</tr>"
-            )
-        if not offer_rows_html:
-            offer_rows_html = "<tr><td colspan='8' style='opacity:0.8;'>No offers received yet (open GE / let plugin POST /suggestion)</td></tr>"
-
-        open_pos_html = ""
-        for r in open_rows:
-            iid = int(r["item_id"])
-            name = str(r["item_name"])
-            open_qty = int(r["open_qty"])
-            open_cost = int(r["open_cost"])
-            avg_buy = int(open_cost / open_qty) if open_qty > 0 else 0
-
-            lp = latest.get(iid) or {}
-            high = int(lp.get("high", 0)) if isinstance(lp, dict) else 0
-            sell_px = max(high - 1, 1) if high > 0 else 0
-            tax = ge_tax_per_unit(iid, sell_px) if sell_px > 0 else 0
-            unreal_per = (sell_px - avg_buy - tax) if sell_px > 0 else 0
-            unreal = unreal_per * open_qty if sell_px > 0 else 0
-
-            open_pos_html += (
-                "<tr>"
-                f"<td>{iid}</td>"
-                f"<td>{html_escape(name)}</td>"
-                f"<td>{open_qty:,}</td>"
-                f"<td>{avg_buy:,}</td>"
-                f"<td>{open_cost:,}</td>"
-                f"<td>{sell_px:,}</td>"
-                f"<td>{unreal:,}</td>"
-                "</tr>"
-            )
-        if not open_pos_html:
-            open_pos_html = "<tr><td colspan='7' style='opacity:0.8;'>No open lots (nothing currently held)</td></tr>"
-
-        hist_html = ""
-        for t in hist_rows:
-            hist_html += (
-                "<tr>"
-                f"<td>{int(t['trade_id'])}</td>"
-                f"<td>{int(t['item_id'])}</td>"
-                f"<td>{html_escape(str(t['item_name']))}</td>"
-                f"<td>{int(t['qty']):,}</td>"
-                f"<td>{int(t['buy_price']):,}</td>"
-                f"<td>{int(t['sell_price']):,}</td>"
-                f"<td>{int(t['profit']):,}</td>"
-                f"<td>{html_escape(format_ts(int(t['sell_ts'])))} </td>"
-                "</tr>"
-            )
-        if not hist_html:
-            hist_html = "<tr><td colspan='8' style='opacity:0.8;'>No realized trades yet</td></tr>"
-
-        html = f"""
+        html = """
         <html>
         <head>
-          <title>SwagFlip Dashboard</title>
+          <title>SwagFlip Dashboard (Reset)</title>
           <style>
-            body {{
+            body {
               font-family: system-ui, sans-serif;
               padding: 22px;
               background: #0f1115;
               color: #f2f2f2;
-            }}
-            a {{ color: #8ab4ff; text-decoration: none; }}
-            a:hover {{ text-decoration: underline; }}
-            .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }}
-            .card {{ background: #161a22; border: 1px solid #2a3243; border-radius: 10px; padding: 12px; }}
-            .k {{ opacity: 0.78; font-size: 12px; }}
-            .v {{ font-size: 20px; font-weight: 800; margin-top: 4px; }}
-            .row {{ display:flex; gap: 10px; flex-wrap: wrap; margin: 10px 0 18px; }}
-            .pill {{ background:#161a22; border:1px solid #2a3243; border-radius:999px; padding:8px 12px; font-size:13px; }}
-            .wrap {{ max-height: 340px; overflow:auto; border-radius: 10px; border: 1px solid #2a3243; }}
-
-            table {{
-              width: 100%;
-              border-collapse: collapse;
-              background: #11151d;
-              color: #f2f2f2;
-            }}
-            th, td {{
+            }
+            .card {
+              background: #161a22;
               border: 1px solid #2a3243;
-              padding: 8px;
-              text-align: left;
-              color: #f2f2f2;
-            }}
-            th {{
-              background: #141a26;
-              position: sticky;
-              top: 0;
-            }}
-            h2 {{ margin: 18px 0 10px; }}
+              border-radius: 10px;
+              padding: 16px;
+              max-width: 640px;
+            }
           </style>
         </head>
         <body>
-          <h1 style="margin:0 0 8px;">SwagFlip Dashboard</h1>
-
-          <div class="row">
-            <div class="pill">DB: <span style="opacity:0.85">{html_escape(DB_PATH)}</span></div>
-            <div class="pill">Prices refreshed: <span style="opacity:0.85">{html_escape(format_ts(int(last_refresh)))}</span></div>
-            <div class="pill">Last plugin status: <span style="opacity:0.85">{html_escape(format_ts(int(last_status_ts)))}</span></div>
-            <div class="pill">Coins (last status): <span style="opacity:0.85">{coins:,}</span></div>
-            <div class="pill">Buy-limit window: <span style="opacity:0.85">{BUY_LIMIT_RESET_SECONDS}s</span></div>
-            <div class="pill">Logs: <a href="/debug/logs" style="opacity:0.95">/debug/logs</a></div>
-          </div>
-
-          <div class="grid">
-            <div class="card"><div class="k">Realized Profit</div><div class="v">{realized_profit:,} gp</div></div>
-            <div class="card"><div class="k">Realized Cost</div><div class="v">{realized_cost:,} gp</div></div>
-            <div class="card"><div class="k">Realized ROI</div><div class="v">{roi*100:.2f}%</div></div>
-            <div class="card"><div class="k">Open Cost Basis</div><div class="v">{open_cost_total:,} gp</div></div>
-          </div>
-
-          <h2>Current Offers (from last /suggestion payload)</h2>
-          <div class="wrap">
-            <table>
-              <tr>
-                <th>Box</th><th>Status</th><th>Active</th><th>Item ID</th><th>Name</th><th>Price</th><th>Traded</th><th>GP to Collect</th>
-              </tr>
-              {offer_rows_html}
-            </table>
-          </div>
-
-          <h2>Open Positions (lots aggregated)</h2>
-          <div class="wrap">
-            <table>
-              <tr>
-                <th>Item ID</th><th>Name</th><th>Open Qty</th><th>Avg Buy</th><th>Open Cost</th><th>Est Sell (high-1)</th><th>Unrealized (est)</th>
-              </tr>
-              {open_pos_html}
-            </table>
-          </div>
-
-          <h2>Recent Realized Trades</h2>
-          <div class="wrap">
-            <table>
-              <tr>
-                <th>ID</th><th>Item ID</th><th>Name</th><th>Qty</th><th>Buy</th><th>Sell</th><th>Profit</th><th>Sold</th>
-              </tr>
-              {hist_html}
-            </table>
+          <h1 style="margin:0 0 12px;">SwagFlip Dashboard</h1>
+          <div class="card">
+            <p style="margin:0;">Dashboard reset. Add new widgets/layout here as we iterate.</p>
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
@@ -3427,59 +3248,59 @@ def suggestion():
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
-            min_margin_eff = max(MIN_MARGIN_GP, 25)
+            min_margin_eff = max(MIN_MARGIN_GP, 10)
             max_buy_mins = 60.0
         elif tf_minutes <= 120:
             min_roi_eff = max(MIN_ROI, 0.006)   # 0.6%
-            min_margin_eff = max(MIN_MARGIN_GP, 50)
+            min_margin_eff = max(MIN_MARGIN_GP, 25)
             max_buy_mins = 240.0
         else:
             min_roi_eff = max(MIN_ROI, 0.010)   # 1.0%
-            min_margin_eff = max(MIN_MARGIN_GP, 100)
+            min_margin_eff = max(MIN_MARGIN_GP, 40)
             max_buy_mins = 720.0
 
 
 
         # 1) sync offers -> DB (fills, lots, trades, rec metrics)
         try:
             sync_offers_and_fills(mapping, offers)
         except Exception:
             logger.exception("[SYNC] error")
         # 1.5) stale offers (abort stale buys, fast-dump stale sells)
         stale_action = maybe_handle_stale_offers(status, mapping, latest, volumes, offers, coins, inv, inv_full)
         if stale_action is not None:
             queued = None
             try:
                 queued = stale_action.pop("_queue_action", None)
             except Exception:
                 queued = None
 
             if queued:
                 try:
                     led2 = load_ledger()
                     led2.setdefault("action_queue", [])
                     led2["action_queue"].append(queued)
                     save_ledger(led2)
                 except Exception:
