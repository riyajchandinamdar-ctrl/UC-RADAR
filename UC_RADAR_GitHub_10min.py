"""
===============================================================
Angel One UC RADAR - Batch Optimized Upper Circuit Scanner
===============================================================

Key Enhancements:
1. Batch Quotes: Fetches market data in chunks of 50 tokens (~15s full pass).
2. Candle Memory Caching: Pulls historical data once per stock per day to prevent AB1021 errors.
3. Strict Volume & Order-Book Validation.
4. READ-ONLY: Never places trading orders.
"""

import os
import time
import math
import traceback
from pathlib import Path
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import pyotp
from dotenv import load_dotenv
from SmartApi import SmartConnect


# ============================================================
# CONFIGURATION
# ============================================================

TEST_MODE = False                # False for live market scanning

SCAN_START = dt_time(9, 15)
SCAN_END = dt_time(15, 30)       # Monitored scan window updated to 3:30 PM

BATCH_SIZE = 50                  # Angel One max token limit per quote request
API_DELAY_SECONDS = 0.2          # Delay between batch requests
CANDLE_DELAY_SECONDS = 0.5       # Rate limit buffer for historical candles

MIN_PRICE = 10.0                 # Exclude micro-pennies (< ₹10)
TOTAL_MARKET_MINUTES = 375.0


# ============================================================
# ENVIRONMENT & SETUP
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
INSTRUMENT_CACHE_FILE = BASE_DIR / "OpenAPIScripMaster.json"

load_dotenv(ENV_FILE)

API_KEY = os.getenv("ANGEL_API_KEY", "").strip()
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE", "").strip()
PASSWORD = os.getenv("ANGEL_PASSWORD", "").strip()
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

ALERTED_STOCKS = {}
CANDLE_CACHE = {}  # In-memory historical candle cache to prevent AB1021 rate limits
IST = ZoneInfo("Asia/Kolkata")


def validate_config():
    missing = []
    if not API_KEY: missing.append("ANGEL_API_KEY")
    if not CLIENT_CODE: missing.append("ANGEL_CLIENT_CODE")
    if not PASSWORD: missing.append("ANGEL_PASSWORD")
    if not TOTP_SECRET: missing.append("ANGEL_TOTP_SECRET")

    if missing:
        print("\nERROR: Missing required environment variables:")
        for item in missing:
            print("  -", item)
        raise SystemExit(1)


def safe_float(value, default=float("nan")):
    try:
        if value is None or value == "":
            return default
        res = float(value)
        return default if math.isnan(res) or math.isinf(res) else res
    except Exception:
        return default


def telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }
        requests.post(url, json=payload, timeout=5)
    except Exception as exc:
        print("Telegram exception:", exc)


# ============================================================
# ANGEL ONE LOGIN & UNIVERSE LOAD
# ============================================================

def login():
    print("Connecting to Angel One API...")
    api = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET).now()
    response = api.generateSession(CLIENT_CODE, PASSWORD, totp)

    if not response or response.get("status") is not True:
        raise RuntimeError(f"Angel One authentication failed: {response}")

    print("Angel One session authenticated.")
    return api


def load_full_nse_universe():
    urls = [
        "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
        "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
    ]
    data = None
    for url in urls:
        try:
            res = requests.get(url, timeout=(5, 30), headers={"User-Agent": "Mozilla/5.0"})
            res.raise_for_status()
            data = res.json()
            if isinstance(data, list) and data:
                break
        except Exception:
            continue

    if not data and INSTRUMENT_CACHE_FILE.exists():
        import json
        with open(INSTRUMENT_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    if not data:
        raise RuntimeError("Unable to load instrument master.")

    universe = {}
    for item in data:
        try:
            if item.get("exch_seg") == "NSE" and str(item.get("symbol", "")).endswith("-EQ"):
                token = str(item["token"])
                universe[token] = {
                    "symbol": item["symbol"],
                    "name": item.get("name", ""),
                }
        except Exception:
            continue

    print(f"Loaded {len(universe):,} NSE equity instruments into scanning universe.")
    return universe


# ============================================================
# BATCH MARKET QUOTES & CACHED CANDLES
# ============================================================

def get_batch_market_quotes(api, token_list):
    """Fetches market quotes in bulk batches of 50 tokens."""
    try:
        res = api.getMarketData("FULL", {"NSE": token_list})
        if res and res.get("status") is True:
            fetched = (res.get("data") or {}).get("fetched") or []
            return {str(item.get("symbolToken")): item for item in fetched}
    except Exception as exc:
        print("Batch quote error:", exc)
    return {}


def get_cached_daily_candles(api, token, days=80):
    """Fetches historical candles once per session and caches in memory."""
    if token in CANDLE_CACHE:
        return CANDLE_CACHE[token]

    time.sleep(CANDLE_DELAY_SECONDS)
    end = datetime.now(IST)
    start = end - timedelta(days=days)
    params = {
        "exchange": "NSE",
        "symboltoken": str(token),
        "interval": "ONE_DAY",
        "fromdate": start.strftime("%Y-%m-%d 09:15"),
        "todate": end.strftime("%Y-%m-%d 15:30"),
    }
    try:
        res = api.getCandleData(params)

        if isinstance(res, dict) and res.get("errorcode") == "AB1021":
            print("⚠️ Rate Limit AB1021 encountered. Pausing 2.5s...")
            time.sleep(2.5)
            return None

        if res and res.get("status") is True:
            rows = res.get("data") or []
            if rows:
                df = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                cleaned_df = df.dropna(subset=["close", "volume"])
                CANDLE_CACHE[token] = cleaned_df
                return cleaned_df
    except Exception:
        pass
    return None


def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


# ============================================================
# ANALYSIS & CLASSIFICATION
# ============================================================

def analyze_stock(quote, candles, elapsed_minutes):
    if not quote or candles is None or len(candles) < 25:
        return None

    ltp = safe_float(quote.get("ltp"))
    upper_circuit = safe_float(quote.get("upperCircuit"))
    live_volume = safe_float(quote.get("tradeVolume"))
    percent_change = safe_float(quote.get("percentChange"), 0)

    if ltp < MIN_PRICE or upper_circuit <= 0 or live_volume < 1000:
        return None

    uc_distance = max(0.0, ((upper_circuit - ltp) / ltp) * 100)

    candles = candles.copy()
    candles["rsi"] = calculate_rsi(candles["close"], 14)
    candles["avg_volume20"] = candles["volume"].rolling(20).mean()
    latest = candles.iloc[-1]

    rsi = safe_float(latest["rsi"])
    avg_daily_vol20 = safe_float(latest["avg_volume20"])

    time_fraction = max(1.0, elapsed_minutes) / TOTAL_MARKET_MINUTES
    expected_vol = avg_daily_vol20 * time_fraction
    vol_ratio = (live_volume / expected_vol) if expected_vol > 0 else float("nan")

    if math.isnan(vol_ratio):
        return None

    # Order Depth
    depth = quote.get("depth") or {}
    buys = depth.get("buy") or []
    sells = depth.get("sell") or []
    best5_buy_qty = sum([safe_float(i.get("quantity"), 0) for i in buys[:5]])
    best5_sell_qty = sum([safe_float(i.get("quantity"), 0) for i in sells[:5]])

    best5_ratio = (best5_buy_qty / best5_sell_qty) if best5_sell_qty > 0 else float("nan")

    category = None
    setup_tag = ""
    priority = 99

    # 🟢 Category A — UC Attack
    if (
        uc_distance <= 2.0 
        and percent_change > 0 
        and vol_ratio >= 1.5 
        and not math.isnan(best5_ratio) 
        and best5_ratio >= 1.5
    ):
        category = "🟢 Category A — UC Attack"
        priority = 1
        if percent_change >= 5.0 and vol_ratio >= 3.0 and rsi >= 65.0:
            setup_tag = " [🟢 A+ SETUP]"

    # 🟡 Category B — UC Watch
    elif (
        uc_distance <= 5.0 
        and percent_change >= 2.0 
        and vol_ratio >= 1.3 
        and not math.isnan(best5_ratio) 
        and best5_ratio >= 1.2
    ):
        category = "🟡 Category B — UC Watch"
        priority = 2

    # 🔵 Category C — Momentum Explosion
    elif percent_change >= 8.0 and vol_ratio >= 2.5 and rsi >= 60.0:
        category = "🔵 Category C — Momentum Explosion"
        priority = 3

    if not category:
        return None

    return {
        "ltp": ltp,
        "upper_circuit": upper_circuit,
        "uc_distance": uc_distance,
        "percent_change": percent_change,
        "volume": live_volume,
        "volume_ratio": vol_ratio,
        "rsi": rsi,
        "best5_ratio": best5_ratio,
        "category": category + setup_tag,
        "priority": priority,
    }


# ============================================================
# SCAN EXECUTION
# ============================================================

def scan_universe_batched(api, universe, elapsed_minutes):
    results = []
    tokens = list(universe.keys())

    # Process market quotes in chunks of 50
    for i in range(0, len(tokens), BATCH_SIZE):
        batch_tokens = tokens[i:i + BATCH_SIZE]
        quotes_dict = get_batch_market_quotes(api, batch_tokens)
        time.sleep(API_DELAY_SECONDS)

        for token in batch_tokens:
            quote = quotes_dict.get(str(token))
            if not quote:
                continue

            ltp = safe_float(quote.get("ltp"))
            uc = safe_float(quote.get("upperCircuit"))
            change = safe_float(quote.get("percentChange"), 0)

            if ltp <= 0 or uc <= 0 or change < 0:
                continue

            uc_dist = ((uc - ltp) / ltp) * 100
            
            # Stage 1 Filter: Skip pulling candle data unless stock is near UC or moving strongly
            if uc_dist > 5.0 and change < 8.0:
                continue

            candles = get_cached_daily_candles(api, token)
            metrics = analyze_stock(quote, candles, elapsed_minutes)

            if metrics:
                metrics["symbol"] = universe[token]["symbol"]
                results.append(metrics)

    return pd.DataFrame(results) if results else pd.DataFrame()


def process_alerts_and_display(df):
    now_str = datetime.now(IST).strftime('%H:%M:%S')
    print("=" * 125)
    print(f"🔥 BATCH UNIVERSE UC RADAR — {now_str}")
    print("=" * 125)

    if df.empty:
        print("No candidates currently meeting mandatory UC/Volume/Order-Book criteria.")
        return

    categories = [
        "🟢 Category A — UC Attack",
        "🟡 Category B — UC Watch",
        "🔵 Category C — Momentum Explosion",
    ]

    # Collect all new/category-upgrade alerts during THIS scan.
    # A single Telegram message will be sent after all categories are processed.
    scan_alerts = []

    for cat_base in categories:
        sub_df = df[df["category"].str.startswith(cat_base)].copy()
        if sub_df.empty:
            continue

        print(f"\n{cat_base.upper()}")
        print("-" * 125)
        print(f"{'Symbol':<18}{'LTP':>10}{'UC':>10}{'UC Dist':>10}{'Live%':>10}{'Vol Surge':>12}{'RSI':>8}{'B/S Best5':>12}")
        print("-" * 125)

        sub_df = sub_df.sort_values(
            by=["uc_distance", "volume_ratio"],
            ascending=[True, False]
        )

        for _, row in sub_df.iterrows():
            symbol = row["symbol"]
            bs_b5 = (
                f"{row['best5_ratio']:.2f}x"
                if not math.isnan(row["best5_ratio"])
                else "N/A"
            )

            print(
                f"{symbol:<18}"
                f"{row['ltp']:>10.2f}"
                f"{row['upper_circuit']:>10.2f}"
                f"{row['uc_distance']:>9.2f}%"
                f"{row['percent_change']:>9.2f}%"
                f"{row['volume_ratio']:>11.2f}x"
                f"{row['rsi']:>8.1f}"
                f"{bs_b5:>12}"
            )

            # Alert only when the stock enters a higher-priority category.
            # Do NOT send Telegram here; collect it for the single scan message.
            prev_priority = ALERTED_STOCKS.get(symbol, 99)

            if row["priority"] < prev_priority:
                ALERTED_STOCKS[symbol] = row["priority"]

                scan_alerts.append({
                    "symbol": symbol,
                    "category": row["category"],
                    "ltp": row["ltp"],
                    "upper_circuit": row["upper_circuit"],
                    "uc_distance": row["uc_distance"],
                    "percent_change": row["percent_change"],
                    "volume_ratio": row["volume_ratio"],
                    "rsi": row["rsi"],
                    "best5_ratio": bs_b5,
                    "priority": row["priority"],
                })

    # ============================================================
    # SINGLE TELEGRAM MESSAGE PER SCAN
    # ============================================================
    if scan_alerts:
        # Sort Category A -> B -> C, then nearest UC first.
        scan_alerts.sort(
            key=lambda x: (x["priority"], x["uc_distance"], -x["volume_ratio"])
        )

        message_lines = [
            f"🚨 <b>UC RADAR — SCAN {now_str}</b>",
            f"📊 <b>{len(scan_alerts)} new/upgrade alert(s)</b>",
            ""
        ]

        current_category = None

        for alert in scan_alerts:
            category = alert["category"]

            # Keep the category heading separate, so all categories are
            # consolidated into the same Telegram message.
            if category != current_category:
                if current_category is not None:
                    message_lines.append("")
                message_lines.append(f"<b>{category}</b>")
                current_category = category

            message_lines.append(
                f"• <b>{alert['symbol']}</b> | "
                f"LTP ₹{alert['ltp']:.2f} | UC ₹{alert['upper_circuit']:.2f} | "
                f"UC Dist {alert['uc_distance']:.2f}% | "
                f"Chg +{alert['percent_change']:.2f}% | "
                f"Vol {alert['volume_ratio']:.2f}x | "
                f"RSI {alert['rsi']:.1f} | "
                f"B/S {alert['best5_ratio']}"
            )

        message_lines.append("")
        message_lines.append("⚠️ <i>Read-only scanner — no orders placed.</i>")

        telegram_message = "\n".join(message_lines)

        # Telegram messages are limited to 4096 characters.
        # Keep the requested behaviour of ONE message per scan.
        if len(telegram_message) > 4096:
            print(
                f"⚠️ Telegram message is {len(telegram_message)} characters; "
                "truncating to remain within Telegram's 4096-character limit."
            )
            telegram_message = telegram_message[:4050] + (
                "\n\n⚠️ <i>Message truncated due to Telegram limit.</i>"
            )

        telegram_send(telegram_message)

    print("\n" + "=" * 125)


# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main():
    """Run exactly ONE market scan. GitHub Actions schedules this every 10 minutes."""
    validate_config()

    now = datetime.now(IST)

    # Never scan on weekends.
    if not TEST_MODE and now.weekday() >= 5:
        print(f"Today is {now.strftime('%A')}. Markets are closed on weekends.")
        return

    current_time = now.time()

    # GitHub Actions controls the 10-minute schedule. These checks are an
    # additional safety guard so the scanner cannot run outside market hours.
    if not TEST_MODE and current_time < SCAN_START:
        print(f"[{now.strftime('%H:%M:%S')}] Before market open (09:15). No scan performed.")
        return

    if not TEST_MODE and current_time > SCAN_END:
        print(f"[{now.strftime('%H:%M:%S')}] After scan window (15:30). No scan performed.")
        return

    api = login()
    universe = load_full_nse_universe()

    print("\n🚀 Batch Universe UC Radar — GitHub single-scan mode.")
    print(f"Scan time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    market_open = datetime.combine(now.date(), SCAN_START, tzinfo=IST)
    elapsed_minutes = max(1.0, (now - market_open).total_seconds() / 60.0)

    try:
        df = scan_universe_batched(api, universe, elapsed_minutes)
        process_alerts_and_display(df)
    except Exception as exc:
        print("SCAN ERROR:", exc)
        traceback.print_exc()
        raise

    print("\n✅ Scan completed. Exiting so the next GitHub Actions schedule can start a fresh scan.")


if __name__ == "__main__":
    main()
