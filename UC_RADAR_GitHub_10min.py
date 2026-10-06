"""
===============================================================
Angel One UC RADAR - Batch Optimized Upper Circuit Scanner
===============================================================

Key Enhancements:
1. Batch Quotes: Fetches market data in chunks of 50 tokens.
2. Candle Memory Caching: Pulls historical data once per stock per day.
3. Strict Volume & Order-Book Validation.
4. READ-ONLY: Never places trading orders.
5. Explicit Asia/Kolkata timezone handling for GitHub Actions.
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
# TIMEZONE
# ============================================================

# GitHub Actions Linux runners use UTC by default.
# NSE market timings are in Indian Standard Time.
IST = ZoneInfo("Asia/Kolkata")


# ============================================================
# CONFIGURATION
# ============================================================

TEST_MODE = False                # False for live market scanning

SCAN_START = dt_time(9, 15)
SCAN_END = dt_time(15, 30)

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
CANDLE_CACHE = {}  # In-memory historical candle cache


# ============================================================
# CONFIG VALIDATION
# ============================================================

def validate_config():
    missing = []

    if not API_KEY:
        missing.append("ANGEL_API_KEY")

    if not CLIENT_CODE:
        missing.append("ANGEL_CLIENT_CODE")

    if not PASSWORD:
        missing.append("ANGEL_PASSWORD")

    if not TOTP_SECRET:
        missing.append("ANGEL_TOTP_SECRET")

    if missing:
        print("\nERROR: Missing required environment variables:")

        for item in missing:
            print("  -", item)

        raise SystemExit(1)


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

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
        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        )

        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }

        response = requests.post(
            url,
            json=payload,
            timeout=5
        )

        if not response.ok:
            print(
                "Telegram API error:",
                response.status_code,
                response.text
            )

    except Exception as exc:
        print("Telegram exception:", exc)


# ============================================================
# ANGEL ONE LOGIN
# ============================================================

def login():
    print("Connecting to Angel One API...")

    api = SmartConnect(api_key=API_KEY)

    totp = pyotp.TOTP(TOTP_SECRET).now()

    response = api.generateSession(
        CLIENT_CODE,
        PASSWORD,
        totp
    )

    if not response or response.get("status") is not True:
        raise RuntimeError(
            f"Angel One authentication failed: {response}"
        )

    print("Angel One session authenticated.")

    return api


# ============================================================
# LOAD NSE UNIVERSE
# ============================================================

def load_full_nse_universe():

    urls = [
        "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
        "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
    ]

    data = None

    for url in urls:

        try:
            res = requests.get(
                url,
                timeout=(5, 30),
                headers={"User-Agent": "Mozilla/5.0"}
            )

            res.raise_for_status()

            data = res.json()

            if isinstance(data, list) and data:
                break

        except Exception:
            continue

    if not data and INSTRUMENT_CACHE_FILE.exists():

        import json

        with open(
            INSTRUMENT_CACHE_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            data = json.load(f)

    if not data:
        raise RuntimeError(
            "Unable to load instrument master."
        )

    universe = {}

    for item in data:

        try:

            if (
                item.get("exch_seg") == "NSE"
                and str(item.get("symbol", "")).endswith("-EQ")
            ):

                token = str(item["token"])

                universe[token] = {
                    "symbol": item["symbol"],
                    "name": item.get("name", ""),
                }

        except Exception:
            continue

    print(
        f"Loaded {len(universe):,} NSE equity "
        f"instruments into scanning universe."
    )

    return universe


# ============================================================
# BATCH MARKET QUOTES
# ============================================================

def get_batch_market_quotes(api, token_list):

    """Fetch market quotes in bulk batches of 50 tokens."""

    try:

        res = api.getMarketData(
            "FULL",
            {"NSE": token_list}
        )

        if res and res.get("status") is True:

            fetched = (
                (res.get("data") or {}).get("fetched")
                or []
            )

            return {
                str(item.get("symbolToken")): item
                for item in fetched
            }

    except Exception as exc:

        print(
            "Batch quote error:",
            exc
        )

    return {}


# ============================================================
# CACHED DAILY CANDLES
# ============================================================

def get_cached_daily_candles(api, token, days=80):

    """
    Fetch historical candles once per session
    and cache them in memory.
    """

    if token in CANDLE_CACHE:
        return CANDLE_CACHE[token]

    time.sleep(CANDLE_DELAY_SECONDS)

    # IMPORTANT:
    # Always use IST because Angel One candle dates
    # are based on Indian market timings.
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

        if (
            isinstance(res, dict)
            and res.get("errorcode") == "AB1021"
        ):

            print(
                "⚠️ Rate Limit AB1021 encountered. "
                "Pausing 2.5s..."
            )

            time.sleep(2.5)

            return None

        if res and res.get("status") is True:

            rows = res.get("data") or []

            if rows:

                df = pd.DataFrame(
                    rows,
                    columns=[
                        "datetime",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume"
                    ]
                )

                for col in [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]:

                    df[col] = pd.to_numeric(
                        df[col],
                        errors="coerce"
                    )

                cleaned_df = df.dropna(
                    subset=[
                        "close",
                        "volume"
                    ]
                )

                CANDLE_CACHE[token] = cleaned_df

                return cleaned_df

    except Exception:
        pass

    return None


# ============================================================
# RSI
# ============================================================

def calculate_rsi(series, period=14):

    delta = series.diff()

    gain = delta.clip(lower=0)

    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / period,
        min_periods=period,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        min_periods=period,
        adjust=False
    ).mean()

    rs = (
        avg_gain /
        avg_loss.replace(0, float("nan"))
    )

    return 100 - (100 / (1 + rs))


# ============================================================
# ANALYSIS & CLASSIFICATION
# ============================================================

def analyze_stock(
    quote,
    candles,
    elapsed_minutes
):

    if (
        not quote
        or candles is None
        or len(candles) < 25
    ):
        return None

    ltp = safe_float(
        quote.get("ltp")
    )

    upper_circuit = safe_float(
        quote.get("upperCircuit")
    )

    live_volume = safe_float(
        quote.get("tradeVolume")
    )

    percent_change = safe_float(
        quote.get("percentChange"),
        0
    )

    if (
        ltp < MIN_PRICE
        or upper_circuit <= 0
        or live_volume < 1000
    ):
        return None

    uc_distance = max(
        0.0,
        ((upper_circuit - ltp) / ltp) * 100
    )

    candles = candles.copy()

    candles["rsi"] = calculate_rsi(
        candles["close"],
        14
    )

    candles["avg_volume20"] = (
        candles["volume"].rolling(20).mean()
    )

    latest = candles.iloc[-1]

    rsi = safe_float(
        latest["rsi"]
    )

    avg_daily_vol20 = safe_float(
        latest["avg_volume20"]
    )

    time_fraction = (
        max(1.0, elapsed_minutes)
        / TOTAL_MARKET_MINUTES
    )

    expected_vol = (
        avg_daily_vol20 *
        time_fraction
    )

    vol_ratio = (
        live_volume / expected_vol
        if expected_vol > 0
        else float("nan")
    )

    if math.isnan(vol_ratio):
        return None

    # ========================================================
    # ORDER DEPTH
    # ========================================================

    depth = quote.get("depth") or {}

    buys = depth.get("buy") or []

    sells = depth.get("sell") or []

    best5_buy_qty = sum(
        [
            safe_float(
                i.get("quantity"),
                0
            )
            for i in buys[:5]
        ]
    )

    best5_sell_qty = sum(
        [
            safe_float(
                i.get("quantity"),
                0
            )
            for i in sells[:5]
        ]
    )

    best5_ratio = (
        best5_buy_qty / best5_sell_qty
        if best5_sell_qty > 0
        else float("nan")
    )

    category = None

    setup_tag = ""

    priority = 99

    # ========================================================
    # CATEGORY A — UC ATTACK
    # ========================================================

    if (
        uc_distance <= 2.0
        and percent_change > 0
        and vol_ratio >= 1.5
        and not math.isnan(best5_ratio)
        and best5_ratio >= 1.5
    ):

        category = (
            "🟢 Category A — UC Attack"
        )

        priority = 1

        if (
            percent_change >= 5.0
            and vol_ratio >= 3.0
            and rsi >= 65.0
        ):

            setup_tag = (
                " [🟢 A+ SETUP]"
            )

    # ========================================================
    # CATEGORY B — UC WATCH
    # ========================================================

    elif (
        uc_distance <= 5.0
        and percent_change >= 2.0
        and vol_ratio >= 1.3
        and not math.isnan(best5_ratio)
        and best5_ratio >= 1.2
    ):

        category = (
            "🟡 Category B — UC Watch"
        )

        priority = 2

    # ========================================================
    # CATEGORY C — MOMENTUM EXPLOSION
    # ========================================================

    elif (
        percent_change >= 8.0
        and vol_ratio >= 2.5
        and rsi >= 60.0
    ):

        category = (
            "🔵 Category C — Momentum Explosion"
        )

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

def scan_universe_batched(
    api,
    universe,
    elapsed_minutes
):

    results = []

    tokens = list(
        universe.keys()
    )

    # Process market quotes in chunks of 50
    for i in range(
        0,
        len(tokens),
        BATCH_SIZE
    ):

        batch_tokens = tokens[
            i:i + BATCH_SIZE
        ]

        quotes_dict = (
            get_batch_market_quotes(
                api,
                batch_tokens
            )
        )

        time.sleep(
            API_DELAY_SECONDS
        )

        for token in batch_tokens:

            quote = quotes_dict.get(
                str(token)
            )

            if not quote:
                continue

            ltp = safe_float(
                quote.get("ltp")
            )

            uc = safe_float(
                quote.get("upperCircuit")
            )

            change = safe_float(
                quote.get("percentChange"),
                0
            )

            if (
                ltp <= 0
                or uc <= 0
                or change < 0
            ):
                continue

            uc_dist = (
                (uc - ltp) /
                ltp
            ) * 100

            # Stage 1 Filter:
            # Skip candle data unless stock is
            # near UC or moving strongly.
            if (
                uc_dist > 5.0
                and change < 8.0
            ):
                continue

            candles = (
                get_cached_daily_candles(
                    api,
                    token
                )
            )

            metrics = analyze_stock(
                quote,
                candles,
                elapsed_minutes
            )

            if metrics:

                metrics["symbol"] = (
                    universe[token]["symbol"]
                )

                results.append(metrics)

    return (
        pd.DataFrame(results)
        if results
        else pd.DataFrame()
    )


# ============================================================
# ALERT PROCESSING & DISPLAY
# ============================================================

def process_alerts_and_display(df):

    # IMPORTANT:
    # Always display Telegram/log timestamp in IST.
    now_str = datetime.now(
        IST
    ).strftime("%H:%M:%S")

    print("=" * 125)

    print(
        f"🔥 BATCH UNIVERSE UC RADAR — {now_str} IST"
    )

    print("=" * 125)

    if df.empty:

        print(
            "No candidates currently meeting "
            "mandatory UC/Volume/Order-Book criteria."
        )

        return

    categories = [

        "🟢 Category A — UC Attack",

        "🟡 Category B — UC Watch",

        "🔵 Category C — Momentum Explosion",
    ]

    # Collect all new/category-upgrade alerts
    # during THIS scan.
    #
    # A single Telegram message will be sent
    # after all categories are processed.

    scan_alerts = []

    for cat_base in categories:

        sub_df = df[
            df["category"].str.startswith(
                cat_base
            )
        ].copy()

        if sub_df.empty:
            continue

        print(
            f"\n{cat_base.upper()}"
        )

        print("-" * 125)

        print(
            f"{'Symbol':<18}"
            f"{'LTP':>10}"
            f"{'UC':>10}"
            f"{'UC Dist':>10}"
            f"{'Live%':>10}"
            f"{'Vol Surge':>12}"
            f"{'RSI':>8}"
            f"{'B/S Best5':>12}"
        )

        print("-" * 125)

        sub_df = sub_df.sort_values(
            by=[
                "uc_distance",
                "volume_ratio"
            ],
            ascending=[
                True,
                False
            ]
        )

        for _, row in sub_df.iterrows():

            symbol = row["symbol"]

            bs_b5 = (

                f"{row['best5_ratio']:.2f}x"

                if not math.isnan(
                    row["best5_ratio"]
                )

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

            # Alert only when the stock enters
            # a higher-priority category.
            #
            # Do NOT send Telegram here.
            # Collect it for the single scan message.

            prev_priority = (
                ALERTED_STOCKS.get(
                    symbol,
                    99
                )
            )

            if row["priority"] < prev_priority:

                ALERTED_STOCKS[
                    symbol
                ] = row["priority"]

                scan_alerts.append({

                    "symbol": symbol,

                    "category": row[
                        "category"
                    ],

                    "ltp": row[
                        "ltp"
                    ],

                    "upper_circuit": row[
                        "upper_circuit"
                    ],

                    "uc_distance": row[
                        "uc_distance"
                    ],

                    "percent_change": row[
                        "percent_change"
                    ],

                    "volume_ratio": row[
                        "volume_ratio"
                    ],

                    "rsi": row[
                        "rsi"
                    ],

                    "best5_ratio": bs_b5,

                    "priority": row[
                        "p
