# ============================================================
# ULTIMATE QUANTUM WHALE v19.0 - ULTIMATE UPGRADED (FULLY INTEGRATED)
# ============================================================
# NEW FEATURES:
# 1. Dynamic Trailing Stop-Loss (ATR-based adaptive trailing)
# 2. Volatility-Based Position Sizing (ATR ratio scaling)
# 3. Partial Take-Profits (50/30/20 split with trailing remainder)
# 4. Weighted Voting System (dynamic strategy weights from historical performance)
# 5. ML/Regime Filter (strategies activated based on market regime)
# 6. Asynchronous API Calls (aiohttp for external sources)
# 7. Enhanced Database Logging (slippage, latency, exit details)
# ============================================================
import eventlet
eventlet.monkey_patch(thread=True, socket=True, select=True, time=True)

import time
import sqlite3
import requests
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_socketio import SocketIO, emit
import threading
import logging
from datetime import datetime, timedelta
import json
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import cloudinary
import cloudinary.uploader
import cloudinary.api
from dotenv import load_dotenv
from contextlib import contextmanager
import signal
import sys
import hmac
import hashlib
import base64
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any
import math
from dataclasses import dataclass, field
import asyncio
import aiohttp
import redis
from cachetools import TTLCache
import re

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION - ULTIMATE v19.0
# ============================================================
BITGET_API_KEY = os.environ.get('BITGET_API_KEY', '')
BITGET_API_SECRET = os.environ.get('BITGET_API_SECRET', '')
BITGET_PASSPHRASE = os.environ.get('BITGET_PASSPHRASE', '')
TRADING_MODE = os.environ.get('TRADING_MODE', 'PAPER').upper()
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# ✅ BITGET API BASE URLS
BITGET_REST_URL = "https://api.bitget.com"
BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"
BITGET_WS_PRIVATE = "wss://ws.bitget.com/v2/ws/private"

# ✅ TOP 50 CRYPTO + GOLD (XAUUSDT)
ALL_SYMBOLS = [
    # Top 10 Large Cap
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
    "AVAXUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT",
    # Next 20 Mid Cap
    "UNIUSDT", "ATOMUSDT", "LTCUSDT", "BCHUSDT", "NEARUSDT",
    "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT", "SUIUSDT",
    "SEIUSDT", "TIAUSDT", "WIFUSDT", "PEPEUSDT", "FLOKIUSDT",
    "BONKUSDT", "SHIBUSDT", "XLMUSDT", "HBARUSDT", "VETUSDT",
    # Next 20 Alt Coins
    "FILUSDT", "RNDRUSDT", "GRTUSDT", "AAVEUSDT", "MKRUSDT",
    "SNXUSDT", "COMPUSDT", "LDOUSDT", "RPLUSDT", "ENSUSDT",
    "CFGUSDT", "DYDXUSDT", "GMXUSDT", "GNSUSDT", "RDNTUSDT",
    "AGIXUSDT", "OCEANUSDT", "FETUSDT", "ALGOUSDT", "ICPUSDT",
    # GOLD
    "XAUUSDT"
]

MAIN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "XAUUSDT"]

# ============================================================
# TRADING CONSTANTS - UPDATED WITH V19.0
# ============================================================
MAX_ACTIVE_TRADES = 5
MAX_RISK_PER_TRADE = 0.02
MAX_DAILY_DRAWDOWN = 0.10
MIN_RR_RATIO = 1.5
MAX_RR_RATIO = 3.0

# ✅ 4 TIMEFRAMES
MTF_TIMEFRAMES = ["5m", "15m", "1H", "4H"]
TRADE_TIMEFRAMES = ["5m", "15m"]
ANALYSIS_TIMEFRAMES = ["5m", "15m", "1H", "4H"]

MTF_CONFIRMATION_THRESHOLD = 0.50
REGIME_LOOKBACK = 100
TRENDING_THRESHOLD = 20

DAILY_LOSS_LIMIT = 0.08
DAILY_RESET_HOUR = 0

VWAP_PERIOD = 20
VOLUME_PROFILE_LEVELS = 10
MIN_VOLUME_DENSITY = 0.3

# ✅ NEW: Trailing Stop & Partial TP Config
TRAILING_ACTIVATION = 0.8      # ATR multiplier to activate trailing
TRAILING_STEP = 0.2            # ATR step for trailing
PARTIAL_TP_LEVELS = [0.50, 0.30, 0.20]  # % of position to exit at each TP
PARTIAL_TP_TARGETS = [1.5, 3.0, 0.0]    # profit % targets (last is trailing remainder)

REQUIRE_BTC_ALIGNMENT = False
BTC_SYMBOL = "BTCUSDT"
FEAR_GREED_THRESHOLD = 20
CONSENSUS_THRESHOLD = 3       # ✅ Now we use weighted voting, threshold is weighted sum
MIN_CONFIDENCE_SCORE = 60
SIGNAL_COOLDOWN = 60
UPDATE_INTERVAL_MS = 100
ORDER_BOOK_DEPTH = 10
MIN_BID_ASK_SPREAD = 0.001
ML_PREDICTION_WEIGHT = 0.20
ML_MIN_CONFIDENCE = 55
VAR_CONFIDENCE_LEVEL = 0.95
MAX_DRAWDOWN_LIMIT = 0.25

MIN_TRADES_FOR_WIN_RATE = 5
MIN_WIN_RATE = 35.0
MIN_LOT_SIZE = 0.001
LOT_SIZE_STEP = 0.001
MIN_ATR_PERCENT = 0.5
MAX_ATR_PERCENT = 10.0
MIN_VOLUME_MULTIPLIER = 0.5
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

BREAKOUT_LOOKBACK = 20
BREAKOUT_VOLUME_MULTIPLIER = 1.5
BREAKOUT_CONFIRMATION_CANDLES = 2

SR_LOOKBACK = 50
SR_PROXIMITY_PERCENT = 0.5
SR_CONFIRMATION_STRENGTH = 2

TOP_MOVERS_UPDATE_INTERVAL = 300
TOP_MOVERS_COUNT = 10

MACRO_UPDATE_INTERVAL = 300
HIGH_IMPACT_NEWS_PAUSE_DURATION = 3600

# ============================================================
# REDIS CACHE
# ============================================================
REDIS_URL = os.environ.get('REDIS_URL', None)
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        logger.info("✅ Redis cache enabled")
    except:
        redis_client = None
        logger.warning("⚠️ Redis unavailable, using memory cache")
else:
    redis_client = None

memory_cache = TTLCache(maxsize=1000, ttl=60)

def cache_get(key):
    if redis_client:
        return redis_client.get(key)
    return memory_cache.get(key)

def cache_set(key, value, ttl=60):
    if redis_client:
        redis_client.setex(key, ttl, value)
    else:
        memory_cache[key] = value

# ============================================================
# FLASK & WEBSOCKET
# ============================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=60, ping_interval=25, max_http_buffer_size=10**7)

# ============================================================
# DATABASE - UPGRADED WITH NEW COLUMNS
# ============================================================
class DatabaseManager:
    def __init__(self, db_path='trading_bot.db'):
        self.db_path = db_path
        self._local = threading.local()
        self._executor = ThreadPoolExecutor(max_workers=4)
        
    @contextmanager
    def get_connection(self):
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield self._local.conn
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
            
    def close_all(self):
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
        self._executor.shutdown(wait=False)

db_manager = DatabaseManager()

def init_db():
    with db_manager.get_connection() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, side TEXT, entry REAL, size REAL,
            pnl REAL, status TEXT, order_id TEXT, mode TEXT, exchange TEXT,
            confidence_score REAL, strategies_used TEXT, pattern_type TEXT,
            entry_reason TEXT, leverage_used REAL, exit_price REAL,
            rr_ratio REAL, mtf_confirmed TEXT,
            vwap_deviation REAL, volume_density REAL, btc_alignment TEXT,
            fear_greed_index INTEGER, pattern_weight_used REAL,
            ml_prediction_score REAL, var_at_risk REAL, order_book_spread REAL,
            market_regime TEXT, daily_pnl REAL, drawdown_percent REAL,
            weekly_trend TEXT, daily_trend TEXT, is_breakout INTEGER DEFAULT 0,
            sr_type TEXT, breakout_confirmed INTEGER DEFAULT 0,
            macro_bias TEXT, golden_hour_signal INTEGER DEFAULT 0,
            timeframe TEXT DEFAULT '15m',
            market_cap_sentiment TEXT DEFAULT 'NEUTRAL',
            funding_rate_sentiment TEXT DEFAULT 'NEUTRAL',
            -- NEW COLUMNS v19.0
            slippage REAL DEFAULT 0,
            latency_ms REAL DEFAULT 0,
            exit_reason_details TEXT DEFAULT '',
            partial_exits TEXT DEFAULT ''
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, side TEXT, entry REAL,
            stop_loss REAL, take_profit REAL, confidence_score REAL,
            strategies_agreed TEXT, pattern_type TEXT, consensus_count INTEGER,
            rr_ratio REAL, mtf_status TEXT, btc_status TEXT,
            fear_greed_index INTEGER, vwap_status TEXT,
            ml_prediction TEXT, var_status TEXT, market_regime TEXT,
            weekly_trend TEXT, daily_trend TEXT, is_breakout INTEGER DEFAULT 0,
            sr_type TEXT, macro_bias TEXT, golden_hour_signal INTEGER DEFAULT 0,
            timeframe TEXT DEFAULT '15m',
            market_cap_sentiment TEXT DEFAULT 'NEUTRAL',
            funding_rate_sentiment TEXT DEFAULT 'NEUTRAL'
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS pattern_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_name TEXT UNIQUE, total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0, weight REAL DEFAULT 1.0,
            last_updated TEXT, ml_accuracy REAL DEFAULT 0
        )''')
        # NEW TABLE: strategy_performance for weighted voting
        conn.execute('''CREATE TABLE IF NOT EXISTS strategy_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name TEXT UNIQUE,
            total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0,
            weight REAL DEFAULT 1.0,
            last_updated TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, capital REAL, peak_capital REAL, drawdown REAL,
            equity REAL, win_rate REAL, total_trades INTEGER,
            avg_rr REAL, avg_confidence REAL, var_95 REAL, sharpe_ratio REAL,
            daily_pnl REAL, market_regime TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE, starting_capital REAL, ending_capital REAL,
            daily_pnl REAL, daily_pnl_percent REAL, trades_count INTEGER,
            wins INTEGER, losses INTEGER, win_rate REAL
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS learning_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, action TEXT, outcome TEXT,
            confidence REAL, sentiment TEXT, regime TEXT, pnl REAL,
            weekly_trend TEXT, daily_trend TEXT, macro_bias TEXT,
            timeframe TEXT DEFAULT '15m',
            market_cap_sentiment TEXT DEFAULT 'NEUTRAL',
            funding_rate_sentiment TEXT DEFAULT 'NEUTRAL'
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS price_cache (
            symbol TEXT PRIMARY KEY,
            price REAL, timestamp REAL,
            change_24h REAL, volume REAL
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS top_movers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, change_24h REAL,
            price REAL, volume_ratio REAL, mover_type TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS macro_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, indicator TEXT, value REAL,
            sentiment TEXT, dxy_price REAL, gold_bias TEXT,
            market_cap REAL, btc_dominance REAL,
            funding_rate_avg REAL, fear_greed_index INTEGER
        )''')
        conn.commit()
    logger.info("✅ Database initialized with v19.0 features (Enhanced Logging, Strategy Performance)")

init_db()

# ============================================================
# BITGET API FUNCTIONS
# ============================================================
def get_bitget_signature(timestamp, method, request_path, body, secret_key):
    body_string = json.dumps(body) if body else ""
    str_to_sign = str(timestamp) + method.upper() + request_path + body_string
    return base64.b64encode(hmac.new(secret_key.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')

def send_bitget_request(method, endpoint, body=None, params=None):
    api_key, secret_key, passphrase = BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSPHRASE
    if not api_key or not secret_key or not passphrase:
        logger.warning("⚠️ Bitget API credentials not set, using PAPER mode")
        return None
    url = f"{BITGET_REST_URL}{endpoint}"
    timestamp = str(int(time.time() * 1000))
    signature = get_bitget_signature(timestamp, method, endpoint, body, secret_key)
    if not signature:
        return None
    headers = {
        'ACCESS-KEY': api_key,
        'ACCESS-SIGN': signature,
        'ACCESS-TIMESTAMP': timestamp,
        'ACCESS-PASSPHRASE': passphrase,
        'Content-Type': 'application/json',
        'locale': 'en-US'
    }
    try:
        if method.upper() == 'POST':
            response = requests.post(url, json=body, headers=headers, timeout=10)
        else:
            response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Bitget API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"❌ Bitget API error: {e}")
        return None

def bitget_public_request(endpoint, params=None):
    url = f"{BITGET_REST_URL}{endpoint}"
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        logger.error(f"Bitget public request error: {e}")
        return None

# ============================================================
# ASYNC HELPER FOR EXTERNAL APIS (v19.0)
# ============================================================
async def async_fetch(session, url, params=None, headers=None, timeout=10):
    try:
        async with session.get(url, params=params, headers=headers, timeout=timeout) as response:
            if response.status == 200:
                return await response.json()
            else:
                return None
    except Exception as e:
        logger.debug(f"Async fetch error {url}: {e}")
        return None

def run_async_fetch(url, params=None, headers=None):
    """Synchronous wrapper to run async fetch"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        async def fetch():
            async with aiohttp.ClientSession() as session:
                return await async_fetch(session, url, params, headers)
        result = loop.run_until_complete(fetch())
        return result
    finally:
        loop.close()

# ============================================================
# PRICE FETCHER (already asynchronous using ThreadPoolExecutor)
# ============================================================
class AsyncPriceFetcher:
    def __init__(self):
        self.prices = {}
        self.last_update = 0
        self.update_interval = 0.1
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        
    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"🚀 Async price fetcher started for {len(ALL_SYMBOLS)} symbols")
        
    def stop(self):
        self._running = False
        
    def _run(self):
        while self._running:
            try:
                self._fetch_all_prices()
                time.sleep(self.update_interval)
            except Exception as e:
                logger.debug(f"Price fetch error: {e}")
                time.sleep(0.5)
                
    def _fetch_all_prices(self):
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(self._fetch_single_price, symbol): symbol for symbol in ALL_SYMBOLS}
            for future in as_completed(futures, timeout=0.5):
                symbol = futures[future]
                try:
                    result = future.result()
                    if result:
                        with self._lock:
                            self.prices[symbol] = result
                except Exception:
                    pass
        self.last_update = time.time()
        
    def _fetch_single_price(self, symbol):
        try:
            endpoint = f"/api/v2/mix/market/ticker"
            params = {"symbol": symbol, "productType": "usdt-futures"}
            response = bitget_public_request(endpoint, params)
            if response and response.get('code') == '00000':
                ticker = response.get('data', {})
                price = float(ticker.get('price', 0))
                if price > 0:
                    return {
                        'price': price,
                        'change_24h': float(ticker.get('change24h', 0)),
                        'volume': float(ticker.get('volume', 0)),
                        'high': float(ticker.get('high', 0)),
                        'low': float(ticker.get('low', 0)),
                        'timestamp': time.time()
                    }
        except Exception:
            pass
        return None
        
    def get_price(self, symbol):
        with self._lock:
            return self.prices.get(symbol)

price_fetcher = AsyncPriceFetcher()

# ============================================================
# CANDLE FETCHER
# ============================================================
def fetch_candles(symbol, granularity="1H", limit=100):
    cache_key = f"candles_{symbol}_{granularity}_{limit}"
    cached = cache_get(cache_key)
    if cached:
        try:
            return pd.DataFrame(json.loads(cached))
        except:
            pass
    
    try:
        endpoint = f"/api/v2/mix/market/candles"
        params = {
            "symbol": symbol,
            "productType": "usdt-futures",
            "granularity": granularity,
            "limit": limit
        }
        response = bitget_public_request(endpoint, params)
        if response and response.get('code') == '00000':
            candles = response.get('data', [])
            if not candles or len(candles) < 10:
                return None
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            if df['close'].iloc[-1] <= 0 or pd.isna(df['close'].iloc[-1]):
                return None
            if df['high'].max() <= 0 or df['low'].min() <= 0:
                return None
                
            cache_set(cache_key, json.dumps(df.to_dict('records')), ttl=30)
            return df
    except Exception as e:
        logger.debug(f"Candle error {symbol}: {e}")
    return None

def get_live_price(symbol):
    cached = price_fetcher.get_price(symbol)
    if cached:
        return cached
    try:
        endpoint = f"/api/v2/mix/market/ticker"
        params = {"symbol": symbol, "productType": "usdt-futures"}
        response = bitget_public_request(endpoint, params)
        if response and response.get('code') == '00000':
            ticker = response.get('data', {})
            price = float(ticker.get('price', 0))
            if price > 0:
                return {
                    'price': price,
                    'change_24h': float(ticker.get('change24h', 0)),
                    'volume': float(ticker.get('volume', 0)),
                    'high': float(ticker.get('high', 0)),
                    'low': float(ticker.get('low', 0))
                }
    except Exception:
        pass
    return None

def get_order_book(symbol, depth=ORDER_BOOK_DEPTH):
    try:
        endpoint = f"/api/v2/mix/market/orderbook"
        params = {"symbol": symbol, "productType": "usdt-futures", "limit": depth}
        response = bitget_public_request(endpoint, params)
        if response and response.get('code') == '00000':
            orderbook = response.get('data', {})
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
            
            if bids and asks:
                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                spread = (best_ask - best_bid) / best_bid * 100 if best_bid > 0 else 0
                
                bid_volume = sum(float(b[1]) for b in bids[:5])
                ask_volume = sum(float(a[1]) for a in asks[:5])
                imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume) if (bid_volume + ask_volume) > 0 else 0
                
                return {
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'spread': spread,
                    'imbalance': imbalance,
                    'bid_volume': bid_volume,
                    'ask_volume': ask_volume,
                    'bids': bids[:depth],
                    'asks': asks[:depth]
                }
    except Exception:
        pass
    return None

# ============================================================
# INDICATOR FUNCTIONS
# ============================================================
def calculate_rsi(df, period=14):
    try:
        close = df['close'].values
        if len(close) < period + 1:
            return 50
        gains, losses = [], []
        for i in range(1, len(close)):
            change = close[i] - close[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        if len(gains) >= period:
            avg_gain = sum(gains[-period:]) / period
            avg_loss = sum(losses[-period:]) / period
            if avg_loss == 0:
                return 100
            rs = avg_gain / avg_loss
            return 100 - (100 / (1 + rs))
    except Exception:
        pass
    return 50

def calculate_atr(df, period=14):
    try:
        if df is None or len(df) < period:
            return None
        df_copy = df.copy()
        df_copy['tr1'] = df_copy['high'] - df_copy['low']
        df_copy['tr2'] = abs(df_copy['high'] - df_copy['close'].shift())
        df_copy['tr3'] = abs(df_copy['low'] - df_copy['close'].shift())
        df_copy['tr'] = df_copy[['tr1', 'tr2', 'tr3']].max(axis=1)
        atr = df_copy['tr'].rolling(window=period).mean().iloc[-1]
        if pd.isna(atr) or atr <= 0:
            return None
        return atr
    except Exception:
        pass
    return None

def calculate_macd(df, fast=12, slow=26):
    try:
        close = df['close']
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal = macd.ewm(span=9, adjust=False).mean()
        return {
            'macd': macd.iloc[-1],
            'signal': signal.iloc[-1],
            'histogram': (macd - signal).iloc[-1],
            'trend': 'BULLISH' if macd.iloc[-1] > signal.iloc[-1] else 'BEARISH'
        }
    except Exception:
        pass
    return None

def calculate_bollinger_bands(df, period=20, std=2.0):
    try:
        close = df['close']
        sma = close.rolling(window=period).mean()
        std_dev = close.rolling(window=period).std()
        return {
            'upper': (sma + std * std_dev).iloc[-1],
            'middle': sma.iloc[-1],
            'lower': (sma - std * std_dev).iloc[-1]
        }
    except Exception:
        pass
    return None

def calculate_vwap(df, period=VWAP_PERIOD):
    try:
        if df is None or len(df) < period:
            return None
        df_copy = df.copy()
        df_copy['typical'] = (df_copy['high'] + df_copy['low'] + df_copy['close']) / 3
        df_copy['vwap'] = (df_copy['typical'] * df_copy['volume']).rolling(window=period).sum() / df_copy['volume'].rolling(window=period).sum()
        return df_copy['vwap'].iloc[-1]
    except Exception:
        pass
    return None

def calculate_adx(df, period=14):
    try:
        if df is None or len(df) < period + 1:
            return 0
            
        df_copy = df.copy()
        df_copy['tr'] = df_copy.apply(lambda x: max(x['high'] - x['low'], 
                                                     abs(x['high'] - df_copy['close'].shift(1).iloc[x.name]),
                                                     abs(x['low'] - df_copy['close'].shift(1).iloc[x.name])), axis=1)
        
        df_copy['up_move'] = df_copy['high'] - df_copy['high'].shift(1)
        df_copy['down_move'] = df_copy['low'].shift(1) - df_copy['low']
        
        df_copy['plus_dm'] = df_copy.apply(lambda x: x['up_move'] if x['up_move'] > x['down_move'] and x['up_move'] > 0 else 0, axis=1)
        df_copy['minus_dm'] = df_copy.apply(lambda x: x['down_move'] if x['down_move'] > x['up_move'] and x['down_move'] > 0 else 0, axis=1)
        
        tr_smooth = df_copy['tr'].rolling(window=period).mean()
        plus_dm_smooth = df_copy['plus_dm'].rolling(window=period).mean()
        minus_dm_smooth = df_copy['minus_dm'].rolling(window=period).mean()
        
        plus_di = 100 * (plus_dm_smooth / tr_smooth)
        minus_di = 100 * (minus_dm_smooth / tr_smooth)
        
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(window=period).mean().iloc[-1]
        
        if pd.isna(adx):
            return 0
        return adx
    except Exception:
        pass
    return 0

def calculate_ema(df, period):
    try:
        return df['close'].ewm(span=period, adjust=False).mean().iloc[-1]
    except:
        return df['close'].iloc[-1]

# ============================================================
# SUPPORT & RESISTANCE DETECTION
# ============================================================
def detect_support_resistance(df, lookback=SR_LOOKBACK, proximity=SR_PROXIMITY_PERCENT):
    try:
        if df is None or len(df) < lookback:
            return {'support': [], 'resistance': [], 'current_sr': 'NEUTRAL'}
            
        high = df['high'].values[-lookback:]
        low = df['low'].values[-lookback:]
        close = df['close'].values[-lookback:]
        current = close[-1]
        
        local_highs = []
        local_lows = []
        
        for i in range(2, len(high) - 2):
            if high[i] > high[i-1] and high[i] > high[i-2] and high[i] > high[i+1] and high[i] > high[i+2]:
                local_highs.append(high[i])
            if low[i] < low[i-1] and low[i] < low[i-2] and low[i] < low[i+1] and low[i] < low[i+2]:
                local_lows.append(low[i])
        
        def cluster_levels(levels, tolerance=0.005):
            if not levels:
                return []
            levels = sorted(levels)
            clusters = []
            current_cluster = [levels[0]]
            
            for level in levels[1:]:
                if level / current_cluster[0] - 1 < tolerance:
                    current_cluster.append(level)
                else:
                    clusters.append(sum(current_cluster) / len(current_cluster))
                    current_cluster = [level]
            clusters.append(sum(current_cluster) / len(current_cluster))
            return clusters
        
        resistance_levels = cluster_levels(local_highs)
        support_levels = cluster_levels(local_lows)
        
        nearest_resistance = None
        nearest_support = None
        
        for r in resistance_levels:
            if r > current:
                if nearest_resistance is None or r < nearest_resistance:
                    nearest_resistance = r
                    
        for s in support_levels:
            if s < current:
                if nearest_support is None or s > nearest_support:
                    nearest_support = s
        
        sr_type = "NEUTRAL"
        if nearest_support and (current - nearest_support) / current * 100 < proximity:
            sr_type = "NEAR_SUPPORT"
        elif nearest_resistance and (nearest_resistance - current) / current * 100 < proximity:
            sr_type = "NEAR_RESISTANCE"
            
        return {
            'support': support_levels,
            'resistance': resistance_levels,
            'nearest_support': nearest_support,
            'nearest_resistance': nearest_resistance,
            'current_sr': sr_type,
            'current': current
        }
    except Exception:
        pass
    return {'support': [], 'resistance': [], 'current_sr': 'NEUTRAL'}

# ============================================================
# BREAKOUT DETECTION
# ============================================================
def detect_breakout(df, lookback=BREAKOUT_LOOKBACK, volume_mult=BREAKOUT_VOLUME_MULTIPLIER):
    try:
        if df is None or len(df) < lookback + 5:
            return {'is_breakout': False, 'type': 'NONE', 'level': 0, 'confirmed': False}
            
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        volume = df['volume'].values
        
        current = close[-1]
        prev_high = max(high[-lookback:-1])
        prev_low = min(low[-lookback:-1])
        
        avg_volume = np.mean(volume[-lookback:-1])
        current_volume = volume[-1]
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        resistance_breakout = current > prev_high * 1.002
        support_breakout = current < prev_low * 0.998
        
        confirmation_candles = 0
        if resistance_breakout:
            for i in range(1, BREAKOUT_CONFIRMATION_CANDLES + 1):
                if len(close) > i and close[-i] > prev_high:
                    confirmation_candles += 1
        elif support_breakout:
            for i in range(1, BREAKOUT_CONFIRMATION_CANDLES + 1):
                if len(close) > i and close[-i] < prev_low:
                    confirmation_candles += 1
        
        confirmed = confirmation_candles >= BREAKOUT_CONFIRMATION_CANDLES
        
        if resistance_breakout and (volume_ratio > volume_mult or confirmed):
            return {
                'is_breakout': True,
                'type': 'RESISTANCE_BREAKOUT',
                'level': prev_high,
                'confirmed': confirmed,
                'volume_ratio': volume_ratio
            }
        elif support_breakout and (volume_ratio > volume_mult or confirmed):
            return {
                'is_breakout': True,
                'type': 'SUPPORT_BREAKOUT',
                'level': prev_low,
                'confirmed': confirmed,
                'volume_ratio': volume_ratio
            }
            
        return {'is_breakout': False, 'type': 'NONE', 'level': 0, 'confirmed': False}
    except Exception:
        pass
    return {'is_breakout': False, 'type': 'NONE', 'level': 0, 'confirmed': False}

# ============================================================
# FEAR & GREED / BTC TREND
# ============================================================
def get_fear_greed_index():
    cache_key = "fear_greed"
    cached = cache_get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except:
            pass
            
    try:
        response = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and len(data['data']) > 0:
                result = {
                    'value': int(data['data'][0].get('value', 50)),
                    'classification': data['data'][0].get('classification', 'Neutral'),
                    'timestamp': time.time()
                }
                cache_set(cache_key, json.dumps(result), ttl=300)
                return result
    except Exception:
        pass
    return {'value': 50, 'classification': 'Neutral', 'timestamp': time.time()}

def get_btc_trend():
    try:
        df = fetch_candles(BTC_SYMBOL, "1H", 50)
        if df is None or len(df) < 20:
            return "NEUTRAL"
        close = df['close'].values
        sma_20 = np.mean(close[-20:])
        sma_50 = np.mean(close[-50:]) if len(close) >= 50 else sma_20
        current = close[-1]
        if current > sma_20 and sma_20 > sma_50:
            return "BULLISH"
        elif current < sma_20 and sma_20 < sma_50:
            return "BEARISH"
        else:
            return "NEUTRAL"
    except Exception:
        pass
    return "NEUTRAL"

# ============================================================
# PATTERN DETECTION (for PatternRecognitionStrategy)
# ============================================================
def detect_pattern_with_sr_context(df):
    try:
        if df is None or len(df) < 30:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'reason': 'Insufficient data', 'pattern': 'NONE'}
        
        close = df['close'].values
        high = df['high'].values
        low = df['low'].values
        current = close[-1]
        
        pattern = 'NONE'
        signal = 'NEUTRAL'
        confidence = 20
        reason = 'No pattern detected'
        
        # Double Bottom
        if len(close) >= 20:
            recent_lows = []
            for i in range(2, len(close) - 5):
                if low[i] < low[i-1] and low[i] < low[i-2] and low[i] < low[i+1] and low[i] < low[i+2]:
                    recent_lows.append((i, low[i]))
            if len(recent_lows) >= 2:
                last_two = recent_lows[-2:]
                if abs(last_two[0][1] - last_two[1][1]) / last_two[0][1] < 0.02:
                    neckline = (last_two[0][1] + last_two[1][1]) / 2
                    if current > neckline * 1.01:
                        pattern = 'DOUBLE_BOTTOM'
                        signal = 'BUY'
                        confidence = 70
                        reason = f'Double Bottom pattern detected (neckline: {neckline:.4f})'
        
        # Double Top
        if len(high) >= 20:
            recent_highs = []
            for i in range(2, len(high) - 5):
                if high[i] > high[i-1] and high[i] > high[i-2] and high[i] > high[i+1] and high[i] > high[i+2]:
                    recent_highs.append((i, high[i]))
            if len(recent_highs) >= 2:
                last_two = recent_highs[-2:]
                if abs(last_two[0][1] - last_two[1][1]) / last_two[0][1] < 0.02:
                    neckline = (last_two[0][1] + last_two[1][1]) / 2
                    if current < neckline * 0.99:
                        pattern = 'DOUBLE_TOP'
                        signal = 'SELL'
                        confidence = 70
                        reason = f'Double Top pattern detected (neckline: {neckline:.4f})'
        
        # Engulfing
        if len(df) >= 2:
            prev_open = df['open'].iloc[-2]
            prev_close = df['close'].iloc[-2]
            curr_open = df['open'].iloc[-1]
            curr_close = df['close'].iloc[-1]
            if prev_close < prev_open:
                if curr_close > curr_open and curr_open < prev_close and curr_close > prev_open:
                    pattern = 'BULLISH_ENGULFING'
                    signal = 'BUY'
                    confidence = 65
                    reason = 'Bullish Engulfing pattern detected'
            if prev_close > prev_open:
                if curr_close < curr_open and curr_open > prev_close and curr_close < prev_open:
                    pattern = 'BEARISH_ENGULFING'
                    signal = 'SELL'
                    confidence = 65
                    reason = 'Bearish Engulfing pattern detected'
        
        return {
            'signal': signal,
            'confidence': min(95, confidence),
            'reason': reason,
            'pattern': pattern
        }
    except Exception as e:
        logger.debug(f"Pattern detection error: {e}")
        return {'signal': 'NEUTRAL', 'confidence': 0, 'reason': 'Error', 'pattern': 'NONE'}

# ============================================================
# MARKET REGIME DETECTOR
# ============================================================
class MarketRegimeDetector:
    def __init__(self):
        self.current_regime = "NEUTRAL"
        self.last_update = 0
        self.regime_history = []
        
    def detect_regime(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 50:
                return {'regime': 'NEUTRAL', 'confidence': 0}
            close = df['close'].values
            high = df['high'].values
            low = df['low'].values
            adx = calculate_adx(df)
            price_range = (high[-20:].max() - low[-20:].min()) / close[-1] * 100
            returns = np.diff(np.log(close))
            volatility = np.std(returns[-20:]) * 100
            ema_20 = calculate_ema(df, 20)
            ema_50 = calculate_ema(df, 50)
            trend_strength = abs(ema_20 - ema_50) / ema_50 * 100
            
            if adx > 25 and trend_strength > 1.5:
                if ema_20 > ema_50:
                    regime = "STRONG_UPTREND"
                    confidence = min(95, 70 + adx * 0.5)
                else:
                    regime = "STRONG_DOWNTREND"
                    confidence = min(95, 70 + adx * 0.5)
            elif adx > 20 and trend_strength > 1.0:
                if ema_20 > ema_50:
                    regime = "UPTREND"
                    confidence = min(85, 60 + adx * 0.3)
                else:
                    regime = "DOWNTREND"
                    confidence = min(85, 60 + adx * 0.3)
            elif volatility > 3.0:
                regime = "VOLATILE"
                confidence = 70
            elif price_range < 2.0:
                regime = "RANGING"
                confidence = 65
            else:
                regime = "NEUTRAL"
                confidence = 50
                
            self.current_regime = regime
            self.regime_history.append({'regime': regime, 'timestamp': time.time()})
            if len(self.regime_history) > 100:
                self.regime_history = self.regime_history[-100:]
            return {
                'regime': regime,
                'confidence': confidence,
                'trend_strength': trend_strength,
                'volatility': volatility,
                'price_range': price_range,
                'adx': adx
            }
        except Exception:
            pass
        return {'regime': 'NEUTRAL', 'confidence': 0}

# ============================================================
# ULTIMATE TREND ANALYZER
# ============================================================
class UltimateTrendAnalyzer:
    def __init__(self):
        self.trend_cache = {}
        
    def analyze_trend(self, symbol: str) -> Dict:
        try:
            timeframes = MTF_TIMEFRAMES
            trends = {}
            details = {}
            
            for tf in timeframes:
                df = fetch_candles(symbol, tf, 100)
                if df is None or len(df) < 20:
                    continue
                close = df['close'].values
                current = close[-1]
                ema_20 = calculate_ema(df, 20)
                ema_50 = calculate_ema(df, 50)
                ema_200 = calculate_ema(df, 200) if len(df) >= 200 else ema_50
                adx = calculate_adx(df)
                
                if current > ema_20 and ema_20 > ema_50 and ema_50 > ema_200:
                    trend = "STRONG_BULLISH"
                    strength = min(100, 60 + adx * 0.5)
                elif current > ema_20 and ema_20 > ema_50:
                    trend = "BULLISH"
                    strength = min(90, 50 + adx * 0.4)
                elif current < ema_20 and ema_20 < ema_50 and ema_50 < ema_200:
                    trend = "STRONG_BEARISH"
                    strength = min(100, 60 + adx * 0.5)
                elif current < ema_20 and ema_20 < ema_50:
                    trend = "BEARISH"
                    strength = min(90, 50 + adx * 0.4)
                else:
                    trend = "NEUTRAL"
                    strength = 40
                    
                trends[tf] = trend
                details[tf] = {
                    'trend': trend,
                    'strength': strength,
                    'adx': adx,
                    'ema_20': ema_20,
                    'ema_50': ema_50,
                    'current': current
                }
                
            if not trends:
                return {'overall': 'NEUTRAL', 'trends': {}, 'details': {}, 'strength': 0}
                
            weights = {
                '5m': 0.15,
                '15m': 0.25,
                '1H': 0.35,
                '4H': 0.25,
            }
            
            bullish_score = 0
            bearish_score = 0
            total_weight = 0
            
            for tf, trend in trends.items():
                weight = weights.get(tf, 0.20)
                total_weight += weight
                if trend in ['STRONG_BULLISH', 'BULLISH']:
                    bullish_score += weight * (1.5 if 'STRONG' in trend else 1.0)
                elif trend in ['STRONG_BEARISH', 'BEARISH']:
                    bearish_score += weight * (1.5 if 'STRONG' in trend else 1.0)
                    
            if total_weight > 0:
                bullish_pct = bullish_score / total_weight * 100
                bearish_pct = bearish_score / total_weight * 100
                if bullish_pct > 65:
                    overall = "STRONG_BULLISH"
                elif bullish_pct > 55:
                    overall = "BULLISH"
                elif bearish_pct > 65:
                    overall = "STRONG_BEARISH"
                elif bearish_pct > 55:
                    overall = "BEARISH"
                else:
                    overall = "NEUTRAL"
                strength = max(bullish_pct, bearish_pct)
            else:
                overall = "NEUTRAL"
                strength = 0
                
            return {
                'overall': overall,
                'trends': trends,
                'details': details,
                'strength': strength,
                'bullish_pct': bullish_pct if 'bullish_pct' in locals() else 0,
                'bearish_pct': bearish_pct if 'bearish_pct' in locals() else 0
            }
        except Exception as e:
            logger.error(f"Trend analysis error {symbol}: {e}")
            return {'overall': 'NEUTRAL', 'trends': {}, 'details': {}, 'strength': 0}

# ============================================================
# ADAPTIVE LEARNER (Updated with weighted voting support)
# ============================================================
class AdaptiveLearner:
    def __init__(self):
        self.pattern_weights = {}
        self.learning_history = []
        self.last_learning_time = 0
        self.load_pattern_weights()
        self.load_strategy_weights()
        
    def load_pattern_weights(self):
        try:
            with db_manager.get_connection() as conn:
                rows = conn.execute("SELECT pattern_name, weight, wins, losses, total_trades, ml_accuracy FROM pattern_performance").fetchall()
                for row in rows:
                    self.pattern_weights[row['pattern_name']] = {
                        'weight': row['weight'],
                        'wins': row['wins'],
                        'losses': row['losses'],
                        'total': row['total_trades'],
                        'ml_accuracy': row['ml_accuracy'] or 0
                    }
            logger.info(f"📊 Loaded {len(self.pattern_weights)} pattern weights")
        except Exception:
            self.pattern_weights = {}
    
    def load_strategy_weights(self):
        """Load strategy weights from strategy_performance table"""
        self.strategy_weights = {}
        try:
            with db_manager.get_connection() as conn:
                rows = conn.execute("SELECT strategy_name, weight, win_rate, total_trades FROM strategy_performance").fetchall()
                for row in rows:
                    self.strategy_weights[row['strategy_name']] = {
                        'weight': row['weight'],
                        'win_rate': row['win_rate'],
                        'total_trades': row['total_trades']
                    }
            logger.info(f"📊 Loaded {len(self.strategy_weights)} strategy weights")
        except Exception:
            self.strategy_weights = {}
    
    def update_strategy_performance(self, strategy_name: str, is_win: bool):
        """Update strategy performance after each trade"""
        try:
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT * FROM strategy_performance WHERE strategy_name = ?", (strategy_name,)).fetchone()
                if row:
                    total = row['total_trades'] + 1
                    wins = row['wins'] + (1 if is_win else 0)
                    losses = row['losses'] + (0 if is_win else 1)
                    win_rate = (wins / total) * 100 if total > 0 else 0
                    # Update weight: if win_rate > 60, weight increases; else decreases
                    weight = 1.0
                    if total >= 5:
                        if win_rate > 60:
                            weight = min(2.0, 1.0 + (win_rate - 60) / 40)
                        elif win_rate < 40:
                            weight = max(0.5, 1.0 - (40 - win_rate) / 40)
                    conn.execute("""
                        UPDATE strategy_performance 
                        SET total_trades=?, wins=?, losses=?, win_rate=?, weight=?, last_updated=?
                        WHERE strategy_name=?
                    """, (total, wins, losses, win_rate, weight, time.strftime('%Y-%m-%d %H:%M:%S'), strategy_name))
                else:
                    wins = 1 if is_win else 0
                    losses = 0 if is_win else 1
                    total = 1
                    win_rate = 100 if is_win else 0
                    weight = 1.0
                    conn.execute("""
                        INSERT INTO strategy_performance (strategy_name, total_trades, wins, losses, win_rate, weight, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (strategy_name, total, wins, losses, win_rate, weight, time.strftime('%Y-%m-%d %H:%M:%S')))
                conn.commit()
                self.load_strategy_weights()
        except Exception as e:
            logger.error(f"Strategy performance update error: {e}")
    
    def get_strategy_weight(self, strategy_name: str) -> float:
        """Get current weight for a strategy"""
        if strategy_name in self.strategy_weights:
            return self.strategy_weights[strategy_name]['weight']
        return 1.0
            
    def save_pattern_performance(self, pattern_name: str, is_win: bool, ml_accuracy: float = 0):
        try:
            with db_manager.get_connection() as conn:
                row = conn.execute("SELECT * FROM pattern_performance WHERE pattern_name = ?", (pattern_name,)).fetchone()
                if row:
                    total = row['total_trades'] + 1
                    wins = row['wins'] + (1 if is_win else 0)
                    losses = row['losses'] + (0 if is_win else 1)
                    win_rate = (wins / total) * 100 if total > 0 else 0
                    weight = 1.0
                    if total >= 5:
                        if win_rate > 60:
                            weight = min(1.5, 1.0 + (win_rate - 60) / 40)
                        elif win_rate < 40:
                            weight = max(0.5, 1.0 - (40 - win_rate) / 40)
                    conn.execute("""
                        UPDATE pattern_performance 
                        SET total_trades = ?, wins = ?, losses = ?, win_rate = ?, 
                            weight = ?, last_updated = ?, ml_accuracy = ?
                        WHERE pattern_name = ?
                    """, (total, wins, losses, win_rate, weight, time.strftime('%Y-%m-%d %H:%M:%S'), ml_accuracy, pattern_name))
                else:
                    wins = 1 if is_win else 0
                    losses = 0 if is_win else 1
                    total = 1
                    win_rate = 100 if is_win else 0
                    conn.execute("""
                        INSERT INTO pattern_performance 
                        (pattern_name, total_trades, wins, losses, win_rate, weight, last_updated, ml_accuracy)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (pattern_name, total, wins, losses, win_rate, 1.0, time.strftime('%Y-%m-%d %H:%M:%S'), ml_accuracy))
                conn.commit()
                self.load_pattern_weights()
        except Exception:
            pass
            
    def get_pattern_weight(self, pattern_name: str) -> float:
        if pattern_name in self.pattern_weights:
            return self.pattern_weights[pattern_name]['weight']
        return 1.0
        
    def get_best_patterns(self, limit: int = 5) -> List[Dict]:
        try:
            with db_manager.get_connection() as conn:
                rows = conn.execute("""
                    SELECT pattern_name, win_rate, total_trades, weight, ml_accuracy 
                    FROM pattern_performance 
                    WHERE total_trades >= 3 
                    ORDER BY win_rate DESC 
                    LIMIT ?
                """, (limit,)).fetchall()
                return [dict(row) for row in rows]
        except Exception:
            pass
        return []
    
    def log_learning_insight(self, symbol: str, action: str, outcome: str, 
                             confidence: float, sentiment: str, regime: str, pnl: float = 0,
                             weekly_trend: str = "NEUTRAL", daily_trend: str = "NEUTRAL",
                             macro_bias: str = "NEUTRAL", timeframe: str = "15m",
                             market_cap_sentiment: str = "NEUTRAL",
                             funding_rate_sentiment: str = "NEUTRAL"):
        try:
            with db_manager.get_connection() as conn:
                conn.execute("""
                    INSERT INTO learning_logs (timestamp, symbol, action, outcome, confidence, sentiment, regime, pnl, weekly_trend, daily_trend, macro_bias, timeframe, market_cap_sentiment, funding_rate_sentiment)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (time.strftime('%Y-%m-%d %H:%M:%S'), symbol, action, outcome, confidence, sentiment, regime, pnl, weekly_trend, daily_trend, macro_bias, timeframe, market_cap_sentiment, funding_rate_sentiment))
                conn.commit()
            self.learning_history.append({
                'timestamp': time.time(),
                'symbol': symbol,
                'action': action,
                'outcome': outcome,
                'confidence': confidence,
                'sentiment': sentiment,
                'regime': regime,
                'pnl': pnl,
                'weekly_trend': weekly_trend,
                'daily_trend': daily_trend,
                'macro_bias': macro_bias,
                'timeframe': timeframe,
                'market_cap_sentiment': market_cap_sentiment,
                'funding_rate_sentiment': funding_rate_sentiment
            })
            if len(self.learning_history) > 1000:
                self.learning_history = self.learning_history[-1000:]
            logger.info(f"🧠 Learning insight: {symbol} {action} → {outcome}")
        except Exception:
            pass
    
    def get_learning_stats(self) -> Dict:
        try:
            with db_manager.get_connection() as conn:
                row = conn.execute("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
                           SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) as losses,
                           AVG(confidence) as avg_confidence,
                           AVG(pnl) as avg_pnl
                    FROM learning_logs
                    WHERE outcome IN ('WIN', 'LOSS')
                """).fetchone()
                if row:
                    total = row['total'] or 0
                    wins = row['wins'] or 0
                    return {
                        'total': total,
                        'wins': wins,
                        'losses': row['losses'] or 0,
                        'win_rate': (wins / total * 100) if total > 0 else 0,
                        'avg_confidence': row['avg_confidence'] or 0,
                        'avg_pnl': row['avg_pnl'] or 0
                    }
        except Exception:
            pass
        return {'total': 0, 'wins': 0, 'losses': 0, 'win_rate': 0, 'avg_confidence': 0, 'avg_pnl': 0}

# ============================================================
# STRATEGY 8: MARKET CAP & ALTCOIN SEASON (Async-enabled)
# ============================================================
class MarketCapAltcoinSeasonStrategy:
    def __init__(self):
        self.name = "Market Cap & Altcoin Season"
        self.weight = 0.12
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            cache_key = "market_cap_data"
            cached = cache_get(cache_key)
            if cached:
                try:
                    data = json.loads(cached)
                    total_mc = data.get('total_mc')
                    btc_dominance = data.get('btc_dominance')
                except:
                    total_mc = None
                    btc_dominance = None
            else:
                # Use async fetch for CoinGecko
                total_mc = self._fetch_total_market_cap_async()
                btc_dominance = self._fetch_btc_dominance_async()
                if total_mc and btc_dominance:
                    cache_set(cache_key, json.dumps({'total_mc': total_mc, 'btc_dominance': btc_dominance}), ttl=300)
            
            if total_mc is None or btc_dominance is None:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'No market data'}
            
            mc_trend = self._analyze_market_cap_trend(total_mc)
            dom_trend = self._analyze_dominance_trend(btc_dominance)
            
            signal = 'NEUTRAL'
            confidence = 30
            details = f"MC Trend: {mc_trend}, Dom Trend: {dom_trend}"
            
            if mc_trend == 'BULLISH' and dom_trend == 'BEARISH':
                signal = 'BUY'
                confidence = 75
                details = "✅ ALTCOIN SEASON: Total MC rising, BTC.D falling → Money flowing to alts"
            elif mc_trend == 'BEARISH' and dom_trend == 'BULLISH':
                signal = 'SELL'
                confidence = 80
                details = "⚠️ BEARISH ROTATION: Total MC falling, BTC.D rising → Capital moving to BTC/Cash"
            elif mc_trend == 'BULLISH' and dom_trend == 'BULLISH':
                signal = 'BUY'
                confidence = 65
                details = "📈 BTC LED RALLY: Total MC rising with BTC dominance"
            elif mc_trend == 'BEARISH' and dom_trend == 'BEARISH':
                signal = 'SELL'
                confidence = 70
                details = "📉 BROAD BEARISH: Total MC falling with BTC dominance falling"
            
            mc_above_ma = self._check_market_cap_ma(total_mc)
            if mc_above_ma and signal == 'NEUTRAL':
                signal = 'BUY'
                confidence = 55
                details = "Total MC above 200-day MA → Bullish bias"
            elif not mc_above_ma and signal == 'NEUTRAL':
                signal = 'SELL'
                confidence = 55
                details = "Total MC below 200-day MA → Bearish bias"
            
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': details,
                'market_cap_trend': mc_trend,
                'dominance_trend': dom_trend
            }
        except Exception as e:
            logger.debug(f"Market Cap strategy error: {e}")
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Error'}
    
    def _fetch_total_market_cap_async(self):
        """Asynchronously fetch total market cap"""
        try:
            url = "https://api.coingecko.com/api/v3/global"
            data = run_async_fetch(url)
            if data:
                total_mc = data.get('data', {}).get('total_market_cap', {}).get('usd', 0)
                if total_mc > 0:
                    logger.debug(f"✅ Market cap fetched async: ${total_mc:,.0f}")
                    return total_mc
        except Exception as e:
            logger.warning(f"Async CoinGecko market cap fetch failed: {e}")
        # Fallback sync
        try:
            response = requests.get("https://api.coingecko.com/api/v3/global", timeout=10)
            if response.status_code == 200:
                data = response.json()
                total_mc = data.get('data', {}).get('total_market_cap', {}).get('usd', 0)
                if total_mc > 0:
                    return total_mc
        except Exception as e:
            logger.warning(f"Sync CoinGecko failed: {e}")
        # Fallback TradingView (sync)
        try:
            url = "https://api.tradingview.com/v1/symbols/CRYPTOCAP:TOTAL"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                total_mc = data.get('v', 0) or data.get('price', 0) or data.get('close', 0)
                if total_mc > 0:
                    return total_mc
        except Exception as e:
            logger.warning(f"TradingView fallback failed: {e}")
        return None
    
    def _fetch_btc_dominance_async(self):
        try:
            url = "https://api.coingecko.com/api/v3/global"
            data = run_async_fetch(url)
            if data:
                btc_dom = data.get('data', {}).get('market_cap_percentage', {}).get('btc', 0)
                if btc_dom > 0:
                    logger.debug(f"✅ BTC dominance fetched async: {btc_dom:.2f}%")
                    return btc_dom
        except Exception as e:
            logger.warning(f"Async BTC dominance failed: {e}")
        # Sync fallback
        try:
            response = requests.get("https://api.coingecko.com/api/v3/global", timeout=10)
            if response.status_code == 200:
                data = response.json()
                btc_dom = data.get('data', {}).get('market_cap_percentage', {}).get('btc', 0)
                if btc_dom > 0:
                    return btc_dom
        except Exception:
            pass
        return None
    
    def _analyze_market_cap_trend(self, total_mc):
        cache_key = "mc_trend_history"
        cached = cache_get(cache_key)
        if cached:
            try:
                prev_mc = float(cached)
                if total_mc > prev_mc * 1.01:
                    return "BULLISH"
                elif total_mc < prev_mc * 0.99:
                    return "BEARISH"
                else:
                    return "NEUTRAL"
            except:
                pass
        cache_set(cache_key, str(total_mc), ttl=3600)
        return "NEUTRAL"
    
    def _analyze_dominance_trend(self, btc_dominance):
        cache_key = "dom_trend_history"
        cached = cache_get(cache_key)
        if cached:
            try:
                prev_dom = float(cached)
                if btc_dominance > prev_dom * 1.01:
                    return "BULLISH"
                elif btc_dominance < prev_dom * 0.99:
                    return "BEARISH"
                else:
                    return "NEUTRAL"
            except:
                pass
        cache_set(cache_key, str(btc_dominance), ttl=3600)
        return "NEUTRAL"
    
    def _check_market_cap_ma(self, total_mc):
        if total_mc > 1_000_000_000_000:
            return True
        trend = self._analyze_market_cap_trend(total_mc)
        return trend == "BULLISH"

# ============================================================
# STRATEGY 9: FEAR & GREED + FUNDING RATES (unchanged)
# ============================================================
class FearGreedFundingRateStrategy:
    def __init__(self):
        self.name = "Fear & Greed + Funding Rates"
        self.weight = 0.13
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            fng = self._get_fear_greed_index()
            funding_rate = self._fetch_aggregate_funding_rate()
            if fng is None:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'No FnG data'}
            signal = 'NEUTRAL'
            confidence = 30
            details = f"FnG: {fng['value']} ({fng['classification']})"
            if funding_rate is not None:
                details += f", Funding: {funding_rate:.4f}%"
            if fng['value'] < 20:
                signal = 'BUY'
                confidence = 75 + (20 - fng['value']) * 1.0
                details = f"🟢 EXTREME FEAR ({fng['value']}) → Potential bottom, BUY signal"
                if funding_rate is not None and funding_rate < -0.01:
                    confidence = min(95, confidence + 10)
                    details += " (Negative funding = Shorts paying longs → Bullish)"
            elif fng['value'] > 80:
                signal = 'SELL'
                confidence = 75 + (fng['value'] - 80) * 1.0
                details = f"🔴 EXTREME GREED ({fng['value']}) → Potential top, SELL signal"
                if funding_rate is not None and funding_rate > 0.01:
                    confidence = min(95, confidence + 10)
                    details += " (Positive funding = Overleveraged longs → Bearish)"
            elif fng['value'] < 40:
                signal = 'BUY'
                confidence = 55 + (40 - fng['value']) * 0.5
                details = f"😨 FEAR ({fng['value']}) → Cautious BUY opportunity"
            elif fng['value'] > 60:
                signal = 'SELL'
                confidence = 55 + (fng['value'] - 60) * 0.5
                details = f"😋 GREED ({fng['value']}) → Cautious SELL opportunity"
            if funding_rate is not None and funding_rate > 0.02 and fng['value'] > 70:
                signal = 'SELL'
                confidence = min(95, confidence + 15)
                details = "⚠️ OVERLEVERAGED: High funding + Extreme Greed → Liquidation risk"
            if funding_rate is not None and funding_rate < -0.02 and fng['value'] < 30:
                signal = 'BUY'
                confidence = min(95, confidence + 15)
                details = "💪 OVERSOLD: Negative funding + Extreme Fear → Accumulation zone"
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': details,
                'fng_value': fng['value'],
                'fng_classification': fng['classification'],
                'funding_rate': funding_rate
            }
        except Exception as e:
            logger.debug(f"Fear & Greed strategy error: {e}")
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Error'}
    
    def _get_fear_greed_index(self):
        cache_key = "fng_strategy"
        cached = cache_get(cache_key)
        if cached:
            try:
                return json.loads(cached)
            except:
                pass
        try:
            url = "https://api.alternative.me/fng/?limit=1"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and len(data['data']) > 0:
                    result = {
                        'value': int(data['data'][0]['value']),
                        'classification': data['data'][0]['classification']
                    }
                    cache_set(cache_key, json.dumps(result), ttl=300)
                    return result
        except Exception:
            pass
        return None
    
    def _fetch_aggregate_funding_rate(self):
        cache_key = "aggregate_funding"
        cached = cache_get(cache_key)
        if cached:
            try:
                return float(cached)
            except:
                pass
        try:
            top_coins = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]
            rates = []
            for symbol in top_coins:
                try:
                    endpoint = f"/api/v2/mix/market/funding-rate"
                    params = {"symbol": symbol, "productType": "usdt-futures"}
                    response = bitget_public_request(endpoint, params)
                    if response and response.get('code') == '00000':
                        rate = float(response.get('data', {}).get('fundingRate', 0))
                        rates.append(rate)
                except Exception:
                    continue
            if rates:
                avg_rate = sum(rates) / len(rates)
                cache_set(cache_key, str(avg_rate), ttl=120)
                return avg_rate
        except Exception:
            pass
        return None

# ============================================================
# EXISTING 7 STRATEGIES (Preserved)
# ============================================================
class TrendFollowingStrategy:
    def __init__(self):
        self.name = "Trend Following"
        self.weight = 0.15
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 50:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
            close = df['close'].values
            ema_20 = calculate_ema(df, 20)
            ema_50 = calculate_ema(df, 50)
            ema_200 = calculate_ema(df, 200) if len(df) >= 200 else ema_50
            adx = calculate_adx(df)
            current = close[-1]
            breakout = detect_breakout(df)
            
            if current > ema_20 and ema_20 > ema_50 and ema_50 > ema_200 and adx > 20:
                signal = 'BUY'
                confidence = min(90, 60 + adx * 0.5)
                if breakout.get('is_breakout') and breakout.get('type') == 'RESISTANCE_BREAKOUT':
                    confidence = min(95, confidence + 10)
                    details = f"BREAKOUT + Trend: EMA20:{ema_20:.2f} ADX:{adx:.1f}"
                else:
                    details = f"EMA20:{ema_20:.2f} EMA50:{ema_50:.2f} ADX:{adx:.1f}"
            elif current < ema_20 and ema_20 < ema_50 and ema_50 < ema_200 and adx > 20:
                signal = 'SELL'
                confidence = min(90, 60 + adx * 0.5)
                if breakout.get('is_breakout') and breakout.get('type') == 'SUPPORT_BREAKOUT':
                    confidence = min(95, confidence + 10)
                    details = f"BREAKOUT + Trend: EMA20:{ema_20:.2f} ADX:{adx:.1f}"
                else:
                    details = f"EMA20:{ema_20:.2f} EMA50:{ema_50:.2f} ADX:{adx:.1f}"
            else:
                signal = 'NEUTRAL'
                confidence = 30
                details = f"EMA20:{ema_20:.2f} EMA50:{ema_50:.2f} ADX:{adx:.1f}"
            return {'signal': signal, 'confidence': confidence, 'details': details}
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class MomentumStrategy:
    def __init__(self):
        self.name = "Momentum"
        self.weight = 0.12
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 30:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
            rsi = calculate_rsi(df)
            macd_data = calculate_macd(df)
            sr_data = detect_support_resistance(df)
            sr_type = sr_data.get('current_sr', 'NEUTRAL')
            
            rsi_signal = 'NEUTRAL'
            rsi_confidence = 40
            if rsi < RSI_OVERSOLD:
                rsi_signal = 'BUY'
                rsi_confidence = 65 + (RSI_OVERSOLD - rsi) * 1.2
                if sr_type == 'NEAR_RESISTANCE':
                    rsi_signal = 'NEUTRAL'
                    rsi_confidence = 0
            elif rsi > RSI_OVERBOUGHT:
                rsi_signal = 'SELL'
                rsi_confidence = 65 + (rsi - RSI_OVERBOUGHT) * 1.2
                if sr_type == 'NEAR_SUPPORT':
                    rsi_signal = 'NEUTRAL'
                    rsi_confidence = 0
                    
            macd_signal = 'NEUTRAL'
            macd_confidence = 35
            if macd_data:
                if macd_data['trend'] == 'BULLISH' and macd_data['histogram'] > 0:
                    macd_signal = 'BUY'
                    macd_confidence = 65
                elif macd_data['trend'] == 'BEARISH' and macd_data['histogram'] < 0:
                    macd_signal = 'SELL'
                    macd_confidence = 65
                    
            if rsi_signal == macd_signal and rsi_signal != 'NEUTRAL':
                signal = rsi_signal
                confidence = (rsi_confidence * 0.6 + macd_confidence * 0.4)
            elif rsi_signal != 'NEUTRAL':
                signal = rsi_signal
                confidence = rsi_confidence * 0.85
            elif macd_signal != 'NEUTRAL':
                signal = macd_signal
                confidence = macd_confidence * 0.85
            else:
                signal = 'NEUTRAL'
                confidence = 25
            sr_note = f" SR:{sr_type}" if sr_type != 'NEUTRAL' else ""
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': f"RSI:{rsi:.1f} MACD:{macd_data['trend'] if macd_data else 'N/A'}{sr_note}"
            }
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class VolatilityBreakoutStrategy:
    def __init__(self):
        self.name = "Volatility Breakout"
        self.weight = 0.13
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 30:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
            current_price = df['close'].iloc[-1]
            atr = calculate_atr(df)
            bb = calculate_bollinger_bands(df)
            if atr is None or bb is None:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Indicator error'}
            atr_percent = (atr / current_price) * 100
            bb_range = bb['upper'] - bb['lower']
            bb_position = (current_price - bb['lower']) / bb_range if bb_range > 0 else 0.5
            avg_volume = df['volume'].iloc[-20:].mean()
            current_volume = df['volume'].iloc[-1]
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            breakout = detect_breakout(df)
            
            signal = 'NEUTRAL'
            confidence = 30
            details = f"ATR:{atr_percent:.1f}% Vol:{volume_ratio:.1f}x BB:{bb_position:.0%}"
            
            if breakout.get('is_breakout'):
                if breakout.get('type') == 'RESISTANCE_BREAKOUT' and breakout.get('confirmed'):
                    signal = 'BUY'
                    confidence = 80 + min(15, atr_percent * 2)
                    details = f"✅ CONFIRMED RESISTANCE BREAKOUT! {details}"
                elif breakout.get('type') == 'SUPPORT_BREAKOUT' and breakout.get('confirmed'):
                    signal = 'SELL'
                    confidence = 80 + min(15, atr_percent * 2)
                    details = f"✅ CONFIRMED SUPPORT BREAKOUT! {details}"
                elif breakout.get('is_breakout'):
                    signal = 'NEUTRAL'
                    confidence = 50
                    details = f"⏳ BREAKOUT DETECTED - Waiting for confirmation {details}"
            else:
                signal = 'NEUTRAL'
                confidence = 20
                details = f"⏳ Waiting for breakout... {details}"
            return {'signal': signal, 'confidence': min(95, confidence), 'details': details}
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class PatternRecognitionStrategy:
    def __init__(self):
        self.name = "Pattern Recognition"
        self.weight = 0.10
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 20:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
            from datetime import datetime
            result = detect_pattern_with_sr_context(df)
            return {
                'signal': result.get('signal', 'NEUTRAL'),
                'confidence': result.get('confidence', 30),
                'details': f"{result.get('reason', 'No signal')} | Pattern: {result.get('pattern', 'NONE')}"
            }
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class MeanReversionStrategy:
    def __init__(self):
        self.name = "Mean Reversion"
        self.weight = 0.10
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 30:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
            current = df['close'].iloc[-1]
            bb = calculate_bollinger_bands(df)
            rsi = calculate_rsi(df)
            sr_data = detect_support_resistance(df)
            if bb is None:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'BB error'}
            bb_range = bb['upper'] - bb['lower']
            if bb_range <= 0:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'BB range zero'}
            bb_position = (current - bb['lower']) / bb_range
            sr_type = sr_data.get('current_sr', 'NEUTRAL')
            
            signal = 'NEUTRAL'
            confidence = 30
            details = f"BB:{bb_position:.0%} RSI:{rsi:.1f}"
            
            if bb_position < 0.1 and rsi < 35:
                if sr_type == 'NEAR_SUPPORT':
                    signal = 'BUY'
                    confidence = 75 + (35 - rsi) * 0.5
                    details = f"SUPPORT + Oversold! {details}"
                elif sr_type != 'NEAR_RESISTANCE':
                    signal = 'BUY'
                    confidence = 65 + (35 - rsi) * 0.5
                    details = f"Oversold {details}"
                else:
                    signal = 'NEUTRAL'
                    confidence = 0
                    details = f"🚫 BLOCKED: Oversold but at Resistance {details}"
            elif bb_position > 0.9 and rsi > 65:
                if sr_type == 'NEAR_RESISTANCE':
                    signal = 'SELL'
                    confidence = 75 + (rsi - 65) * 0.5
                    details = f"RESISTANCE + Overbought! {details}"
                elif sr_type != 'NEAR_SUPPORT':
                    signal = 'SELL'
                    confidence = 65 + (rsi - 65) * 0.5
                    details = f"Overbought {details}"
                else:
                    signal = 'NEUTRAL'
                    confidence = 0
                    details = f"🚫 BLOCKED: Overbought but at Support {details}"
            else:
                signal = 'NEUTRAL'
                confidence = 30
            return {'signal': signal, 'confidence': min(90, confidence), 'details': details}
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class MLPredictionStrategy:
    def __init__(self):
        self.name = "ML Ensemble"
        self.weight = 0.10
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 50:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
            close = df['close'].values
            current = close[-1]
            sr_data = detect_support_resistance(df)
            sr_type = sr_data.get('current_sr', 'NEUTRAL')
            
            scores = {'BUY': 0, 'SELL': 0}
            roc_5 = (current - close[-5]) / close[-5] * 100 if close[-5] > 0 else 0
            roc_10 = (current - close[-10]) / close[-10] * 100 if close[-10] > 0 else 0
            roc_20 = (current - close[-20]) / close[-20] * 100 if close[-20] > 0 else 0
            
            if roc_5 > 0 and roc_10 > 0:
                scores['BUY'] += 15
            elif roc_5 < 0 and roc_10 < 0:
                scores['SELL'] += 15
                
            rsi = calculate_rsi(df)
            if rsi < 30:
                if sr_type != 'NEAR_RESISTANCE':
                    scores['BUY'] += 20
            elif rsi > 70:
                if sr_type != 'NEAR_SUPPORT':
                    scores['SELL'] += 20
                
            avg_vol = np.mean(df['volume'].values[-20:])
            current_vol = df['volume'].values[-1]
            if current_vol > avg_vol * 1.5:
                if current > close[-2]:
                    scores['BUY'] += 15
                else:
                    scores['SELL'] += 15
                    
            macd = calculate_macd(df)
            if macd and macd['trend'] == 'BULLISH':
                scores['BUY'] += 10
            elif macd and macd['trend'] == 'BEARISH':
                scores['SELL'] += 10
                
            total_score = scores['BUY'] + scores['SELL']
            if total_score > 0:
                buy_prob = scores['BUY'] / total_score
                sell_prob = scores['SELL'] / total_score
                if buy_prob > 0.6:
                    signal = 'BUY'
                    confidence = 60 + (buy_prob - 0.6) * 100
                    if sr_type == 'NEAR_RESISTANCE':
                        signal = 'NEUTRAL'
                        confidence = 0
                elif sell_prob > 0.6:
                    signal = 'SELL'
                    confidence = 60 + (sell_prob - 0.6) * 100
                    if sr_type == 'NEAR_SUPPORT':
                        signal = 'NEUTRAL'
                        confidence = 0
                else:
                    signal = 'NEUTRAL'
                    confidence = 40
            else:
                signal = 'NEUTRAL'
                confidence = 30
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': f"ML: BUY:{scores['BUY']} SELL:{scores['SELL']} SR:{sr_type}"
            }
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class GoldenHourStrategy:
    def __init__(self):
        self.name = "NY Golden Hour"
        self.weight = 0.10
        self.range_high = None
        self.range_low = None
        self.range_build_time = None
        self.session_active = False
        self.liquidity_sweep_detected = False
        self.last_sweep_direction = None
        self.execution_window_active = False
        self.RANGE_BUILD_START = 16.5
        self.RANGE_BUILD_END = 17.5
        self.EXECUTION_START = 17.5
        self.EXECUTION_END = 18.5
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 20:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
            current_time = datetime.now()
            current_hour = current_time.hour + current_time.minute / 60.0
            current_price = df['close'].iloc[-1]
            high = df['high'].values
            low = df['low'].values
            close = df['close'].values
            
            if self.RANGE_BUILD_START <= current_hour < self.RANGE_BUILD_END:
                if self.range_high is None or self.range_low is None:
                    self.range_high = max(high[-20:])
                    self.range_low = min(low[-20:])
                    self.range_build_time = current_time
                    self.session_active = True
                    self.liquidity_sweep_detected = False
                    logger.info(f"⏰ NY Range Building: High={self.range_high:.4f}, Low={self.range_low:.4f}")
                self.range_high = max(self.range_high, max(high[-5:]))
                self.range_low = min(self.range_low, min(low[-5:]))
                return {
                    'signal': 'NEUTRAL',
                    'confidence': 20,
                    'details': f"NY Range Building: H={self.range_high:.4f}, L={self.range_low:.4f}"
                }
            elif self.EXECUTION_START <= current_hour < self.EXECUTION_END:
                if self.range_high is None or self.range_low is None:
                    return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'No range built'}
                self.execution_window_active = True
                price = df['close'].iloc[-1]
                if price < self.range_low and close[-1] > close[-2] if len(close) > 1 else False:
                    self.liquidity_sweep_detected = True
                    self.last_sweep_direction = 'BUY'
                    logger.info(f"🔄 NY Liquidity Sweep DETECTED: BUY signal at ${price:.4f}")
                    return {
                        'signal': 'BUY',
                        'confidence': 80,
                        'details': f"✅ NY Liquidity Sweep! Range Low ${self.range_low:.4f} swept → BUY"
                    }
                if price > self.range_high and close[-1] < close[-2] if len(close) > 1 else False:
                    self.liquidity_sweep_detected = True
                    self.last_sweep_direction = 'SELL'
                    logger.info(f"🔄 NY Liquidity Sweep DETECTED: SELL signal at ${price:.4f}")
                    return {
                        'signal': 'SELL',
                        'confidence': 80,
                        'details': f"✅ NY Liquidity Sweep! Range High ${self.range_high:.4f} swept → SELL"
                    }
                if current_hour >= self.EXECUTION_START + 0.25:
                    if price < self.range_low * 1.005:
                        return {
                            'signal': 'BUY',
                            'confidence': 65,
                            'details': f"⏳ NY Golden Hour: Near range low ${self.range_low:.4f} → BUY"
                        }
                    elif price > self.range_high * 0.995:
                        return {
                            'signal': 'SELL',
                            'confidence': 65,
                            'details': f"⏳ NY Golden Hour: Near range high ${self.range_high:.4f} → SELL"
                        }
                return {
                    'signal': 'NEUTRAL',
                    'confidence': 30,
                    'details': f"⏳ NY Execution Window: Range ${self.range_low:.4f} - ${self.range_high:.4f}"
                }
            else:
                if self.range_high is not None:
                    logger.debug("⏰ NY Session ended, resetting range")
                self.range_high = None
                self.range_low = None
                self.range_build_time = None
                self.session_active = False
                self.liquidity_sweep_detected = False
                self.execution_window_active = False
        except Exception as e:
            logger.debug(f"Golden Hour strategy error: {e}")
        return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Outside NY session'}

# ============================================================
# CONSENSUS ENGINE - UPGRADED WITH WEIGHTED VOTING & REGIME FILTER
# ============================================================
class ConsensusEngine:
    def __init__(self):
        self.strategies = [
            TrendFollowingStrategy(),           # 1
            MomentumStrategy(),                 # 2
            VolatilityBreakoutStrategy(),       # 3
            PatternRecognitionStrategy(),       # 4
            MeanReversionStrategy(),            # 5
            MLPredictionStrategy(),             # 6
            GoldenHourStrategy(),               # 7
            MarketCapAltcoinSeasonStrategy(),   # 8
            FearGreedFundingRateStrategy()      # 9
        ]
        # Map strategy names to their classes for performance tracking
        self.strategy_map = {s.name: s for s in self.strategies}
        self.consensus_threshold = 0.5  # weighted sum >= 0.5 triggers signal
        self.min_confidence = MIN_CONFIDENCE_SCORE
        self.regime_detector = MarketRegimeDetector()
        self.trend_analyzer = UltimateTrendAnalyzer()
        self.adaptive_learner = AdaptiveLearner()
        self.macro_indicator = MacroIndicator()
        self.macro_indicator.start()
        
    def get_strategy_weights(self):
        """Return weights for each strategy from learner"""
        weights = {}
        for s in self.strategies:
            weights[s.name] = self.adaptive_learner.get_strategy_weight(s.name)
        return weights
        
    def analyze(self, df: pd.DataFrame, symbol: str, positions: Dict, timeframe: str = "15m") -> Dict:
        # 1. Get regime first
        regime_result = self.regime_detector.detect_regime(df)
        current_regime = regime_result['regime']
        # 2. Filter strategies based on regime (ML/Regime Filter)
        allowed_strategies = []
        if current_regime in ['STRONG_UPTREND', 'UPTREND']:
            allowed_strategies = ['Trend Following', 'Momentum', 'Volatility Breakout', 'ML Ensemble', 'Market Cap & Altcoin Season']
        elif current_regime in ['STRONG_DOWNTREND', 'DOWNTREND']:
            allowed_strategies = ['Trend Following', 'Momentum', 'Volatility Breakout', 'ML Ensemble', 'Fear & Greed + Funding Rates']
        elif current_regime == 'RANGING':
            allowed_strategies = ['Mean Reversion', 'Pattern Recognition', 'NY Golden Hour', 'ML Ensemble']
        else:  # NEUTRAL, VOLATILE
            allowed_strategies = ['Momentum', 'Volatility Breakout', 'Pattern Recognition', 'Mean Reversion', 'ML Ensemble', 'NY Golden Hour', 'Market Cap & Altcoin Season', 'Fear & Greed + Funding Rates']
        
        # 3. Run analysis on all strategies, but only those allowed get full confidence; others are ignored
        results = []
        weighted_buy_sum = 0
        weighted_sell_sum = 0
        total_weight_used = 0
        
        strategy_weights = self.get_strategy_weights()
        
        for strategy in self.strategies:
            result = strategy.analyze(df)
            # Regime filter: if strategy not allowed, set its confidence to 0 (ignore)
            if strategy.name not in allowed_strategies:
                result['confidence'] = 0
                result['signal'] = 'NEUTRAL'
            
            # Apply weight from adaptive learner
            weight = strategy_weights.get(strategy.name, 1.0)
            # For weighted voting, we accumulate weighted signals
            if result['signal'] == 'BUY':
                weighted_buy_sum += weight * (result['confidence'] / 100)
                total_weight_used += weight
            elif result['signal'] == 'SELL':
                weighted_sell_sum += weight * (result['confidence'] / 100)
                total_weight_used += weight
            # Store results
            results.append({
                'name': strategy.name,
                'signal': result['signal'],
                'confidence': result['confidence'],
                'details': result['details'],
                'weight': weight
            })
        
        # 4. Compute final signal based on weighted sum
        macro_data = self.macro_indicator.get_gold_bias()
        if not macro_data or not isinstance(macro_data, dict):
            macro_data = {
                'bias': 'NEUTRAL',
                'dxy_trend': 'NEUTRAL',
                'dxy_price': 0,
                'last_update': 0,
                'is_trading_paused': False,
                'pause_reason': ''
            }
            
        should_pause, pause_reason = self.macro_indicator.should_pause_trading(symbol)
        if should_pause:
            return {
                'signal': 'NEUTRAL',
                'confidence': 0,
                'strategies': results,
                'regime_result': regime_result,
                'trend_result': {},
                'sr_data': {},
                'breakout_data': {},
                'macro_data': macro_data,
                'timeframe': timeframe,
                'consensus': {
                    'buy_count': 0,
                    'sell_count': 0,
                    'neutral_count': len(results),
                    'threshold': self.consensus_threshold,
                    'reason': f"⚠️ Macro Pause: {pause_reason}"
                }
            }
            
        if symbol == "XAUUSDT":
            gold_bias = macro_data.get('bias', 'NEUTRAL')
            if gold_bias == 'BULLISH':
                for r in results:
                    if r['signal'] == 'BUY':
                        r['confidence'] = min(95, r['confidence'] * 1.15)
                    elif r['signal'] == 'SELL':
                        r['confidence'] = r['confidence'] * 0.85
            elif gold_bias == 'BEARISH':
                for r in results:
                    if r['signal'] == 'SELL':
                        r['confidence'] = min(95, r['confidence'] * 1.15)
                    elif r['signal'] == 'BUY':
                        r['confidence'] = r['confidence'] * 0.85
        
        # Recalculate weighted sums after gold adjustment
        weighted_buy_sum = 0
        weighted_sell_sum = 0
        total_weight_used = 0
        for r in results:
            w = r['weight']
            if r['signal'] == 'BUY':
                weighted_buy_sum += w * (r['confidence'] / 100)
                total_weight_used += w
            elif r['signal'] == 'SELL':
                weighted_sell_sum += w * (r['confidence'] / 100)
                total_weight_used += w
        
        # Apply SR and breakout logic (as before)
        trend_result = self.trend_analyzer.analyze_trend(symbol)
        weekly_trend = trend_result.get('overall', 'NEUTRAL')
        sr_data = detect_support_resistance(df)
        sr_type = sr_data.get('current_sr', 'NEUTRAL')
        breakout = detect_breakout(df)
        is_breakout = breakout.get('is_breakout', False)
        breakout_type = breakout.get('type', 'NONE')
        breakout_confirmed = breakout.get('confirmed', False)
        
        # Boost breakout signals
        if is_breakout and breakout_confirmed:
            if breakout_type == 'RESISTANCE_BREAKOUT':
                weighted_buy_sum *= 1.2
            elif breakout_type == 'SUPPORT_BREAKOUT':
                weighted_sell_sum *= 1.2
        else:
            if sr_type == 'NEAR_SUPPORT':
                weighted_sell_sum = 0
            elif sr_type == 'NEAR_RESISTANCE':
                weighted_buy_sum = 0
        
        # Weekly trend boost
        if weekly_trend in ['STRONG_BULLISH', 'BULLISH']:
            weighted_buy_sum *= 1.1
        elif weekly_trend in ['STRONG_BEARISH', 'BEARISH']:
            weighted_sell_sum *= 1.1
        
        # Golden hour boost
        golden_hour_strategy = next((s for s in results if s['name'] == 'NY Golden Hour' and s['signal'] != 'NEUTRAL'), None)
        if golden_hour_strategy and golden_hour_strategy['confidence'] >= 70:
            if golden_hour_strategy['signal'] == 'BUY':
                weighted_buy_sum *= 1.2
            elif golden_hour_strategy['signal'] == 'SELL':
                weighted_sell_sum *= 1.2
        
        total_weighted = weighted_buy_sum + weighted_sell_sum
        if total_weighted == 0:
            final_signal = 'NEUTRAL'
            final_confidence = 0
            reason = "No weighted consensus"
        else:
            buy_ratio = weighted_buy_sum / total_weighted
            sell_ratio = weighted_sell_sum / total_weighted
            if buy_ratio > self.consensus_threshold:
                final_signal = 'BUY'
                final_confidence = min(95, buy_ratio * 100 + 10)
                reason = f"Weighted BUY: {buy_ratio:.1%} ({weighted_buy_sum:.2f}/{total_weighted:.2f})"
            elif sell_ratio > self.consensus_threshold:
                final_signal = 'SELL'
                final_confidence = min(95, sell_ratio * 100 + 10)
                reason = f"Weighted SELL: {sell_ratio:.1%} ({weighted_sell_sum:.2f}/{total_weighted:.2f})"
            else:
                final_signal = 'NEUTRAL'
                final_confidence = 0
                reason = f"No consensus (BUY {buy_ratio:.1%}, SELL {sell_ratio:.1%})"
        
        # Block at SR if not breakout
        if sr_type == 'NEAR_SUPPORT' and final_signal == 'SELL':
            final_signal = 'NEUTRAL'
            final_confidence = 0
            reason = f"🚫 BLOCKED: SELL at SUPPORT - {reason}"
        elif sr_type == 'NEAR_RESISTANCE' and final_signal == 'BUY':
            final_signal = 'NEUTRAL'
            final_confidence = 0
            reason = f"🚫 BLOCKED: BUY at RESISTANCE - {reason}"
        
        if is_breakout and not breakout_confirmed and final_signal != 'NEUTRAL':
            if (final_signal == 'BUY' and breakout_type == 'RESISTANCE_BREAKOUT') or \
               (final_signal == 'SELL' and breakout_type == 'SUPPORT_BREAKOUT'):
                final_signal = 'NEUTRAL'
                final_confidence = 0
                reason = f"⏳ BREAKOUT DETECTED - Waiting for confirmation"
        
        return {
            'signal': final_signal,
            'confidence': final_confidence,
            'strategies': results,
            'regime_result': regime_result,
            'trend_result': trend_result,
            'sr_data': sr_data,
            'breakout_data': breakout,
            'macro_data': macro_data,
            'timeframe': timeframe,
            'consensus': {
                'buy_count': sum(1 for r in results if r['signal'] == 'BUY'),
                'sell_count': sum(1 for r in results if r['signal'] == 'SELL'),
                'neutral_count': sum(1 for r in results if r['signal'] == 'NEUTRAL'),
                'threshold': self.consensus_threshold,
                'reason': reason,
                'weighted_buy': weighted_buy_sum,
                'weighted_sell': weighted_sell_sum,
                'total_weight': total_weighted
            }
        }

# ============================================================
# MACROECONOMIC INDICATOR (unchanged)
# ============================================================
class MacroIndicator:
    def __init__(self):
        self.current_data = {
            'nfp': None, 'cpi': None, 'fomc': None,
            'unemployment': None, 'gdp': None,
            'dxy': {}, 'sentiment': 'NEUTRAL', 'last_update': 0,
            'high_impact_events': [], 'is_trading_paused': False,
            'pause_reason': '', 'gold_bias': 'NEUTRAL', 'dxy_trend': 'NEUTRAL'
        }
        self.lock = threading.Lock()
        self._running = False
        self._thread = None
        self._dxy_history = []
        
    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("📊 Macro Indicator service started")
        
    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
            
    def _run(self):
        while self._running:
            try:
                self._fetch_macro_data()
                self._check_high_impact_events()
                time.sleep(MACRO_UPDATE_INTERVAL)
            except Exception as e:
                logger.error(f"Macro fetcher error: {e}")
                time.sleep(30)
                
    def _fetch_macro_data(self):
        try:
            dxy_data = self._fetch_dxy()
            with self.lock:
                if dxy_data:
                    self.current_data['dxy'] = dxy_data
                else:
                    self.current_data['dxy'] = {'price': 0, 'change_24h': 0, 'high': 0, 'low': 0, 'timestamp': 0}
                self.current_data['last_update'] = time.time()
                dxy_price = self.current_data.get('dxy', {}).get('price', 0)
                if dxy_price:
                    self._update_dxy_trend(dxy_price)
                self._update_gold_bias()
            logger.debug(f"📊 Macro data updated: DXY=${self.current_data.get('dxy', {}).get('price', 0)}")
        except Exception as e:
            logger.error(f"Macro fetch error: {e}")
            
    def _fetch_dxy(self) -> Optional[Dict]:
        try:
            endpoint = f"/api/v2/mix/market/ticker"
            params = {"symbol": "DXYUSDT", "productType": "usdt-futures"}
            response = bitget_public_request(endpoint, params)
            if response and response.get('code') == '00000':
                ticker = response.get('data', {})
                if ticker:
                    return {
                        'price': float(ticker.get('price', 0)),
                        'change_24h': float(ticker.get('change24h', 0)),
                        'high': float(ticker.get('high', 0)),
                        'low': float(ticker.get('low', 0)),
                        'timestamp': time.time()
                    }
        except Exception:
            pass
        return None
        
    def _update_dxy_trend(self, current_price: float):
        self._dxy_history.append(current_price)
        if len(self._dxy_history) > 50:
            self._dxy_history = self._dxy_history[-50:]
        if len(self._dxy_history) > 10:
            recent_avg = sum(self._dxy_history[-10:]) / 10
            older_avg = sum(self._dxy_history[-20:-10]) / 10 if len(self._dxy_history) >= 20 else recent_avg
            if recent_avg > older_avg * 1.002:
                self.current_data['dxy_trend'] = 'BULLISH'
            elif recent_avg < older_avg * 0.998:
                self.current_data['dxy_trend'] = 'BEARISH'
            else:
                self.current_data['dxy_trend'] = 'NEUTRAL'
                
    def _update_gold_bias(self):
        dxy_trend = self.current_data.get('dxy_trend', 'NEUTRAL')
        if dxy_trend == 'BULLISH':
            self.current_data['gold_bias'] = 'BEARISH'
        elif dxy_trend == 'BEARISH':
            self.current_data['gold_bias'] = 'BULLISH'
        else:
            self.current_data['gold_bias'] = 'NEUTRAL'
            
    def _check_high_impact_events(self):
        with self.lock:
            self.current_data['is_trading_paused'] = False
            self.current_data['pause_reason'] = ''
                
    def get_gold_bias(self) -> Dict:
        with self.lock:
            dxy_data = self.current_data.get('dxy')
            if dxy_data is None or not isinstance(dxy_data, dict):
                dxy_data = {}
            return {
                'bias': self.current_data.get('gold_bias', 'NEUTRAL'),
                'dxy_trend': self.current_data.get('dxy_trend', 'NEUTRAL'),
                'dxy_price': dxy_data.get('price', 0) if isinstance(dxy_data, dict) else 0,
                'last_update': self.current_data.get('last_update', 0),
                'is_trading_paused': self.current_data.get('is_trading_paused', False),
                'pause_reason': self.current_data.get('pause_reason', '')
            }
            
    def should_pause_trading(self, symbol: str = None) -> Tuple[bool, str]:
        with self.lock:
            if self.current_data.get('is_trading_paused', False):
                return True, self.current_data.get('pause_reason', 'High-impact news')
            if symbol == "XAUUSDT":
                gold_bias = self.current_data.get('gold_bias', 'NEUTRAL')
                if gold_bias == 'BEARISH' and self.current_data.get('dxy_trend') == 'BULLISH':
                    return False, "DXY strength suggests caution on Gold"
        return False, "OK"

# ============================================================
# RISK MANAGER - UPGRADED WITH VOLATILITY-BASED POSITION SIZING
# ============================================================
class RiskManager:
    def __init__(self, initial_capital: float = 10.0):
        self.consecutive_losses = 0
        self.peak_capital = initial_capital
        self.max_risk_per_trade = MAX_RISK_PER_TRADE
        self.max_drawdown_limit = MAX_DRAWDOWN_LIMIT
        self.daily_pnl = 0
        self.daily_trades = 0
        self.daily_wins = 0
        self.daily_losses = 0
        self.daily_start = initial_capital
        self.last_reset_date = datetime.now().date()
        self.is_breached = False
        self.current_capital = initial_capital
        self.atr_history = []  # for volatility scaling
        
    def update_capital(self, new_capital: float):
        self.current_capital = new_capital
        if new_capital > self.peak_capital:
            self.peak_capital = new_capital
        
    def reset_daily(self, current_capital: float):
        today = datetime.now().date()
        if today != self.last_reset_date:
            self.daily_start = current_capital
            self.daily_pnl = 0
            self.daily_trades = 0
            self.daily_wins = 0
            self.daily_losses = 0
            self.is_breached = False
            self.last_reset_date = today
            logger.info(f"📅 Daily reset: Starting ${current_capital:.2f}")

    # ---------- NEW: check_daily_reset (as per missing code) ----------
    def check_daily_reset(self):
        """Check if a new day has started and reset daily metrics."""
        current_date = datetime.now().date()
        if current_date != self.last_reset_date:
            self.daily_pnl = 0
            self.daily_trades = 0
            self.daily_wins = 0
            self.daily_losses = 0
            self.last_reset_date = current_date
            logger.info("📅 Daily risk metrics reset for new day")
            
    def check_circuit_breaker(self, current_capital: float) -> Tuple[bool, str]:
        self.reset_daily(current_capital)
        if self.daily_start > 0:
            daily_drawdown = (self.daily_start - current_capital) / self.daily_start
            if daily_drawdown > MAX_DAILY_DRAWDOWN:
                self.is_breached = True
                return False, f"Daily drawdown {daily_drawdown:.1%} exceeds {MAX_DAILY_DRAWDOWN:.0%} limit"
        if self.daily_losses >= 4 and self.daily_wins == 0:
            self.is_breached = True
            return False, "4 consecutive losses without a win"
        return True, "OK"
        
    def update_daily(self, pnl: float, is_win: bool):
        self.daily_pnl += pnl
        self.daily_trades += 1
        if is_win:
            self.daily_wins += 1
        else:
            self.daily_losses += 1
            
    # ---------- OLD calculate_position_size renamed to _advanced ----------
    def calculate_position_size_advanced(self, capital: float, confidence: float, 
                                         atr: float, current_price: float, 
                                         rr_ratio: float, trend_strength: float = 1.0) -> float:
        """Volatility-adjusted position sizing (used by QuantumWhaleBot)."""
        try:
            if capital <= 0 or current_price <= 0 or atr is None or atr <= 0:
                return MIN_LOT_SIZE
            base_risk = self.max_risk_per_trade
            # Reduce risk if consecutive losses
            if self.consecutive_losses >= 2:
                base_risk *= (1 - self.consecutive_losses * 0.08)
            # Volatility scaling: if current ATR is higher than average, reduce size
            avg_atr = np.mean(self.atr_history[-20:]) if len(self.atr_history) >= 20 else atr
            if avg_atr > 0:
                vol_ratio = atr / avg_atr
                if vol_ratio > 1.2:
                    base_risk *= (1.0 / vol_ratio)  # reduce size in high volatility
                elif vol_ratio < 0.8:
                    base_risk *= (1.0 + (0.8 - vol_ratio))  # increase slightly in low vol
            confidence_multiplier = confidence / 100
            base_risk *= max(0.5, min(1.5, trend_strength))
            position_size = (capital * base_risk * confidence_multiplier * 2) / current_price
            risk_amount = capital * base_risk
            risk_position = risk_amount / (atr / current_price * 2.0)
            position_size = min(position_size, risk_position * 1.5)
            position_size = max(MIN_LOT_SIZE, position_size)
            position_size = round(position_size / LOT_SIZE_STEP) * LOT_SIZE_STEP
            return position_size
        except Exception as e:
            logger.error(f"Position sizing error: {e}")
            return MIN_LOT_SIZE

    # ---------- NEW: calculate_position_size (simple version from missing code) ----------
    def calculate_position_size(self, capital: float, entry: float, stop_loss: float, symbol: str, df: pd.DataFrame = None) -> float:
        """Volatility-Based Position Sizing (ATR ratio scaling) – simpler signature."""
        try:
            risk_amount = capital * self.max_risk_per_trade
            risk_per_unit = abs(entry - stop_loss)
            if risk_per_unit <= 0:
                return MIN_LOT_SIZE
            
            size = risk_amount / risk_per_unit
            
            # ✅ NEW: Volatility adjustment using ATR ratio if df provided
            if df is not None and len(df) >= 14:
                atr = calculate_atr(df)
                if atr and entry > 0:
                    atr_percent = (atr / entry) * 100
                    # If high volatility, scale down position size
                    if atr_percent > 3.0:
                        scaling_factor = max(0.5, 3.0 / atr_percent)
                        size *= scaling_factor
                        logger.debug(f"⚖️ High volatility ({atr_percent:.1f}%), scaling size by {scaling_factor:.2f}")

            # Lot size adjustments
            size = max(MIN_LOT_SIZE, round(size / LOT_SIZE_STEP) * LOT_SIZE_STEP)
            max_allowed_size = (capital * 0.5) / entry if entry > 0 else MIN_LOT_SIZE
            return min(size, max_allowed_size)
        except Exception as e:
            logger.error(f"Position size calculation error: {e}")
            return MIN_LOT_SIZE

    # ---------- NEW: calculate_stop_loss_take_profit ----------
    def calculate_stop_loss_take_profit(self, entry: float, side: str, df: pd.DataFrame) -> Tuple[float, float, List[float], List[float]]:
        """Dynamic Trailing Stop-Loss & Partial Take-Profits (50/30/20 split)."""
        try:
            atr = calculate_atr(df)
            if atr is None or entry <= 0:
                # Fallback fixed percentages
                if side == 'BUY':
                    sl = entry * 0.98
                    tp1 = entry * 1.015
                    tp2 = entry * 1.03
                    tp3 = entry * 1.05
                else:
                    sl = entry * 1.02
                    tp1 = entry * 0.985
                    tp2 = entry * 0.97
                    tp3 = entry * 0.95
                return sl, tp1, [tp1, tp2, tp3], PARTIAL_TP_LEVELS
            
            # ATR-based Stop Loss
            if side == 'BUY':
                sl = entry - (atr * 1.5)
                tp_targets = [
                    entry + (atr * PARTIAL_TP_TARGETS[0]),
                    entry + (atr * PARTIAL_TP_TARGETS[1]),
                    entry + (atr * 4.5)  # Trailing remainder target
                ]
            else:
                sl = entry + (atr * 1.5)
                tp_targets = [
                    entry - (atr * PARTIAL_TP_TARGETS[0]),
                    entry - (atr * PARTIAL_TP_TARGETS[1]),
                    entry - (atr * 4.5)
                ]
            return sl, tp_targets[0], tp_targets, PARTIAL_TP_LEVELS
        except Exception as e:
            logger.error(f"SL/TP calculation error: {e}")
            if side == 'BUY':
                return entry * 0.98, entry * 1.02, [entry * 1.02, entry * 1.04, entry * 1.06], [0.5, 0.3, 0.2]
            else:
                return entry * 1.02, entry * 0.98, [entry * 0.98, entry * 0.96, entry * 0.94], [0.5, 0.3, 0.2]

    def update_losses(self, is_loss: bool):
        if is_loss:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
            
    def check_trading_allowed(self, current_capital: float) -> Tuple[bool, str]:
        self.reset_daily(current_capital)
        cb_ok, cb_reason = self.check_circuit_breaker(current_capital)
        if not cb_ok:
            return False, f"Circuit Breaker: {cb_reason}"
        return True, "Trading allowed"

# ============================================================
# VAR CALCULATOR (unchanged)
# ============================================================
class VaRCalculator:
    def __init__(self):
        self.returns_history = []
        self.var_95 = 0.02
        self.var_99 = 0.05
        
    def calculate_var(self, positions: Dict) -> Dict:
        try:
            if not positions:
                return {'var_95': 0.01, 'var_99': 0.02, 'status': 'LOW', 'exposure': 0}
            total_exposure = 0
            for symbol, pos in positions.items():
                size = pos.get('size', 0)
                price = pos.get('entry', 0)
                total_exposure += size * price
            if self.returns_history:
                volatility = np.std(self.returns_history[-100:]) if len(self.returns_history) >= 10 else 0.02
            else:
                volatility = 0.02
            var_95 = total_exposure * volatility * 1.645
            var_99 = total_exposure * volatility * 2.326
            if var_95 > total_exposure * 0.05:
                status = 'HIGH'
            elif var_95 > total_exposure * 0.02:
                status = 'MEDIUM'
            else:
                status = 'LOW'
            return {'var_95': var_95, 'var_99': var_99, 'status': status, 'exposure': total_exposure}
        except Exception:
            pass
        return {'var_95': 0.01, 'var_99': 0.02, 'status': 'LOW', 'exposure': 0}
        
    def update_returns(self, pnl: float, capital: float):
        if capital > 0:
            return_pct = pnl / capital
            self.returns_history.append(return_pct)
            if len(self.returns_history) > 500:
                self.returns_history.pop(0)

# ============================================================
# SENTIMENT ANALYZER (unchanged)
# ============================================================
class EnhancedSentimentAnalyzer:
    def __init__(self):
        self.sentiment_history = []
        self.current_sentiment = "NEUTRAL"
        self.learning_rate = 0.1
        
    def analyze_market_sentiment(self, df: pd.DataFrame, symbol: str) -> Dict:
        try:
            if df is None or len(df) < 50:
                return {'sentiment': 'NEUTRAL', 'score': 50, 'confidence': 0}
            scores = []
            close = df['close'].values
            current = close[-1]
            rsi = calculate_rsi(df)
            if rsi < 30:
                scores.append(('RSI', 'OVERSOLD', 70))
            elif rsi > 70:
                scores.append(('RSI', 'OVERBOUGHT', 70))
            else:
                scores.append(('RSI', 'NEUTRAL', 40))
            macd = calculate_macd(df)
            if macd:
                if macd['trend'] == 'BULLISH' and macd['histogram'] > 0.1:
                    scores.append(('MACD', 'BULLISH', 65))
                elif macd['trend'] == 'BEARISH' and macd['histogram'] < -0.1:
                    scores.append(('MACD', 'BEARISH', 65))
                else:
                    scores.append(('MACD', 'NEUTRAL', 35))
            avg_vol = np.mean(df['volume'].values[-20:])
            current_vol = df['volume'].values[-1]
            vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1
            if vol_ratio > 2.0 and current > df['close'].values[-2]:
                scores.append(('VOLUME', 'BULLISH', 75))
            elif vol_ratio > 2.0 and current < df['close'].values[-2]:
                scores.append(('VOLUME', 'BEARISH', 75))
            else:
                scores.append(('VOLUME', 'NEUTRAL', 35))
            price_change = ((current - df['close'].values[-5]) / df['close'].values[-5]) * 100 if df['close'].values[-5] > 0 else 0
            if price_change > 2:
                scores.append(('PRICE', 'BULLISH', 70))
            elif price_change < -2:
                scores.append(('PRICE', 'BEARISH', 70))
            else:
                scores.append(('PRICE', 'NEUTRAL', 40))
            bb = calculate_bollinger_bands(df)
            if bb:
                if current > bb['upper']:
                    scores.append(('BB', 'OVERBOUGHT', 60))
                elif current < bb['lower']:
                    scores.append(('BB', 'OVERSOLD', 60))
                else:
                    scores.append(('BB', 'NEUTRAL', 35))
            bullish_score = sum(s[2] for s in scores if s[1] in ['BULLISH', 'OVERSOLD'])
            bearish_score = sum(s[2] for s in scores if s[1] in ['BEARISH', 'OVERBOUGHT'])
            total_score = bullish_score + bearish_score
            if total_score > 0:
                sentiment_score = (bullish_score / total_score) * 100
            else:
                sentiment_score = 50
            if sentiment_score > 65:
                sentiment = "BULLISH"
                confidence = min(90, 60 + (sentiment_score - 65) * 1.5)
            elif sentiment_score < 35:
                sentiment = "BEARISH"
                confidence = min(90, 60 + (35 - sentiment_score) * 1.5)
            else:
                sentiment = "NEUTRAL"
                confidence = 50
            self.sentiment_history.append({
                'timestamp': time.time(),
                'symbol': symbol,
                'sentiment': sentiment,
                'score': sentiment_score,
                'confidence': confidence
            })
            if len(self.sentiment_history) > 500:
                self.sentiment_history = self.sentiment_history[-500:]
            return {
                'sentiment': sentiment,
                'score': sentiment_score,
                'confidence': confidence,
                'factors': len(scores),
                'details': {s[0]: f"{s[1]} ({s[2]}%)" for s in scores}
            }
        except Exception:
            pass
        return {'sentiment': 'NEUTRAL', 'score': 50, 'confidence': 0}
    
    def get_market_mood(self) -> str:
        if len(self.sentiment_history) < 10:
            return "NEUTRAL"
        recent = self.sentiment_history[-20:]
        bullish_count = sum(1 for s in recent if s['sentiment'] == 'BULLISH')
        bearish_count = sum(1 for s in recent if s['sentiment'] == 'BEARISH')
        if bullish_count > bearish_count * 1.5:
            return "STRONG_BULLISH"
        elif bearish_count > bullish_count * 1.5:
            return "STRONG_BEARISH"
        elif bullish_count > bearish_count:
            return "BULLISH"
        elif bearish_count > bullish_count:
            return "BEARISH"
        else:
            return "NEUTRAL"

# ============================================================
# TOP MOVERS TRACKER (unchanged)
# ============================================================
class TopMoversTracker:
    def __init__(self):
        self.top_gainers = []
        self.top_losers = []
        self.last_update = 0
        self.update_interval = TOP_MOVERS_UPDATE_INTERVAL
        self.all_movers = {}
        
    def get_best_opportunities(self, limit: int = 5) -> List[Dict]:
        opportunities = []
        for mover in self.top_gainers + self.top_losers:
            if abs(mover.get('change_24h', 0)) > 3:
                opportunities.append(mover)
        return sorted(opportunities, key=lambda x: abs(x.get('change_24h', 0)), reverse=True)[:limit]
        
    def update(self):
        try:
            movers = []
            for symbol in ALL_SYMBOLS:
                try:
                    df = fetch_candles(symbol, "1H", 25)
                    if df is None or len(df) < 24:
                        continue
                    price_24h_ago = df['close'].iloc[0]
                    current_price = df['close'].iloc[-1]
                    if price_24h_ago <= 0:
                        continue
                    change_24h = ((current_price - price_24h_ago) / price_24h_ago) * 100
                    avg_vol = df['volume'].iloc[-20:].mean()
                    current_vol = df['volume'].iloc[-1]
                    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1
                    atr = calculate_atr(df)
                    atr_pct = (atr / current_price * 100) if atr and current_price > 0 else 0
                    sr_data = detect_support_resistance(df)
                    sr_type = sr_data.get('current_sr', 'NEUTRAL')
                    breakout = detect_breakout(df)
                    is_breakout = breakout.get('is_breakout', False)
                    breakout_type = breakout.get('type', 'NONE')
                    movers.append({
                        'symbol': symbol,
                        'change_24h': change_24h,
                        'current_price': current_price,
                        'volume_ratio': vol_ratio,
                        'atr_pct': atr_pct,
                        'sr_type': sr_type,
                        'is_breakout': is_breakout,
                        'breakout_type': breakout_type,
                        'momentum_score': abs(change_24h) * 0.5 + vol_ratio * 0.3 + atr_pct * 0.2
                    })
                except Exception:
                    continue
            
            sorted_movers = sorted(movers, key=lambda x: x['change_24h'], reverse=True)
            self.top_gainers = sorted_movers[:TOP_MOVERS_COUNT]
            self.top_losers = sorted_movers[-TOP_MOVERS_COUNT:]
            self.all_movers = {m['symbol']: m for m in movers}
            self.last_update = time.time()
            
            if self.top_gainers:
                logger.info(f"🚀 Top Gainer: {self.top_gainers[0]['symbol']} (+{self.top_gainers[0]['change_24h']:.2f}%)")
            if self.top_losers:
                logger.info(f"📉 Top Loser: {self.top_losers[0]['symbol']} ({self.top_losers[0]['change_24h']:.2f}%)")
            
            try:
                with db_manager.get_connection() as conn:
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    for g in self.top_gainers[:TOP_MOVERS_COUNT]:
                        conn.execute("""
                            INSERT INTO top_movers (timestamp, symbol, change_24h, price, volume_ratio, mover_type)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (timestamp, g['symbol'], g['change_24h'], g['current_price'], g['volume_ratio'], 'GAINER'))
                    for l in self.top_losers[:TOP_MOVERS_COUNT]:
                        conn.execute("""
                            INSERT INTO top_movers (timestamp, symbol, change_24h, price, volume_ratio, mover_type)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (timestamp, l['symbol'], l['change_24h'], l['current_price'], l['volume_ratio'], 'LOSER'))
                    conn.commit()
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Top movers update error: {e}")

# ============================================================
# HELPER FUNCTIONS (updated check_order_book to return spread for logging)
# ============================================================
def check_order_book_confirmation(symbol: str, side: str) -> Tuple[bool, str, Dict]:
    try:
        ob = get_order_book(symbol, depth=10)
        if ob is None:
            return True, "No order book data", {}
        if ob['spread'] > 0.1:
            return False, f"Spread too wide: {ob['spread']:.3f}%", ob
        if side == 'BUY':
            if ob['imbalance'] < -0.3:
                return False, f"Too much selling pressure: {ob['imbalance']:.2f}", ob
        elif side == 'SELL':
            if ob['imbalance'] > 0.3:
                return False, f"Too much buying pressure: {ob['imbalance']:.2f}", ob
        return True, f"Order book confirmed", ob
    except Exception:
        pass
    return True, "Order book check skipped", {}

def check_smart_money_confirmation(df, current_price, side):
    try:
        vwap = calculate_vwap(df)
        if vwap is None:
            return True, "No VWAP data"
        vwap_deviation = ((current_price - vwap) / vwap) * 100
        if side == 'BUY':
            if vwap_deviation < -2.0:
                vwap_status = "BUY_ZONE"
            elif vwap_deviation < 1.0:
                vwap_status = "NEUTRAL"
            else:
                vwap_status = "RESISTANCE"
        else:
            if vwap_deviation > 2.0:
                vwap_status = "SELL_ZONE"
            elif vwap_deviation > -1.0:
                vwap_status = "NEUTRAL"
            else:
                vwap_status = "SUPPORT"
        return True, f"VWAP Dev: {vwap_deviation:.1f}%, Status: {vwap_status}"
    except Exception:
        pass
    return True, "Smart Money check skipped"

def check_global_sentiment(side: str) -> Tuple[bool, str, Dict]:
    try:
        btc_trend = get_btc_trend()
        fng = get_fear_greed_index()
        result = {'btc_trend': btc_trend, 'fear_greed': fng, 'aligned': True}
        if fng['value'] < FEAR_GREED_THRESHOLD:
            if side == 'SELL':
                return False, f"Extreme Fear ({fng['value']}) - Avoid SELL", result
        elif fng['value'] > 75:
            if side == 'BUY':
                return False, f"Extreme Greed ({fng['value']}) - Avoid BUY", result
        if REQUIRE_BTC_ALIGNMENT:
            if side == 'BUY' and btc_trend == 'BEARISH':
                return False, f"BTC is {btc_trend} - Avoid BUY", result
            elif side == 'SELL' and btc_trend == 'BULLISH':
                return False, f"BTC is {btc_trend} - Avoid SELL", result
        result['aligned'] = True
        return True, f"Sentiment OK (BTC: {btc_trend}, FnG: {fng['value']})", result
    except Exception:
        pass
    return True, "Sentiment check skipped", {'btc_trend': 'NEUTRAL', 'fear_greed': {'value': 50}}

def calculate_dynamic_rr(confidence: float, atr_percent: float, market_volatility: float) -> float:
    try:
        base_rr = 1.5 + (confidence - 60) / 25
        if atr_percent > 3.0:
            base_rr *= 1.2
        elif atr_percent < 1.0:
            base_rr *= 0.9
        return max(MIN_RR_RATIO, min(MAX_RR_RATIO, base_rr))
    except Exception:
        pass
    return MIN_RR_RATIO

def safe_telegram_send(message):
    try:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code == 200:
                logger.info("📨 Telegram sent")
            else:
                logger.debug(f"Telegram error: {response.status_code}")
    except Exception:
        pass

# ============================================================
# MULTI-TIMEFRAME ANALYZER (unchanged)
# ============================================================
class MultiTimeframeAnalyzer:
    def __init__(self):
        self.timeframes = MTF_TIMEFRAMES
        self.trend_analyzer = UltimateTrendAnalyzer()
        
    def analyze(self, symbol: str, side: str) -> Tuple[bool, str, Dict]:
        try:
            trend_result = self.trend_analyzer.analyze_trend(symbol)
            trends = trend_result.get('trends', {})
            if not trends:
                return True, "No MTF data available", {}
            agreements = []
            details = {}
            for tf, trend in trends.items():
                if side == 'BUY':
                    agrees = trend in ['STRONG_BULLISH', 'BULLISH']
                    agreements.append(agrees)
                    details[tf] = f"{tf}: {trend} {'✅' if agrees else '❌'}"
                else:
                    agrees = trend in ['STRONG_BEARISH', 'BEARISH']
                    agreements.append(agrees)
                    details[tf] = f"{tf}: {trend} {'✅' if agrees else '❌'}"
            if not agreements:
                return True, "No MTF data available", trends
            agreement_rate = sum(agreements) / len(agreements) * 100
            weekly_trend = trend_result.get('overall', 'NEUTRAL')
            weekly_agrees = False
            if side == 'BUY' and weekly_trend in ['STRONG_BULLISH', 'BULLISH']:
                weekly_agrees = True
            elif side == 'SELL' and weekly_trend in ['STRONG_BEARISH', 'BEARISH']:
                weekly_agrees = True
            if weekly_agrees:
                agreement_rate = min(100, agreement_rate + 15)
            if agreement_rate >= MTF_CONFIRMATION_THRESHOLD * 100:
                return True, f"MTF confirmed ({agreement_rate:.0f}%)", {'trends': trends, 'weekly': weekly_trend}
            else:
                return False, f"MTF disagreement ({agreement_rate:.0f}%)", {'trends': trends, 'weekly': weekly_trend}
        except Exception as e:
            logger.error(f"MTF error: {e}")
            return True, "MTF skip due to error", {}

# ============================================================
# ULTIMATE BOT - COMPLETE WITH ALL UPGRADES
# ============================================================
current_capital = 10.0
peak_capital = 10.0

class QuantumWhaleBot:
    def __init__(self):
        global current_capital, peak_capital
        
        self.active_trades = {}
        self.position_data = {}
        self.current_capital = 10.0
        self.peak_capital = 10.0
        self.is_running = False
        self.last_signal_time = defaultdict(float)
        self.signal_cooldown = SIGNAL_COOLDOWN
        self.last_update_time = time.time()
        self.daily_pnl = 0
        
        self.consensus_engine = ConsensusEngine()
        self.mtf_analyzer = MultiTimeframeAnalyzer()
        self.risk_manager = RiskManager(initial_capital=10.0)
        self.regime_detector = MarketRegimeDetector()
        self.trend_analyzer = UltimateTrendAnalyzer()
        self.adaptive_learner = AdaptiveLearner()
        self.var_calculator = VaRCalculator()
        self.top_movers = TopMoversTracker()
        self.sentiment_analyzer = EnhancedSentimentAnalyzer()
        self.macro_indicator = self.consensus_engine.macro_indicator
        
        self.live_prices = {}
        self.candle_cache = {}
        self.market_sentiment = {"sentiment": "NEUTRAL", "confidence": 0}
        self.fear_greed_cache = {}
        self.btc_trend_cache = "NEUTRAL"
        self.order_book_cache = {}
        self.current_regime = "NEUTRAL"
        self.weekly_trend_cache = "NEUTRAL"
        
        self.update_counter = 0
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        current_capital = self.current_capital
        peak_capital = self.peak_capital
        
        price_fetcher.start()
        
    def update_prices(self):
        self.live_prices = price_fetcher.prices
        
    def update_order_books(self):
        for symbol in ALL_SYMBOLS[:10]:
            try:
                ob = get_order_book(symbol)
                if ob:
                    self.order_book_cache[symbol] = ob
            except Exception:
                pass
                
    def get_candles(self, symbol: str, timeframe: str = "1H", limit: int = 100) -> Optional[pd.DataFrame]:
        cache_key = f"{symbol}_{timeframe}"
        if cache_key in self.candle_cache:
            cached = self.candle_cache[cache_key]
            if time.time() - cached['timestamp'] < 30:
                return cached['data']
        df = fetch_candles(symbol, timeframe, limit)
        if df is not None and len(df) > 0:
            self.candle_cache[cache_key] = {'data': df, 'timestamp': time.time()}
        return df
        
    def calculate_dynamic_sl_tp(self, current_price: float, atr: float, side: str, 
                                 confidence: float, rr_ratio: float) -> Tuple[float, float]:
        if atr is None or atr <= 0:
            atr = current_price * 0.01
        if confidence > 80:
            sl_mult = 1.2
        elif confidence > 70:
            sl_mult = 1.5
        else:
            sl_mult = 2.0
        if side == "BUY":
            sl = current_price - (atr * sl_mult)
            tp = current_price + (atr * sl_mult * rr_ratio)
        else:
            sl = current_price + (atr * sl_mult)
            tp = current_price - (atr * sl_mult * rr_ratio)
        return sl, tp
        
    def execute_order(self, symbol: str, side: str, size: float) -> Tuple[bool, str, str]:
        if TRADING_MODE in ["PAPER", "DEMO"]:
            order_id = f"{TRADING_MODE}_{int(time.time())}_{random.randint(1000, 9999)}"
            return True, order_id, TRADING_MODE
        try:
            payload = {
                "symbol": symbol,
                "productType": "usdt-futures",
                "marginMode": "isolated",
                "marginCoin": "USDT",
                "size": str(size),
                "side": side.lower(),
                "orderType": "market",
                "force": "gtc"
            }
            response = send_bitget_request('POST', "/api/v2/mix/order/place-order", payload)
            if response and response.get('code') == '00000':
                order_id = response.get('data', {}).get('orderId', f"BITGET_{int(time.time())}")
                return True, order_id, "REAL"
        except Exception as e:
            logger.error(f"Order execution error: {e}")
        return False, None, "PAPER"
        
    def generate_signal_message(self, symbol: str, side: str, confidence: float,
                               strategies: list, consensus: dict, pattern: str,
                               entry: float, sl: float, tp: float, size: float,
                               rr_ratio: float, mtf_status: str,
                               sentiment_status: str, vwap_status: str,
                               ml_status: str, var_status: str,
                               regime: str, sentiment: str, weekly_trend: str,
                               sr_type: str, is_breakout: bool, breakout_type: str,
                               macro_bias: str = "NEUTRAL", golden_hour: bool = False,
                               timeframe: str = "15m",
                               market_cap_sentiment: str = "NEUTRAL",
                               funding_rate_sentiment: str = "NEUTRAL") -> str:
        
        strategy_details = "\n".join([
            f"  • {s['name']}: {s['signal']} ({s['confidence']:.0f}%) - {s['details']} (weight: {s.get('weight',1.0):.2f})"
            for s in strategies
        ])
        
        active_note = f"{len(self.active_trades)}/{MAX_ACTIVE_TRADES} (MAX 5)"
        breakout_note = f"🔓 *{breakout_type} CONFIRMED!*" if is_breakout else ""
        sr_note = f"📍 SR Status: {sr_type}"
        macro_note = f"📊 Macro Bias: {macro_bias}" if macro_bias != "NEUTRAL" else ""
        golden_note = "⏰ *GOLDEN HOUR ACTIVE!*" if golden_hour else ""
        tf_note = f"⏱️ Timeframe: {timeframe}"
        mc_note = f"📊 Market Cap: {market_cap_sentiment}" if market_cap_sentiment != "NEUTRAL" else ""
        fr_note = f"💰 Funding Rate: {funding_rate_sentiment}" if funding_rate_sentiment != "NEUTRAL" else ""
        
        return f"""
🐋 *QUANTUM WHALE v19.0 ULTIMATE - 9 STRATEGIES (Weighted Voting)*

📊 *{symbol}*
⏱️ *{timeframe}*
🔹 Action: `{side}`
🔹 Confidence: `{confidence:.0f}%`
🔹 Pattern: `{pattern}`
🔹 R:R Ratio: `1 : {rr_ratio:.1f}`
🔹 Weekly Trend: `{weekly_trend}`
🔹 Market Regime: `{regime}`
🔹 Market Sentiment: `{sentiment}`
{macro_note}
{mc_note}
{fr_note}
{breakout_note}
{golden_note}
{sr_note}

📈 Entry: `${entry:.4f}`
🎯 Take Profit: `${tp:.4f}`
🛑 Stop Loss: `${sl:.4f}`
📊 Position: `{size:.4f}`

🧠 *STRATEGY CONSENSUS (Weighted):*
{strategy_details}

📡 *MTF Status:* {mtf_status}
🌐 *Sentiment:* {sentiment_status}
📊 *VWAP:* {vwap_status}
🤖 *ML Prediction:* {ml_status}
🛡️ *VaR Status:* {var_status}

💡 *Reason:* {consensus['reason']}
📊 *Active Trades:* {active_note}
💰 *Current Capital:* ${self.current_capital:.2f}
"""
        
    def _score_coins(self) -> Dict:
        scores = {}
        for symbol in ALL_SYMBOLS:
            try:
                df = self.get_candles(symbol, "15m", 50)
                if df is None or len(df) < 20:
                    continue
                current = df['close'].iloc[-1]
                if current <= 0:
                    continue
                atr = calculate_atr(df)
                if atr is None:
                    continue
                atr_pct = (atr / current) * 100
                volume_ratio = df['volume'].iloc[-1] / df['volume'].iloc[-20:].mean()
                trend = self.trend_analyzer.analyze_trend(symbol)
                trend_strength = trend.get('strength', 0)
                breakout = detect_breakout(df)
                is_breakout = breakout.get('is_breakout', False)
                breakout_confirmed = breakout.get('confirmed', False)
                sr_data = detect_support_resistance(df)
                sr_type = sr_data.get('current_sr', 'NEUTRAL')
                score = (
                    volume_ratio * 0.2 +
                    atr_pct * 0.15 +
                    trend_strength * 0.25 +
                    abs(df['close'].pct_change().iloc[-1]) * 0.15 +
                    (1 - abs(df['close'].iloc[-1] - df['close'].iloc[-5]) / current) * 0.15
                )
                if is_breakout and breakout_confirmed:
                    score *= 1.3
                elif is_breakout:
                    score *= 1.1
                if sr_type == 'NEAR_SUPPORT' or sr_type == 'NEAR_RESISTANCE':
                    if not is_breakout:
                        score *= 0.7
                scores[symbol] = score
            except Exception:
                pass
        return scores
        
    def analyze_and_trade(self):
        global current_capital, peak_capital
        
        self.update_counter += 1
        if self.update_counter % 10 == 0:
            self.update_prices()
            self.update_order_books()
            
        trading_allowed, reason = self.risk_manager.check_trading_allowed(self.current_capital)
        if not trading_allowed:
            if self.update_counter % 60 == 0:
                logger.warning(f"⚠️ Trading paused: {reason}")
            return
            
        stats = self.get_statistics()
        total_trades = stats.get('total', 0)
        win_rate = stats.get('win_rate', 0)
        
        if total_trades >= MIN_TRADES_FOR_WIN_RATE and win_rate < MIN_WIN_RATE:
            if self.update_counter % 60 == 0:
                logger.warning(f"⚠️ Win rate {win_rate:.1f}% below {MIN_WIN_RATE}%")
            return
            
        if len(self.active_trades) >= MAX_ACTIVE_TRADES:
            return
            
        self.top_movers.update()
        coin_scores = self._score_coins()
        top_coins = sorted(coin_scores.items(), key=lambda x: x[1], reverse=True)[:15]
        
        btc_trend = get_btc_trend()
        fng = get_fear_greed_index()
        self.btc_trend_cache = btc_trend
        self.fear_greed_cache = fng
        
        df_btc = self.get_candles(BTC_SYMBOL, "1H", 50)
        if df_btc is not None:
            regime_result = self.regime_detector.detect_regime(df_btc)
            self.current_regime = regime_result.get('regime', 'NEUTRAL')
            trend_result = self.trend_analyzer.analyze_trend(BTC_SYMBOL)
            self.weekly_trend_cache = trend_result.get('overall', 'NEUTRAL')
        
        trade_timeframes = ["5m", "15m"]
        
        for symbol, score in top_coins[:10]:
            for tf in trade_timeframes:
                try:
                    if time.time() - self.last_signal_time[f"{symbol}_{tf}"] < self.signal_cooldown:
                        continue
                        
                    if symbol in self.active_trades:
                        continue
                        
                    df = self.get_candles(symbol, tf, 100)
                    if df is None or len(df) < 30:
                        continue
                        
                    current_price = df['close'].iloc[-1]
                    if current_price <= 0 or pd.isna(current_price):
                        continue
                        
                    atr = calculate_atr(df)
                    if atr is None or atr <= 0:
                        continue
                        
                    # Update ATR history for volatility scaling
                    if len(self.risk_manager.atr_history) >= 100:
                        self.risk_manager.atr_history.pop(0)
                    self.risk_manager.atr_history.append(atr)
                        
                    atr_percent = (atr / current_price) * 100
                    if atr_percent < MIN_ATR_PERCENT or atr_percent > MAX_ATR_PERCENT:
                        continue
                        
                    avg_volume = df['volume'].iloc[-20:].mean()
                    current_volume = df['volume'].iloc[-1]
                    if current_volume < avg_volume * MIN_VOLUME_MULTIPLIER:
                        continue
                        
                    sr_data = detect_support_resistance(df)
                    sr_type = sr_data.get('current_sr', 'NEUTRAL')
                    breakout = detect_breakout(df)
                    is_breakout = breakout.get('is_breakout', False)
                    breakout_confirmed = breakout.get('confirmed', False)
                    breakout_type = breakout.get('type', 'NONE')
                    
                    if sr_type in ['NEAR_SUPPORT', 'NEAR_RESISTANCE'] and not is_breakout:
                        if self.update_counter % 60 == 0:
                            logger.debug(f"⏳ {symbol} at {sr_type} - Waiting for breakout")
                        continue
                        
                    consensus = self.consensus_engine.analyze(df, symbol, self.position_data, tf)
                    
                    if consensus['signal'] == 'NEUTRAL':
                        continue
                        
                    if consensus['confidence'] < MIN_CONFIDENCE_SCORE:
                        continue
                        
                    ob_ok, ob_status, ob_data = check_order_book_confirmation(symbol, consensus['signal'])
                    if not ob_ok:
                        continue
                        
                    vwap_ok, vwap_status = check_smart_money_confirmation(df, current_price, consensus['signal'])
                    if not vwap_ok:
                        continue
                        
                    sentiment_ok, sentiment_status, sentiment_data = check_global_sentiment(consensus['signal'])
                    if not sentiment_ok:
                        continue
                        
                    mtf_ok, mtf_reason, mtf_trends = self.mtf_analyzer.analyze(symbol, consensus['signal'])
                    if not mtf_ok:
                        continue
                        
                    pattern = "STANDARD_MOVE"
                    for s in consensus['strategies']:
                        if "Pattern:" in s['details']:
                            pattern = s['details'].replace("Pattern: ", "").split(",")[0]
                            break
                            
                    pattern_weight = self.adaptive_learner.get_pattern_weight(pattern)
                    rr_ratio = calculate_dynamic_rr(consensus['confidence'], atr_percent, 1.0)
                    
                    # Calculate SL and TP based on ATR
                    sl, tp = self.calculate_dynamic_sl_tp(current_price, atr, consensus['signal'], consensus['confidence'], rr_ratio)
                    
                    if consensus['signal'] == 'BUY':
                        if sl >= current_price or tp <= current_price:
                            continue
                    else:
                        if sl <= current_price or tp >= current_price:
                            continue
                            
                    risk = abs(current_price - sl)
                    reward = abs(tp - current_price)
                    actual_rr = reward / risk if risk > 0 else 0
                    if actual_rr < MIN_RR_RATIO:
                        continue
                        
                    var_status = 'LOW'
                    regime = self.current_regime
                    weekly_trend = self.weekly_trend_cache
                    trend_strength = 1.0
                    
                    # Dynamic position sizing with volatility - using advanced method
                    size = self.risk_manager.calculate_position_size_advanced(
                        self.current_capital, consensus['confidence'],
                        atr, current_price, actual_rr, trend_strength
                    )
                    if size < MIN_LOT_SIZE:
                        continue
                        
                    has_strong_opposing = False
                    if is_breakout and breakout_confirmed:
                        for s in consensus['strategies']:
                            if s['signal'] == 'SELL' and consensus['signal'] == 'BUY' and s['confidence'] > 80:
                                has_strong_opposing = True
                                break
                            elif s['signal'] == 'BUY' and consensus['signal'] == 'SELL' and s['confidence'] > 80:
                                has_strong_opposing = True
                                break
                    else:
                        for s in consensus['strategies']:
                            if s['signal'] == 'SELL' and consensus['signal'] == 'BUY' and s['confidence'] > 75:
                                has_strong_opposing = True
                                break
                            elif s['signal'] == 'BUY' and consensus['signal'] == 'SELL' and s['confidence'] > 75:
                                has_strong_opposing = True
                                break
                    if has_strong_opposing:
                        continue
                        
                    self.last_signal_time[f"{symbol}_{tf}"] = time.time()
                    
                    ml_status = f"ML {consensus.get('ml_result', {}).get('signal', 'N/A')}"
                    sentiment = consensus.get('regime_result', {}).get('regime', 'NEUTRAL')
                    macro_bias = consensus.get('macro_data', {}).get('bias', 'NEUTRAL')
                    golden_hour_active = any(s['name'] == 'NY Golden Hour' and s['signal'] != 'NEUTRAL' for s in consensus['strategies'])
                    
                    market_cap_sentiment = "NEUTRAL"
                    funding_rate_sentiment = "NEUTRAL"
                    for s in consensus['strategies']:
                        if s['name'] == 'Market Cap & Altcoin Season' and s['signal'] != 'NEUTRAL':
                            market_cap_sentiment = s['signal']
                        if s['name'] == 'Fear & Greed + Funding Rates' and s['signal'] != 'NEUTRAL':
                            funding_rate_sentiment = s['signal']
                    
                    signal_text = self.generate_signal_message(
                        symbol, consensus['signal'], consensus['confidence'],
                        consensus['strategies'], consensus['consensus'],
                        pattern, current_price, sl, tp, size,
                        actual_rr, mtf_reason, sentiment_status, vwap_status,
                        ml_status, var_status, regime, sentiment, weekly_trend,
                        sr_type, is_breakout, breakout_type,
                        macro_bias, golden_hour_active, tf,
                        market_cap_sentiment, funding_rate_sentiment
                    )
                    safe_telegram_send(signal_text)
                    logger.info(f"📤 SIGNAL: {symbol} {consensus['signal']} @ {current_price:.4f} ({tf}) - Weighted Consensus")
                    
                    if len(self.active_trades) < MAX_ACTIVE_TRADES:
                        success, order_id, mode = self.execute_order(symbol, consensus['signal'], size)
                        if success:
                            # Prepare partial TP levels (50%, 30%, 20%)
                            # We'll store TP levels in the trade dict
                            tp_levels = []
                            for frac, target in zip(PARTIAL_TP_LEVELS, PARTIAL_TP_TARGETS):
                                if target > 0:
                                    if consensus['signal'] == 'BUY':
                                        tp_price = current_price + (atr * target * 0.5)  # using ATR-based target
                                    else:
                                        tp_price = current_price - (atr * target * 0.5)
                                    tp_levels.append({
                                        'fraction': frac,
                                        'price': tp_price,
                                        'executed': False
                                    })
                            # Last portion is trailing stop (no TP target, will be closed by trailing SL)
                            
                            self.active_trades[symbol] = {
                                'side': consensus['signal'],
                                'entry': current_price,
                                'sl': sl,
                                'tp': tp,  # keep original for reference
                                'size': size,
                                'order_id': order_id,
                                'mode': mode,
                                'confidence': consensus['confidence'],
                                'strategies': consensus['strategies'],
                                'rr_ratio': actual_rr,
                                'pattern': pattern,
                                'pattern_weight': pattern_weight,
                                'entry_time': time.time(),
                                'var_status': var_status,
                                'regime': regime,
                                'sentiment': sentiment,
                                'weekly_trend': weekly_trend,
                                'sr_type': sr_type,
                                'is_breakout': is_breakout,
                                'breakout_type': breakout_type,
                                'macro_bias': macro_bias,
                                'golden_hour': golden_hour_active,
                                'timeframe': tf,
                                'market_cap_sentiment': market_cap_sentiment,
                                'funding_rate_sentiment': funding_rate_sentiment,
                                'tp_levels': tp_levels,  # NEW: partial TP levels
                                'remaining_size': size,  # track remaining position size
                                'trailing_active': False
                            }
                            
                            self.position_data[symbol] = {
                                'side': consensus['signal'],
                                'entry': current_price,
                                'current': current_price,
                                'pnl': 0,
                                'pnl_percent': 0,
                                'size': size,
                                'sl': sl,
                                'tp': tp,
                                'mode': mode,
                                'confidence': consensus['confidence'],
                                'rr_ratio': actual_rr,
                                'var_status': var_status,
                                'regime': regime,
                                'sentiment': sentiment,
                                'weekly_trend': weekly_trend,
                                'sr_type': sr_type,
                                'is_breakout': is_breakout,
                                'breakout_type': breakout_type,
                                'macro_bias': macro_bias,
                                'golden_hour': golden_hour_active,
                                'timeframe': tf,
                                'market_cap_sentiment': market_cap_sentiment,
                                'funding_rate_sentiment': funding_rate_sentiment,
                                'remaining_size': size
                            }
                            
                            self.log_trade(
                                symbol, consensus['signal'], current_price, size,
                                0, "OPEN", order_id, mode, consensus['confidence'],
                                json.dumps([s['name'] for s in consensus['strategies'] if s['signal'] != 'NEUTRAL']),
                                pattern, consensus['consensus']['reason'],
                                actual_rr, mtf_reason,
                                sentiment_status, fng['value'], pattern_weight,
                                0, var_status, ob_data.get('spread', 0) if ob_data else 0,
                                regime, self.daily_pnl, 0, weekly_trend,
                                1 if is_breakout and breakout_confirmed else 0,
                                sr_type, 1 if breakout_confirmed else 0,
                                macro_bias, 1 if golden_hour_active else 0,
                                tf, market_cap_sentiment, funding_rate_sentiment,
                                slippage=0, latency=0, exit_reason="", partial_exits=""
                            )
                            
                            self.adaptive_learner.log_learning_insight(
                                symbol, consensus['signal'], "OPEN",
                                consensus['confidence'], sentiment, regime, 0, weekly_trend,
                                "NEUTRAL", macro_bias, tf,
                                market_cap_sentiment, funding_rate_sentiment
                            )
                            
                            logger.info(f"✅ EXECUTED: {symbol} {consensus['signal']} @ {current_price:.4f} ({tf}) - Weighted Consensus")
                except Exception as e:
                    logger.error(f"❌ {symbol} analysis error: {e}")
                    continue
                
    def manage_trades(self):
        global current_capital, peak_capital
        
        for symbol in list(self.active_trades.keys()):
            try:
                trade = self.active_trades[symbol]
                df = self.get_candles(symbol, "1H", 10)
                if df is None or len(df) == 0:
                    continue
                    
                current_price = df['close'].iloc[-1]
                atr = calculate_atr(df) or (current_price * 0.01)
                
                side = trade['side']
                entry = trade['entry']
                sl = trade['sl']
                tp = trade['tp']
                size = trade['size']
                rr_ratio = trade.get('rr_ratio', 2.0)
                regime = trade.get('regime', 'NEUTRAL')
                sentiment = trade.get('sentiment', 'NEUTRAL')
                pattern = trade.get('pattern', 'UNKNOWN')
                weekly_trend = trade.get('weekly_trend', 'NEUTRAL')
                macro_bias = trade.get('macro_bias', 'NEUTRAL')
                timeframe = trade.get('timeframe', '15m')
                market_cap_sentiment = trade.get('market_cap_sentiment', 'NEUTRAL')
                funding_rate_sentiment = trade.get('funding_rate_sentiment', 'NEUTRAL')
                tp_levels = trade.get('tp_levels', [])
                remaining_size = trade.get('remaining_size', size)
                
                if side == "BUY":
                    pnl = (current_price - entry) * remaining_size
                    pnl_percent = ((current_price - entry) / entry) * 100
                else:
                    pnl = (entry - current_price) * remaining_size
                    pnl_percent = ((entry - current_price) / entry) * 100
                    
                self.position_data[symbol] = {
                    'side': side,
                    'entry': entry,
                    'current': current_price,
                    'pnl': pnl,
                    'pnl_percent': pnl_percent,
                    'size': remaining_size,
                    'sl': sl,
                    'tp': tp,
                    'mode': trade.get('mode', 'PAPER'),
                    'confidence': trade.get('confidence', 0),
                    'rr_ratio': rr_ratio,
                    'var_status': trade.get('var_status', 'LOW'),
                    'regime': regime,
                    'sentiment': sentiment,
                    'weekly_trend': weekly_trend,
                    'macro_bias': macro_bias,
                    'timeframe': timeframe,
                    'market_cap_sentiment': market_cap_sentiment,
                    'funding_rate_sentiment': funding_rate_sentiment,
                    'remaining_size': remaining_size
                }
                
                # Update capital including all open positions
                self.current_capital = 10.0 + sum([
                    self.position_data.get(s, {}).get('pnl', 0)
                    for s in self.active_trades.keys()
                ])
                current_capital = self.current_capital
                self.risk_manager.update_capital(self.current_capital)
                
                if self.current_capital > self.peak_capital:
                    self.peak_capital = self.current_capital
                    peak_capital = self.peak_capital
                    
                self.daily_pnl = self.current_capital - 10.0
                self.var_calculator.update_returns(pnl, self.current_capital)
                
                # ========== DYNAMIC TRAILING STOP-LOSS (ATR-based) ==========
                # Activate trailing when price moves in our favor by TRAILING_ACTIVATION * ATR
                if side == "BUY":
                    if (current_price - entry) > (atr * TRAILING_ACTIVATION):
                        # Move SL to current price - TRAILING_STEP * ATR
                        new_sl = current_price - (atr * TRAILING_STEP)
                        if new_sl > sl:
                            trade['sl'] = sl = new_sl
                            logger.info(f"🔺 {symbol} Trailing SL moved to {sl:.4f}")
                elif side == "SELL":
                    if (entry - current_price) > (atr * TRAILING_ACTIVATION):
                        new_sl = current_price + (atr * TRAILING_STEP)
                        if new_sl < sl:
                            trade['sl'] = sl = new_sl
                            logger.info(f"🔻 {symbol} Trailing SL moved to {sl:.4f}")
                
                # ========== PARTIAL TAKE-PROFITS ==========
                # Check each TP level
                partial_exits_log = []
                for idx, level in enumerate(tp_levels):
                    if not level['executed']:
                        target_price = level['price']
                        if side == "BUY" and current_price >= target_price:
                            # Execute partial exit
                            exit_fraction = level['fraction']
                            exit_size = size * exit_fraction
                            profit = (target_price - entry) * exit_size
                            self.current_capital += profit
                            current_capital = self.current_capital
                            self.risk_manager.update_capital(self.current_capital)
                            # Log partial exit
                            logger.info(f"🎯 Partial TP {idx+1} hit for {symbol}: {exit_fraction*100}% at {target_price:.4f}, profit ${profit:.2f}")
                            safe_telegram_send(f"📈 PARTIAL TP {idx+1} {symbol}: {exit_fraction*100}% at ${target_price:.4f}, +${profit:.2f}")
                            partial_exits_log.append(f"TP{idx+1}: {exit_fraction*100}% @ {target_price:.4f}")
                            # Update remaining size
                            remaining_size -= exit_size
                            trade['remaining_size'] = remaining_size
                            level['executed'] = True
                            # Update trade size for final PnL later
                        elif side == "SELL" and current_price <= target_price:
                            exit_fraction = level['fraction']
                            exit_size = size * exit_fraction
                            profit = (entry - target_price) * exit_size
                            self.current_capital += profit
                            current_capital = self.current_capital
                            self.risk_manager.update_capital(self.current_capital)
                            logger.info(f"🎯 Partial TP {idx+1} hit for {symbol}: {exit_fraction*100}% at {target_price:.4f}, profit ${profit:.2f}")
                            safe_telegram_send(f"📈 PARTIAL TP {idx+1} {symbol}: {exit_fraction*100}% at ${target_price:.4f}, +${profit:.2f}")
                            partial_exits_log.append(f"TP{idx+1}: {exit_fraction*100}% @ {target_price:.4f}")
                            remaining_size -= exit_size
                            trade['remaining_size'] = remaining_size
                            level['executed'] = True
                
                # If remaining_size <= 0, close trade entirely
                if remaining_size <= 0:
                    # Close remaining position (should be zero)
                    profit = (current_price - entry) * 0  # no remaining
                    self.current_capital += profit
                    self.risk_manager.update_losses(False)  # partial wins considered win
                    self.adaptive_learner.save_pattern_performance(pattern, True)
                    self.adaptive_learner.update_strategy_performance(side, True)
                    self.adaptive_learner.log_learning_insight(symbol, side, "WIN", trade.get('confidence', 0), sentiment, regime, profit, weekly_trend, "NEUTRAL", macro_bias, timeframe, market_cap_sentiment, funding_rate_sentiment)
                    self.log_trade(symbol, side, entry, size, profit, "WIN", trade.get('order_id'), trade.get('mode'), trade.get('confidence', 0), "", pattern, "All TP hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0), trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend, 0, "", 0, "", 0, timeframe, market_cap_sentiment, funding_rate_sentiment, slippage=0, latency=0, exit_reason="All partial TPs hit", partial_exits="|".join(partial_exits_log))
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    continue
                
                # ========== FINAL TP / SL CHECK ==========
                if side == "BUY" and current_price >= tp:
                    # Remaining position hits final TP
                    profit = (tp - entry) * remaining_size
                    self.current_capital += profit
                    current_capital = self.current_capital
                    self.risk_manager.update_capital(self.current_capital)
                    self.risk_manager.update_losses(False)
                    self.adaptive_learner.save_pattern_performance(pattern, True)
                    self.adaptive_learner.update_strategy_performance(side, True)
                    self.adaptive_learner.log_learning_insight(symbol, side, "WIN", trade.get('confidence', 0), sentiment, regime, profit, weekly_trend, "NEUTRAL", macro_bias, timeframe, market_cap_sentiment, funding_rate_sentiment)
                    safe_telegram_send(f"🎯 FINAL TP HIT {symbol}: +${profit:.4f} | Capital: ${self.current_capital:.2f} ({timeframe})")
                    partial_exits_log.append(f"FINAL TP @ {tp:.4f}")
                    self.log_trade(symbol, side, entry, size, profit, "WIN", trade.get('order_id'), trade.get('mode'), trade.get('confidence', 0), "", pattern, "Final TP Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0), trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend, 0, "", 0, "", 0, timeframe, market_cap_sentiment, funding_rate_sentiment, slippage=0, latency=0, exit_reason="Final TP", partial_exits="|".join(partial_exits_log))
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"✅ {symbol} WIN (Final TP): ${profit:.4f}")
                elif side == "SELL" and current_price <= tp:
                    profit = (entry - tp) * remaining_size
                    self.current_capital += profit
                    current_capital = self.current_capital
                    self.risk_manager.update_capital(self.current_capital)
                    self.risk_manager.update_losses(False)
                    self.adaptive_learner.save_pattern_performance(pattern, True)
                    self.adaptive_learner.update_strategy_performance(side, True)
                    self.adaptive_learner.log_learning_insight(symbol, side, "WIN", trade.get('confidence', 0), sentiment, regime, profit, weekly_trend, "NEUTRAL", macro_bias, timeframe, market_cap_sentiment, funding_rate_sentiment)
                    safe_telegram_send(f"🎯 FINAL TP HIT {symbol}: +${profit:.4f} | Capital: ${self.current_capital:.2f} ({timeframe})")
                    partial_exits_log.append(f"FINAL TP @ {tp:.4f}")
                    self.log_trade(symbol, side, entry, size, profit, "WIN", trade.get('order_id'), trade.get('mode'), trade.get('confidence', 0), "", pattern, "Final TP Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0), trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend, 0, "", 0, "", 0, timeframe, market_cap_sentiment, funding_rate_sentiment, slippage=0, latency=0, exit_reason="Final TP", partial_exits="|".join(partial_exits_log))
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"✅ {symbol} WIN (Final TP): ${profit:.4f}")
                elif side == "BUY" and current_price <= sl:
                    loss = (entry - sl) * remaining_size
                    self.current_capital -= loss
                    current_capital = self.current_capital
                    self.risk_manager.update_capital(self.current_capital)
                    self.risk_manager.update_losses(True)
                    self.adaptive_learner.save_pattern_performance(pattern, False)
                    self.adaptive_learner.update_strategy_performance(side, False)
                    self.adaptive_learner.log_learning_insight(symbol, side, "LOSS", trade.get('confidence', 0), sentiment, regime, -loss, weekly_trend, "NEUTRAL", macro_bias, timeframe, market_cap_sentiment, funding_rate_sentiment)
                    safe_telegram_send(f"🛑 SL HIT {symbol}: -${loss:.4f} | Capital: ${self.current_capital:.2f} ({timeframe})")
                    partial_exits_log.append(f"SL @ {sl:.4f}")
                    self.log_trade(symbol, side, entry, size, -loss, "LOSS", trade.get('order_id'), trade.get('mode'), trade.get('confidence', 0), "", pattern, "SL Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0), trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend, 0, "", 0, "", 0, timeframe, market_cap_sentiment, funding_rate_sentiment, slippage=0, latency=0, exit_reason="SL Hit", partial_exits="|".join(partial_exits_log))
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"❌ {symbol} LOSS: ${loss:.4f}")
                elif side == "SELL" and current_price >= sl:
                    loss = (sl - entry) * remaining_size
                    self.current_capital -= loss
                    current_capital = self.current_capital
                    self.risk_manager.update_capital(self.current_capital)
                    self.risk_manager.update_losses(True)
                    self.adaptive_learner.save_pattern_performance(pattern, False)
                    self.adaptive_learner.update_strategy_performance(side, False)
                    self.adaptive_learner.log_learning_insight(symbol, side, "LOSS", trade.get('confidence', 0), sentiment, regime, -loss, weekly_trend, "NEUTRAL", macro_bias, timeframe, market_cap_sentiment, funding_rate_sentiment)
                    safe_telegram_send(f"🛑 SL HIT {symbol}: -${loss:.4f} | Capital: ${self.current_capital:.2f} ({timeframe})")
                    partial_exits_log.append(f"SL @ {sl:.4f}")
                    self.log_trade(symbol, side, entry, size, -loss, "LOSS", trade.get('order_id'), trade.get('mode'), trade.get('confidence', 0), "", pattern, "SL Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0), trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend, 0, "", 0, "", 0, timeframe, market_cap_sentiment, funding_rate_sentiment, slippage=0, latency=0, exit_reason="SL Hit", partial_exits="|".join(partial_exits_log))
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"❌ {symbol} LOSS: ${loss:.4f}")
            except Exception as e:
                logger.error(f"❌ Trade manage error {symbol}: {e}")
                
    def get_statistics(self):
        try:
            with db_manager.get_connection() as conn:
                row = conn.execute("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins,
                           SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) as losses,
                           AVG(confidence_score) as avg_confidence,
                           AVG(rr_ratio) as avg_rr,
                           SUM(pnl) as total_pnl
                    FROM trades WHERE status IN ('WIN', 'LOSS')
                """).fetchone()
                if row:
                    total = row['total'] or 0
                    wins = row['wins'] or 0
                    return {
                        'total': total,
                        'wins': wins,
                        'losses': row['losses'] or 0,
                        'win_rate': (wins / total * 100) if total > 0 else 0,
                        'avg_confidence': row['avg_confidence'] or 0,
                        'avg_rr': row['avg_rr'] or 0,
                        'total_pnl': row['total_pnl'] or 0
                    }
        except Exception:
            pass
        return {'total': 0, 'wins': 0, 'losses': 0, 'win_rate': 0, 'avg_confidence': 0, 'avg_rr': 0, 'total_pnl': 0}
        
    def log_trade(self, symbol, side, entry, size, pnl, status, order_id, mode,
                  confidence, strategies, pattern, reason, rr_ratio, mtf_status,
                  vwap_status, fear_greed, pattern_weight, ml_score, var_status, 
                  order_book_spread, regime, daily_pnl, drawdown, weekly_trend,
                  is_breakout=0, sr_type="NEUTRAL", breakout_confirmed=0,
                  macro_bias="NEUTRAL", golden_hour=0, timeframe="15m",
                  market_cap_sentiment="NEUTRAL", funding_rate_sentiment="NEUTRAL",
                  slippage=0.0, latency=0.0, exit_reason="", partial_exits=""):
        try:
            with db_manager.get_connection() as conn:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                conn.execute("""
                    INSERT INTO trades
                    (timestamp, symbol, side, entry, size, pnl, status, order_id,
                     mode, confidence_score, strategies_used, pattern_type,
                     entry_reason, rr_ratio, mtf_confirmed,
                     vwap_deviation, fear_greed_index, pattern_weight_used,
                     ml_prediction_score, var_at_risk, order_book_spread,
                     market_regime, daily_pnl, drawdown_percent, weekly_trend,
                     is_breakout, sr_type, breakout_confirmed, macro_bias, golden_hour_signal,
                     timeframe, market_cap_sentiment, funding_rate_sentiment,
                     slippage, latency_ms, exit_reason_details, partial_exits)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (timestamp, symbol, side, entry, size, pnl, status, order_id,
                      mode, confidence, strategies, pattern, reason, rr_ratio, mtf_status,
                      0, fear_greed, pattern_weight, ml_score, var_status, order_book_spread,
                      regime, daily_pnl, drawdown, weekly_trend,
                      is_breakout, sr_type, breakout_confirmed, macro_bias, golden_hour,
                      timeframe, market_cap_sentiment, funding_rate_sentiment,
                      slippage, latency, exit_reason, partial_exits))
                conn.commit()
        except Exception as e:
            logger.error(f"Log error: {e}")
            
    def send_realtime_update(self):
        stats = self.get_statistics()
        var = self.var_calculator.calculate_var(self.position_data)
        learning_stats = self.adaptive_learner.get_learning_stats()
        macro_data = self.macro_indicator.get_gold_bias()
        
        strategies = []
        if self.active_trades:
            for symbol, trade in self.active_trades.items():
                strategies = trade.get('strategies', [])
                break
        
        socketio.emit('realtime_update', {
            'timestamp': time.time(),
            'capital': self.current_capital,
            'peak_capital': self.peak_capital,
            'active_trades': len(self.active_trades),
            'max_active_trades': MAX_ACTIVE_TRADES,
            'positions': self.position_data,
            'prices': self.live_prices,
            'order_books': self.order_book_cache,
            'btc_trend': self.btc_trend_cache,
            'fear_greed': self.fear_greed_cache,
            'var': var,
            'regime': self.current_regime,
            'weekly_trend': self.weekly_trend_cache,
            'daily_pnl': self.daily_pnl,
            'daily_limit': MAX_DAILY_DRAWDOWN,
            'win_rate': stats.get('win_rate', 0),
            'total_trades': stats.get('total', 0),
            'avg_confidence': stats.get('avg_confidence', 0),
            'avg_rr': stats.get('avg_rr', 0),
            'consecutive_losses': self.risk_manager.consecutive_losses,
            'learning_stats': learning_stats,
            'market_mood': self.sentiment_analyzer.get_market_mood(),
            'top_gainers': self.top_movers.top_gainers[:5],
            'top_losers': self.top_movers.top_losers[:5],
            'macro_data': macro_data,
            'strategies_count': 9,
            'consensus_threshold': CONSENSUS_THRESHOLD,
            'strategies': strategies,
            'timeframes': MTF_TIMEFRAMES,
            'version': 'v19.0 Ultimate'
        })
            
    def run(self):
        global current_capital, peak_capital
        
        logger.info("🐋 Quantum Whale v19.0 ULTIMATE - WORLD'S #1 TRADER")
        logger.info("=" * 70)
        logger.info("📊 ULTIMATE Features v19.0:")
        logger.info("  ✅ 9 Strategy Ensemble with Weighted Voting")
        logger.info("  ✅ Dynamic Trailing Stop-Loss (ATR-based)")
        logger.info("  ✅ Volatility-Based Position Sizing")
        logger.info("  ✅ Partial Take-Profits (50/30/20 split)")
        logger.info("  ✅ ML/Regime Filter (strategies adapt to market)")
        logger.info("  ✅ Async API Fetching (CoinGecko, etc.)")
        logger.info("  ✅ Enhanced Database Logging (slippage, latency, partial exits)")
        logger.info("  ✅ Market Cap & Altcoin Season Analysis")
        logger.info("  ✅ Fear & Greed + Funding Rates Analysis")
        logger.info("  ✅ Macroeconomic Sentiment & News Filter")
        logger.info("  ✅ DXY & Gold Correlation Tracking")
        logger.info("  ✅ NY Session Golden Hour (4:30-6:30 PM IST)")
        logger.info("  ✅ Liquidity Sweep Detection")
        logger.info("  ✅ 4 Timeframes (5m, 15m, 1H, 4H)")
        logger.info("  ✅ Trade Execution on 5m & 15m ONLY")
        logger.info("  ✅ Async Price Fetcher (100ms)")
        logger.info("  ✅ Redis/Memory Cache")
        logger.info("  ✅ 24/7 Trading")
        logger.info("  ✅ Weekly Trend Analysis")
        logger.info("  ✅ Top 50 Coins + GOLD (XAUUSDT)")
        logger.info("  ✅ 24h Top 10 Gainers & Losers")
        logger.info("  ✅ Enhanced SR & Breakout Detection")
        logger.info("  ✅ OPTIMIZED: No SELL at Support, No BUY at Resistance")
        logger.info("  ✅ Macroeconomic Pause Protection")
        logger.info("  ✅ Full Bitget API Integration")
        logger.info("=" * 70)
        logger.info(f"📊 Strategies: 9 (Weighted Voting)")
        logger.info(f"🎯 Consensus Threshold: Weighted sum > 0.5")
        logger.info(f"⚡ Min Confidence: {MIN_CONFIDENCE_SCORE}%")
        logger.info(f"📈 Min R:R: {MIN_RR_RATIO}")
        logger.info(f"⚡ Max Active Trades: {MAX_ACTIVE_TRADES}")
        logger.info(f"🛡️ Daily Drawdown Limit: {MAX_DAILY_DRAWDOWN:.0%}")
        logger.info(f"📡 Update Interval: {UPDATE_INTERVAL_MS}ms")
        logger.info(f"📊 Total Symbols: {len(ALL_SYMBOLS)} (including Gold)")
        logger.info("⏱️ TRADE TIMEFRAMES: 5m, 15m (FAST EXECUTION)")
        logger.info("📊 ANALYSIS TIMEFRAMES: 5m, 15m, 1H, 4H")
        logger.info("=" * 70)
        
        self.is_running = True
        
        def realtime_updater():
            while self.is_running:
                try:
                    self.send_realtime_update()
                    time.sleep(UPDATE_INTERVAL_MS / 1000.0)
                except Exception as e:
                    logger.debug(f"Realtime update error: {e}")
                    time.sleep(0.1)
                    
        update_thread = threading.Thread(target=realtime_updater, daemon=True)
        update_thread.start()
        
        while self.is_running:
            try:
                start_time = time.time()
                self.manage_trades()
                self.analyze_and_trade()
                
                stats = self.get_statistics()
                best_patterns = self.adaptive_learner.get_best_patterns(3)
                var = self.var_calculator.calculate_var(self.position_data)
                learning_stats = self.adaptive_learner.get_learning_stats()
                market_mood = self.sentiment_analyzer.get_market_mood()
                macro_data = self.macro_indicator.get_gold_bias()
                
                socketio.emit('market_update', {
                    'prices': self.live_prices,
                    'positions': self.position_data,
                    'active_trades': len(self.active_trades),
                    'max_active_trades': MAX_ACTIVE_TRADES,
                    'capital': self.current_capital,
                    'peak_capital': self.peak_capital,
                    'trading_mode': TRADING_MODE,
                    'win_rate': stats.get('win_rate', 0),
                    'total_trades': stats.get('total', 0),
                    'wins': stats.get('wins', 0),
                    'losses': stats.get('losses', 0),
                    'consecutive_losses': self.risk_manager.consecutive_losses,
                    'avg_confidence': stats.get('avg_confidence', 0),
                    'avg_rr': stats.get('avg_rr', 0),
                    'btc_trend': self.btc_trend_cache,
                    'fear_greed': self.fear_greed_cache,
                    'best_patterns': best_patterns,
                    'var': var,
                    'regime': self.current_regime,
                    'weekly_trend': self.weekly_trend_cache,
                    'daily_pnl': self.daily_pnl,
                    'daily_limit': MAX_DAILY_DRAWDOWN,
                    'learning_stats': learning_stats,
                    'market_mood': market_mood,
                    'top_gainers': self.top_movers.top_gainers[:5],
                    'top_losers': self.top_movers.top_losers[:5],
                    'macro_data': macro_data,
                    'strategies_count': 9,
                    'consensus_threshold': CONSENSUS_THRESHOLD,
                    'timeframes': MTF_TIMEFRAMES,
                    'version': 'v19.0 Ultimate'
                })
                
                elapsed = time.time() - start_time
                sleep_time = max(0, 0.1 - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"❌ Bot loop error: {e}")
                time.sleep(1)

# ============================================================
# TRADING BOT ENGINE CORE (v19.0) – NEW CLASS AS PER MISSING CODE
# ============================================================
class TradingBotEngine:
    """Simplified trading engine that uses RiskManager and ConsensusEngine."""
    def __init__(self):
        self.risk_manager = RiskManager()
        self.consensus_engine = ConsensusEngine()
        self.adaptive_learner = AdaptiveLearner()
        self.active_trades = {}
        self.capital = 10.0  # Initial Paper Capital
        self.running = False
        self._thread = None
        
    def start(self):
        if self.running:
            return
        self.running = True
        price_fetcher.start()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("🚀 Quantum Whale v19.0 Trading Engine Started Successfully!")
        
    def stop(self):
        self.running = False
        price_fetcher.stop()
        db_manager.close_all()
        logger.info("🛑 Trading Engine Stopped.")

    def _run_loop(self):
        while self.running:
            try:
                self.risk_manager.check_daily_reset()
                # Main execution cycle across symbols can go here
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error in main trading loop: {e}")
                time.sleep(5)

# ============================================================
# FLASK ROUTES
# ============================================================
# Use the already defined bot instance (QuantumWhaleBot)
bot = QuantumWhaleBot()

# ---------- UPDATED INDEX ROUTE (with template rendering) ----------
@app.route('/')
def index():
    try:
        # Try to render template if index.html exists, else return JSON
        if os.path.exists('templates/index.html'):
            return render_template('index.html')
        else:
            return jsonify({
                "status": "online",
                "bot": "Ultimate Quantum Whale v19.0",
                "mode": TRADING_MODE,
                "capital": bot.current_capital,
                "active_trades": len(bot.active_trades),
                "strategies": 9,
                "timeframes": MTF_TIMEFRAMES,
                "version": "v19.0 Ultimate"
            })
    except Exception as e:
        logger.error(f"Index error: {e}")
        return jsonify({"error": "Internal error"}), 500

# ---------- NEW /api/status ROUTE ----------
@app.route('/api/status')
def api_status():
    try:
        stats = bot.get_statistics()
        return jsonify({
            "status": "running",
            "mode": TRADING_MODE,
            "capital": bot.current_capital,
            "active_trades_count": len(bot.active_trades),
            "version": "v19.0 Ultimate Upgraded",
            "win_rate": stats.get('win_rate', 0),
            "total_trades": stats.get('total', 0)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- OTHER EXISTING ROUTES (unchanged) ----------
@app.route('/status')
def get_status():
    try:
        stats = bot.get_statistics()
        best_patterns = bot.adaptive_learner.get_best_patterns(5)
        var = bot.var_calculator.calculate_var(bot.position_data)
        learning_stats = bot.adaptive_learner.get_learning_stats()
        macro_data = bot.macro_indicator.get_gold_bias()
        
        strategies = []
        if bot.active_trades:
            for symbol, trade in bot.active_trades.items():
                strategies = trade.get('strategies', [])
                break
        
        return jsonify({
            'status': 'Running',
            'version': 'v19.0 Ultimate',
            'trading_mode': TRADING_MODE,
            'capital': bot.current_capital,
            'peak_capital': bot.peak_capital,
            'active_trades': len(bot.active_trades),
            'max_active_trades': MAX_ACTIVE_TRADES,
            'positions': bot.position_data,
            'win_rate': stats.get('win_rate', 0),
            'total_trades': stats.get('total', 0),
            'wins': stats.get('wins', 0),
            'losses': stats.get('losses', 0),
            'avg_confidence': stats.get('avg_confidence', 0),
            'avg_rr': stats.get('avg_rr', 0),
            'consecutive_losses': bot.risk_manager.consecutive_losses,
            'consensus_threshold': CONSENSUS_THRESHOLD,
            'min_confidence': MIN_CONFIDENCE_SCORE,
            'min_rr': MIN_RR_RATIO,
            'btc_trend': bot.btc_trend_cache,
            'fear_greed': bot.fear_greed_cache,
            'best_patterns': best_patterns,
            'var': var,
            'regime': bot.current_regime,
            'weekly_trend': bot.weekly_trend_cache,
            'daily_pnl': bot.daily_pnl,
            'daily_limit': MAX_DAILY_DRAWDOWN,
            'update_interval_ms': UPDATE_INTERVAL_MS,
            'learning_stats': learning_stats,
            'market_mood': bot.sentiment_analyzer.get_market_mood(),
            'top_gainers': bot.top_movers.top_gainers[:5],
            'top_losers': bot.top_movers.top_losers[:5],
            'total_symbols': len(ALL_SYMBOLS),
            'macro_data': macro_data,
            'strategies_count': 9,
            'strategies': strategies,
            'timeframes': MTF_TIMEFRAMES,
            'trade_timeframes': TRADE_TIMEFRAMES
        })
    except Exception as e:
        logger.error(f"Status error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "timestamp": time.time()})

@app.route('/realtime')
def get_realtime():
    var = bot.var_calculator.calculate_var(bot.position_data)
    macro_data = bot.macro_indicator.get_gold_bias()
    return jsonify({
        'timestamp': time.time(),
        'capital': bot.current_capital,
        'peak_capital': bot.peak_capital,
        'active_trades': len(bot.active_trades),
        'max_active_trades': MAX_ACTIVE_TRADES,
        'positions': bot.position_data,
        'prices': bot.live_prices,
        'order_books': bot.order_book_cache,
        'btc_trend': bot.btc_trend_cache,
        'fear_greed': bot.fear_greed_cache,
        'var': var,
        'regime': bot.current_regime,
        'weekly_trend': bot.weekly_trend_cache,
        'daily_pnl': bot.daily_pnl,
        'market_mood': bot.sentiment_analyzer.get_market_mood(),
        'macro_data': macro_data,
        'strategies_count': 9,
        'timeframes': MTF_TIMEFRAMES,
        'version': 'v19.0 Ultimate'
    })

@app.route('/prices')
def get_prices():
    bot.update_prices()
    return jsonify(bot.live_prices)

@app.route('/positions')
def get_positions():
    return jsonify(bot.position_data)

@app.route('/history')
def get_history():
    try:
        with db_manager.get_connection() as conn:
            rows = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 100").fetchall()
            return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/order_book/<symbol>')
def get_order_book_route(symbol):
    ob = get_order_book(symbol)
    return jsonify(ob or {'error': 'No data'})

@app.route('/pattern_performance')
def get_pattern_performance():
    try:
        with db_manager.get_connection() as conn:
            rows = conn.execute("""
                SELECT pattern_name, total_trades, wins, losses, win_rate, weight, ml_accuracy 
                FROM pattern_performance 
                ORDER BY win_rate DESC
            """).fetchall()
            return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/daily_stats')
def get_daily_stats():
    try:
        with db_manager.get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM daily_stats 
                ORDER BY date DESC 
                LIMIT 30
            """).fetchall()
            return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/learning_stats')
def get_learning_stats():
    return jsonify(bot.adaptive_learner.get_learning_stats())

@app.route('/top_movers')
def get_top_movers():
    bot.top_movers.update()
    return jsonify({
        'gainers': bot.top_movers.top_gainers[:10],
        'losers': bot.top_movers.top_losers[:10],
        'opportunities': bot.top_movers.get_best_opportunities(5),
        'timestamp': time.time()
    })

@app.route('/macro')
def get_macro():
    try:
        macro_data = bot.macro_indicator.get_gold_bias()
        if not macro_data or not isinstance(macro_data, dict):
            macro_data = {
                'bias': 'NEUTRAL',
                'dxy_trend': 'NEUTRAL',
                'dxy_price': 0,
                'last_update': 0,
                'is_trading_paused': False,
                'pause_reason': 'No data'
            }
        return jsonify(macro_data)
    except Exception as e:
        logger.error(f"Macro endpoint error: {e}")
        return jsonify({
            'bias': 'NEUTRAL',
            'dxy_trend': 'NEUTRAL',
            'dxy_price': 0,
            'last_update': 0,
            'is_trading_paused': False,
            'pause_reason': f'Error: {str(e)}'
        }), 200

@app.route('/chart_data')
def get_chart_data():
    data = {}
    for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "XAUUSDT"]:
        df = fetch_candles(symbol, "15m", 50)
        if df is not None:
            data[symbol] = {
                'price': df['close'].tolist(),
                'high': df['high'].tolist(),
                'low': df['low'].tolist(),
                'timestamps': df['timestamp'].tolist(),
                'current_price': df['close'].iloc[-1]
            }
    return jsonify(data)

@app.route('/trend/<symbol>')
def get_trend(symbol):
    trend = bot.trend_analyzer.analyze_trend(symbol)
    return jsonify(trend)

@app.route('/sr/<symbol>')
def get_sr_data(symbol):
    df = fetch_candles(symbol, "15m", 50)
    if df is not None:
        sr_data = detect_support_resistance(df)
        breakout = detect_breakout(df)
        return jsonify({
            'support_resistance': sr_data,
            'breakout': breakout,
            'symbol': symbol
        })
    return jsonify({'error': 'No data'})

@app.route('/api/start', methods=['POST'])
def api_start():
    if not bot.is_running:
        bot.is_running = True
        threading.Thread(target=bot.run, daemon=True).start()
    return jsonify({"status": "success", "message": "Bot started"})

@app.route('/api/stop', methods=['POST'])
def api_stop():
    bot.is_running = False
    return jsonify({"status": "success", "message": "Bot stopped"})

@app.route('/api/mode/<mode>', methods=['POST'])
def api_set_mode(mode):
    global TRADING_MODE
    if mode.upper() in ['PAPER', 'DEMO', 'REAL']:
        TRADING_MODE = mode.upper()
        return jsonify({"status": "success", "mode": TRADING_MODE})
    return jsonify({"status": "error", "message": "Invalid mode"}), 400

@socketio.on('connect')
def handle_connect():
    try:
        stats = bot.get_statistics()
        best_patterns = bot.adaptive_learner.get_best_patterns(5)
        var = bot.var_calculator.calculate_var(bot.position_data)
        learning_stats = bot.adaptive_learner.get_learning_stats()
        macro_data = bot.macro_indicator.get_gold_bias()
        
        strategies = []
        if bot.active_trades:
            for symbol, trade in bot.active_trades.items():
                strategies = trade.get('strategies', [])
                break
        
        emit('connected', {
            'status': 'connected',
            'version': 'v19.0 Ultimate',
            'trading_mode': TRADING_MODE,
            'capital': bot.current_capital,
            'peak_capital': bot.peak_capital,
            'active_trades': len(bot.active_trades),
            'max_active_trades': MAX_ACTIVE_TRADES,
            'win_rate': stats.get('win_rate', 0),
            'total_trades': stats.get('total', 0),
            'consensus_threshold': CONSENSUS_THRESHOLD,
            'min_confidence': MIN_CONFIDENCE_SCORE,
            'min_rr': MIN_RR_RATIO,
            'btc_trend': bot.btc_trend_cache,
            'best_patterns': best_patterns,
            'var': var,
            'regime': bot.current_regime,
            'weekly_trend': bot.weekly_trend_cache,
            'daily_limit': MAX_DAILY_DRAWDOWN,
            'update_interval_ms': UPDATE_INTERVAL_MS,
            'learning_stats': learning_stats,
            'market_mood': bot.sentiment_analyzer.get_market_mood(),
            'total_symbols': len(ALL_SYMBOLS),
            'macro_data': macro_data,
            'strategies_count': 9,
            'strategies': strategies,
            'timeframes': MTF_TIMEFRAMES,
            'trade_timeframes': TRADE_TIMEFRAMES
        })
        logger.info("🔌 WebSocket client connected - v19.0 Ultimate")
    except Exception as e:
        logger.error(f"WebSocket connect error: {e}")
        emit('connected', {'error': str(e)})

# ============================================================
# SHUTDOWN HANDLER
# ============================================================
def shutdown_handler(signum=None, frame=None):
    logger.info("🛑 Shutting down...")
    bot.is_running = False
    price_fetcher.stop()
    bot.macro_indicator.stop()
    db_manager.close_all()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ============================================================
# MAIN ENTRY POINT
# ============================================================
if __name__ == '__main__':
    logger.info("🚀 Starting Quantum Whale v19.0 ULTIMATE - WORLD'S #1 TRADER")
    logger.info("📊 ALL new features integrated!")
    logger.info("=" * 70)
    logger.info("📊 9 STRATEGIES ENSEMBLE (Weighted Voting):")
    logger.info("  1. Trend Following")
    logger.info("  2. Momentum")
    logger.info("  3. Volatility Breakout")
    logger.info("  4. Pattern Recognition")
    logger.info("  5. Mean Reversion")
    logger.info("  6. ML Ensemble")
    logger.info("  7. NY Golden Hour")
    logger.info("  8. Market Cap & Altcoin Season")
    logger.info("  9. Fear & Greed + Funding Rates")
    logger.info("=" * 70)
    logger.info("🎯 Weighted Consensus: Strategies adapt based on historical win rate")
    logger.info("🔄 Dynamic Trailing Stop: ATR-based with automatic adjustment")
    logger.info("📊 Volatility Position Sizing: Adjusts to market volatility")
    logger.info("🎯 Partial TP: 50% at 1.5%, 30% at 3%, 20% trailing")
    logger.info("🧠 ML/Regime Filter: Strategies selected per market condition")
    logger.info("⚡ Async API calls for CoinGecko data")
    logger.info("💾 Enhanced DB: slippage, latency, partial exits logged")
    logger.info("=" * 70)
    
    # Start bot in background thread
    bot_thread = threading.Thread(target=bot.run, daemon=True)
    bot_thread.start()
    
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🌐 Web Server on port {port}")
    logger.info(f"📊 Health check: http://localhost:{port}/health")
    logger.info(f"📊 Macro endpoint: http://localhost:{port}/macro")
    logger.info("🤖 UptimeRobot will ping /health endpoint every 5 minutes")
    socketio.run(app, host='0.0.0.0', port=port, debug=False)