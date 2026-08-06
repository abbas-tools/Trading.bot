# ============================================================
# ULTIMATE QUANTUM WHALE v18.0 - WORLD'S #1 TRADER
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
from flask import Flask, render_template, jsonify, request
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
import aiohttp  # ✅ FIXED: was 'aiothttp'
import redis
from cachetools import TTLCache

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================
BITGET_API_KEY = os.environ.get('BITGET_API_KEY', '')
BITGET_API_SECRET = os.environ.get('BITGET_API_SECRET', '')
BITGET_PASSPHRASE = os.environ.get('BITGET_PASSPHRASE', '')
TRADING_MODE = os.environ.get('TRADING_MODE', 'PAPER').upper()
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

ALL_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", 
               "DOGEUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT",
               "LTCUSDT", "BCHUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
               "INJUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "WIFUSDT", "PEPEUSDT"]

MAIN_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]

# ============================================================
# v18 TRADING CONSTANTS - WORLD'S #1 TRADER
# ============================================================
MAX_ACTIVE_TRADES = 3
MAX_RISK_PER_TRADE = 0.03
MAX_DAILY_DRAWDOWN = 0.08
MIN_RR_RATIO = 2.0
MAX_RR_RATIO = 6.0

# ✅ MULTI-TIMEFRAME WITH WEEKLY TREND
MTF_TIMEFRAMES = ["15m", "1H", "4H", "1D", "1W"]
MTF_CONFIRMATION_THRESHOLD = 0.60

# ✅ ENHANCED REGIME DETECTION
REGIME_LOOKBACK = 100
TRENDING_THRESHOLD = 20

# ✅ CIRCUIT BREAKER
DAILY_LOSS_LIMIT = 0.08
DAILY_RESET_HOUR = 0

# ✅ PROFESSIONAL FEATURES
VWAP_PERIOD = 20
VOLUME_PROFILE_LEVELS = 10
MIN_VOLUME_DENSITY = 0.3
TRAILING_ACTIVATION = 1.2
TRAILING_STEP = 0.3
REQUIRE_BTC_ALIGNMENT = False
BTC_SYMBOL = "BTCUSDT"
FEAR_GREED_THRESHOLD = 20
CONSENSUS_THRESHOLD = 3
MIN_CONFIDENCE_SCORE = 70
SIGNAL_COOLDOWN = 120
UPDATE_INTERVAL_MS = 100
ORDER_BOOK_DEPTH = 10
MIN_BID_ASK_SPREAD = 0.001
ML_PREDICTION_WEIGHT = 0.20
ML_MIN_CONFIDENCE = 55
VAR_CONFIDENCE_LEVEL = 0.95
MAX_DRAWDOWN_LIMIT = 0.25

# ✅ FIXED MISSING VARIABLES
MIN_TRADES_FOR_WIN_RATE = 5
MIN_WIN_RATE = 35.0
MIN_LOT_SIZE = 0.001
LOT_SIZE_STEP = 0.001
MIN_ATR_PERCENT = 0.05
MAX_ATR_PERCENT = 15.0
MIN_VOLUME_MULTIPLIER = 0.3
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

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
# DATABASE (v18 ULTIMATE)
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
            weekly_trend TEXT, daily_trend TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, side TEXT, entry REAL,
            stop_loss REAL, take_profit REAL, confidence_score REAL,
            strategies_agreed TEXT, pattern_type TEXT, consensus_count INTEGER,
            rr_ratio REAL, mtf_status TEXT, btc_status TEXT,
            fear_greed_index INTEGER, vwap_status TEXT,
            ml_prediction TEXT, var_status TEXT, market_regime TEXT,
            weekly_trend TEXT, daily_trend TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS pattern_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_name TEXT UNIQUE, total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0, weight REAL DEFAULT 1.0,
            last_updated TEXT, ml_accuracy REAL DEFAULT 0
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
            weekly_trend TEXT, daily_trend TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS price_cache (
            symbol TEXT PRIMARY KEY,
            price REAL, timestamp REAL,
            change_24h REAL, volume REAL
        )''')
        conn.commit()
    logger.info("✅ Database initialized with v18 Ultimate features")

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
        return None
    url = f"https://api.bitget.com{endpoint}"
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
            response = requests.post(url, json=body, headers=headers, timeout=5)
        else:
            response = requests.get(url, params=params, headers=headers, timeout=5)
        return response.json()
    except Exception as e:
        logger.error(f"❌ Bitget API error: {e}")
        return None

# ============================================================
# ASYNC PRICE FETCHER
# ============================================================
class AsyncPriceFetcher:
    def __init__(self):
        self.prices = {}
        self.last_update = 0
        self.update_interval = 0.1
        self._running = False
        self._thread = None
        
    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("🚀 Async price fetcher started (100ms updates)")
        
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
                        self.prices[symbol] = result
                except Exception as e:
                    logger.debug(f"Price fetch error {symbol}: {e}")
        self.last_update = time.time()
        
    def _fetch_single_price(self, symbol):
        try:
            url = f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={symbol}&productType=usdt-futures"
            response = requests.get(url, timeout=1)
            data = response.json()
            if data.get('code') == '00000':
                ticker = data.get('data', {})
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
        except Exception as e:
            pass
        return None
        
    def get_price(self, symbol):
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
        url = f"https://api.bitget.com/api/v2/mix/market/candles?symbol={symbol}&productType=usdt-futures&granularity={granularity}&limit={limit}"
        response = requests.get(url, timeout=5)
        data = response.json()
        if data.get('code') == '00000':
            candles = data.get('data', [])
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
        url = f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={symbol}&productType=usdt-futures"
        response = requests.get(url, timeout=3)
        data = response.json()
        if data.get('code') == '00000':
            ticker = data.get('data', {})
            price = float(ticker.get('price', 0))
            if price > 0:
                return {
                    'price': price,
                    'change_24h': float(ticker.get('change24h', 0)),
                    'volume': float(ticker.get('volume', 0)),
                    'high': float(ticker.get('high', 0)),
                    'low': float(ticker.get('low', 0))
                }
    except Exception as e:
        logger.debug(f"Price error {symbol}: {e}")
    return None

def get_bitget_balance():
    try:
        if TRADING_MODE == "PAPER":
            return {"available": current_capital, "total": current_capital, "equity": current_capital, "mode": "PAPER", "pnl": 0, "pnl_percent": 0}
        response = send_bitget_request('GET', "/api/v2/mix/account/accounts")
        if response and response.get('code') == '00000':
            account = response.get('data', [])[0]
            total = float(account.get('total', 0))
            available = float(account.get('available', 0))
            unrealized_pnl = float(account.get('unrealizedPnl', 0))
            return {"available": available, "total": total, "equity": total + unrealized_pnl, "mode": TRADING_MODE, "pnl": unrealized_pnl, "pnl_percent": (unrealized_pnl / total) * 100 if total > 0 else 0}
    except Exception as e:
        logger.debug(f"Balance fetch error: {e}")
    return {"available": current_capital, "total": current_capital, "equity": current_capital, "mode": "PAPER", "pnl": 0, "pnl_percent": 0}

# ============================================================
# ORDER BOOK FUNCTIONS
# ============================================================
def get_order_book(symbol, depth=ORDER_BOOK_DEPTH):
    try:
        url = f"https://api.bitget.com/api/v2/mix/market/orderbook?symbol={symbol}&productType=usdt-futures&limit={depth}"
        response = requests.get(url, timeout=5)
        data = response.json()
        if data.get('code') == '00000':
            orderbook = data.get('data', {})
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
    except Exception as e:
        logger.debug(f"Order book error {symbol}: {e}")
    return None

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
                
        return True, f"Order book confirmed: {ob['best_bid']:.4f}/{ob['best_ask']:.4f}", ob
        
    except Exception as e:
        logger.debug(f"Order book confirmation error {symbol}: {e}")
    return True, "Order book check skipped", {}

# ============================================================
# v18 INDICATOR FUNCTIONS
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
    except Exception as e:
        logger.debug(f"RSI error: {e}")
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
    except Exception as e:
        logger.debug(f"ATR error: {e}")
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
    except Exception as e:
        logger.debug(f"MACD error: {e}")
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
    except Exception as e:
        logger.debug(f"BB error: {e}")
    return None

def calculate_vwap(df, period=VWAP_PERIOD):
    try:
        if df is None or len(df) < period:
            return None
        df_copy = df.copy()
        df_copy['typical'] = (df_copy['high'] + df_copy['low'] + df_copy['close']) / 3
        df_copy['vwap'] = (df_copy['typical'] * df_copy['volume']).rolling(window=period).sum() / df_copy['volume'].rolling(window=period).sum()
        return df_copy['vwap'].iloc[-1]
    except Exception as e:
        logger.debug(f"VWAP error: {e}")
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
    except Exception as e:
        logger.debug(f"ADX error: {e}")
    return 0

def calculate_ema(df, period):
    try:
        return df['close'].ewm(span=period, adjust=False).mean().iloc[-1]
    except:
        return df['close'].iloc[-1]

# ============================================================
# VOLUME PROFILE
# ============================================================
def calculate_volume_profile(df, levels=VOLUME_PROFILE_LEVELS):
    try:
        if df is None or len(df) < 20:
            return None
        high = df['high'].max()
        low = df['low'].min()
        price_range = high - low
        if price_range <= 0:
            return None
            
        bin_size = price_range / levels
        volume_bins = {}
        
        for i in range(len(df)):
            price = df['close'].iloc[i]
            volume = df['volume'].iloc[i]
            bin_idx = int((price - low) / bin_size)
            bin_idx = min(bin_idx, levels - 1)
            key = low + (bin_idx + 0.5) * bin_size
            volume_bins[key] = volume_bins.get(key, 0) + volume
            
        if not volume_bins:
            return None
            
        poc_price = max(volume_bins, key=volume_bins.get)
        total_volume = sum(volume_bins.values())
        density = volume_bins.get(poc_price, 0) / total_volume if total_volume > 0 else 0
        
        return {
            'poc': poc_price,
            'density': density,
            'levels': volume_bins
        }
    except Exception as e:
        logger.debug(f"Volume Profile error: {e}")
    return None

def check_smart_money_confirmation(df, current_price, side):
    try:
        vwap = calculate_vwap(df)
        vp = calculate_volume_profile(df)
        
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
    except Exception as e:
        logger.debug(f"Smart Money check error: {e}")
    return True, "Smart Money check skipped"

# ============================================================
# FEAR & GREED / BTC TREND
# ============================================================
def get_fear_greed_index():
    try:
        response = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and len(data['data']) > 0:
                value = int(data['data'][0].get('value', 50))
                classification = data['data'][0].get('classification', 'Neutral')
                return {
                    'value': value,
                    'classification': classification,
                    'timestamp': time.time()
                }
    except Exception as e:
        logger.debug(f"Fear & Greed error: {e}")
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
    except Exception as e:
        logger.debug(f"BTC trend error: {e}")
    return "NEUTRAL"

def check_global_sentiment(side: str) -> Tuple[bool, str, Dict]:
    try:
        btc_trend = get_btc_trend()
        fng = get_fear_greed_index()
        
        result = {
            'btc_trend': btc_trend,
            'fear_greed': fng,
            'aligned': True
        }
        
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
    except Exception as e:
        logger.debug(f"Global sentiment error: {e}")
    return True, "Sentiment check skipped", {'btc_trend': 'NEUTRAL', 'fear_greed': {'value': 50}}

# ============================================================
# v18 ULTIMATE MARKET REGIME DETECTION
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
        except Exception as e:
            logger.debug(f"Regime detection error: {e}")
        return {'regime': 'NEUTRAL', 'confidence': 0}
        
    def get_strategy_for_regime(self, regime: str) -> str:
        if regime in ["STRONG_UPTREND", "UPTREND", "STRONG_DOWNTREND", "DOWNTREND"]:
            return "TREND_FOLLOWING"
        elif regime == "RANGING":
            return "MEAN_REVERSION"
        elif regime == "VOLATILE":
            return "BREAKOUT"
        else:
            return "CONSERVATIVE"

# ============================================================
# v18 ULTIMATE TREND ANALYZER
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
                '15m': 0.10,
                '1H': 0.20,
                '4H': 0.25,
                '1D': 0.30,
                '1W': 0.15
            }
            
            bullish_score = 0
            bearish_score = 0
            total_weight = 0
            
            for tf, trend in trends.items():
                weight = weights.get(tf, 0.15)
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
# ADAPTIVE LEARNER
# ============================================================
class AdaptiveLearner:
    def __init__(self):
        self.pattern_weights = {}
        self.learning_history = []
        self.last_learning_time = 0
        self.load_pattern_weights()
        
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
        except Exception as e:
            logger.debug(f"Load pattern weights error: {e}")
            self.pattern_weights = {}
            
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
                
        except Exception as e:
            logger.error(f"Save pattern performance error: {e}")
            
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
        except Exception as e:
            logger.debug(f"Get best patterns error: {e}")
        return []
    
    def log_learning_insight(self, symbol: str, action: str, outcome: str, 
                             confidence: float, sentiment: str, regime: str, pnl: float = 0,
                             weekly_trend: str = "NEUTRAL", daily_trend: str = "NEUTRAL"):
        try:
            with db_manager.get_connection() as conn:
                conn.execute("""
                    INSERT INTO learning_logs (timestamp, symbol, action, outcome, confidence, sentiment, regime, pnl, weekly_trend, daily_trend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (time.strftime('%Y-%m-%d %H:%M:%S'), symbol, action, outcome, confidence, sentiment, regime, pnl, weekly_trend, daily_trend))
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
                'daily_trend': daily_trend
            })
            
            if len(self.learning_history) > 1000:
                self.learning_history = self.learning_history[-1000:]
                
            logger.info(f"🧠 Learning insight: {symbol} {action} → {outcome}")
            
        except Exception as e:
            logger.debug(f"Learning log error: {e}")
    
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
        except Exception as e:
            logger.debug(f"Learning stats error: {e}")
        return {'total': 0, 'wins': 0, 'losses': 0, 'win_rate': 0, 'avg_confidence': 0, 'avg_pnl': 0}

def calculate_dynamic_rr(confidence: float, atr_percent: float, market_volatility: float) -> float:
    try:
        base_rr = 2.0 + (confidence - 70) / 20
        if atr_percent > 3.0:
            base_rr *= 1.2
        elif atr_percent < 1.0:
            base_rr *= 0.9
        return max(MIN_RR_RATIO, min(MAX_RR_RATIO, base_rr))
    except Exception as e:
        logger.debug(f"Dynamic RR error: {e}")
    return MIN_RR_RATIO

# ============================================================
# v18 PATTERN DETECTION
# ============================================================
def detect_liquidity_sweep(df):
    try:
        if df is None or len(df) < 20:
            return "NONE"
        last_candle = df.iloc[-1]
        prev_high = df['high'].iloc[-20:-1].max()
        prev_low = df['low'].iloc[-20:-1].min()
        if last_candle['low'] < prev_low and last_candle['close'] > prev_low:
            return "BULLISH_SWEEP"
        elif last_candle['high'] > prev_high and last_candle['close'] < prev_high:
            return "BEARISH_SWEEP"
    except Exception:
        pass
    return "NONE"

def smart_trend_predictor(df):
    try:
        if df is None or len(df) < 20:
            return "NEUTRAL"
        close_values = df['close'].values
        sma_fast = np.mean(close_values[-5:])
        sma_slow = np.mean(close_values[-20:])
        if sma_fast > sma_slow * 1.002:
            return "BULLISH"
        elif sma_fast < sma_slow * 0.998:
            return "BEARISH"
    except Exception:
        pass
    return "NEUTRAL"

def detect_candlestick_patterns(df):
    try:
        if df is None or len(df) < 5:
            return "NEUTRAL_CANDLE"
            
        c1, c2 = df.iloc[-1], df.iloc[-2]
        body1 = abs(c1['close'] - c1['open'])
        range1 = c1['high'] - c1['low']
        
        if range1 > 0 and body1 / range1 < 0.1:
            return "DOJI"
            
        if c2['close'] > c2['open'] and c1['open'] > c2['close'] and c1['close'] < c2['open']:
            if (c1['high'] - c1['low']) > (c2['high'] - c2['low']) * 1.1:
                return "BEARISH_ENGULFING"
                
        if c2['close'] < c2['open'] and c1['open'] < c2['close'] and c1['close'] > c2['open']:
            if (c1['high'] - c1['low']) > (c2['high'] - c2['low']) * 1.1:
                return "BULLISH_ENGULFING"
                
        if c1['close'] > c1['open']:
            lower_wick = c1['open'] - c1['low']
            if lower_wick > body1 * 2 and lower_wick > (c1['high'] - c1['close']):
                return "HAMMER"
        else:
            upper_wick = c1['high'] - c1['open']
            if upper_wick > body1 * 2 and upper_wick > (c1['close'] - c1['low']):
                return "SHOOTING_STAR"
                
        return "STANDARD_MOVE"
    except Exception as e:
        logger.debug(f"Pattern detection error: {e}")
    return "STANDARD_MOVE"

# ============================================================
# v18 STRATEGIES (6 STRATEGIES)
# ============================================================
class TrendFollowingStrategy:
    def __init__(self):
        self.name = "Trend Following"
        self.weight = 0.25
        
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
            
            if current > ema_20 and ema_20 > ema_50 and ema_50 > ema_200 and adx > 20:
                signal = 'BUY'
                confidence = min(90, 60 + adx * 0.5)
            elif current < ema_20 and ema_20 < ema_50 and ema_50 < ema_200 and adx > 20:
                signal = 'SELL'
                confidence = min(90, 60 + adx * 0.5)
            else:
                signal = 'NEUTRAL'
                confidence = 30
                
            return {
                'signal': signal,
                'confidence': confidence,
                'details': f"EMA20:{ema_20:.2f} EMA50:{ema_50:.2f} ADX:{adx:.1f}"
            }
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class MomentumStrategy:
    def __init__(self):
        self.name = "Momentum"
        self.weight = 0.20
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 30:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
                
            rsi = calculate_rsi(df)
            macd_data = calculate_macd(df)
            
            rsi_signal = 'NEUTRAL'
            rsi_confidence = 40
            
            if rsi < RSI_OVERSOLD:
                rsi_signal = 'BUY'
                rsi_confidence = 65 + (RSI_OVERSOLD - rsi) * 1.2
            elif rsi > RSI_OVERBOUGHT:
                rsi_signal = 'SELL'
                rsi_confidence = 65 + (rsi - RSI_OVERBOUGHT) * 1.2
                
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
                
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': f"RSI:{rsi:.1f} MACD:{macd_data['trend'] if macd_data else 'N/A'}"
            }
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class VolatilityBreakoutStrategy:
    def __init__(self):
        self.name = "Volatility Breakout"
        self.weight = 0.20
        
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
            
            signal = 'NEUTRAL'
            confidence = 30
            
            if current_price > bb['upper'] and volume_ratio > 1.5:
                signal = 'BUY'
                confidence = 65 + min(25, atr_percent * 3)
            elif current_price < bb['lower'] and volume_ratio > 1.5:
                signal = 'SELL'
                confidence = 65 + min(25, atr_percent * 3)
            elif atr_percent > 2.0 and volume_ratio > 2.0:
                if current_price > df['close'].iloc[-2]:
                    signal = 'BUY'
                    confidence = 60
                else:
                    signal = 'SELL'
                    confidence = 60
                    
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': f"ATR:{atr_percent:.1f}% Vol:{volume_ratio:.1f}x BB:{bb_position:.0%}"
            }
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

class PatternRecognitionStrategy:
    def __init__(self):
        self.name = "Pattern Recognition"
        self.weight = 0.15
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 20:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
                
            current_price = df['close'].iloc[-1]
            pattern = detect_candlestick_patterns(df)
            sweep = detect_liquidity_sweep(df)
            
            recent_high = df['high'].iloc[-20:].max()
            recent_low = df['low'].iloc[-20:].min()
            
            confidence = 30
            signal = 'NEUTRAL'
            final_pattern = pattern
            
            if pattern in ["BULLISH_ENGULFING", "HAMMER"]:
                confidence = 70
                signal = 'BUY'
            elif pattern in ["BEARISH_ENGULFING", "SHOOTING_STAR"]:
                confidence = 70
                signal = 'SELL'
                
            if current_price <= recent_low * 1.01:
                if signal == 'BUY':
                    confidence += 15
                elif signal == 'NEUTRAL':
                    signal = 'BUY'
                    confidence = 60
                final_pattern = "SUPPORT_BOUNCE"
                
            if current_price >= recent_high * 0.99:
                if signal == 'SELL':
                    confidence += 15
                elif signal == 'NEUTRAL':
                    signal = 'SELL'
                    confidence = 60
                final_pattern = "RESISTANCE_REJECT"
                
            if sweep == "BULLISH_SWEEP" and signal == 'BUY':
                confidence += 10
            elif sweep == "BEARISH_SWEEP" and signal == 'SELL':
                confidence += 10
                
            return {
                'signal': signal,
                'confidence': min(90, confidence),
                'details': f"Pattern:{final_pattern} Sweep:{sweep}"
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
            vwap = calculate_vwap(df)
            
            if bb is None:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'BB error'}
                
            bb_range = bb['upper'] - bb['lower']
            if bb_range <= 0:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'BB range zero'}
                
            bb_position = (current - bb['lower']) / bb_range
            
            if bb_position < 0.1 and rsi < 35:
                signal = 'BUY'
                confidence = 70 + (35 - rsi) * 0.5
            elif bb_position > 0.9 and rsi > 65:
                signal = 'SELL'
                confidence = 70 + (rsi - 65) * 0.5
            else:
                signal = 'NEUTRAL'
                confidence = 30
                
            return {
                'signal': signal,
                'confidence': min(90, confidence),
                'details': f"BB:{bb_position:.0%} RSI:{rsi:.1f}"
            }
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
                scores['BUY'] += 20
            elif rsi > 70:
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
                elif sell_prob > 0.6:
                    signal = 'SELL'
                    confidence = 60 + (sell_prob - 0.6) * 100
                else:
                    signal = 'NEUTRAL'
                    confidence = 40
            else:
                signal = 'NEUTRAL'
                confidence = 30
                
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': f"ML Ensemble: BUY:{scores['BUY']} SELL:{scores['SELL']}"
            }
        except Exception as e:
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

# ============================================================
# v18 ULTIMATE CONSENSUS ENGINE
# ============================================================
class ConsensusEngine:
    def __init__(self):
        self.strategies = [
            TrendFollowingStrategy(),
            MomentumStrategy(),
            VolatilityBreakoutStrategy(),
            PatternRecognitionStrategy(),
            MeanReversionStrategy(),
            MLPredictionStrategy()
        ]
        self.consensus_threshold = CONSENSUS_THRESHOLD
        self.min_confidence = MIN_CONFIDENCE_SCORE
        self.regime_detector = MarketRegimeDetector()
        self.trend_analyzer = UltimateTrendAnalyzer()
        self.adaptive_learner = AdaptiveLearner()
        
    def analyze(self, df: pd.DataFrame, symbol: str, positions: Dict) -> Dict:
        results = []
        for strategy in self.strategies:
            result = strategy.analyze(df)
            results.append({
                'name': strategy.name,
                'signal': result['signal'],
                'confidence': result['confidence'],
                'details': result['details']
            })
            
        regime_result = self.regime_detector.detect_regime(df)
        current_regime = regime_result['regime']
        
        trend_result = self.trend_analyzer.analyze_trend(symbol)
        weekly_trend = trend_result.get('overall', 'NEUTRAL')
        trend_strength = trend_result.get('strength', 50) / 100
        
        buy_count = sum(1 for r in results if r['signal'] == 'BUY' and r['confidence'] >= self.min_confidence * 0.7)
        sell_count = sum(1 for r in results if r['signal'] == 'SELL' and r['confidence'] >= self.min_confidence * 0.7)
        neutral_count = len(results) - buy_count - sell_count
        
        buy_confidence = sum(r['confidence'] for r in results if r['signal'] == 'BUY') / max(1, buy_count)
        sell_confidence = sum(r['confidence'] for r in results if r['signal'] == 'SELL') / max(1, sell_count)
        
        if weekly_trend in ['STRONG_BULLISH', 'BULLISH']:
            buy_confidence *= 1.1
        elif weekly_trend in ['STRONG_BEARISH', 'BEARISH']:
            sell_confidence *= 1.1
            
        final_signal = 'NEUTRAL'
        final_confidence = 0
        reason = ""
        
        if buy_count >= self.consensus_threshold:
            final_signal = 'BUY'
            final_confidence = min(95, buy_confidence + 10)
            if weekly_trend in ['STRONG_BULLISH', 'BULLISH']:
                final_confidence = min(95, final_confidence * 1.05)
            reason = f"{buy_count}/6 strategies agree on BUY (Weekly: {weekly_trend}, Regime: {current_regime})"
            
        elif sell_count >= self.consensus_threshold:
            final_signal = 'SELL'
            final_confidence = min(95, sell_confidence + 10)
            if weekly_trend in ['STRONG_BEARISH', 'BEARISH']:
                final_confidence = min(95, final_confidence * 1.05)
            reason = f"{sell_count}/6 strategies agree on SELL (Weekly: {weekly_trend}, Regime: {current_regime})"
        else:
            reason = f"No consensus ({buy_count} BUY, {sell_count} SELL, {neutral_count} NEUTRAL)"
            
        return {
            'signal': final_signal,
            'confidence': final_confidence,
            'strategies': results,
            'regime_result': regime_result,
            'trend_result': trend_result,
            'consensus': {
                'buy_count': buy_count,
                'sell_count': sell_count,
                'neutral_count': neutral_count,
                'threshold': self.consensus_threshold,
                'reason': reason
            }
        }

# ============================================================
# v18 ULTIMATE MULTI-TIMEFRAME ANALYZER
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
# v18 ULTIMATE RISK MANAGER
# ============================================================
class RiskManager:
    def __init__(self):
        self.consecutive_losses = 0
        self.peak_capital = 10.0
        self.max_risk_per_trade = MAX_RISK_PER_TRADE
        self.max_drawdown_limit = MAX_DRAWDOWN_LIMIT
        self.daily_pnl = 0
        self.daily_trades = 0
        self.daily_wins = 0
        self.daily_losses = 0
        self.daily_start = 10.0
        self.last_reset_date = datetime.now().date()
        self.is_breached = False
        self.var_calculator = None
        
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
            
    def check_circuit_breaker(self, current_capital: float) -> Tuple[bool, str]:
        self.reset_daily(current_capital)
        
        if self.daily_start > 0:
            daily_drawdown = (self.daily_start - current_capital) / self.daily_start
            if daily_drawdown > MAX_DAILY_DRAWDOWN:
                self.is_breached = True
                return False, f"Daily drawdown {daily_drawdown:.1%} exceeds {MAX_DAILY_DRAWDOWN:.0%} limit"
                
        if self.daily_losses >= 3 and self.daily_wins == 0:
            self.is_breached = True
            return False, "3 consecutive losses without a win"
            
        return True, "OK"
        
    def update_daily(self, pnl: float, is_win: bool):
        self.daily_pnl += pnl
        self.daily_trades += 1
        if is_win:
            self.daily_wins += 1
        else:
            self.daily_losses += 1
            
    def calculate_position_size(self, capital: float, confidence: float, 
                                atr: float, current_price: float, 
                                rr_ratio: float, trend_strength: float = 1.0) -> float:
        try:
            if capital <= 0 or current_price <= 0 or atr is None or atr <= 0:
                return MIN_LOT_SIZE
                
            base_risk = self.max_risk_per_trade
            
            if self.consecutive_losses >= 2:
                base_risk *= (1 - self.consecutive_losses * 0.1)
                
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
            
    def update_losses(self, is_loss: bool):
        if is_loss:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
            
    def check_drawdown(self, current_capital: float) -> Tuple[bool, float]:
        if self.peak_capital > 0:
            drawdown = (self.peak_capital - current_capital) / self.peak_capital
            if drawdown > self.max_drawdown_limit:
                return False, drawdown
            return True, drawdown
        return True, 0
        
    def check_trading_allowed(self, current_capital: float) -> Tuple[bool, str]:
        self.reset_daily(current_capital)
        cb_ok, cb_reason = self.check_circuit_breaker(current_capital)
        if not cb_ok:
            return False, f"Circuit Breaker: {cb_reason}"
        dd_ok, dd = self.check_drawdown(current_capital)
        if not dd_ok:
            return False, f"Drawdown {dd:.1%} exceeds {self.max_drawdown_limit:.0%} limit"
        return True, "Trading allowed"

# ============================================================
# V17.1 CIRCUIT BREAKER (PRESERVED)
# ============================================================
class CircuitBreaker:
    def __init__(self):
        self.daily_starting_capital = 10.0
        self.daily_pnl = 0
        self.daily_trades = 0
        self.daily_wins = 0
        self.daily_losses = 0
        self.is_breached = False
        self.breach_reason = ""
        self.last_reset_date = datetime.now().date()
        
    def reset_daily(self, current_capital: float):
        today = datetime.now().date()
        if today != self.last_reset_date:
            self.daily_starting_capital = current_capital
            self.daily_pnl = 0
            self.daily_trades = 0
            self.daily_wins = 0
            self.daily_losses = 0
            self.is_breached = False
            self.breach_reason = ""
            self.last_reset_date = today
            logger.info(f"📅 Daily reset: Starting capital ${current_capital:.2f}")
            
    def check_circuit_breaker(self, current_capital: float) -> Tuple[bool, str]:
        if self.daily_starting_capital > 0:
            daily_drawdown = (self.daily_starting_capital - current_capital) / self.daily_starting_capital
            if daily_drawdown > MAX_DAILY_DRAWDOWN:
                self.is_breached = True
                self.breach_reason = f"Daily drawdown {daily_drawdown:.1%} exceeds {MAX_DAILY_DRAWDOWN:.0%} limit"
                return False, self.breach_reason
        if self.daily_losses >= 3 and self.daily_wins == 0:
            self.is_breached = True
            self.breach_reason = "3 consecutive losses without a win"
            return False, self.breach_reason
        return True, "OK"
        
    def update_daily(self, pnl: float, is_win: bool):
        self.daily_pnl += pnl
        self.daily_trades += 1
        if is_win:
            self.daily_wins += 1
        else:
            self.daily_losses += 1

# ============================================================
# ML PREDICTOR (PRESERVED)
# ============================================================
class MLPredictor:
    def __init__(self):
        self.prediction_history = []
        self.accuracy = 0.5
        
    def predict(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 50:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
                
            close = df['close'].values
            current_price = close[-1]
            
            scores = []
            
            if len(close) >= 10:
                roc = (close[-1] - close[-5]) / close[-5] * 100 if close[-5] > 0 else 0
                if roc > 2:
                    scores.append(('Momentum', 'BUY', 70))
                elif roc < -2:
                    scores.append(('Momentum', 'SELL', 70))
                else:
                    scores.append(('Momentum', 'NEUTRAL', 40))
                    
            atr = calculate_atr(df)
            if atr and current_price > 0:
                atr_pct = (atr / current_price) * 100
                if atr_pct > 3:
                    scores.append(('Volatility', 'BUY' if close[-1] > close[-2] else 'SELL', 65))
                else:
                    scores.append(('Volatility', 'NEUTRAL', 35))
                    
            avg_vol = np.mean(df['volume'].values[-20:])
            current_vol = df['volume'].values[-1]
            if avg_vol > 0 and current_vol > avg_vol * 2:
                scores.append(('Volume', 'BUY' if close[-1] > close[-2] else 'SELL', 70))
            else:
                scores.append(('Volume', 'NEUTRAL', 30))
                
            rsi = calculate_rsi(df)
            if rsi < 30:
                scores.append(('RSI', 'BUY', 75))
            elif rsi > 70:
                scores.append(('RSI', 'SELL', 75))
            else:
                scores.append(('RSI', 'NEUTRAL', 40))
                
            buy_score = sum(s[2] for s in scores if s[1] == 'BUY')
            sell_score = sum(s[2] for s in scores if s[1] == 'SELL')
            total_score = buy_score + sell_score
            
            if total_score > 0:
                buy_prob = buy_score / total_score
                sell_prob = sell_score / total_score
                
                if buy_prob > 0.6:
                    signal = 'BUY'
                    confidence = 60 + (buy_prob - 0.6) * 100
                elif sell_prob > 0.6:
                    signal = 'SELL'
                    confidence = 60 + (sell_prob - 0.6) * 100
                else:
                    signal = 'NEUTRAL'
                    confidence = 40
            else:
                signal = 'NEUTRAL'
                confidence = 30
                
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': f"ML Ensemble: {len([s for s in scores if s[1] != 'NEUTRAL'])} active indicators"
            }
        except Exception as e:
            logger.debug(f"ML prediction error: {e}")
        return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'ML error'}

# ============================================================
# VAR CALCULATOR (PRESERVED)
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
                
            return {
                'var_95': var_95,
                'var_99': var_99,
                'status': status,
                'exposure': total_exposure
            }
        except Exception as e:
            logger.debug(f"VaR calculation error: {e}")
        return {'var_95': 0.01, 'var_99': 0.02, 'status': 'LOW', 'exposure': 0}
        
    def update_returns(self, pnl: float, capital: float):
        if capital > 0:
            return_pct = pnl / capital
            self.returns_history.append(return_pct)
            if len(self.returns_history) > 500:
                self.returns_history.pop(0)

# ============================================================
# ENHANCED SENTIMENT ANALYZER (PRESERVED)
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
            
        except Exception as e:
            logger.debug(f"Sentiment analyzer error: {e}")
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
# TOP MOVERS TRACKER (PRESERVED)
# ============================================================
class TopMoversTracker:
    def __init__(self):
        self.top_gainers = []
        self.top_losers = []
        self.last_update = 0
        self.update_interval = 300
        
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
                    
                    movers.append({
                        'symbol': symbol,
                        'change_24h': change_24h,
                        'current_price': current_price,
                        'volume_ratio': vol_ratio,
                        'atr_pct': atr_pct,
                        'momentum_score': abs(change_24h) * 0.5 + vol_ratio * 0.3 + atr_pct * 0.2
                    })
                except Exception as e:
                    logger.debug(f"Top mover error {symbol}: {e}")
                    continue
            
            self.top_gainers = sorted(movers, key=lambda x: x['change_24h'], reverse=True)[:10]
            self.top_losers = sorted(movers, key=lambda x: x['change_24h'])[:10]
            self.last_update = time.time()
            
            logger.info(f"📊 Top Gainers: {[g['symbol'] for g in self.top_gainers[:5]]}")
            logger.info(f"📉 Top Losers: {[l['symbol'] for l in self.top_losers[:5]]}")
            
        except Exception as e:
            logger.error(f"Top movers error: {e}")
            
    def get_top_gainer_symbols(self, count=5):
        return [g['symbol'] for g in self.top_gainers[:count]]
        
    def get_top_loser_symbols(self, count=5):
        return [l['symbol'] for l in self.top_losers[:count]]
        
    def get_best_opportunities(self, count=5):
        opportunities = []
        for g in self.top_gainers[:10]:
            if g['volume_ratio'] > 1.5 and g['atr_pct'] > 1.0:
                opportunities.append({
                    'symbol': g['symbol'],
                    'score': g['momentum_score'],
                    'type': 'GAINER',
                    'change': g['change_24h']
                })
        
        for l in self.top_losers[:10]:
            if l['volume_ratio'] > 2.0 and l['atr_pct'] > 1.5:
                opportunities.append({
                    'symbol': l['symbol'],
                    'score': -l['momentum_score'] * 0.8,
                    'type': 'REVERSAL',
                    'change': l['change_24h']
                })
        
        opportunities.sort(key=lambda x: x['score'], reverse=True)
        return opportunities[:count]

# ============================================================
# NEWS FILTER (DISABLED - PRESERVED)
# ============================================================
class NewsFilter:
    def __init__(self):
        self.high_impact_events = []
        self.last_check = 0
        
    def check_high_impact_news(self) -> Tuple[bool, str]:
        return False, "News filter disabled - Trading allowed 24/7"

# ============================================================
# v18 ULTIMATE BOT
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
        self.risk_manager = RiskManager()
        self.regime_detector = MarketRegimeDetector()
        self.trend_analyzer = UltimateTrendAnalyzer()
        self.adaptive_learner = AdaptiveLearner()
        self.var_calculator = VaRCalculator()
        self.circuit_breaker = CircuitBreaker()
        self.top_movers = TopMoversTracker()
        self.sentiment_analyzer = EnhancedSentimentAnalyzer()
        self.ml_predictor = MLPredictor()
        self.news_filter = NewsFilter()
        
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
            except Exception as e:
                logger.debug(f"Order book error {symbol}: {e}")
                
    def get_candles(self, symbol: str, timeframe: str = "1H", limit: int = 100) -> Optional[pd.DataFrame]:
        cache_key = f"{symbol}_{timeframe}"
        if cache_key in self.candle_cache:
            cached = self.candle_cache[cache_key]
            if time.time() - cached['timestamp'] < 30:
                return cached['data']
                
        df = fetch_candles(symbol, timeframe, limit)
        if df is not None and len(df) > 0:
            self.candle_cache[cache_key] = {
                'data': df,
                'timestamp': time.time()
            }
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
        if TRADING_MODE == "PAPER":
            order_id = f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}"
            return True, order_id, "PAPER"
            
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
                               regime: str, sentiment: str, weekly_trend: str) -> str:
        
        strategy_details = "\n".join([
            f"  • {s['name']}: {s['signal']} ({s['confidence']:.0f}%) - {s['details']}"
            for s in strategies
        ])
        
        active_note = f"{len(self.active_trades)}/{MAX_ACTIVE_TRADES} (MAX 3)"
        
        return f"""
🐋 *QUANTUM WHALE v18.0 ULTIMATE*

📊 *{symbol}*
🔹 Action: `{side}`
🔹 Confidence: `{confidence:.0f}%`
🔹 Pattern: `{pattern}`
🔹 R:R Ratio: `1 : {rr_ratio:.1f}`
🔹 Weekly Trend: `{weekly_trend}`
🔹 Market Regime: `{regime}`
🔹 Market Sentiment: `{sentiment}`

📈 Entry: `${entry:.4f}`
🎯 Take Profit: `${tp:.4f}`
🛑 Stop Loss: `${sl:.4f}`
📊 Position: `{size:.4f}`

🧠 *STRATEGY CONSENSUS (6 STRATEGIES):*
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
                df = self.get_candles(symbol, "1H", 50)
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
                
                score = (
                    volume_ratio * 0.2 +
                    atr_pct * 0.15 +
                    trend_strength * 0.3 +
                    abs(df['close'].pct_change().iloc[-1]) * 0.15 +
                    (1 - abs(df['close'].iloc[-1] - df['close'].iloc[-5]) / current) * 0.2
                )
                
                scores[symbol] = score
                
            except Exception as e:
                logger.debug(f"Scoring error {symbol}: {e}")
                
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
            
        if self.circuit_breaker.daily_losses >= 3 and self.circuit_breaker.daily_wins == 0:
            return
            
        # Update top movers
        self.top_movers.update()
        opportunities = self.top_movers.get_best_opportunities(5)
        
        # Score coins
        coin_scores = self._score_coins()
        top_coins = sorted(coin_scores.items(), key=lambda x: x[1], reverse=True)[:15]
        
        # BTC trend and regime
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
        
        for symbol, score in top_coins[:10]:
            try:
                if time.time() - self.last_signal_time[symbol] < self.signal_cooldown:
                    continue
                    
                if symbol in self.active_trades:
                    continue
                    
                df = self.get_candles(symbol, "1H", 100)
                if df is None or len(df) < 30:
                    continue
                    
                current_price = df['close'].iloc[-1]
                if current_price <= 0 or pd.isna(current_price):
                    continue
                    
                atr = calculate_atr(df)
                if atr is None or atr <= 0:
                    continue
                    
                atr_percent = (atr / current_price) * 100
                if atr_percent < MIN_ATR_PERCENT or atr_percent > MAX_ATR_PERCENT:
                    continue
                    
                avg_volume = df['volume'].iloc[-20:].mean()
                current_volume = df['volume'].iloc[-1]
                if current_volume < avg_volume * MIN_VOLUME_MULTIPLIER:
                    continue
                    
                # Consensus analysis
                consensus = self.consensus_engine.analyze(df, symbol, self.position_data)
                
                if consensus['signal'] == 'NEUTRAL':
                    continue
                    
                if consensus['confidence'] < MIN_CONFIDENCE_SCORE:
                    continue
                    
                # Order Book Check
                ob_ok, ob_status, ob_data = check_order_book_confirmation(symbol, consensus['signal'])
                if not ob_ok:
                    continue
                    
                # Smart Money
                vwap_ok, vwap_status = check_smart_money_confirmation(df, current_price, consensus['signal'])
                if not vwap_ok:
                    continue
                    
                # Global Sentiment
                sentiment_ok, sentiment_status, sentiment_data = check_global_sentiment(consensus['signal'])
                if not sentiment_ok:
                    continue
                    
                # MTF confirmation
                mtf_ok, mtf_reason, mtf_trends = self.mtf_analyzer.analyze(symbol, consensus['signal'])
                if not mtf_ok:
                    continue
                    
                pattern = "STANDARD_MOVE"
                for s in consensus['strategies']:
                    if "Pattern:" in s['details']:
                        pattern = s['details'].replace("Pattern: ", "").split(",")[0]
                        break
                        
                pattern_weight = self.adaptive_learner.get_pattern_weight(pattern)
                
                # Dynamic R:R
                rr_ratio = calculate_dynamic_rr(consensus['confidence'], atr_percent, 1.0)
                
                # SL/TP
                sl, tp = self.calculate_dynamic_sl_tp(
                    current_price, atr, consensus['signal'], consensus['confidence'], rr_ratio
                )
                
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
                    
                # Position size
                var_status = 'LOW'
                regime = self.current_regime
                weekly_trend = self.weekly_trend_cache
                trend_strength = 1.0
                
                size = self.risk_manager.calculate_position_size(
                    self.current_capital, consensus['confidence'],
                    atr, current_price, actual_rr, trend_strength
                )
                
                if size < MIN_LOT_SIZE:
                    continue
                    
                # Check opposing signals
                has_strong_opposing = False
                for s in consensus['strategies']:
                    if s['signal'] == 'SELL' and consensus['signal'] == 'BUY' and s['confidence'] > 70:
                        has_strong_opposing = True
                        break
                    elif s['signal'] == 'BUY' and consensus['signal'] == 'SELL' and s['confidence'] > 70:
                        has_strong_opposing = True
                        break
                        
                if has_strong_opposing:
                    continue
                    
                self.last_signal_time[symbol] = time.time()
                
                ml_status = f"ML {consensus.get('ml_result', {}).get('signal', 'N/A')}"
                sentiment = consensus.get('regime_result', {}).get('regime', 'NEUTRAL')
                
                signal_text = self.generate_signal_message(
                    symbol, consensus['signal'], consensus['confidence'],
                    consensus['strategies'], consensus['consensus'],
                    pattern, current_price, sl, tp, size,
                    actual_rr, mtf_reason, sentiment_status, vwap_status,
                    ml_status, var_status, regime, sentiment, weekly_trend
                )
                
                safe_telegram_send(signal_text)
                logger.info(f"📤 SIGNAL: {symbol} {consensus['signal']} @ {current_price:.4f}")
                
                if len(self.active_trades) < MAX_ACTIVE_TRADES:
                    success, order_id, mode = self.execute_order(
                        symbol, consensus['signal'], size
                    )
                    
                    if success:
                        self.active_trades[symbol] = {
                            'side': consensus['signal'],
                            'entry': current_price,
                            'sl': sl,
                            'tp': tp,
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
                            'weekly_trend': weekly_trend
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
                            'weekly_trend': weekly_trend
                        }
                        
                        self.log_trade(
                            symbol, consensus['signal'], current_price, size,
                            0, "OPEN", order_id, mode, consensus['confidence'],
                            json.dumps([s['name'] for s in consensus['strategies'] if s['signal'] != 'NEUTRAL']),
                            pattern, consensus['consensus']['reason'],
                            actual_rr, mtf_reason,
                            sentiment_status, fng['value'], pattern_weight,
                            consensus.get('ml_result', {}).get('confidence', 0) if consensus.get('ml_result') else 0,
                            var_status, ob_data.get('spread', 0) if ob_data else 0,
                            regime, self.daily_pnl, 0, weekly_trend
                        )
                        
                        self.adaptive_learner.log_learning_insight(
                            symbol, consensus['signal'], "OPEN",
                            consensus['confidence'], sentiment, regime, 0, weekly_trend
                        )
                        
                        logger.info(f"✅ EXECUTED: {symbol} {consensus['signal']} @ {current_price:.4f}")
                        
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
                
                if side == "BUY":
                    pnl = (current_price - entry) * size
                    pnl_percent = ((current_price - entry) / entry) * 100
                else:
                    pnl = (entry - current_price) * size
                    pnl_percent = ((entry - current_price) / entry) * 100
                    
                self.position_data[symbol] = {
                    'side': side,
                    'entry': entry,
                    'current': current_price,
                    'pnl': pnl,
                    'pnl_percent': pnl_percent,
                    'size': size,
                    'sl': sl,
                    'tp': tp,
                    'mode': trade.get('mode', 'PAPER'),
                    'confidence': trade.get('confidence', 0),
                    'rr_ratio': rr_ratio,
                    'var_status': trade.get('var_status', 'LOW'),
                    'regime': regime,
                    'sentiment': sentiment,
                    'weekly_trend': weekly_trend
                }
                
                self.current_capital = 10.0 + sum([
                    self.position_data.get(s, {}).get('pnl', 0)
                    for s in self.active_trades.keys()
                ])
                
                current_capital = self.current_capital
                
                if self.current_capital > self.peak_capital:
                    self.peak_capital = self.current_capital
                    peak_capital = self.peak_capital
                    
                self.daily_pnl = self.current_capital - 10.0
                self.var_calculator.update_returns(pnl, self.current_capital)
                
                # Trailing Stop
                if side == "BUY" and (current_price - entry) > (atr * TRAILING_ACTIVATION):
                    new_sl = current_price - (atr * TRAILING_STEP)
                    if new_sl > sl:
                        trade['sl'] = sl = new_sl
                        
                elif side == "SELL" and (entry - current_price) > (atr * TRAILING_ACTIVATION):
                    new_sl = current_price + (atr * TRAILING_STEP)
                    if new_sl < sl:
                        trade['sl'] = sl = new_sl
                        
                # TP Check
                if side == "BUY" and current_price >= tp:
                    profit = (tp - entry) * size
                    self.current_capital += profit
                    current_capital = self.current_capital
                    self.risk_manager.update_losses(False)
                    self.circuit_breaker.update_daily(profit, True)
                    self.adaptive_learner.save_pattern_performance(pattern, True)
                    self.adaptive_learner.log_learning_insight(symbol, side, "WIN", trade.get('confidence', 0), sentiment, regime, profit, weekly_trend)
                    safe_telegram_send(f"🎯 TP HIT {symbol}: +${profit:.4f} | Capital: ${self.current_capital:.2f}")
                    self.log_trade(symbol, side, entry, size, profit, "WIN", 
                                  trade.get('order_id'), trade.get('mode'), 
                                  trade.get('confidence', 0), "", pattern,
                                  "TP Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0),
                                  trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend)
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"✅ {symbol} WIN: ${profit:.4f}")
                    
                elif side == "SELL" and current_price <= tp:
                    profit = (entry - tp) * size
                    self.current_capital += profit
                    current_capital = self.current_capital
                    self.risk_manager.update_losses(False)
                    self.circuit_breaker.update_daily(profit, True)
                    self.adaptive_learner.save_pattern_performance(pattern, True)
                    self.adaptive_learner.log_learning_insight(symbol, side, "WIN", trade.get('confidence', 0), sentiment, regime, profit, weekly_trend)
                    safe_telegram_send(f"🎯 TP HIT {symbol}: +${profit:.4f} | Capital: ${self.current_capital:.2f}")
                    self.log_trade(symbol, side, entry, size, profit, "WIN",
                                  trade.get('order_id'), trade.get('mode'),
                                  trade.get('confidence', 0), "", pattern,
                                  "TP Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0),
                                  trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend)
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"✅ {symbol} WIN: ${profit:.4f}")
                    
                # SL Check
                elif side == "BUY" and current_price <= sl:
                    loss = (entry - sl) * size
                    self.current_capital -= loss
                    current_capital = self.current_capital
                    self.risk_manager.update_losses(True)
                    self.circuit_breaker.update_daily(-loss, False)
                    self.adaptive_learner.save_pattern_performance(pattern, False)
                    self.adaptive_learner.log_learning_insight(symbol, side, "LOSS", trade.get('confidence', 0), sentiment, regime, -loss, weekly_trend)
                    safe_telegram_send(f"🛑 SL HIT {symbol}: -${loss:.4f} | Capital: ${self.current_capital:.2f}")
                    self.log_trade(symbol, side, entry, size, -loss, "LOSS",
                                  trade.get('order_id'), trade.get('mode'),
                                  trade.get('confidence', 0), "", pattern,
                                  "SL Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0),
                                  trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend)
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"❌ {symbol} LOSS: ${loss:.4f}")
                    
                elif side == "SELL" and current_price >= sl:
                    loss = (sl - entry) * size
                    self.current_capital -= loss
                    current_capital = self.current_capital
                    self.risk_manager.update_losses(True)
                    self.circuit_breaker.update_daily(-loss, False)
                    self.adaptive_learner.save_pattern_performance(pattern, False)
                    self.adaptive_learner.log_learning_insight(symbol, side, "LOSS", trade.get('confidence', 0), sentiment, regime, -loss, weekly_trend)
                    safe_telegram_send(f"🛑 SL HIT {symbol}: -${loss:.4f} | Capital: ${self.current_capital:.2f}")
                    self.log_trade(symbol, side, entry, size, -loss, "LOSS",
                                  trade.get('order_id'), trade.get('mode'),
                                  trade.get('confidence', 0), "", pattern,
                                  "SL Hit", rr_ratio, "", "", 0, trade.get('pattern_weight', 1.0),
                                  trade.get('var_status', 'LOW'), 0, regime, self.daily_pnl, 0, weekly_trend)
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
        except Exception as e:
            logger.error(f"Statistics error: {e}")
        return {'total': 0, 'wins': 0, 'losses': 0, 'win_rate': 0, 'avg_confidence': 0, 'avg_rr': 0, 'total_pnl': 0}
        
    def log_trade(self, symbol, side, entry, size, pnl, status, order_id, mode,
                  confidence, strategies, pattern, reason, rr_ratio, mtf_status,
                  vwap_status, fear_greed, pattern_weight, ml_score, var_status, 
                  order_book_spread, regime, daily_pnl, drawdown, weekly_trend):
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
                     market_regime, daily_pnl, drawdown_percent, weekly_trend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (timestamp, symbol, side, entry, size, pnl, status, order_id,
                      mode, confidence, strategies, pattern, reason, rr_ratio, mtf_status,
                      0, fear_greed, pattern_weight, ml_score, var_status, order_book_spread,
                      regime, daily_pnl, drawdown, weekly_trend))
                conn.commit()
        except Exception as e:
            logger.error(f"Log error: {e}")
            
    def send_realtime_update(self):
        stats = self.get_statistics()
        var = self.var_calculator.calculate_var(self.position_data)
        learning_stats = self.adaptive_learner.get_learning_stats()
        
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
            'market_mood': self.sentiment_analyzer.get_market_mood()
        })
            
    def run(self):
        global current_capital, peak_capital
        
        logger.info("🐋 Quantum Whale v18.0 - WORLD'S #1 TRADER")
        logger.info("=" * 70)
        logger.info("📊 ULTIMATE Features:")
        logger.info("  ✅ 6 Strategy Ensemble")
        logger.info("  ✅ 5 Timeframes (15m, 1H, 4H, 1D, 1W)")
        logger.info("  ✅ Async Price Fetcher (100ms)")
        logger.info("  ✅ Redis/Memory Cache")
        logger.info("  ✅ Concurrent Analysis")
        logger.info("  ✅ 24/7 Trading")
        logger.info("  ✅ Weekly Trend Analysis")
        logger.info("=" * 70)
        logger.info(f"📊 Strategies: {len(self.consensus_engine.strategies)}")
        logger.info(f"🎯 Consensus Threshold: {CONSENSUS_THRESHOLD}/{len(self.consensus_engine.strategies)}")
        logger.info(f"⚡ Min Confidence: {MIN_CONFIDENCE_SCORE}%")
        logger.info(f"📈 Min R:R: {MIN_RR_RATIO}")
        logger.info(f"⚡ Max Active Trades: {MAX_ACTIVE_TRADES}")
        logger.info(f"🛡️ Daily Drawdown Limit: {MAX_DAILY_DRAWDOWN:.0%}")
        logger.info(f"📡 Update Interval: {UPDATE_INTERVAL_MS}ms")
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
                    'top_losers': self.top_movers.top_losers[:5]
                })
                
                elapsed = time.time() - start_time
                sleep_time = max(0, 0.1 - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"❌ Bot loop error: {e}")
                time.sleep(1)

# ============================================================
# FLASK ROUTES
# ============================================================
bot = QuantumWhaleBot()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/status')
def get_status():
    stats = bot.get_statistics()
    best_patterns = bot.adaptive_learner.get_best_patterns(5)
    var = bot.var_calculator.calculate_var(bot.position_data)
    learning_stats = bot.adaptive_learner.get_learning_stats()
    return jsonify({
        'status': 'Running',
        'version': 'v18.0 Ultimate',
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
        'top_losers': bot.top_movers.top_losers[:5]
    })

@app.route('/realtime')
def get_realtime():
    var = bot.var_calculator.calculate_var(bot.position_data)
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
        'market_mood': bot.sentiment_analyzer.get_market_mood()
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
        'opportunities': bot.top_movers.get_best_opportunities(5)
    })

@app.route('/chart_data')
def get_chart_data():
    data = {}
    for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]:
        df = fetch_candles(symbol, "1H", 50)
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

@socketio.on('connect')
def handle_connect():
    stats = bot.get_statistics()
    best_patterns = bot.adaptive_learner.get_best_patterns(5)
    var = bot.var_calculator.calculate_var(bot.position_data)
    learning_stats = bot.adaptive_learner.get_learning_stats()
    emit('connected', {
        'status': 'connected',
        'version': 'v18.0 Ultimate',
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
        'market_mood': bot.sentiment_analyzer.get_market_mood()
    })

# ============================================================
# SHUTDOWN
# ============================================================
def shutdown_handler(signum=None, frame=None):
    logger.info("🛑 Shutting down...")
    bot.is_running = False
    price_fetcher.stop()
    db_manager.close_all()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ============================================================
# STARTUP
# ============================================================
if __name__ == '__main__':
    logger.info("🚀 Starting Quantum Whale v18.0 - WORLD'S #1 TRADER")
    logger.info("📊 All features integrated!")
    logger.info("📡 Real-time updates every 100ms")
    logger.info("⚡ MAX 3 Concurrent Trades")
    logger.info("📊 6 Strategies Ensemble")
    logger.info("📈 5 Timeframes (15m, 1H, 4H, 1D, 1W)")
    logger.info("🧠 Weekly Trend Analysis ENABLED")
    
    bot_thread = threading.Thread(target=bot.run, daemon=True)
    bot_thread.start()
    
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🌐 Web Server on port {port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=False)