# ============================================================
# IMPORTANT: eventlet.monkey_patch() MUST be FIRST!
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
from concurrent.futures import ThreadPoolExecutor
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

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================

REQUIRED_ENV_VARS = [
    'CLOUDINARY_CLOUD_NAME',
    'CLOUDINARY_API_KEY',
    'CLOUDINARY_API_SECRET',
    'TELEGRAM_BOT_TOKEN',
    'TELEGRAM_CHAT_ID'
]

BITGET_API_KEY = os.environ.get('BITGET_API_KEY', '')
BITGET_API_SECRET = os.environ.get('BITGET_API_SECRET', '')
BITGET_PASSPHRASE = os.environ.get('BITGET_PASSPHRASE', '')

# TRADING MODE: 'PAPER' | 'DEMO' | 'REAL'
TRADING_MODE = os.environ.get('TRADING_MODE', 'PAPER').upper()

logger.info(f"📊 Trading Mode: {TRADING_MODE}")

missing_vars = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
if missing_vars:
    logger.warning(f"⚠️ Missing: {', '.join(missing_vars)}")
else:
    logger.info("✅ All required environment variables are set")

# ============================================================
# FEATURE: REAL-TIME BITGET BALANCE
# ============================================================

def get_bitget_balance():
    """Fetch real-time Bitget account balance"""
    try:
        if TRADING_MODE == "PAPER":
            return {"available": current_capital, "total": current_capital, "mode": "PAPER"}
        
        response = send_bitget_request('GET', "/api/v2/mix/account/accounts")
        if response and response.get('code') == '00000':
            data = response.get('data', [])
            if data and len(data) > 0:
                account = data[0]
                return {
                    "available": float(account.get('available', 0)),
                    "total": float(account.get('total', 0)),
                    "mode": TRADING_MODE
                }
    except Exception as e:
        logger.debug(f"Balance fetch error: {e}")
    
    return {"available": current_capital, "total": current_capital, "mode": "PAPER"}

# ============================================================
# FEATURE: BITGET ORDER BOOK
# ============================================================

def get_order_book(symbol, limit=20):
    """Fetch Bitget order book"""
    try:
        url = f"{EXCHANGES['bitget']['url']}/api/v2/mix/market/orderbook?symbol={symbol}&productType={PRODUCT_TYPE}&limit={limit}"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        if data.get('code') == '00000':
            result = data.get('data', {})
            return {
                'bids': [[float(x[0]), float(x[1])] for x in result.get('bids', [])],
                'asks': [[float(x[0]), float(x[1])] for x in result.get('asks', [])],
                'timestamp': result.get('ts', time.time())
            }
    except Exception as e:
        logger.debug(f"Order book error {symbol}: {e}")
    return None

def analyze_order_book(symbol):
    """Analyze order book for support/resistance"""
    try:
        book = get_order_book(symbol, 20)
        if not book or not book['bids'] or not book['asks']:
            return None
        
        bids = book['bids']
        asks = book['asks']
        
        best_bid = bids[0][0] if bids else 0
        best_ask = asks[0][0] if asks else 0
        spread = best_ask - best_bid if best_ask > best_bid else 0
        
        support = best_bid
        for i in range(1, min(5, len(bids))):
            if bids[i][1] > bids[i-1][1]:
                support = bids[i][0]
                break
        
        resistance = best_ask
        for i in range(1, min(5, len(asks))):
            if asks[i][1] > asks[i-1][1]:
                resistance = asks[i][0]
                break
        
        bid_volume = sum(b[1] for b in bids[:10])
        ask_volume = sum(a[1] for a in asks[:10])
        total_volume = bid_volume + ask_volume
        imbalance = (bid_volume - ask_volume) / total_volume if total_volume > 0 else 0
        liquidity_score = min(100, (bid_volume + ask_volume) / 100)
        
        return {
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread': spread,
            'support': support,
            'resistance': resistance,
            'bid_volume': bid_volume,
            'ask_volume': ask_volume,
            'imbalance': imbalance,
            'liquidity_score': liquidity_score,
            'signal': 'BUY' if imbalance > 0.3 else 'SELL' if imbalance < -0.3 else 'NEUTRAL'
        }
    except Exception as e:
        logger.debug(f"Order book analysis error {symbol}: {e}")
    return None

def get_order_book_signal(symbol, current_price):
    """Get trading signal from order book"""
    try:
        analysis = analyze_order_book(symbol)
        if not analysis:
            return {'signal': 'NEUTRAL', 'confidence': 0}
        
        signal = 'NEUTRAL'
        confidence = 0
        reasons = []
        
        support = analysis.get('support', 0)
        resistance = analysis.get('resistance', 0)
        imbalance = analysis.get('imbalance', 0)
        
        if current_price and support > 0:
            support_distance = (current_price - support) / current_price * 100
            if support_distance < 0.5 and imbalance > 0.1:
                signal = 'BUY'
                confidence = min(80, 50 + abs(imbalance * 50))
                reasons.append(f"Near support (${support:.2f})")
        
        if resistance > 0:
            resistance_distance = (resistance - current_price) / current_price * 100
            if resistance_distance < 0.5 and imbalance < -0.1:
                signal = 'SELL'
                confidence = min(80, 50 + abs(imbalance * 50))
                reasons.append(f"Near resistance (${resistance:.2f})")
        
        return {'signal': signal, 'confidence': confidence, 'reasons': reasons, 'analysis': analysis}
    except Exception as e:
        logger.debug(f"Order book signal error {symbol}: {e}")
    
    return {'signal': 'NEUTRAL', 'confidence': 0}

# ============================================================
# FEATURE: NEWS SENTIMENT ANALYSIS
# ============================================================

BULLISH_KEYWORDS = [
    "bull", "bullish", "rally", "surge", "moon", "pump", "green", "profit",
    "adoption", "institutional", "ETF approved", "partnership", "breakthrough",
    "all-time high", "ATH", "buy", "accumulate", "positive", "growth"
]

BEARISH_KEYWORDS = [
    "bear", "bearish", "dump", "crash", "plunge", "red", "loss", "rejection",
    "ban", "regulation", "SEC", "law suit", "fraud", "scam", "hack", "security",
    "decline", "negative", "risk", "concern", "inflation", "war", "crisis"
]

class CryptoNewsAnalyzer:
    def __init__(self):
        self.news_cache = []
        self.sentiment_score = 0
        self.last_update = 0
        self.latest_news = []
        self.alert_triggers = []
    
    def fetch_news(self):
        all_news = []
        try:
            response = requests.get("https://api.coingecko.com/api/v3/news", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    for item in data['data'][:10]:
                        all_news.append({
                            'title': item.get('title', ''),
                            'description': item.get('description', ''),
                            'url': item.get('url', ''),
                            'source': 'CoinGecko',
                            'timestamp': item.get('created_at', time.time())
                        })
        except Exception:
            pass
        
        try:
            response = requests.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=10", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('Data'):
                    for item in data['Data'][:10]:
                        all_news.append({
                            'title': item.get('title', ''),
                            'description': item.get('body', '')[:200],
                            'url': item.get('url', ''),
                            'source': 'CryptoCompare',
                            'timestamp': item.get('published_on', time.time())
                        })
        except Exception:
            pass
        
        seen_titles = set()
        unique_news = []
        for news in all_news:
            if news['title'] not in seen_titles:
                seen_titles.add(news['title'])
                unique_news.append(news)
        
        self.latest_news = unique_news[:10]
        return unique_news
    
    def analyze_sentiment(self, text):
        text_lower = text.lower()
        bullish_score = sum(1 for word in BULLISH_KEYWORDS if word.lower() in text_lower)
        bearish_score = sum(1 for word in BEARISH_KEYWORDS if word.lower() in text_lower)
        
        if bullish_score > bearish_score:
            return "BULLISH", bullish_score - bearish_score
        elif bearish_score > bullish_score:
            return "BEARISH", bearish_score - bullish_score
        else:
            return "NEUTRAL", 0
    
    def get_market_sentiment(self):
        try:
            news = self.fetch_news()
            if not news:
                return {"sentiment": "NEUTRAL", "score": 0, "news": []}
            
            total_sentiment = 0
            bullish_count = 0
            bearish_count = 0
            analyzed_news = []
            
            for item in news[:5]:
                sentiment, score = self.analyze_sentiment(item['title'] + " " + item.get('description', ''))
                analyzed_news.append({
                    'title': item['title'][:100],
                    'sentiment': sentiment,
                    'score': score,
                    'source': item['source']
                })
                
                if sentiment == "BULLISH":
                    bullish_count += 1
                    total_sentiment += score
                elif sentiment == "BEARISH":
                    bearish_count += 1
                    total_sentiment -= score
            
            if bullish_count > bearish_count:
                overall = "BULLISH"
                confidence = min(100, (bullish_count / max(1, bullish_count + bearish_count)) * 100)
            elif bearish_count > bullish_count:
                overall = "BEARISH"
                confidence = min(100, (bearish_count / max(1, bullish_count + bearish_count)) * 100)
            else:
                overall = "NEUTRAL"
                confidence = 50
            
            alerts = []
            for item in analyzed_news:
                if item['sentiment'] == "BEARISH" and item['score'] > 2:
                    alerts.append(f"⚠️ Bearish: {item['title'][:60]}")
                elif item['sentiment'] == "BULLISH" and item['score'] > 2:
                    alerts.append(f"📈 Bullish: {item['title'][:60]}")
            
            self.alert_triggers = alerts
            
            return {
                "sentiment": overall,
                "score": total_sentiment,
                "confidence": confidence,
                "bullish_count": bullish_count,
                "bearish_count": bearish_count,
                "news": analyzed_news,
                "alerts": alerts[:3],
                "timestamp": time.time()
            }
        except Exception as e:
            logger.error(f"❌ Sentiment error: {e}")
        
        return {"sentiment": "NEUTRAL", "score": 0, "news": []}

# ============================================================
# FEATURE: BITNODES (Bitcoin Network Data)
# ============================================================

def get_bitnodes_data():
    """Fetch Bitcoin network node data"""
    try:
        url = "https://bitnodes.io/api/v1/snapshots/latest/"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            nodes = data.get('nodes', {})
            total_nodes = len(nodes)
            health_score = min(100, (total_nodes / 20000) * 100) if total_nodes > 0 else 50
            
            health_status = "HEALTHY"
            if total_nodes < 10000:
                health_status = "WARNING"
            elif total_nodes < 5000:
                health_status = "CRITICAL"
            
            return {
                "total_nodes": total_nodes,
                "health_score": health_score,
                "health_status": health_status,
                "timestamp": time.time()
            }
    except Exception:
        pass
    
    return {
        "total_nodes": 0,
        "health_score": 50,
        "health_status": "UNKNOWN",
        "timestamp": time.time()
    }

def get_btc_network_health():
    data = get_bitnodes_data()
    
    if data['health_status'] == "CRITICAL":
        action = "REDUCE_RISK"
        reason = f"Network critically low ({data['total_nodes']})"
    elif data['health_status'] == "WARNING":
        action = "CAUTION"
        reason = f"Network decreasing ({data['total_nodes']})"
    else:
        action = "NORMAL"
        reason = f"Network healthy ({data['total_nodes']} nodes)"
    
    return {
        "network_health": data,
        "action": action,
        "reason": reason,
        "risk_level": "HIGH" if action == "REDUCE_RISK" else "MEDIUM" if action == "CAUTION" else "LOW"
    }

# ============================================================
# TRADE FILTERS - Advanced Strategy Filters
# ============================================================

MIN_WIN_RATE = 65.0
MIN_TRADES_FOR_WIN_RATE = 10
MIN_ATR_PERCENT = 0.3
MAX_ATR_PERCENT = 2.5
MIN_VOLUME_MULTIPLIER = 1.3
RSI_OVERBOUGHT = 68
RSI_OVERSOLD = 32
REQUIRE_MTF_CONFIRMATION = True

KELLY_MODE = "HALF"
MAX_CONSECUTIVE_LOSSES = 3
MIN_Kelly_FRACTION = 0.03
MAX_Kelly_FRACTION = 0.15

MTF_TIMEFRAMES = ["15m", "1H"]
REQUIRE_ALL_MTF_ALIGN = True

ATR_SL_MULTIPLIER_BASE = 1.5
ATR_TP_MULTIPLIER_BASE = 2.5
ATR_DYNAMIC_ADJUSTMENT = True

ENABLE_SESSION_FILTER = False
TRADING_SESSIONS = {
    "LONDON": {"start": 8, "end": 17},
    "NEWYORK": {"start": 13, "end": 22},
    "ASIAN": {"start": 0, "end": 8}
}
PREFERRED_SESSIONS = ["LONDON", "NEWYORK"]

logger.info(f"🎯 Win Rate Filter: {MIN_WIN_RATE}%")
logger.info(f"📊 Advanced Filters: ATR {MIN_ATR_PERCENT}%-{MAX_ATR_PERCENT}%, Volume {MIN_VOLUME_MULTIPLIER}x")
logger.info(f"💰 Kelly Mode: {KELLY_MODE}, Max Consecutive Losses: {MAX_CONSECUTIVE_LOSSES}")
logger.info(f"📈 MTF Alignment: {MTF_TIMEFRAMES}, Strict: {REQUIRE_ALL_MTF_ALIGN}")
logger.info(f"📰 News Sentiment: ENABLED, Bitnodes: ENABLED, Order Book: ENABLED")

# ============================================================
# FLASK APP
# ============================================================

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=60, ping_interval=25)

# ============================================================
# CLOUDINARY
# ============================================================

CLOUDINARY_CONFIG = {
    "cloud_name": os.environ.get('CLOUDINARY_CLOUD_NAME'),
    "api_key": os.environ.get('CLOUDINARY_API_KEY'),
    "api_secret": os.environ.get('CLOUDINARY_API_SECRET')
}

if all(CLOUDINARY_CONFIG.values()):
    try:
        cloudinary.config(**CLOUDINARY_CONFIG)
        CLOUDINARY_ENABLED = True
        logger.info("☁️ Cloudinary enabled")
    except Exception as e:
        CLOUDINARY_ENABLED = False
        logger.error(f"❌ Cloudinary error: {e}")
else:
    CLOUDINARY_ENABLED = False
    logger.warning("☁️ Cloudinary not configured")

# ============================================================
# CONFIGURATIONS
# ============================================================

PRIMARY_EXCHANGE = "bitget"

EXCHANGES = {
    "bitget": {
        "url": "https://api.bitget.com",
        "key": BITGET_API_KEY,
        "secret": BITGET_API_SECRET,
        "pass": BITGET_PASSPHRASE,
        "priority": 1,
        "execution": True
    },
    "binance": {"url": "https://api.binance.com", "priority": 2, "execution": False},
    "bybit": {"url": "https://api.bybit.com", "priority": 3, "execution": False},
    "okx": {"url": "https://www.okx.com", "priority": 4, "execution": False}
}

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

SYMBOLS = ["XAUUSDT", "BTCUSDT", "ETHUSDT"]
PRODUCT_TYPE = "usdt-futures"

INITIAL_CAPITAL = 10.0
current_capital = 10.0
peak_capital = 10.0
RISK_REWARD_RATIO = 2.5
MAX_POSITION_SIZE = 0.01
MIN_POSITION_SIZE = 0.001

bot_status = "Running"
trade_logs = []
active_trades = {}
position_data = {}
circuit_breaker_active = False
bot_thread = None
_shutting_down = False
consecutive_losses = 0
risk_reduction_active = False

exchange_prices = {exchange: {} for exchange in EXCHANGES.keys()}

# Cached data
candle_data = {symbol: {} for symbol in SYMBOLS}
pattern_data = {symbol: [] for symbol in SYMBOLS}
multi_timeframe_data = {symbol: {} for symbol in SYMBOLS}
live_prices = {symbol: 0 for symbol in SYMBOLS}

# News & Network caches
news_analyzer = CryptoNewsAnalyzer()
market_sentiment_cache = {"sentiment": "NEUTRAL", "score": 0, "news": [], "alerts": []}
network_health_cache = {"action": "NORMAL", "risk_level": "LOW", "last_update": 0}
orderbook_cache = {}

# ============================================================
# BITGET API FUNCTIONS
# ============================================================

def get_bitget_signature(timestamp, method, request_path, body, secret_key):
    try:
        body_string = json.dumps(body) if body else ""
        str_to_sign = str(timestamp) + method.upper() + request_path + body_string
        mac = hmac.new(secret_key.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode('utf-8')
    except Exception as e:
        logger.error(f"❌ Signature error: {e}")
        return None

def send_bitget_request(method, endpoint, body=None, params=None):
    api_key = EXCHANGES['bitget']['key']
    secret_key = EXCHANGES['bitget']['secret']
    passphrase = EXCHANGES['bitget']['pass']

    if not api_key or not secret_key or not passphrase:
        logger.warning("⚠️ Bitget API credentials missing - using PAPER mode")
        return None

    url = EXCHANGES['bitget']['url'] + endpoint
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
        return response.json()
    except Exception as e:
        logger.error(f"❌ Bitget API error: {e}")
        return None

def bitget_get_account_info():
    return send_bitget_request('GET', "/api/v2/mix/account/account")

def bitget_get_positions(symbol=None):
    endpoint = "/api/v2/mix/position/single-position"
    params = {"productType": PRODUCT_TYPE}
    if symbol:
        params["symbol"] = symbol
    return send_bitget_request('GET', endpoint, params=params)

# ============================================================
# DATABASE
# ============================================================

class DatabaseManager:
    def __init__(self, db_path='trading_bot.db'):
        self.db_path = db_path
        self._local = threading.local()

    @contextmanager
    def get_connection(self):
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield self._local.conn
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            pass

    def close_all(self):
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

db_manager = DatabaseManager()

def init_db():
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, symbol TEXT, side TEXT,
                entry REAL, size REAL, pnl REAL, status TEXT,
                order_id TEXT, mode TEXT, exchange TEXT, chart_url TEXT,
                kelly_used REAL, atr_used REAL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, capital REAL, peak_capital REAL,
                drawdown REAL, chart_url TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, symbol TEXT, side TEXT, entry REAL,
                stop_loss REAL, take_profit REAL, timeframe TEXT,
                pattern_type TEXT, exchange TEXT, chart_url TEXT,
                filters_passed TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exchange_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, exchange TEXT, symbol TEXT,
                price REAL, volume REAL
            )
        ''')
        conn.commit()
        logger.info("✅ Database initialized")

init_db()

# --- DATABASE FUNCTIONS ---
def log_to_db(symbol, side, entry, size, pnl, status, order_id=None, mode='PAPER', exchange='bitget', chart_url=None, kelly_used=None, atr_used=None):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                "INSERT INTO trades (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url, kelly_used, atr_used) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url, kelly_used, atr_used)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"❌ DB Error: {e}")

def log_performance(capital, peak, drawdown, chart_url=None):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                "INSERT INTO performance (timestamp, capital, peak_capital, drawdown, chart_url) "
                "VALUES (?, ?, ?, ?, ?)",
                (timestamp, capital, peak, drawdown, chart_url)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"❌ Performance error: {e}")

def get_trade_history(limit=50):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"❌ History error: {e}")
        return []

def get_statistics():
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as total, SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins, SUM(pnl) as total_pnl FROM trades")
            row = cursor.fetchone()
            if row:
                total = row['total'] or 0
                wins = row['wins'] or 0
                total_pnl = row['total_pnl'] or 0
                win_rate = (wins / total * 100) if total > 0 else 0
                return {"total": total, "wins": wins, "win_rate": win_rate, "total_pnl": total_pnl}
    except Exception as e:
        logger.error(f"❌ Statistics error: {e}")
    return {"total": 0, "wins": 0, "win_rate": 0, "total_pnl": 0}

# ============================================================
# CLOUDINARY
# ============================================================

def upload_to_cloudinary(image_buffer, filename, folder="quantum_whale"):
    if not CLOUDINARY_ENABLED:
        return None
    try:
        image_buffer.seek(0)
        result = cloudinary.uploader.upload(
            image_buffer, folder=folder, public_id=filename,
            overwrite=True, resource_type="image", format="png",
            transformation=[{'quality': 'auto'}, {'fetch_format': 'auto'}]
        )
        return result['secure_url']
    except Exception as e:
        logger.error(f"❌ Cloudinary error: {e}")
        return None

# ============================================================
# TELEGRAM
# ============================================================

def safe_telegram_send(message, photo_buf=None, cloudinary_url=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        if cloudinary_url:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            data = {'chat_id': TELEGRAM_CHAT_ID, 'photo': cloudinary_url, 'caption': message, 'parse_mode': 'Markdown'}
            requests.post(url, data=data, timeout=5)
        elif photo_buf:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            files = {'photo': ('chart.png', photo_buf, 'image/png')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': message, 'parse_mode': 'Markdown'}
            requests.post(url, files=files, data=data, timeout=5)
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
            requests.post(url, json=payload, timeout=5)
    except Exception:
        pass

# ============================================================
# CHART GENERATORS
# ============================================================

def generate_equity_chart():
    try:
        trades = get_trade_history(limit=1000)
        plt.figure(figsize=(6, 3), facecolor='#0d1117')
        ax = plt.axes()
        ax.set_facecolor('#161b22')
        equity = [INITIAL_CAPITAL]
        for t in reversed(trades):
            equity.append(equity[-1] + (t.get('pnl', 0) or 0))
        ax.plot(equity, color='#238636' if equity[-1] >= INITIAL_CAPITAL else '#da3633', linewidth=2, marker='o', markersize=3)
        plt.title("Equity Performance ($)", color='white', fontsize=10)
        ax.tick_params(colors='#8b949e', labelsize=8)
        plt.grid(color='#21262d', linestyle='--', linewidth=0.5)
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#0d1117')
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"❌ Chart error: {e}")
        return None

# ============================================================
# ENHANCEMENT FUNCTIONS
# ============================================================

def calculate_dynamic_kelly():
    global consecutive_losses
    
    try:
        stats = get_statistics()
        total = stats['total']
        
        if total < 5:
            base_kelly = 0.10
        else:
            win_rate = stats['win_rate'] / 100
            kelly = win_rate - (1 - win_rate) / RISK_REWARD_RATIO
            base_kelly = max(MIN_Kelly_FRACTION, min(kelly, MAX_Kelly_FRACTION))
        
        if KELLY_MODE == "HALF":
            adjusted_kelly = base_kelly * 0.5
        elif KELLY_MODE == "QUARTER":
            adjusted_kelly = base_kelly * 0.25
        else:
            adjusted_kelly = base_kelly
        
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            reduction_factor = max(0.3, 1 - (consecutive_losses - MAX_CONSECUTIVE_LOSSES) * 0.2)
            adjusted_kelly *= reduction_factor
        
        # Apply risk reduction from news/network
        if risk_reduction_active:
            adjusted_kelly *= 0.5
        
        return max(MIN_Kelly_FRACTION, min(adjusted_kelly, MAX_Kelly_FRACTION))
        
    except Exception as e:
        logger.error(f"❌ Kelly error: {e}")
        return 0.08

def calculate_position_size(capital, kelly, atr, price):
    if atr is None or atr <= 0:
        atr = price * 0.01 if price > 0 else 1.0
    if price <= 0:
        price = 1.0
    if capital <= 0:
        capital = 1.0
    
    position_size = capital * kelly / price
    max_risk_size = capital * 0.02 / price
    final_size = min(position_size, max_risk_size)
    
    return max(MIN_POSITION_SIZE, min(final_size, MAX_POSITION_SIZE))

def check_strict_mtf_confirmation(symbol, side):
    if not REQUIRE_MTF_CONFIRMATION:
        return True, "MTF disabled"
    
    try:
        mtf_trends = {}
        mtf_results = []
        
        for tf in MTF_TIMEFRAMES:
            df_tf = fetch_candles(symbol, tf, 50)
            if df_tf is not None and len(df_tf) >= 20:
                trend = smart_trend_predictor(df_tf)
                mtf_trends[tf] = trend
                mtf_results.append(f"{tf}:{trend}")
        
        if not mtf_trends:
            return True, "No MTF data"
        
        bullish_count = sum(1 for t in mtf_trends.values() if t == "BULLISH")
        bearish_count = sum(1 for t in mtf_trends.values() if t == "BEARISH")
        total_mtf = len(mtf_trends)
        
        if side == "BUY":
            if REQUIRE_ALL_MTF_ALIGN and bullish_count != total_mtf:
                return False, f"MTF mismatch: {', '.join(mtf_results)}"
            elif bullish_count < total_mtf * 0.5:
                return False, f"MTF weak: {', '.join(mtf_results)}"
        else:
            if REQUIRE_ALL_MTF_ALIGN and bearish_count != total_mtf:
                return False, f"MTF mismatch: {', '.join(mtf_results)}"
            elif bearish_count < total_mtf * 0.5:
                return False, f"MTF weak: {', '.join(mtf_results)}"
        
        return True, f"MTF confirmed: {', '.join(mtf_results)}"
        
    except Exception as e:
        logger.error(f"❌ MTF check error: {e}")
    return True, "MTF skip"

def calculate_dynamic_sl_tp(current_price, atr, side):
    if atr is None or atr <= 0:
        atr = current_price * 0.01 if current_price > 0 else 1.0
    
    if ATR_DYNAMIC_ADJUSTMENT:
        atr_percent = (atr / current_price) * 100 if current_price > 0 else 1.0
        
        if atr_percent < 0.5:
            sl_multiplier = ATR_SL_MULTIPLIER_BASE * 0.8
            tp_multiplier = ATR_TP_MULTIPLIER_BASE * 0.9
        elif atr_percent > 2.0:
            sl_multiplier = ATR_SL_MULTIPLIER_BASE * 1.3
            tp_multiplier = ATR_TP_MULTIPLIER_BASE * 1.2
        else:
            sl_multiplier = ATR_SL_MULTIPLIER_BASE
            tp_multiplier = ATR_TP_MULTIPLIER_BASE
    else:
        sl_multiplier = ATR_SL_MULTIPLIER_BASE
        tp_multiplier = ATR_TP_MULTIPLIER_BASE
    
    if side == "BUY":
        sl = current_price - (atr * sl_multiplier)
        tp = current_price + (atr * tp_multiplier)
    else:
        sl = current_price + (atr * sl_multiplier)
        tp = current_price - (atr * tp_multiplier)
    
    return sl, tp, sl_multiplier, tp_multiplier

def check_session_filter():
    if not ENABLE_SESSION_FILTER:
        return True, "Session filter disabled"
    
    try:
        current_hour = datetime.utcnow().hour
        
        for session in PREFERRED_SESSIONS:
            if session in TRADING_SESSIONS:
                start = TRADING_SESSIONS[session]["start"]
                end = TRADING_SESSIONS[session]["end"]
                
                if start < end:
                    if start <= current_hour < end:
                        return True, f"✅ {session} session"
                else:
                    if current_hour >= start or current_hour < end:
                        return True, f"✅ {session} session"
        
        return False, f"⏰ Outside preferred sessions"
        
    except Exception as e:
        logger.error(f"❌ Session filter error: {e}")
    return True, "Session skip"

# ============================================================
# ADVANCED FILTER FUNCTIONS
# ============================================================

def calculate_rsi(df, period=14):
    try:
        close = df['close'].values
        gains = []
        losses = []
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
        logger.error(f"❌ RSI error: {e}")
    return 50

def check_volume_condition(df):
    try:
        volume = df['volume'].values
        if len(volume) >= 20:
            avg_volume = np.mean(volume[-20:])
            current_volume = volume[-1]
            return current_volume > avg_volume * MIN_VOLUME_MULTIPLIER
    except Exception as e:
        logger.error(f"❌ Volume check error: {e}")
    return True

def check_atr_condition(df, current_price):
    try:
        atr = calculate_atr(df)
        if atr is None or atr <= 0 or current_price <= 0:
            return True
        atr_percent = (atr / current_price) * 100
        return MIN_ATR_PERCENT <= atr_percent <= MAX_ATR_PERCENT
    except Exception as e:
        logger.error(f"❌ ATR condition error: {e}")
    return True

def check_rsi_condition(df, side):
    try:
        rsi = calculate_rsi(df)
        if side == "BUY" and rsi > RSI_OVERBOUGHT:
            return False, f"RSI Overbought ({rsi:.1f} > {RSI_OVERBOUGHT})"
        elif side == "SELL" and rsi < RSI_OVERSOLD:
            return False, f"RSI Oversold ({rsi:.1f} < {RSI_OVERSOLD})"
        return True, f"RSI {rsi:.1f}"
    except Exception as e:
        logger.error(f"❌ RSI condition error: {e}")
    return True, "RSI OK"

# ============================================================
# CORE BOT FUNCTIONS
# ============================================================

def fetch_candles(symbol, granularity="1H", limit=100):
    url = f"{EXCHANGES['bitget']['url']}/api/v2/mix/market/candles?symbol={symbol}&productType={PRODUCT_TYPE}&granularity={granularity}&limit={limit}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if data.get('code') == '00000':
            df = pd.DataFrame(data['data'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp').reset_index(drop=True)

            candle_data[symbol] = {
                'timestamps': df['timestamp'].values[-50:].tolist(),
                'open': df['open'].values[-50:].tolist(),
                'high': df['high'].values[-50:].tolist(),
                'low': df['low'].values[-50:].tolist(),
                'close': df['close'].values[-50:].tolist(),
                'volume': df['volume'].values[-50:].tolist()
            }

            live_prices[symbol] = df['close'].iloc[-1]
            pattern_data[symbol] = detect_patterns(df, symbol)
            return df
    except Exception as e:
        logger.error(f"❌ Candle Error ({symbol}): {e}")
    return None

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
        return atr if not pd.isna(atr) else None
    except Exception as e:
        logger.error(f"❌ ATR error: {e}")
        return None

def detect_patterns(df, symbol):
    patterns = []
    try:
        if df is None or len(df) < 20:
            return patterns
        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values
        current_price = closes[-1] if len(closes) > 0 else 0

        sma_20 = np.mean(closes[-20:]) if len(closes) >= 20 else current_price
        sma_50 = np.mean(closes[-50:]) if len(closes) >= 50 else sma_20

        if sma_20 > sma_50:
            patterns.append({"type": "TREND", "label": "📈 BULLISH", "price": current_price})
        elif sma_20 < sma_50:
            patterns.append({"type": "TREND", "label": "📉 BEARISH", "price": current_price})
        else:
            patterns.append({"type": "TREND", "label": "➡️ NEUTRAL", "price": current_price})

        rsi = calculate_rsi(df)
        if rsi > RSI_OVERBOUGHT:
            patterns.append({"type": "RSI", "label": f"🔴 RSI {rsi:.1f} (Overbought)", "price": current_price})
        elif rsi < RSI_OVERSOLD:
            patterns.append({"type": "RSI", "label": f"🟢 RSI {rsi:.1f} (Oversold)", "price": current_price})
        else:
            patterns.append({"type": "RSI", "label": f"⚪ RSI {rsi:.1f}", "price": current_price})

        if len(highs) >= 20 and len(lows) >= 20:
            recent_high = max(highs[-20:])
            recent_low = min(lows[-20:])
            if current_price >= recent_high * 0.98:
                patterns.append({"type": "RESISTANCE", "label": "🛑 Resistance", "price": recent_high})
            if current_price <= recent_low * 1.02:
                patterns.append({"type": "SUPPORT", "label": "🟢 Support", "price": recent_low})
            if current_price > recent_high:
                patterns.append({"type": "BREAKOUT", "label": "🚀 BREAKOUT", "price": current_price})
            elif current_price < recent_low:
                patterns.append({"type": "BREAKDOWN", "label": "💥 BREAKDOWN", "price": current_price})

        patterns.append({"type": "CURRENT_PRICE", "label": f"💰 ${current_price:.2f}", "price": current_price})
    except Exception as e:
        logger.error(f"❌ Pattern error: {e}")
    return patterns

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
        if sma_fast > sma_slow:
            return "BULLISH"
        elif sma_fast < sma_slow:
            return "BEARISH"
    except Exception:
        pass
    return "NEUTRAL"

def execute_order(symbol, side, size, exchange=PRIMARY_EXCHANGE):
    global TRADING_MODE

    if TRADING_MODE in ['DEMO', 'REAL']:
        if not BITGET_API_KEY or not BITGET_API_SECRET or not BITGET_PASSPHRASE:
            logger.warning("⚠️ Bitget API keys missing! Using PAPER mode")
            TRADING_MODE = 'PAPER'
        else:
            account_info = bitget_get_account_info()
            if account_info and account_info.get('code') == '00000':
                logger.info(f"✅ Bitget {TRADING_MODE} connected")
            else:
                logger.warning(f"⚠️ Bitget {TRADING_MODE} connection failed! Using PAPER mode")
                TRADING_MODE = 'PAPER'

    if TRADING_MODE == "PAPER":
        logger.info(f"📝 PAPER TRADE: {symbol} {side} {size:.4f}")
        order_id = f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}"
        return True, order_id, "PAPER", exchange

    endpoint = "/api/v2/mix/order/place-order"
    payload = {
        "symbol": symbol, "productType": PRODUCT_TYPE,
        "marginMode": "isolated", "marginCoin": "USDT",
        "size": str(size), "side": side.lower(),
        "orderType": "market", "force": "gtc"
    }

    response = send_bitget_request('POST', endpoint, payload)

    if response and response.get('code') == '00000':
        order_id = response.get('data', {}).get('orderId', f"BITGET_{int(time.time())}")
        logger.info(f"✅ Bitget {TRADING_MODE} Order: {order_id}")
        return True, order_id, TRADING_MODE, exchange
    else:
        logger.error(f"❌ Bitget order failed: {response.get('msg') if response else 'Unknown'}")
        order_id = f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}"
        return True, order_id, "PAPER", exchange

def manage_trades():
    global active_trades, current_capital, consecutive_losses

    for symbol in list(active_trades.keys()):
        try:
            trade = active_trades[symbol]
            df = fetch_candles(symbol, "1H", 10)
            if df is None:
                continue

            current_price = df['close'].iloc[-1]
            atr = calculate_atr(df)
            if atr is None or atr <= 0:
                atr = current_price * 0.01 if current_price > 0 else 1.0

            side = trade['side']
            entry = trade['entry']
            tp = trade['tp']
            sl = trade['sl']
            size = trade['size']
            order_id = trade.get('order_id', 'PAPER')
            mode = trade.get('mode', 'PAPER')
            exchange = trade.get('exchange', PRIMARY_EXCHANGE)

            if side == "BUY":
                pnl_percent = ((current_price - entry) / entry) * 100 if entry > 0 else 0
                pnl = (current_price - entry) * (size / entry) if entry > 0 else 0
            else:
                pnl_percent = ((entry - current_price) / entry) * 100 if entry > 0 else 0
                pnl = (entry - current_price) * (size / entry) if entry > 0 else 0

            position_data[symbol] = {
                'side': side, 'entry': entry, 'current': current_price,
                'pnl': pnl, 'pnl_percent': pnl_percent, 'size': size,
                'sl': sl, 'tp': tp, 'mode': mode, 'exchange': exchange
            }

            if side == "BUY" and (current_price - entry) > (atr * 1.0):
                new_sl = current_price - (atr * 1.5)
                if new_sl > sl:
                    trade['sl'] = sl = new_sl
                    logger.info(f"📊 {symbol} SL moved to {sl:.2f}")
            elif side == "SELL" and (entry - current_price) > (atr * 1.0):
                new_sl = current_price + (atr * 1.5)
                if new_sl < sl:
                    trade['sl'] = sl = new_sl
                    logger.info(f"📊 {symbol} SL moved to {sl:.2f}")

            if side == "BUY":
                if current_price >= tp:
                    profit = (tp - entry) * (size / entry) if entry > 0 else 0
                    current_capital += profit
                    consecutive_losses = 0
                    safe_telegram_send(f"🎯 WIN {symbol}: +${profit:.2f}")
                    log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange, None, atr)
                    del active_trades[symbol]
                    del position_data[symbol]
                    logger.info(f"✅ {symbol} WIN: ${profit:.2f}")
                elif current_price <= sl:
                    loss = (entry - sl) * (size / entry) if entry > 0 else 0
                    current_capital -= loss
                    consecutive_losses += 1
                    safe_telegram_send(f"🛑 LOSS {symbol}: -${loss:.2f}")
                    log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange, None, atr)
                    del active_trades[symbol]
                    del position_data[symbol]
                    logger.info(f"❌ {symbol} LOSS: ${loss:.2f}")
            else:
                if current_price <= tp:
                    profit = (entry - tp) * (size / entry) if entry > 0 else 0
                    current_capital += profit
                    consecutive_losses = 0
                    safe_telegram_send(f"🎯 WIN {symbol}: +${profit:.2f}")
                    log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange, None, atr)
                    del active_trades[symbol]
                    del position_data[symbol]
                    logger.info(f"✅ {symbol} WIN: ${profit:.2f}")
                elif current_price >= sl:
                    loss = (sl - entry) * (size / entry) if entry > 0 else 0
                    current_capital -= loss
                    consecutive_losses += 1
                    safe_telegram_send(f"🛑 LOSS {symbol}: -${loss:.2f}")
                    log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange, None, atr)
                    del active_trades[symbol]
                    del position_data[symbol]
                    logger.info(f"❌ {symbol} LOSS: ${loss:.2f}")

        except Exception as e:
            logger.error(f"❌ Trade error {symbol}: {e}")

def analyze_and_trade():
    global active_trades, consecutive_losses, market_sentiment_cache, network_health_cache, risk_reduction_active

    # ============================================================
    # CHECK MARKET SENTIMENT (News) - Every 5 minutes
    # ============================================================
    current_time = time.time()
    if current_time - getattr(news_analyzer, 'last_update', 0) > 300:
        try:
            sentiment_data = news_analyzer.get_market_sentiment()
            market_sentiment_cache = sentiment_data
            
            if sentiment_data.get('alerts'):
                for alert in sentiment_data['alerts']:
                    if "BEARISH" in alert:
                        logger.warning(f"📰 Bearish news alert: {alert}")
                        safe_telegram_send(f"📰 *News Alert*\n{alert}")
            
            logger.info(f"📰 Market Sentiment: {sentiment_data['sentiment']} (Confidence: {sentiment_data.get('confidence', 0):.0f}%)")
            
            if sentiment_data.get('sentiment') == "BEARISH" and sentiment_data.get('confidence', 0) > 70:
                risk_reduction_active = True
                logger.info("🛑 Strong bearish sentiment detected - reducing risk")
            else:
                risk_reduction_active = False
                
        except Exception as e:
            logger.error(f"❌ Sentiment update error: {e}")
        
        news_analyzer.last_update = current_time
    
    # ============================================================
    # CHECK BITCOIN NETWORK HEALTH - Every 10 minutes
    # ============================================================
    if current_time - network_health_cache.get('last_update', 0) > 600:
        try:
            network_data = get_btc_network_health()
            network_health_cache = network_data
            network_health_cache['last_update'] = current_time
            
            if network_data['action'] == "REDUCE_RISK":
                logger.warning(f"🔴 Bitcoin network health critical: {network_data['reason']}")
                safe_telegram_send(f"🔴 *Network Alert*\n{network_data['reason']}")
            elif network_data['action'] == "CAUTION":
                logger.info(f"🟡 Bitcoin network caution: {network_data['reason']}")
                
        except Exception as e:
            logger.error(f"❌ Network health error: {e}")
    
    # Get statistics
    stats = get_statistics()
    total_trades = stats.get('total', 0)
    win_rate = stats.get('win_rate', 0)
    
    win_rate_ok = True
    if total_trades >= MIN_TRADES_FOR_WIN_RATE:
        if win_rate < MIN_WIN_RATE:
            win_rate_ok = False
            logger.info(f"⏭️ SKIPPING ALL TRADES: Win Rate {win_rate:.1f}% < {MIN_WIN_RATE}%")
        else:
            logger.info(f"✅ WIN RATE OK: {win_rate:.1f}% >= {MIN_WIN_RATE}%")
    else:
        logger.info(f"📊 Cold Start: {total_trades}/{MIN_TRADES_FOR_WIN_RATE} trades")

    for symbol in SYMBOLS:
        if symbol in active_trades:
            continue

        if not win_rate_ok:
            continue

        try:
            # Session filter
            session_ok, session_msg = check_session_filter()
            if not session_ok:
                logger.info(f"⏭️ SESSION SKIP {symbol}: {session_msg}")
                continue

            df = fetch_candles(symbol, "1H", 100)
            if df is None:
                continue

            current_price = df['close'].iloc[-1]
            trend = smart_trend_predictor(df)
            sweep = detect_liquidity_sweep(df)

            # Get order book signal
            orderbook_signal = get_order_book_signal(symbol, current_price)
            orderbook_cache[symbol] = orderbook_signal

            # Determine side
            side = None
            if trend == "BULLISH" or sweep == "BULLISH_SWEEP":
                side = "BUY"
            elif trend == "BEARISH" or sweep == "BEARISH_SWEEP":
                side = "SELL"

            if not side:
                continue

            # ============================================================
            # ADVANCED FILTERS
            # ============================================================
            filter_results = []
            passed_all = True
            
            if not check_atr_condition(df, current_price):
                passed_all = False
                filter_results.append("❌ ATR out of range")
            else:
                filter_results.append("✅ ATR OK")
            
            if not check_volume_condition(df):
                passed_all = False
                filter_results.append("❌ Low volume")
            else:
                filter_results.append("✅ Volume OK")
            
            rsi_ok, rsi_msg = check_rsi_condition(df, side)
            if not rsi_ok:
                passed_all = False
                filter_results.append(f"❌ {rsi_msg}")
            else:
                filter_results.append(f"✅ {rsi_msg}")
            
            mtf_ok, mtf_msg = check_strict_mtf_confirmation(symbol, side)
            if not mtf_ok:
                passed_all = False
                filter_results.append(f"❌ {mtf_msg}")
            else:
                filter_results.append(f"✅ {mtf_msg}")
            
            # Order book filter
            if orderbook_signal and orderbook_signal.get('signal') != 'NEUTRAL':
                if orderbook_signal['signal'] != side:
                    passed_all = False
                    filter_results.append(f"❌ Order book conflict: {orderbook_signal['signal']}")
                else:
                    filter_results.append(f"✅ Order book: {orderbook_signal['signal']} ({orderbook_signal.get('confidence', 0)}%)")
            else:
                filter_results.append("⚪ Order book: NEUTRAL")
            
            if sweep != "NONE":
                filter_results.append(f"✅ Sweep: {sweep}")
            
            logger.info(f"🔍 {symbol} Filters: {' | '.join(filter_results)}")
            
            if not passed_all:
                logger.info(f"⏭️ SKIPPED {symbol}: Filter failed")
                continue

            atr = calculate_atr(df)
            if atr is None or atr <= 0:
                atr = current_price * 0.01 if current_price > 0 else 1.0
            
            sl, tp, sl_mult, tp_mult = calculate_dynamic_sl_tp(current_price, atr, side)

            kelly_fraction = calculate_dynamic_kelly()
            size = calculate_position_size(current_capital, kelly_fraction, atr, current_price)

            success, order_id, mode, exchange = execute_order(symbol, side.lower(), size)

            if success:
                active_trades[symbol] = {
                    "side": side, "entry": current_price, "sl": sl, "tp": tp,
                    "size": size, "order_id": order_id, "mode": mode, "exchange": exchange
                }

                position_data[symbol] = {
                    'side': side, 'entry': current_price, 'current': current_price,
                    'pnl': 0, 'pnl_percent': 0, 'size': size, 'sl': sl, 'tp': tp,
                    'mode': mode, 'exchange': exchange
                }

                signal_text = (
                    f"🚀 *Signal: {symbol}*\n"
                    f"🔹 Action: `{side}`\n"
                    f"📊 Win Rate: `{win_rate:.1f}%`\n"
                    f"📊 Trades: `{total_trades}`\n"
                    f"💰 Kelly: `{kelly_fraction:.2%}` | Size: `{size:.4f}`\n"
                    f"🛑 SL: `${round(sl, 2)}` ({sl_mult:.1f}x ATR) | 🎯 TP: `${round(tp, 2)}` ({tp_mult:.1f}x ATR)\n"
                    f"📈 MTF: `{mtf_msg}`\n"
                    f"📰 Sentiment: `{market_sentiment_cache.get('sentiment', 'NEUTRAL')}`\n"
                    f"🌐 Network: `{network_health_cache.get('action', 'NORMAL')}`\n"
                    f"📊 Order Book: `{orderbook_signal.get('signal', 'NEUTRAL')}`\n"
                    f"🔍 Filters: {', '.join([f.split()[1] for f in filter_results if f.startswith('✅')])}\n"
                    f"📝 Mode: `{mode}`"
                )
                safe_telegram_send(signal_text)
                logger.info(f"✅ {mode} order: {symbol} {side} @ {current_price:.2f} (Kelly: {kelly_fraction:.2%})")

        except Exception as e:
            logger.error(f"❌ Analysis error {symbol}: {e}")

def run_bot():
    logger.info("🤖 Bot started - 24/7 Auto Trading")
    logger.info(f"📊 Mode: {TRADING_MODE}")
    logger.info(f"🎯 Min Win Rate: {MIN_WIN_RATE}%")
    logger.info(f"💰 Kelly Mode: {KELLY_MODE}, Max Consecutive Losses: {MAX_CONSECUTIVE_LOSSES}")
    logger.info(f"📈 MTF Alignment: {MTF_TIMEFRAMES}, Strict: {REQUIRE_ALL_MTF_ALIGN}")
    logger.info(f"📰 News Sentiment: ENABLED, Bitnodes: ENABLED, Order Book: ENABLED")

    while not _shutting_down:
        try:
            manage_trades()
            analyze_and_trade()

            balance = get_bitget_balance()
            
            socketio.emit('market_update', {
                'prices': live_prices,
                'positions': position_data,
                'active_trades': len(active_trades),
                'capital': balance.get('total', current_capital),
                'available_balance': balance.get('available', current_capital),
                'trading_mode': TRADING_MODE,
                'win_rate': get_statistics().get('win_rate', 0),
                'total_trades': get_statistics().get('total', 0),
                'min_win_rate': MIN_WIN_RATE,
                'consecutive_losses': consecutive_losses,
                'kelly_mode': KELLY_MODE,
                'market_sentiment': {
                    'sentiment': market_sentiment_cache.get('sentiment', 'NEUTRAL'),
                    'confidence': market_sentiment_cache.get('confidence', 0),
                    'alerts': market_sentiment_cache.get('alerts', [])[:2]
                },
                'network_health': {
                    'status': network_health_cache.get('action', 'NORMAL'),
                    'risk_level': network_health_cache.get('risk_level', 'LOW'),
                    'nodes': network_health_cache.get('network_health', {}).get('total_nodes', 0)
                },
                'orderbook': orderbook_cache,
                'filters': {
                    'atr': f"{MIN_ATR_PERCENT}%-{MAX_ATR_PERCENT}%",
                    'volume': f"{MIN_VOLUME_MULTIPLIER}x",
                    'rsi': f"{RSI_OVERSOLD}-{RSI_OVERBOUGHT}",
                    'mtf': REQUIRE_MTF_CONFIRMATION,
                    'mtf_timeframes': MTF_TIMEFRAMES,
                    'session_filter': ENABLE_SESSION_FILTER
                }
            })

            time.sleep(30)
        except Exception as e:
            logger.error(f"❌ Bot error: {e}")
            time.sleep(60)

# ============================================================
# WEBSOCKET EVENTS
# ============================================================

@socketio.on('connect')
def handle_connect():
    logger.info("🔌 WebSocket connected")
    stats = get_statistics()
    balance = get_bitget_balance()
    emit('connected', {
        'status': 'connected',
        'primary_exchange': PRIMARY_EXCHANGE,
        'cloudinary_enabled': CLOUDINARY_ENABLED,
        'trading_mode': TRADING_MODE,
        'win_rate': stats.get('win_rate', 0),
        'total_trades': stats.get('total', 0),
        'min_win_rate': MIN_WIN_RATE,
        'min_trades_required': MIN_TRADES_FOR_WIN_RATE,
        'kelly_mode': KELLY_MODE,
        'consecutive_losses': consecutive_losses,
        'mtf_timeframes': MTF_TIMEFRAMES,
        'balance': balance,
        'market_sentiment': market_sentiment_cache,
        'network_health': network_health_cache
    })

# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
def index():
    stats = get_statistics()
    balance = get_bitget_balance()
    return render_template('index.html', 
        status=bot_status, 
        logs=trade_logs[-50:], 
        capital=balance.get('total', current_capital),
        available_balance=balance.get('available', current_capital),
        win_rate=stats.get('win_rate', 0),
        total_trades=stats.get('total', 0),
        min_win_rate=MIN_WIN_RATE,
        min_trades=MIN_TRADES_FOR_WIN_RATE,
        trading_mode=TRADING_MODE,
        kelly_mode=KELLY_MODE,
        consecutive_losses=consecutive_losses,
        mtf_timeframes=MTF_TIMEFRAMES,
        market_sentiment=market_sentiment_cache,
        network_health=network_health_cache
    )

@app.route('/chart_data')
def get_chart_data():
    try:
        chart_data = {}
        for symbol in SYMBOLS:
            if not candle_data.get(symbol) or not candle_data[symbol].get('close'):
                fetch_candles(symbol, "1H", 50)
            
            if candle_data.get(symbol) and candle_data[symbol].get('close'):
                data = candle_data[symbol]
                chart_data[symbol] = {
                    "price": data.get('close', [])[-50:],
                    "high": data.get('high', [])[-50:],
                    "low": data.get('low', [])[-50:],
                    "volume": data.get('volume', [])[-50:],
                    "timestamps": data.get('timestamps', [])[-50:],
                    "patterns": pattern_data.get(symbol, []),
                    "current_price": live_prices.get(symbol, 0),
                    "sma_20": sum(data.get('close', [0])[-20:]) / 20 if len(data.get('close', [])) >= 20 else 0,
                    "mode": TRADING_MODE,
                    "has_data": True,
                    "orderbook": orderbook_cache.get(symbol, {}),
                    "filters": {
                        'atr': f"{MIN_ATR_PERCENT}%-{MAX_ATR_PERCENT}%",
                        'volume': f"{MIN_VOLUME_MULTIPLIER}x",
                        'rsi': f"{RSI_OVERSOLD}-{RSI_OVERBOUGHT}",
                        'mtf_timeframes': MTF_TIMEFRAMES,
                        'kelly_mode': KELLY_MODE
                    }
                }
            else:
                chart_data[symbol] = {
                    "price": [], "high": [], "low": [], "volume": [], "timestamps": [],
                    "patterns": [], "current_price": 0, "sma_20": 0, "mode": TRADING_MODE,
                    "has_data": False
                }
        return jsonify(chart_data)
    except Exception as e:
        logger.error(f"❌ Chart data error: {e}")
        return jsonify({"error": str(e), "data": {}}), 200

@app.route('/orderbook/<symbol>')
def get_orderbook(symbol):
    signal = get_order_book_signal(symbol, live_prices.get(symbol, 0))
    return jsonify(signal)

@app.route('/balance')
def get_balance():
    return jsonify(get_bitget_balance())

@app.route('/news/sentiment')
def get_news_sentiment():
    return jsonify(market_sentiment_cache)

@app.route('/network/health')
def get_network_health():
    return jsonify(get_btc_network_health())

@app.route('/positions')
def get_positions():
    return jsonify(position_data)

@app.route('/history')
def get_history():
    return jsonify(get_trade_history(limit=50))

@app.route('/status')
def get_status():
    stats = get_statistics()
    balance = get_bitget_balance()
    return jsonify({
        "status": bot_status,
        "logs": trade_logs[-50:],
        "capital": balance.get('total', current_capital),
        "available_balance": balance.get('available', current_capital),
        "active_trades": len(active_trades),
        "positions": position_data,
        "circuit_breaker": circuit_breaker_active,
        "mode": TRADING_MODE,
        "primary_exchange": PRIMARY_EXCHANGE,
        "cloudinary_enabled": CLOUDINARY_ENABLED,
        "win_rate": stats.get('win_rate', 0),
        "total_trades": stats.get('total', 0),
        "min_win_rate": MIN_WIN_RATE,
        "min_trades_required": MIN_TRADES_FOR_WIN_RATE,
        "trades_remaining": max(0, MIN_TRADES_FOR_WIN_RATE - stats.get('total', 0)),
        "win_rate_ok": stats.get('total', 0) >= MIN_TRADES_FOR_WIN_RATE and stats.get('win_rate', 0) >= MIN_WIN_RATE,
        "consecutive_losses": consecutive_losses,
        "kelly_mode": KELLY_MODE,
        "mtf_timeframes": MTF_TIMEFRAMES,
        "session_filter_enabled": ENABLE_SESSION_FILTER,
        "market_sentiment": market_sentiment_cache,
        "network_health": network_health_cache,
        "orderbook": orderbook_cache,
        "filters": {
            "atr": f"{MIN_ATR_PERCENT}%-{MAX_ATR_PERCENT}%",
            "volume": f"{MIN_VOLUME_MULTIPLIER}x",
            "rsi": f"{RSI_OVERSOLD}-{RSI_OVERBOUGHT}",
            "mtf": REQUIRE_MTF_CONFIRMATION
        }
    })

@app.route('/stats')
def get_stats():
    stats = get_statistics()
    stats['trading_mode'] = TRADING_MODE
    stats['active_positions'] = len(active_trades)
    stats['min_win_rate'] = MIN_WIN_RATE
    stats['min_trades_required'] = MIN_TRADES_FOR_WIN_RATE
    stats['trades_remaining'] = max(0, MIN_TRADES_FOR_WIN_RATE - stats.get('total', 0))
    stats['win_rate_ok'] = stats.get('total', 0) >= MIN_TRADES_FOR_WIN_RATE and stats.get('win_rate', 0) >= MIN_WIN_RATE
    stats['consecutive_losses'] = consecutive_losses
    stats['kelly_mode'] = KELLY_MODE
    stats['mtf_timeframes'] = MTF_TIMEFRAMES
    stats['session_filter_enabled'] = ENABLE_SESSION_FILTER
    stats['market_sentiment'] = market_sentiment_cache
    stats['network_health'] = network_health_cache
    stats['filters'] = {
        "atr": f"{MIN_ATR_PERCENT}%-{MAX_ATR_PERCENT}%",
        "volume": f"{MIN_VOLUME_MULTIPLIER}x",
        "rsi": f"{RSI_OVERSOLD}-{RSI_OVERBOUGHT}",
        "mtf": REQUIRE_MTF_CONFIRMATION
    }
    return jsonify(stats)

@app.route('/trading_mode')
def get_trading_mode():
    return jsonify({
        "mode": TRADING_MODE,
        "bitget_configured": bool(BITGET_API_KEY and BITGET_API_SECRET and BITGET_PASSPHRASE),
        "description": {"PAPER": "Simulation", "DEMO": "Bitget Demo", "REAL": "Bitget Live"},
        "kelly_mode": KELLY_MODE,
        "mtf_timeframes": MTF_TIMEFRAMES,
        "session_filter": ENABLE_SESSION_FILTER
    })

# ============================================================
# SHUTDOWN
# ============================================================

def shutdown_handler(signum=None, frame=None):
    global _shutting_down
    logger.info("🛑 Shutting down...")
    _shutting_down = True
    try:
        db_manager.close_all()
    except:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ============================================================
# STARTUP
# ============================================================

logger.info("✅ Starting Quantum Whale Terminal...")
logger.info(f"📊 Mode: {TRADING_MODE}")
logger.info(f"🎯 Min Win Rate: {MIN_WIN_RATE}% (min {MIN_TRADES_FOR_WIN_RATE} trades)")
logger.info(f"💰 Kelly Mode: {KELLY_MODE}, Max Consecutive Losses: {MAX_CONSECUTIVE_LOSSES}")
logger.info(f"📈 MTF Alignment: {MTF_TIMEFRAMES}, Strict: {REQUIRE_ALL_MTF_ALIGN}")
logger.info(f"📰 News Sentiment: ENABLED, Bitnodes: ENABLED, Order Book: ENABLED")

# Pre-fetch initial data
for symbol in SYMBOLS:
    fetch_candles(symbol, "1H", 50)

bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()
logger.info("✅ Bot started!")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🚀 Server on port {port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=False)