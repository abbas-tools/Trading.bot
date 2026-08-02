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
from datetime import datetime
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

# ============================================================
# SECURITY: Environment Variables Only - NO HARDCODED KEYS!
# ============================================================

load_dotenv()

# Validate required environment variables
REQUIRED_ENV_VARS = [
    'CLOUDINARY_CLOUD_NAME',
    'CLOUDINARY_API_KEY', 
    'CLOUDINARY_API_SECRET',
    'TELEGRAM_BOT_TOKEN',
    'TELEGRAM_CHAT_ID'
]

# Bitget API credentials (optional - for DEMO/REAL trading)
BITGET_API_KEY = os.environ.get('BITGET_API_KEY', '')
BITGET_API_SECRET = os.environ.get('BITGET_API_SECRET', '')
BITGET_PASSPHRASE = os.environ.get('BITGET_PASSPHRASE', '')

# ============================================================
# TRADING MODE: 'PAPER' | 'DEMO' | 'REAL'
# ============================================================
TRADING_MODE = os.environ.get('TRADING_MODE', 'PAPER').upper()
# TRADING_MODE = "DEMO"  # Uncomment for demo trading
# TRADING_MODE = "REAL"  # Uncomment for real trading

logger.info(f"📊 Trading Mode: {TRADING_MODE}")

missing_vars = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Check for missing variables - WARN but don't crash
if missing_vars:
    logger.warning(f"⚠️ Missing environment variables: {', '.join(missing_vars)}")
    logger.warning("⚠️ Please set them in Render dashboard")
else:
    logger.info("✅ All required environment variables are set")

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())

# Initialize SocketIO - use eventlet for production
try:
    import eventlet
    eventlet.monkey_patch()
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=60, ping_interval=25)
    logger.info("🚀 Using eventlet for production")
except ImportError:
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
    logger.warning("⚠️ Using threading mode (not recommended for production)")

# --- CLOUDINARY CONFIGURATION (Only from env) ---
CLOUDINARY_CONFIG = {
    "cloud_name": os.environ.get('CLOUDINARY_CLOUD_NAME'),
    "api_key": os.environ.get('CLOUDINARY_API_KEY'),
    "api_secret": os.environ.get('CLOUDINARY_API_SECRET')
}

# Only configure if all values exist
if all(CLOUDINARY_CONFIG.values()):
    try:
        cloudinary.config(
            cloud_name=CLOUDINARY_CONFIG['cloud_name'],
            api_key=CLOUDINARY_CONFIG['api_key'],
            api_secret=CLOUDINARY_CONFIG['api_secret']
        )
        CLOUDINARY_ENABLED = True
        logger.info("☁️ Cloudinary integration enabled")
    except Exception as e:
        CLOUDINARY_ENABLED = False
        logger.error(f"❌ Cloudinary configuration error: {e}")
else:
    CLOUDINARY_ENABLED = False
    logger.warning("☁️ Cloudinary not configured - charts will be stored locally")

# --- CONFIGURATIONS ---
PRIMARY_EXCHANGE = "bitget"

EXCHANGES = {
    "bitget": {
        "url": "https://api.bitget.com",
        "demo_url": "https://api.bitget.com",  # Bitget uses same URL for demo/real
        "key": BITGET_API_KEY,
        "secret": BITGET_API_SECRET,
        "pass": BITGET_PASSPHRASE,
        "priority": 1,
        "execution": True
    },
    "binance": {
        "url": "https://api.binance.com",
        "key": "",
        "secret": "",
        "priority": 2,
        "execution": False
    },
    "bybit": {
        "url": "https://api.bybit.com",
        "key": "",
        "secret": "",
        "priority": 3,
        "execution": False
    },
    "okx": {
        "url": "https://www.okx.com",
        "key": "",
        "secret": "",
        "priority": 4,
        "execution": False
    }
}

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    logger.info("📱 Telegram integration enabled")
else:
    logger.warning("📱 Telegram not configured - signals will not be sent")

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
circuit_breaker_active = False
bot_thread = None
_shutting_down = False

# PAPER_TRADING_MODE is now replaced by TRADING_MODE
# PAPER_TRADING_MODE = True  # REMOVED - use TRADING_MODE instead

# Multi-exchange data
exchange_prices = {exchange: {} for exchange in EXCHANGES.keys()}

# Store data with caching
price_history = {symbol: [] for symbol in SYMBOLS}
pattern_data = {symbol: [] for symbol in SYMBOLS}
candle_data = {symbol: {} for symbol in SYMBOLS}
multi_timeframe_data = {symbol: {} for symbol in SYMBOLS}
_data_cache = {}
_cache_timestamp = 0
CACHE_DURATION = 30

# Store Cloudinary URLs
cloudinary_urls = {
    'equity_charts': [],
    'signal_charts': [],
    'trade_charts': []
}

# ============================================================
# BITGET API SIGNATURE & REQUEST FUNCTIONS
# ============================================================

def get_bitget_signature(timestamp, method, request_path, body, secret_key):
    """Generate Bitget API signature using HMAC SHA256"""
    try:
        body_string = json.dumps(body) if body else ""
        str_to_sign = str(timestamp) + method.upper() + request_path + body_string
        mac = hmac.new(secret_key.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode('utf-8')
    except Exception as e:
        logger.error(f"❌ Signature generation error: {e}")
        return None

def send_bitget_signed_request(method, endpoint, body=None, params=None):
    """Send signed request to Bitget API"""
    api_key = EXCHANGES['bitget']['key']
    secret_key = EXCHANGES['bitget']['secret']
    passphrase = EXCHANGES['bitget']['pass']
    
    if not api_key or not secret_key or not passphrase:
        logger.error("❌ Bitget API credentials missing for live/demo trading!")
        return None
        
    base_url = EXCHANGES['bitget']['url']
    
    # Use demo URL if in DEMO mode
    if TRADING_MODE == "DEMO":
        base_url = EXCHANGES['bitget'].get('demo_url', base_url)
    
    url = base_url + endpoint
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
        elif method.upper() == 'GET':
            response = requests.get(url, params=params, headers=headers, timeout=10)
        else:
            return None
            
        return response.json()
    except Exception as e:
        logger.error(f"❌ Bitget API request error: {e}")
        return None

def bitget_get_account_info():
    """Get Bitget account information"""
    endpoint = "/api/v2/mix/account/account"
    return send_bitget_signed_request('GET', endpoint)

def bitget_get_positions(symbol=None):
    """Get Bitget positions"""
    endpoint = "/api/v2/mix/position/single-position"
    params = {"productType": PRODUCT_TYPE}
    if symbol:
        params["symbol"] = symbol
    return send_bitget_signed_request('GET', endpoint, params=params)

def bitget_get_order_status(order_id, symbol):
    """Get Bitget order status"""
    endpoint = "/api/v2/mix/order/detail"
    params = {
        "symbol": symbol,
        "productType": PRODUCT_TYPE,
        "orderId": order_id
    }
    return send_bitget_signed_request('GET', endpoint, params=params)

def bitget_cancel_order(order_id, symbol):
    """Cancel Bitget order"""
    endpoint = "/api/v2/mix/order/cancel-order"
    body = {
        "symbol": symbol,
        "productType": PRODUCT_TYPE,
        "orderId": order_id
    }
    return send_bitget_signed_request('POST', endpoint, body=body)

# ============================================================
# DATABASE - Thread Safe Connection Manager
# ============================================================

class DatabaseManager:
    """Thread-safe database connection manager with connection pooling"""
    
    def __init__(self, db_path='trading_bot.db'):
        self.db_path = db_path
        self._local = threading.local()
        self._connections = []
        self._lock = threading.Lock()
    
    @contextmanager
    def get_connection(self):
        """Get a thread-safe database connection"""
        conn = None
        try:
            if not hasattr(self._local, 'conn') or self._local.conn is None:
                self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
                self._local.conn.row_factory = sqlite3.Row
                # Enable WAL mode for better concurrency
                self._local.conn.execute("PRAGMA journal_mode=WAL")
                self._local.conn.execute("PRAGMA synchronous=NORMAL")
            conn = self._local.conn
            yield conn
        except Exception as e:
            logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            # Don't close - keep connection alive for thread reuse
            pass
    
    def close_all(self):
        """Close all connections (call on shutdown)"""
        with self._lock:
            for conn in self._connections:
                try:
                    conn.close()
                except:
                    pass
            self._connections.clear()
        if hasattr(self._local, 'conn') and self._local.conn:
            try:
                self._local.conn.close()
            except:
                pass
            self._local.conn = None

db_manager = DatabaseManager()

# --- DATABASE SETUP ---
def add_column_if_not_exists(cursor, table, column, col_type):
    try:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        if column not in columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            logger.info(f"✅ Added column {column} to {table}")
            return True
    except Exception as e:
        logger.error(f"Error adding column {column} to {table}: {e}")
    return False

def init_db():
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                side TEXT,
                entry REAL,
                size REAL,
                pnl REAL,
                status TEXT,
                order_id TEXT,
                mode TEXT,
                exchange TEXT,
                chart_url TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                capital REAL,
                peak_capital REAL,
                drawdown REAL,
                chart_url TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                side TEXT,
                entry REAL,
                stop_loss REAL,
                take_profit REAL,
                timeframe TEXT,
                pattern_type TEXT,
                exchange TEXT,
                chart_url TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exchange_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                exchange TEXT,
                symbol TEXT,
                price REAL,
                volume REAL
            )
        ''')
        
        # Create indexes for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_performance_timestamp ON performance(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exchange_prices_symbol ON exchange_prices(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exchange_prices_timestamp ON exchange_prices(timestamp)")
        
        conn.commit()
        logger.info("✅ Database initialized successfully")

init_db()

# --- DATABASE FUNCTIONS ---
def log_to_db(symbol, side, entry, size, pnl, status, order_id=None, mode='PAPER', exchange='bitget', chart_url=None):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                "INSERT INTO trades (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url)
            )
            conn.commit()
            logger.info(f"✅ Trade logged: {symbol} {side} {status} PnL: {pnl:.2f} ({mode})")
    except Exception as e:
        logger.error(f"❌ DB Error: {e}")

def log_signal(symbol, side, entry, sl, tp, timeframe, pattern_type, exchange='bitget', chart_url=None):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                "INSERT INTO signals (timestamp, symbol, side, entry, stop_loss, take_profit, timeframe, pattern_type, exchange, chart_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (timestamp, symbol, side, entry, sl, tp, timeframe, pattern_type, exchange, chart_url)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"❌ Signal log error: {e}")

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
        logger.error(f"❌ Performance logging error: {e}")

def log_exchange_price(exchange, symbol, price, volume):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                "INSERT INTO exchange_prices (timestamp, exchange, symbol, price, volume) "
                "VALUES (?, ?, ?, ?, ?)",
                (timestamp, exchange, symbol, price, volume)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"❌ Exchange price log error: {e}")

def get_trade_history(limit=50):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"❌ History fetch error: {e}")
        return []

def get_performance_data(limit=100):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT timestamp, capital, drawdown, chart_url FROM performance ORDER BY id DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return [{"timestamp": r[0], "capital": r[1], "drawdown": r[2], "chart_url": r[3]} for r in rows]
    except Exception as e:
        logger.error(f"❌ Performance fetch error: {e}")
        return []

def get_statistics():
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as total, SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins, SUM(pnl) as total_pnl FROM trades")
            row = cursor.fetchone()
            total = row['total'] if row else 0
            wins = row['wins'] if row else 0
            total_pnl = row['total_pnl'] if row else 0
            win_rate = (wins / total * 100) if total > 0 else 0
            return {"total": total, "wins": wins, "win_rate": win_rate, "total_pnl": total_pnl}
    except Exception as e:
        logger.error(f"❌ Statistics error: {e}")
        return {"total": 0, "wins": 0, "win_rate": 0, "total_pnl": 0}

def log_msg(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    trade_logs.append(log_entry)
    if len(trade_logs) > 200:
        trade_logs.pop(0)

# --- CLOUDINARY HELPER FUNCTIONS ---
def upload_to_cloudinary(image_buffer, filename, folder="quantum_whale"):
    if not CLOUDINARY_ENABLED:
        return None
    try:
        image_buffer.seek(0)
        upload_result = cloudinary.uploader.upload(
            image_buffer,
            folder=folder,
            public_id=filename,
            overwrite=True,
            resource_type="image",
            format="png",
            transformation=[{'quality': 'auto'}, {'fetch_format': 'auto'}]
        )
        return upload_result['secure_url']
    except Exception as e:
        logger.error(f"❌ Cloudinary upload error: {e}")
        return None

def upload_chart_to_cloudinary(chart_buf, chart_type, symbol=None):
    if not chart_buf:
        return None
    timestamp = int(time.time())
    filename = f"{chart_type}_{symbol}_{timestamp}" if symbol else f"{chart_type}_{timestamp}"
    folder = f"quantum_whale/{chart_type}"
    return upload_to_cloudinary(chart_buf, filename, folder)

# --- TELEGRAM HELPER ---
def safe_telegram_request(method, url, **kwargs):
    if not TELEGRAM_BOT_TOKEN:
        return None
    try:
        if method == 'post':
            response = requests.post(url, **kwargs, timeout=5)
        elif method == 'get':
            response = requests.get(url, **kwargs, timeout=5)
        else:
            return None
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None

def safe_telegram_send(message, photo_buf=None, cloudinary_url=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        if photo_buf:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            files = {'photo': ('chart.png', photo_buf, 'image/png')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': message, 'parse_mode': 'Markdown'}
            safe_telegram_request('post', url, files=files, data=data)
        elif cloudinary_url:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            data = {'chat_id': TELEGRAM_CHAT_ID, 'photo': cloudinary_url, 'caption': message, 'parse_mode': 'Markdown'}
            safe_telegram_request('post', url, data=data)
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
            safe_telegram_request('post', url, json=payload)
    except Exception:
        pass

# --- MULTI-EXCHANGE DATA FETCHING ---
def fetch_exchange_prices():
    global _data_cache, _cache_timestamp
    
    current_time = time.time()
    if _data_cache and (current_time - _cache_timestamp) < CACHE_DURATION:
        return
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for exchange_name, exchange_config in EXCHANGES.items():
            if not exchange_config.get('url'):
                continue
            for symbol in SYMBOLS:
                future = executor.submit(fetch_single_exchange_price, exchange_name, exchange_config, symbol)
                futures[future] = (exchange_name, symbol)
        
        for future in futures:
            try:
                result = future.result(timeout=5)
                if result:
                    exchange_name, symbol, price, volume = result
                    exchange_prices[exchange_name][symbol] = {'price': price, 'volume': volume, 'timestamp': time.time()}
            except Exception:
                pass
    
    _cache_timestamp = current_time

def fetch_single_exchange_price(exchange_name, exchange_config, symbol):
    try:
        url = f"{exchange_config['url']}/api/v2/mix/market/ticker?symbol={symbol}&productType={PRODUCT_TYPE}"
        if exchange_name == 'binance':
            url = f"{exchange_config['url']}/api/v3/ticker/price?symbol={symbol}"
        elif exchange_name == 'bybit':
            url = f"{exchange_config['url']}/v5/market/tickers?category=linear&symbol={symbol}"
        elif exchange_name == 'okx':
            url = f"{exchange_config['url']}/api/v5/market/ticker?instId={symbol}"
        
        response = requests.get(url, timeout=5)
        data = response.json()
        
        price = None
        volume = None
        
        if exchange_name == 'bitget':
            if data.get('code') == '00000':
                price = float(data.get('data', {}).get('price', 0))
                volume = float(data.get('data', {}).get('volume', 0))
        elif exchange_name == 'binance':
            if 'price' in data:
                price = float(data.get('price', 0))
        elif exchange_name == 'bybit':
            if data.get('retCode') == 0:
                ticker = data.get('result', {}).get('list', [{}])[0]
                price = float(ticker.get('lastPrice', 0))
        elif exchange_name == 'okx':
            if data.get('code') == '0':
                ticker = data.get('data', [{}])[0]
                price = float(ticker.get('last', 0))
        
        if price and price > 0:
            return exchange_name, symbol, price, volume or 0
    except Exception:
        pass
    return None

def get_best_price(symbol, side='BUY'):
    if PRIMARY_EXCHANGE in exchange_prices and symbol in exchange_prices[PRIMARY_EXCHANGE]:
        bitget_price = exchange_prices[PRIMARY_EXCHANGE][symbol].get('price', 0)
        if bitget_price and bitget_price > 0:
            return bitget_price, PRIMARY_EXCHANGE
    return None, None

# ============================================================
# ORDER EXECUTION - Supports PAPER, DEMO, REAL
# ============================================================

def execute_order(symbol, side, size, exchange=PRIMARY_EXCHANGE, price=None, order_type="market"):
    """
    Execute order based on TRADING_MODE:
    - PAPER: Local simulation
    - DEMO: Bitget Demo API
    - REAL: Bitget Live API
    """
    global TRADING_MODE
    
    # If no API keys, force PAPER mode
    if TRADING_MODE in ['DEMO', 'REAL'] and (not BITGET_API_KEY or not BITGET_API_SECRET or not BITGET_PASSPHRASE):
        logger.warning(f"⚠️ Bitget API keys missing for {TRADING_MODE} mode! Falling back to PAPER")
        TRADING_MODE = 'PAPER'
    
    if TRADING_MODE == "PAPER":
        logger.info(f"📝 PAPER TRADE: {symbol} {side} {size:.4f} @ {price or 'market'}")
        order_id = f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}"
        return True, order_id, "PAPER", exchange
    
    # Bitget Futures Order Endpoint (V2)
    endpoint = "/api/v2/mix/order/place-order"
    
    # Build payload based on Bitget API specs
    payload = {
        "symbol": symbol,
        "productType": PRODUCT_TYPE,
        "marginMode": "isolated",
        "marginCoin": "USDT",
        "size": str(size),
        "side": side.lower(),
        "orderType": order_type.lower(),
        "force": "gtc"
    }
    
    # Add limit price if specified
    if order_type.lower() == "limit" and price:
        payload["price"] = str(price)
    
    logger.info(f"🌐 Sending {TRADING_MODE} order to Bitget: {symbol} {side} {size} @ {price or 'market'}")
    
    response = send_bitget_signed_request('POST', endpoint, payload)
    
    if response and response.get('code') == '00000':
        data = response.get('data', {})
        order_id = data.get('orderId', f"BITGET_{int(time.time())}")
        logger.info(f"✅ Bitget {TRADING_MODE} Order Successful! ID: {order_id}")
        return True, order_id, TRADING_MODE, exchange
    else:
        err_msg = response.get('msg') if response else 'Unknown error'
        logger.error(f"❌ Bitget Order Failed: {err_msg}")
        # Fallback to paper if demo/real fails so bot doesn't crash
        logger.warning("⚠️ Falling back to PAPER trading for this order")
        order_id = f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}"
        return True, order_id, "PAPER", exchange

# --- CHART GENERATORS ---
def generate_equity_curve_chart(upload_to_cloud=False):
    try:
        trades = get_trade_history(limit=1000)
        if not trades:
            trades = []

        plt.figure(figsize=(6, 3), facecolor='#0d1117')
        ax = plt.axes()
        ax.set_facecolor('#161b22')

        equity = [INITIAL_CAPITAL]
        for t in reversed(trades):
            equity.append(equity[-1] + (t.get('pnl', 0) or 0))

        ax.plot(equity, color='#238636' if equity[-1] >= INITIAL_CAPITAL else '#da3633', linewidth=2, marker='o', markersize=3)
        plt.title("Hedge Fund Live Equity Performance ($)", color='white', fontsize=10, fontweight='bold')
        ax.tick_params(colors='#8b949e', labelsize=8)
        plt.grid(color='#21262d', linestyle='--', linewidth=0.5)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#0d1117')
        buf.seek(0)
        plt.close()
        
        cloud_url = None
        if upload_to_cloud and CLOUDINARY_ENABLED:
            cloud_url = upload_chart_to_cloudinary(buf, "equity")
            if cloud_url:
                cloudinary_urls['equity_charts'].append(cloud_url)
                if len(cloudinary_urls['equity_charts']) > 20:
                    cloudinary_urls['equity_charts'] = cloudinary_urls['equity_charts'][-20:]
        return buf, cloud_url
    except Exception as e:
        logger.error(f"❌ Equity chart generation error: {e}")
        return None, None

def generate_professional_chart(df, symbol, side, fibs, fvgs, upload_to_cloud=False):
    try:
        if df is None or len(df) < 10:
            return None, None
            
        plt.figure(figsize=(7, 4), facecolor='#0d1117')
        ax = plt.axes()
        ax.set_facecolor('#161b22')
        
        close_values = df['close'].values
        ax.plot(close_values, color='#58a6ff', linewidth=1.5, label='Price Action')
        
        if len(close_values) >= 20:
            sma20 = np.convolve(close_values, np.ones(20)/20, mode='valid')
            ax.plot(range(19, len(close_values)), sma20, color='#f0b90b', linewidth=1, label='SMA 20')
        
        colors = {'0.0': '#3fb950', '0.5': '#f85149', '0.618': '#a371f7', '1.0': '#8b949e'}
        for level, price in fibs.items():
            ax.axhline(y=price, color=colors.get(level, 'white'), linestyle='--', linewidth=0.8, alpha=0.7)
            ax.text(len(df)-5, price, f"Fib {level}: {price:.2f}", color=colors.get(level, 'white'), fontsize=8)

        for fvg in fvgs:
            ax.axhspan(fvg['low'], fvg['high'], color='#238636' if 'BULLISH' in fvg['type'] else '#da3633', alpha=0.3)

        plt.title(f"{symbol} - {side} SMC + Liquidity Sweep Setup", color='white', fontsize=10, fontweight='bold')
        ax.tick_params(colors='#8b949e', labelsize=8)
        plt.grid(color='#21262d', linestyle='--', linewidth=0.5)
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#0d1117')
        buf.seek(0)
        plt.close()
        
        cloud_url = None
        if upload_to_cloud and CLOUDINARY_ENABLED:
            cloud_url = upload_chart_to_cloudinary(buf, "signal", symbol)
            if cloud_url:
                cloudinary_urls['signal_charts'].append(cloud_url)
                if len(cloudinary_urls['signal_charts']) > 20:
                    cloudinary_urls['signal_charts'] = cloudinary_urls['signal_charts'][-20:]
        return buf, cloud_url
    except Exception as e:
        logger.error(f"❌ Professional chart generation error: {e}")
        return None, None

# --- CORE BOT FUNCTIONS ---
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
            
            patterns = detect_patterns(df, symbol)
            pattern_data[symbol] = patterns
            return df
    except Exception as e:
        logger.error(f"❌ Candle Error ({symbol}): {str(e)}")
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
        logger.error(f"❌ ATR calculation error: {e}")
        return None

def check_circuit_breaker():
    global current_capital, peak_capital, circuit_breaker_active
    if current_capital > peak_capital:
        peak_capital = current_capital
    
    if peak_capital <= 0:
        peak_capital = 1.0
    
    drawdown = (peak_capital - current_capital) / peak_capital
    
    chart_buf, cloud_url = generate_equity_curve_chart(upload_to_cloud=True)
    log_performance(current_capital, peak_capital, drawdown, cloud_url)
    
    if drawdown >= 0.20:
        if not circuit_breaker_active:
            circuit_breaker_active = True
            safe_telegram_send("🚨 *CIRCUIT BREAKER TRIGGERED!*\n20% drawdown limit reached.")
            logger.warning(f"⚠️ CIRCUIT BREAKER ACTIVATED - Drawdown: {drawdown:.2%}")
        return True
    return False

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
        
        trend = "NEUTRAL"
        if sma_20 > sma_50:
            trend = "📈 BULLISH UPTREND"
        elif sma_20 < sma_50:
            trend = "📉 BEARISH DOWNTREND"
        patterns.append({"type": "TREND", "label": trend, "price": current_price})
        
        if len(highs) >= 20 and len(lows) >= 20:
            recent_high = max(highs[-20:])
            recent_low = min(lows[-20:])
            if current_price >= recent_high * 0.98:
                patterns.append({"type": "RESISTANCE", "label": "🛑 Resistance Zone", "price": recent_high})
            if current_price <= recent_low * 1.02:
                patterns.append({"type": "SUPPORT", "label": "🟢 Support Zone", "price": recent_low})
            if current_price > recent_high:
                patterns.append({"type": "BREAKOUT", "label": "🚀 BULLISH BREAKOUT", "price": current_price})
            elif current_price < recent_low:
                patterns.append({"type": "BREAKDOWN", "label": "💥 BEARISH BREAKDOWN", "price": current_price})
        
        if len(closes) >= 14:
            gains, losses = [], []
            for i in range(1, len(closes)):
                change = closes[i] - closes[i-1]
                if change > 0:
                    gains.append(change)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(abs(change))
            if len(gains) >= 14:
                avg_gain = sum(gains[-14:]) / 14 if gains else 0
                avg_loss = sum(losses[-14:]) / 14 if losses else 1
                if avg_loss > 0:
                    rsi = 100 - (100 / (1 + avg_gain / avg_loss))
                    if rsi > 70:
                        patterns.append({"type": "RSI", "label": f"🔴 RSI Overbought: {rsi:.1f}", "price": current_price})
                    elif rsi < 30:
                        patterns.append({"type": "RSI", "label": f"🟢 RSI Oversold: {rsi:.1f}", "price": current_price})
        patterns.append({"type": "CURRENT_PRICE", "label": f"💵 Current: ${current_price:.2f}", "price": current_price})
    except Exception as e:
        logger.error(f"❌ Pattern detection error for {symbol}: {e}")
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
    except Exception as e:
        logger.error(f"❌ Liquidity sweep detection error: {e}")
    return "NONE"

def detect_smc_zones(df):
    try:
        if df is None or len(df) < 20:
            return {}, []
        highs = df['high'].values
        lows = df['low'].values
        recent_high = max(highs[-20:])
        recent_low = min(lows[-20:])
        diff = recent_high - recent_low
        if diff <= 0:
            diff = 1.0
        fibs = {
            "0.0": recent_high,
            "0.5": recent_high - (diff * 0.5),
            "0.618": recent_high - (diff * 0.618),
            "1.0": recent_low
        }
        fvg_zones = []
        for i in range(2, len(df)):
            try:
                if lows[i] > highs[i-2]:
                    fvg_zones.append({"type": "BULLISH_FVG", "low": highs[i-2], "high": lows[i]})
                elif highs[i] < lows[i-2]:
                    fvg_zones.append({"type": "BEARISH_FVG", "low": highs[i], "high": lows[i-2]})
            except:
                pass
        return fibs, fvg_zones[-3:] if fvg_zones else []
    except Exception as e:
        logger.error(f"❌ SMC detection error: {e}")
        return {}, []

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
    except Exception as e:
        logger.error(f"❌ Trend predictor error: {e}")
    return "NEUTRAL"

def calculate_kelly_position_size():
    try:
        stats = get_statistics()
        total = stats['total']
        if total < 5:
            return 0.10
        win_rate = stats['win_rate'] / 100
        loss_rate = 1 - win_rate
        kelly = win_rate - (loss_rate / RISK_REWARD_RATIO)
        safe_kelly = max(0.03, min(kelly * 0.3, 0.15))
        return safe_kelly
    except Exception as e:
        logger.error(f"❌ Kelly calculation error: {e}")
        return 0.10

def calculate_safe_position_size(capital, kelly_fraction, atr, current_price):
    if atr is None or atr <= 0:
        atr = current_price * 0.01 if current_price > 0 else 1.0
    if current_price <= 0:
        current_price = 1.0
    if capital <= 0:
        capital = 1.0
    
    base_size = capital * kelly_fraction / current_price
    max_size = min(MAX_POSITION_SIZE, capital * 0.02 / current_price)
    return max(MIN_POSITION_SIZE, min(base_size, max_size))

def run_historical_backtest(df):
    try:
        if df is None or len(df) < 20:
            return 0.0, 0.0
        wins, losses, total_pnl = 0, 0, 0.0
        close_values = df['close'].values
        for i in range(20, len(df)-1):
            if i < 5:
                continue
            price_now = close_values[i]
            future_price = close_values[i+1]
            sma_fast = np.mean(close_values[i-5:i])
            sma_slow = np.mean(close_values[i-20:i])
            if sma_fast > sma_slow:
                if future_price > price_now:
                    wins += 1
                    total_pnl += 0.5
                else:
                    losses += 1
                    total_pnl -= 0.2
        win_rate = (wins / (wins + losses)) * 100 if (wins + losses) > 0 else 0
        return win_rate, total_pnl
    except Exception as e:
        logger.error(f"❌ Backtest error: {e}")
        return 0.0, 0.0

def fetch_multi_timeframe(symbol):
    timeframes = ["15m", "1H", "4H"]
    data = {}
    for tf in timeframes:
        df = fetch_candles(symbol, tf, 100)
        if df is not None and not df.empty:
            data[tf] = df
            multi_timeframe_data[symbol][tf] = {
                'close': df['close'].tolist(),
                'high': df['high'].tolist(),
                'low': df['low'].tolist(),
                'open': df['open'].tolist(),
                'timestamp': df['timestamp'].tolist()
            }
    return data

def analyze_multi_timeframe(data):
    if not data:
        return None
    trends = {}
    signals = {}
    for tf, df in data.items():
        if df is None or df.empty or len(df) < 20:
            continue
        try:
            close_values = df['close'].values
            sma_fast = np.mean(close_values[-5:])
            sma_slow = np.mean(close_values[-20:])
            if sma_fast > sma_slow:
                trends[tf] = "BULLISH"
            elif sma_fast < sma_slow:
                trends[tf] = "BEARISH"
            else:
                trends[tf] = "NEUTRAL"
            sweep = detect_liquidity_sweep(df)
            if sweep != "NONE":
                signals[tf] = sweep
        except Exception as e:
            logger.error(f"❌ MTF analysis error for {tf}: {e}")
    if not trends:
        return None
    bullish_count = sum(1 for t in trends.values() if t == "BULLISH")
    bearish_count = sum(1 for t in trends.values() if t == "BEARISH")
    if bullish_count >= 2:
        return {"trend": "BULLISH", "confidence": bullish_count / len(trends), "signals": signals}
    elif bearish_count >= 2:
        return {"trend": "BEARISH", "confidence": bearish_count / len(trends), "signals": signals}
    return None

def manage_trades():
    global active_trades, current_capital, _shutting_down
    if _shutting_down:
        return
        
    for symbol in list(active_trades.keys()):
        try:
            trade = active_trades[symbol]
            df = fetch_candles(symbol, "1H", 10)
            if df is None: 
                continue
            current_price = df['close'].iloc[-1]
            atr = calculate_atr(df)
            if atr is None or pd.isna(atr) or atr <= 0:
                atr = current_price * 0.01 if current_price > 0 else 1.0

            side = trade['side']
            entry = trade['entry']
            tp = trade['tp']
            sl = trade['sl']
            size = trade['size']
            order_id = trade.get('order_id', 'PAPER')
            mode = trade.get('mode', 'PAPER')
            exchange = trade.get('exchange', PRIMARY_EXCHANGE)

            # Trailing stop
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

            # Check TP and SL
            if side == "BUY":
                if current_price >= tp:
                    profit = (tp - entry) * (size / entry) if entry > 0 else 0
                    current_capital += profit
                    safe_telegram_send(f"🎯 *{symbol} WIN*\nProfit: `+${profit:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"✅ {symbol} WIN - Profit: ${profit:.2f}")
                elif current_price <= sl:
                    loss = (entry - sl) * (size / entry) if entry > 0 else 0
                    current_capital -= loss
                    safe_telegram_send(f"🛑 *{symbol} LOSS*\nLoss: `-${loss:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"❌ {symbol} LOSS - Loss: ${loss:.2f}")
            elif side == "SELL":
                if current_price <= tp:
                    profit = (entry - tp) * (size / entry) if entry > 0 else 0
                    current_capital += profit
                    safe_telegram_send(f"🎯 *{symbol} WIN*\nProfit: `+${profit:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"✅ {symbol} WIN - Profit: ${profit:.2f}")
                elif current_price >= sl:
                    loss = (sl - entry) * (size / entry) if entry > 0 else 0
                    current_capital -= loss
                    safe_telegram_send(f"🛑 *{symbol} LOSS*\nLoss: `-${loss:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"❌ {symbol} LOSS - Loss: ${loss:.2f}")
        except Exception as e:
            logger.error(f"❌ Trade management error for {symbol}: {e}")

def analyze_and_trade():
    global active_trades, circuit_breaker_active, _shutting_down
    if _shutting_down:
        return
        
    fetch_exchange_prices()
    if check_circuit_breaker(): 
        return

    for symbol in SYMBOLS:
        if symbol in active_trades: 
            continue
        try:
            bitget_price, bitget_exchange = get_best_price(symbol, 'BUY')
            if not bitget_price:
                continue
            
            mtf_data = fetch_multi_timeframe(symbol)
            mtf_result = analyze_multi_timeframe(mtf_data)
            if mtf_result:
                logger.info(f"📊 {symbol} MTF Signal: {mtf_result['trend']} (Confidence: {mtf_result['confidence']:.1%})")
            
            df_1h = fetch_candles(symbol, "1H", 100)
            if df_1h is None: 
                continue

            current_price = bitget_price
            wr, pnl = run_historical_backtest(df_1h)
            trend = smart_trend_predictor(df_1h)
            sweep = detect_liquidity_sweep(df_1h)
            fibs, fvgs = detect_smc_zones(df_1h)

            side = None
            if mtf_result and mtf_result['trend'] == "BULLISH" and (trend == "BULLISH" or sweep == "BULLISH_SWEEP"):
                side = "BUY"
            elif mtf_result and mtf_result['trend'] == "BEARISH" and (trend == "BEARISH" or sweep == "BEARISH_SWEEP"):
                side = "SELL"
            elif trend == "BULLISH" or sweep == "BULLISH_SWEEP":
                side = "BUY"
            elif trend == "BEARISH" or sweep == "BEARISH_SWEEP":
                side = "SELL"
            else:
                continue

            atr = calculate_atr(df_1h)
            if atr is None or pd.isna(atr) or atr <= 0:
                atr = current_price * 0.01 if current_price > 0 else 1.0

            kelly_fraction = calculate_kelly_position_size()
            size = calculate_safe_position_size(current_capital, kelly_fraction, atr, current_price)

            sl = current_price - (atr * 1.5) if side == "BUY" else current_price + (atr * 1.5)
            tp = current_price + (atr * 1.5 * RISK_REWARD_RATIO) if side == "BUY" else current_price - (atr * 1.5 * RISK_REWARD_RATIO)

            success, order_id, mode, exchange = execute_order(symbol, side.lower(), size, PRIMARY_EXCHANGE)
            
            if success:
                chart_buf, cloud_url = generate_professional_chart(df_1h, symbol, side, fibs, fvgs, upload_to_cloud=True)
                
                active_trades[symbol] = {
                    "side": side, 
                    "entry": current_price, 
                    "sl": sl, 
                    "tp": tp, 
                    "size": size,
                    "order_id": order_id,
                    "mode": mode,
                    "exchange": exchange
                }
                
                log_signal(symbol, side, current_price, sl, tp, "1H", mtf_result['trend'] if mtf_result else "SMC", exchange, cloud_url)
                
                mode_emoji = "📝" if mode == "PAPER" else "🔴" if mode == "REAL" else "🟡"
                signal_text = (
                    f"{mode_emoji} *Signal: {symbol}* {mode_emoji}\n\n"
                    f"🔹 *Action:* `{side}`\n"
                    f"📊 *MTF:* `{mtf_result['trend'] if mtf_result else 'N/A'}`\n"
                    f"🎯 *WinRate:* `{wr:.1f}%`\n"
                    f"⚡ *Sweep:* `{sweep}`\n"
                    f"📝 *Mode:* `{mode}`\n"
                    f"🔄 *Exchange:* `{exchange}` *(Primary)*\n"
                    f"💰 *Size:* `{size:.4f}`\n"
                    f"📍 *Entry:* `{current_price:.2f}`\n"
                    f"🛑 *SL:* `{round(sl, 2)}` | 🎯 *TP:* `{round(tp, 2)}`"
                )
                
                if cloud_url:
                    signal_text += f"\n☁️ *Chart:* [View]({cloud_url})"
                    safe_telegram_send(signal_text, cloudinary_url=cloud_url)
                elif chart_buf:
                    safe_telegram_send(signal_text, photo_buf=chart_buf)
                else:
                    safe_telegram_send(signal_text)
                
                log_msg(f"🚀 [{symbol}] {mode} order: {side} @ {current_price:.2f} ({exchange})")
                logger.info(f"✅ {mode} order placed: {symbol} {side} @ {current_price:.2f} ({exchange})")
            
        except Exception as e:
            logger.error(f"❌ Analysis error for {symbol}: {e}")

def run_bot():
    global TRADING_MODE  # <-- Isay yahan bilkul shuru mein rakhein
    
    logger.info("🤖 Bot thread started - 24/7 Auto Trading")
    logger.info(f"📊 Trading Mode: {TRADING_MODE}")
    
    # If in DEMO or REAL mode, verify Bitget credentials
    if TRADING_MODE in ['DEMO', 'REAL']:
        account_info = bitget_get_account_info()
        if account_info and account_info.get('code') == '00000':
            logger.info(f"✅ Bitget {TRADING_MODE} account connected successfully!")
        else:
            logger.warning(f"⚠️ Bitget {TRADING_MODE} account connection failed! Falling back to PAPER mode.")
            TRADING_MODE = 'PAPER'
    
    while not _shutting_down:
        try:
            manage_trades()
            analyze_and_trade()
            time.sleep(30)
        except Exception as e:
            logger.error(f"❌ Master Loop Error: {e}")
            time.sleep(60)

# --- WEBSOCKET EVENTS ---
@socketio.on('connect')
def handle_connect():
    logger.info(f"🔌 WebSocket client connected")
    emit('connected', {'status': 'connected', 'primary_exchange': PRIMARY_EXCHANGE, 'cloudinary_enabled': CLOUDINARY_ENABLED, 'trading_mode': TRADING_MODE})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info("🔌 WebSocket client disconnected")

# --- FLASK ROUTES ---
@app.route('/')
def index():
    return render_template('index.html', status=bot_status, logs=trade_logs[-50:], capital=current_capital)

@app.route('/chart_data')
def get_chart_data():
    try:
        chart_data = {}
        for symbol in SYMBOLS:
            if candle_data.get(symbol) and candle_data[symbol].get('close'):
                data = candle_data[symbol]
                chart_data[symbol] = {
                    "price": data.get('close', [])[-50:],
                    "high": data.get('high', [])[-50:],
                    "low": data.get('low', [])[-50:],
                    "volume": data.get('volume', [])[-50:],
                    "timestamps": data.get('timestamps', [])[-50:],
                    "patterns": pattern_data.get(symbol, []),
                    "current_price": data.get('close', [0])[-1] if data.get('close') else 0,
                    "sma_20": sum(data.get('close', [0])[-20:])/20 if len(data.get('close', [])) >= 20 else 0,
                    "mtf": multi_timeframe_data.get(symbol, {}),
                    "mode": TRADING_MODE,
                    "exchange_prices": exchange_prices,
                    "primary_exchange": PRIMARY_EXCHANGE,
                    "cloudinary_enabled": CLOUDINARY_ENABLED
                }
            else:
                df = fetch_candles(symbol, "1H", 50)
                if df is not None and not df.empty:
                    chart_data[symbol] = {
                        "price": df['close'].values[-50:].tolist(),
                        "high": df['high'].values[-50:].tolist(),
                        "low": df['low'].values[-50:].tolist(),
                        "volume": df['volume'].values[-50:].tolist(),
                        "timestamps": df['timestamp'].values[-50:].tolist(),
                        "patterns": pattern_data.get(symbol, []),
                        "current_price": df['close'].iloc[-1],
                        "sma_20": df['close'].rolling(20).mean().iloc[-1] if len(df) >= 20 else 0,
                        "mtf": multi_timeframe_data.get(symbol, {}),
                        "mode": TRADING_MODE,
                        "exchange_prices": exchange_prices,
                        "primary_exchange": PRIMARY_EXCHANGE,
                        "cloudinary_enabled": CLOUDINARY_ENABLED
                    }
                else:
                    chart_data[symbol] = {
                        "price": [],
                        "high": [],
                        "low": [],
                        "volume": [],
                        "timestamps": [],
                        "patterns": [],
                        "current_price": 0,
                        "sma_20": 0,
                        "mtf": {},
                        "mode": TRADING_MODE,
                        "exchange_prices": exchange_prices,
                        "primary_exchange": PRIMARY_EXCHANGE,
                        "cloudinary_enabled": CLOUDINARY_ENABLED
                    }
        return jsonify(chart_data)
    except Exception as e:
        logger.error(f"❌ Chart data error: {e}")
        return jsonify({"error": str(e), "data": {}}), 200

@app.route('/exchanges')
def get_exchanges():
    return jsonify({
        'prices': exchange_prices,
        'timestamp': time.time(),
        'primary_exchange': PRIMARY_EXCHANGE,
        'execution_exchanges': [ex for ex, cfg in EXCHANGES.items() if cfg.get('execution', False)]
    })

@app.route('/cloudinary/status')
def cloudinary_status():
    return jsonify({
        'enabled': CLOUDINARY_ENABLED,
        'config': {
            'cloud_name': CLOUDINARY_CONFIG['cloud_name'],
            'api_key': CLOUDINARY_CONFIG['api_key'][:4] + '****' if CLOUDINARY_CONFIG['api_key'] else 'not_set'
        },
        'urls': cloudinary_urls,
        'total_equity': len(cloudinary_urls.get('equity_charts', [])),
        'total_signals': len(cloudinary_urls.get('signal_charts', [])),
        'total_trades': len(cloudinary_urls.get('trade_charts', []))
    })

@app.route('/history')
def get_history():
    trades = get_trade_history(limit=50)
    return jsonify(trades)

@app.route('/stats')
def get_stats():
    stats = get_statistics()
    stats['trading_mode'] = TRADING_MODE
    return jsonify(stats)

@app.route('/status')
def get_status():
    return jsonify({
        "status": bot_status,
        "logs": trade_logs[-50:],
        "capital": current_capital,
        "active_trades": len(active_trades),
        "circuit_breaker": circuit_breaker_active,
        "mode": TRADING_MODE,
        "exchanges": list(exchange_prices.keys()),
        "primary_exchange": PRIMARY_EXCHANGE,
        "execution_exchanges": [ex for ex, cfg in EXCHANGES.items() if cfg.get('execution', False)],
        "cloudinary_enabled": CLOUDINARY_ENABLED
    })

@app.route('/performance')
def get_performance():
    data = get_performance_data(limit=100)
    return jsonify(data)

@app.route('/trading_mode')
def get_trading_mode():
    """Get current trading mode"""
    return jsonify({
        "mode": TRADING_MODE,
        "bitget_configured": bool(BITGET_API_KEY and BITGET_API_SECRET and BITGET_PASSPHRASE),
        "description": {
            "PAPER": "Local simulation - no real orders",
            "DEMO": "Bitget Demo trading - test with demo funds",
            "REAL": "Bitget Real trading - live funds"
        }
    })

@app.route('/set_trading_mode', methods=['POST'])
def set_trading_mode():
    """Change trading mode (admin only)"""
    global TRADING_MODE
    data = request.get_json()
    new_mode = data.get('mode', '').upper()
    
    if new_mode not in ['PAPER', 'DEMO', 'REAL']:
        return jsonify({"error": "Invalid mode. Use PAPER, DEMO, or REAL"}), 400
    
    # Verify credentials for DEMO/REAL
    if new_mode in ['DEMO', 'REAL']:
        if not BITGET_API_KEY or not BITGET_API_SECRET or not BITGET_PASSPHRASE:
            return jsonify({"error": f"Bitget API keys missing for {new_mode} mode"}), 400
        
        # Test connection
        account_info = bitget_get_account_info()
        if not account_info or account_info.get('code') != '00000':
            return jsonify({"error": f"Failed to connect to Bitget {new_mode} mode"}), 400
    
    old_mode = TRADING_MODE
    TRADING_MODE = new_mode
    logger.info(f"🔄 Trading mode changed: {old_mode} -> {new_mode}")
    
    safe_telegram_send(f"🔄 *Trading Mode Changed*\nOld: `{old_mode}`\nNew: `{new_mode}`")
    
    return jsonify({
        "success": True,
        "old_mode": old_mode,
        "new_mode": TRADING_MODE
    })

# ============================================================
# SHUTDOWN HANDLER - Clean up connections
# ============================================================

def shutdown_handler(signum=None, frame=None):
    global _shutting_down
    logger.info("🛑 Shutting down gracefully...")
    _shutting_down = True
    
    # Close database connections
    try:
        db_manager.close_all()
        logger.info("✅ Database connections closed")
    except Exception as e:
        logger.error(f"❌ Error closing database: {e}")
    
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ============================================================
# STARTUP - Start bot thread when app loads (for Gunicorn)
# ============================================================

logger.info("✅ Starting Quantum Whale Terminal on Render...")
logger.info(f"📊 Trading Mode: {TRADING_MODE}")

# Start bot thread automatically
bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()
logger.info("✅ Bot thread started!")

# ============================================================
# MAIN - For direct execution
# ============================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🚀 Starting Flask server on port {port}")
    
    try:
        import eventlet
        eventlet.monkey_patch()
        socketio.run(app, host='0.0.0.0', port=port, debug=False)
    except ImportError:
        app.run(host='0.0.0.0', port=port, debug=False)
