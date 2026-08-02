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

# ============================================================
# LOGGING SETUP - Must be before any logger calls!
# ============================================================

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# TRADING MODE CONFIGURATION
# ============================================================

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

# Now logger is defined, so this is safe
logger.info(f"📊 Trading Mode: {TRADING_MODE}")

missing_vars = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]

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
        "demo_url": "https://api.bitget.com",
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
        print(f"❌ Signature generation error: {e}")
        return None

# ============================================================
# FIXED: send_bitget_signed_request - NO RECURSION
# ============================================================

def send_bitget_signed_request(method, endpoint, body=None, params=None):
    """Send signed request to Bitget API"""
    api_key = EXCHANGES['bitget']['key']
    secret_key = EXCHANGES['bitget']['secret']
    passphrase = EXCHANGES['bitget']['pass']
    
    if not api_key or not secret_key or not passphrase:
        print("❌ Bitget API credentials missing for live/demo trading!")
        return None
        
    base_url = EXCHANGES['bitget']['url']
    
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
        print(f"❌ Bitget API error: {str(e)}")
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
                self._local.conn.execute("PRAGMA journal_mode=WAL")
                self._local.conn.execute("PRAGMA synchronous=NORMAL")
            conn = self._local.conn
            yield conn
        except Exception as e:
            print(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
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
            print(f"✅ Added column {column} to {table}")
            return True
    except Exception as e:
        print(f"Error adding column {column} to {table}: {e}")
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
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_performance_timestamp ON performance(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exchange_prices_symbol ON exchange_prices(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_exchange_prices_timestamp ON exchange_prices(timestamp)")
        
        conn.commit()
        print("✅ Database initialized successfully")

init_db()

# ============================================================
# FIXED: CLOUDINARY UPLOAD - NO RECURSION
# ============================================================

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
        print(f"❌ Cloudinary upload error: {str(e)}")
        return None

def upload_chart_to_cloudinary(chart_buf, chart_type, symbol=None):
    if not chart_buf:
        return None
    timestamp = int(time.time())
    filename = f"{chart_type}_{symbol}_{timestamp}" if symbol else f"{chart_type}_{timestamp}"
    folder = f"quantum_whale/{chart_type}"
    return upload_to_cloudinary(chart_buf, filename, folder)

# ============================================================
# FIXED: run_bot function - global at top
# ============================================================

def run_bot():
    global TRADING_MODE  # <-- FIXED: global at top of function
    logger.info("🤖 Bot thread started - 24/7 Auto Trading")
    logger.info(f"📊 Trading Mode: {TRADING_MODE}")
    
    if TRADING_MODE in ['DEMO', 'REAL']:
        account_info = bitget_get_account_info()
        if account_info and account_info.get('code') == '00000':
            logger.info(f"✅ Bitget {TRADING_MODE} account connected successfully!")
        else:
            logger.warning(f"⚠️ Bitget {TRADING_MODE} account connection failed! Falling back to PAPER mode.")
            TRADING_MODE = 'PAPER'
    
    while not _shutting_down:
        try:
            # manage_trades()
            # analyze_and_trade()
            time.sleep(30)
        except Exception as e:
            logger.error(f"❌ Master Loop Error: {e}")
            time.sleep(60)

# ... rest of the code remains the same ...

# ============================================================
# MAIN
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
