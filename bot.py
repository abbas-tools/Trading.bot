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
import queue
import cloudinary
import cloudinary.uploader
import cloudinary.api
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'quantum_whale_secret_2026'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
# Cache bypass update

# --- CLOUDINARY CONFIGURATION ---
CLOUDINARY_CONFIG = {
    "cloud_name": os.environ.get('CLOUDINARY_CLOUD_NAME', 'ir3o1qak'),
    "api_key": os.environ.get('CLOUDINARY_API_KEY', '946661756842871'),
    "api_secret": os.environ.get('CLOUDINARY_API_SECRET', 'K5n3FOdqV-e7ebSZzW6SrBvST-A')
}

# Configure Cloudinary
cloudinary.config(
    cloud_name=CLOUDINARY_CONFIG['cloud_name'],
    api_key=CLOUDINARY_CONFIG['api_key'],
    api_secret=CLOUDINARY_CONFIG['api_secret']
)

# Check if Cloudinary is configured properly
CLOUDINARY_ENABLED = all([
    CLOUDINARY_CONFIG['cloud_name'] != 'your_cloud_name',
    CLOUDINARY_CONFIG['api_key'] != 'your_api_key',
    CLOUDINARY_CONFIG['api_secret'] != 'your_api_secret'
])

if CLOUDINARY_ENABLED:
    logger.info("☁️ Cloudinary integration enabled")
else:
    logger.warning("☁️ Cloudinary not configured - charts will be stored locally")

# --- CONFIGURATIONS ---
PRIMARY_EXCHANGE = "bitget"

EXCHANGES = {
    "bitget": {
        "url": "https://api.bitget.com",
        "key": os.environ.get('BITGET_API_KEY', 'bg_9f59c57ce1fe9d7860375b653a3966b6'),
        "secret": os.environ.get('BITGET_API_SECRET', '0b633c421f0e1b8bd23f2612bb4fc515d8a5ce779c047f57c8920db070864514'),
        "pass": os.environ.get('BITGET_PASSPHRASE', 'whale12345'),
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

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', "8936793656:AAHgykZP0mDEV3aJjEJ04aPMnFqJixTCMEs")
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', "8510851952")
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', "YOUR_DISCORD_WEBHOOK_URL_HERE")

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
last_telegram_update_id = 0
circuit_breaker_active = False
bot_thread = None
websocket_clients = set()

PAPER_TRADING_MODE = True

# Multi-exchange data
exchange_data = {exchange: {} for exchange in EXCHANGES.keys()}
exchange_prices = {exchange: {} for exchange in EXCHANGES.keys()}
exchange_patterns = {exchange: {} for exchange in EXCHANGES.keys()}

# Store data
price_history = {symbol: [] for symbol in SYMBOLS}
pattern_data = {symbol: [] for symbol in SYMBOLS}
candle_data = {symbol: {} for symbol in SYMBOLS}
multi_timeframe_data = {symbol: {} for symbol in SYMBOLS}

# Store Cloudinary URLs
cloudinary_urls = {
    'equity_charts': [],
    'signal_charts': [],
    'trade_charts': []
}

# --- CLOUDINARY HELPER FUNCTIONS ---
def upload_to_cloudinary(image_buffer, filename, folder="quantum_whale"):
    """Upload image to Cloudinary and return URL"""
    if not CLOUDINARY_ENABLED:
        return None
    
    try:
        # Reset buffer position
        image_buffer.seek(0)
        
        # Upload to Cloudinary
        upload_result = cloudinary.uploader.upload(
            image_buffer,
            folder=folder,
            public_id=filename,
            overwrite=True,
            resource_type="image",
            format="png",
            transformation=[
                {'quality': 'auto'},
                {'fetch_format': 'auto'}
            ]
        )
        
        logger.info(f"✅ Uploaded to Cloudinary: {upload_result['secure_url']}")
        return upload_result['secure_url']
    except Exception as e:
        logger.error(f"Cloudinary upload error: {e}")
        return None

def upload_chart_to_cloudinary(chart_buf, chart_type, symbol=None):
    """Upload chart to Cloudinary with proper naming"""
    if not chart_buf:
        return None
    
    timestamp = int(time.time())
    if symbol:
        filename = f"{chart_type}_{symbol}_{timestamp}"
    else:
        filename = f"{chart_type}_{timestamp}"
    
    folder = f"quantum_whale/{chart_type}"
    return upload_to_cloudinary(chart_buf, filename, folder)

def get_cloudinary_url(public_id, transformation=None):
    """Get Cloudinary URL with transformations"""
    if not CLOUDINARY_ENABLED:
        return None
    
    try:
        url = cloudinary.utils.cloudinary_url(
            public_id,
            transformation=transformation or []
        )
        return url[0] if isinstance(url, tuple) else url
    except Exception as e:
        logger.error(f"Cloudinary URL generation error: {e}")
        return None

def delete_from_cloudinary(public_id):
    """Delete image from Cloudinary"""
    if not CLOUDINARY_ENABLED:
        return False
    
    try:
        result = cloudinary.uploader.destroy(public_id)
        return result.get('result') == 'ok'
    except Exception as e:
        logger.error(f"Cloudinary delete error: {e}")
        return False

def get_chart_url_from_cloudinary(chart_type, symbol=None, limit=10):
    """Get latest chart URLs from Cloudinary"""
    if not CLOUDINARY_ENABLED:
        return []
    
    try:
        folder = f"quantum_whale/{chart_type}"
        if symbol:
            folder = f"{folder}/{symbol}"
        
        result = cloudinary.api.resources(
            type='upload',
            prefix=folder,
            max_results=limit,
            resource_type='image'
        )
        
        urls = [resource['secure_url'] for resource in result.get('resources', [])]
        return urls
    except Exception as e:
        logger.error(f"Cloudinary fetch error: {e}")
        return []

# --- DATABASE SETUP WITH MIGRATION ---
def add_column_if_not_exists(cursor, table, column, col_type):
    """Add column to table if it doesn't exist"""
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
    conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
    cursor = conn.cursor()
    
    # Create tables
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
            exchange TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            capital REAL,
            peak_capital REAL,
            drawdown REAL
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
            exchange TEXT
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
    
    # --- MIGRATIONS: Add chart_url columns if they don't exist ---
    add_column_if_not_exists(cursor, 'trades', 'chart_url', 'TEXT')
    add_column_if_not_exists(cursor, 'signals', 'chart_url', 'TEXT')
    add_column_if_not_exists(cursor, 'performance', 'chart_url', 'TEXT')
    
    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_performance_timestamp ON performance(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_exchange_prices_symbol ON exchange_prices(symbol)")
    
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully with indexes and migrations")

init_db()

# --- DATABASE FUNCTIONS WITH CHART URL ---
def log_to_db(symbol, side, entry, size, pnl, status, order_id=None, mode='PAPER', exchange='bitget', chart_url=None):
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Check if chart_url column exists
        cursor.execute("PRAGMA table_info(trades)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'chart_url' in columns:
            cursor.execute("INSERT INTO trades (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url))
        else:
            cursor.execute("INSERT INTO trades (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange))
        
        conn.commit()
        conn.close()
        logger.info(f"Trade logged: {symbol} {side} {status} PnL: {pnl} ({mode} @ {exchange})")
    except Exception as e:
        logger.error(f"DB Error: {e}")

def log_signal(symbol, side, entry, sl, tp, timeframe, pattern_type, exchange='bitget', chart_url=None):
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Check if chart_url column exists
        cursor.execute("PRAGMA table_info(signals)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'chart_url' in columns:
            cursor.execute("INSERT INTO signals (timestamp, symbol, side, entry, stop_loss, take_profit, timeframe, pattern_type, exchange, chart_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (timestamp, symbol, side, entry, sl, tp, timeframe, pattern_type, exchange, chart_url))
        else:
            cursor.execute("INSERT INTO signals (timestamp, symbol, side, entry, stop_loss, take_profit, timeframe, pattern_type, exchange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (timestamp, symbol, side, entry, sl, tp, timeframe, pattern_type, exchange))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Signal log error: {e}")

def log_performance(capital, peak, drawdown, chart_url=None):
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Check if chart_url column exists
        cursor.execute("PRAGMA table_info(performance)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'chart_url' in columns:
            cursor.execute("INSERT INTO performance (timestamp, capital, peak_capital, drawdown, chart_url) VALUES (?, ?, ?, ?, ?)",
                           (timestamp, capital, peak, drawdown, chart_url))
        else:
            cursor.execute("INSERT INTO performance (timestamp, capital, peak_capital, drawdown) VALUES (?, ?, ?, ?)",
                           (timestamp, capital, peak, drawdown))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Performance logging error: {e}")

def log_exchange_price(exchange, symbol, price, volume):
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO exchange_prices (timestamp, exchange, symbol, price, volume) VALUES (?, ?, ?, ?, ?)",
                       (timestamp, exchange, symbol, price, volume))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Exchange price log error: {e}")

def log_msg(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    trade_logs.append(log_entry)
    if len(trade_logs) > 200:
        trade_logs.pop(0)

# --- MULTI-EXCHANGE DATA FETCHING ---
def fetch_exchange_prices():
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
                    log_exchange_price(exchange_name, symbol, price, volume)
            except Exception as e:
                logger.debug(f"Exchange fetch error: {e}")

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
    except Exception as e:
        logger.debug(f"Exchange {exchange_name} error for {symbol}: {e}")
    return None

def get_best_price(symbol, side='BUY'):
    bitget_price = None
    best_price = None
    best_exchange = PRIMARY_EXCHANGE
    
    if PRIMARY_EXCHANGE in exchange_prices and symbol in exchange_prices[PRIMARY_EXCHANGE]:
        bitget_price = exchange_prices[PRIMARY_EXCHANGE][symbol].get('price', 0)
        if bitget_price and bitget_price > 0:
            best_price = bitget_price
    
    price_comparison = {}
    for exchange_name, prices in exchange_prices.items():
        if symbol in prices and prices[symbol].get('price', 0) > 0:
            price = prices[symbol]['price']
            price_comparison[exchange_name] = price
    
    if bitget_price and bitget_price > 0:
        logger.debug(f"📊 {symbol} Price Comparison: Bitget=${bitget_price}, Others={price_comparison}")
        return bitget_price, PRIMARY_EXCHANGE
    
    return best_price, best_exchange

# --- TELEGRAM HELPER ---
def safe_telegram_request(method, url, **kwargs):
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
    try:
        if photo_buf:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            files = {'photo': ('chart.png', photo_buf, 'image/png')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': message, 'parse_mode': 'Markdown'}
            safe_telegram_request('post', url, files=files, data=data)
        elif cloudinary_url:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            data = {
                'chat_id': TELEGRAM_CHAT_ID,
                'photo': cloudinary_url,
                'caption': message,
                'parse_mode': 'Markdown'
            }
            safe_telegram_request('post', url, data=data)
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
            safe_telegram_request('post', url, json=payload)
    except Exception:
        pass

def send_telegram_alert(message, photo_buf=None, cloudinary_url=None):
    safe_telegram_send(message, photo_buf, cloudinary_url)

# --- ORDER EXECUTION ---
def execute_order(symbol, side, size, exchange=PRIMARY_EXCHANGE, price=None, order_type="market"):
    mode = "PAPER"
    
    if exchange != PRIMARY_EXCHANGE:
        logger.info(f"🔄 Redirecting order to {PRIMARY_EXCHANGE} (primary exchange)")
        exchange = PRIMARY_EXCHANGE
    
    if PAPER_TRADING_MODE:
        logger.info(f"📝 PAPER TRADE: {symbol} {side} {size} @ {price or 'market'} ({exchange})")
        order_id = f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}"
        return True, order_id, mode, exchange
    
    try:
        logger.info(f"🔴 REAL TRADE: {symbol} {side} {size} @ {price or 'market'} ({exchange})")
        order_id = f"REAL_{int(time.time())}_{random.randint(1000, 9999)}"
        mode = "REAL"
        return True, order_id, mode, exchange
    except Exception as e:
        logger.error(f"Real order failed: {e}")
        return False, None, "PAPER", exchange

# --- CHART GENERATORS WITH CLOUDINARY ---
def generate_equity_curve_chart(upload_to_cloud=False):
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("SELECT id, pnl FROM trades ORDER BY id ASC")
        rows = cursor.fetchall()
        conn.close()

        plt.figure(figsize=(6, 3), facecolor='#0d1117')
        ax = plt.axes()
        ax.set_facecolor('#161b22')

        equity = [INITIAL_CAPITAL]
        for r in rows:
            equity.append(equity[-1] + r[1])

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
        logger.error(f"Equity chart generation error: {e}")
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
        logger.error(f"Professional chart generation error: {e}")
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
        logger.error(f"Candle Error ({symbol}): {str(e)}")
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
        logger.error(f"ATR calculation error: {e}")
        return None

def check_circuit_breaker():
    global current_capital, peak_capital, circuit_breaker_active
    if current_capital > peak_capital:
        peak_capital = current_capital
    drawdown = (peak_capital - current_capital) / peak_capital if peak_capital > 0 else 0
    
    chart_buf, cloud_url = generate_equity_curve_chart(upload_to_cloud=True)
    log_performance(current_capital, peak_capital, drawdown, cloud_url)
    
    if drawdown >= 0.20:
        if not circuit_breaker_active:
            circuit_breaker_active = True
            safe_telegram_send("🚨 *CIRCUIT BREAKER TRIGGERED!*\n20% drawdown limit reached.")
            logger.warning(f"CIRCUIT BREAKER ACTIVATED - Drawdown: {drawdown:.2%}")
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
                avg_gain = sum(gains[-14:]) / 14
                avg_loss = sum(losses[-14:]) / 14
                
                if avg_loss > 0:
                    rsi = 100 - (100 / (1 + avg_gain / avg_loss))
                    if rsi > 70:
                        patterns.append({"type": "RSI", "label": f"🔴 RSI Overbought: {rsi:.1f}", "price": current_price})
                    elif rsi < 30:
                        patterns.append({"type": "RSI", "label": f"🟢 RSI Oversold: {rsi:.1f}", "price": current_price})
        
        patterns.append({"type": "CURRENT_PRICE", "label": f"💵 Current: ${current_price:.2f}", "price": current_price})
        
    except Exception as e:
        logger.error(f"Pattern detection error for {symbol}: {e}")
    
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
        logger.error(f"Liquidity sweep detection error: {e}")
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
        logger.error(f"SMC detection error: {e}")
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
        logger.error(f"Trend predictor error: {e}")
    return "NEUTRAL"

def calculate_kelly_position_size():
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM trades WHERE status IN ('WIN', 'LOSS')")
        rows = cursor.fetchall()
        conn.close()

        if len(rows) < 5:
            return 0.10

        wins = sum(1 for r in rows if r[0] == 'WIN')
        total = len(rows)
        win_rate = wins / total
        loss_rate = 1 - win_rate

        kelly = win_rate - (loss_rate / RISK_REWARD_RATIO)
        safe_kelly = max(0.03, min(kelly * 0.3, 0.15))
        return safe_kelly
    except Exception as e:
        logger.error(f"Kelly calculation error: {e}")
        return 0.10

def calculate_safe_position_size(capital, kelly_fraction, atr, current_price):
    if atr <= 0 or current_price <= 0:
        return MIN_POSITION_SIZE
    
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
        logger.error(f"Backtest error: {e}")
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
            logger.error(f"MTF analysis error for {tf}: {e}")
    
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
    global active_trades, current_capital
    for symbol in list(active_trades.keys()):
        try:
            trade = active_trades[symbol]
            df = fetch_candles(symbol, "1H", 10)
            if df is None: 
                continue
            current_price = df['close'].iloc[-1]
            atr = calculate_atr(df)
            if atr is None or pd.isna(atr):
                atr = current_price * 0.01

            side = trade['side']
            entry = trade['entry']
            tp = trade['tp']
            sl = trade['sl']
            size = trade['size']
            order_id = trade.get('order_id', 'PAPER')
            mode = trade.get('mode', 'PAPER')
            exchange = trade.get('exchange', PRIMARY_EXCHANGE)

            if side == "BUY" and (current_price - entry) > (atr * 1.0):
                new_sl = current_price - (atr * 1.5)
                if new_sl > sl: 
                    trade['sl'] = sl = new_sl
                    logger.info(f"{symbol} SL moved to {sl:.2f}")
            elif side == "SELL" and (entry - current_price) > (atr * 1.0):
                new_sl = current_price + (atr * 1.5)
                if new_sl < sl: 
                    trade['sl'] = sl = new_sl
                    logger.info(f"{symbol} SL moved to {sl:.2f}")

            if side == "BUY":
                if current_price >= tp:
                    profit = (tp - entry) * (size / entry)
                    current_capital += profit
                    safe_telegram_send(f"🎯 *{symbol} WIN*\nProfit: `+${profit:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"{symbol} WIN - Profit: ${profit:.2f}")
                elif current_price <= sl:
                    loss = (entry - sl) * (size / entry)
                    current_capital -= loss
                    safe_telegram_send(f"🛑 *{symbol} LOSS*\nLoss: `-${loss:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"{symbol} LOSS - Loss: ${loss:.2f}")
            elif side == "SELL":
                if current_price <= tp:
                    profit = (entry - tp) * (size / entry)
                    current_capital += profit
                    safe_telegram_send(f"🎯 *{symbol} WIN*\nProfit: `+${profit:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"{symbol} WIN - Profit: ${profit:.2f}")
                elif current_price >= sl:
                    loss = (sl - entry) * (size / entry)
                    current_capital -= loss
                    safe_telegram_send(f"🛑 *{symbol} LOSS*\nLoss: `-${loss:.2f}`\nCapital: `${current_capital:.2f}`")
                    log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange)
                    del active_trades[symbol]
                    logger.info(f"{symbol} LOSS - Loss: ${loss:.2f}")
        except Exception as e:
            logger.error(f"Trade management error for {symbol}: {e}")

def analyze_and_trade():
    global active_trades, circuit_breaker_active
    
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
            if atr is None or pd.isna(atr): 
                atr = current_price * 0.01

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
                
                signal_text = (
                    f"🚀 *Signal: {symbol}* 🚀\n\n"
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
            logger.error(f"Analysis error for {symbol}: {e}")

def run_bot():
    logger.info("🤖 Bot thread started - 24/7 Auto Trading")
    while True:
        try:
            manage_trades()
            analyze_and_trade()
            
            socketio.emit('market_update', {
                'timestamp': time.time(),
                'prices': exchange_prices,
                'trades': len(active_trades),
                'capital': current_capital,
                'primary_exchange': PRIMARY_EXCHANGE,
                'cloudinary_enabled': CLOUDINARY_ENABLED
            })
            
            time.sleep(30)
        except Exception as e:
            logger.error(f"Master Loop Error: {e}")
            time.sleep(60)

# --- WEBSOCKET EVENTS ---
@socketio.on('connect')
def handle_connect():
    logger.info(f"WebSocket client connected")
    emit('connected', {'status': 'connected', 'primary_exchange': PRIMARY_EXCHANGE, 'cloudinary_enabled': CLOUDINARY_ENABLED})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info("WebSocket client disconnected")

@socketio.on('request_update')
def handle_update_request():
    emit('market_update', {
        'prices': exchange_prices,
        'trades': len(active_trades),
        'capital': current_capital,
        'status': bot_status,
        'circuit_breaker': circuit_breaker_active,
        'primary_exchange': PRIMARY_EXCHANGE,
        'cloudinary_enabled': CLOUDINARY_ENABLED,
        'cloudinary_urls': cloudinary_urls
    })

# --- FLASK ROUTES ---
@app.route('/')
def index():
    return render_template('index.html', status=bot_status, logs=trade_logs, capital=current_capital)

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
                    "mode": "PAPER" if PAPER_TRADING_MODE else "REAL",
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
                        "mode": "PAPER" if PAPER_TRADING_MODE else "REAL",
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
                        "mode": "PAPER" if PAPER_TRADING_MODE else "REAL",
                        "exchange_prices": exchange_prices,
                        "primary_exchange": PRIMARY_EXCHANGE,
                        "cloudinary_enabled": CLOUDINARY_ENABLED
                    }
        return jsonify(chart_data)
    except Exception as e:
        logger.error(f"Chart data error: {e}")
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
    """Get Cloudinary status and stored images"""
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

@app.route('/cloudinary/charts')
def get_cloudinary_charts():
    """Get all chart URLs from Cloudinary"""
    try:
        if not CLOUDINARY_ENABLED:
            return jsonify({'error': 'Cloudinary not configured'}), 400
        
        equity_charts = get_chart_url_from_cloudinary('equity', limit=20)
        signal_charts = []
        for symbol in SYMBOLS:
            charts = get_chart_url_from_cloudinary('signal', symbol, limit=10)
            signal_charts.extend(charts)
        
        return jsonify({
            'equity_charts': equity_charts,
            'signal_charts': signal_charts,
            'total': len(equity_charts) + len(signal_charts)
        })
    except Exception as e:
        logger.error(f"Cloudinary charts error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/history')
def get_history():
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 50")
        rows = cursor.fetchall()
        trades_list = [dict(row) for row in rows]
        conn.close()
        return jsonify(trades_list)
    except Exception as e:
        logger.error(f"History fetch error: {e}")
        return jsonify([])

@app.route('/status')
def get_status():
    return jsonify({
        "status": bot_status,
        "logs": trade_logs[-50:],
        "capital": current_capital,
        "active_trades": len(active_trades),
        "circuit_breaker": circuit_breaker_active,
        "mode": "PAPER" if PAPER_TRADING_MODE else "REAL",
        "exchanges": list(exchange_prices.keys()),
        "primary_exchange": PRIMARY_EXCHANGE,
        "execution_exchanges": [ex for ex, cfg in EXCHANGES.items() if cfg.get('execution', False)],
        "cloudinary_enabled": CLOUDINARY_ENABLED
    })

@app.route('/performance')
def get_performance():
    try:
        conn = sqlite3.connect('trading_bot.db', check_same_thread=False)
        cursor = conn.cursor()
        # Check if chart_url column exists
        cursor.execute("PRAGMA table_info(performance)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'chart_url' in columns:
            cursor.execute("SELECT timestamp, capital, drawdown, chart_url FROM performance ORDER BY id DESC LIMIT 100")
            rows = cursor.fetchall()
            conn.close()
            return jsonify([{"timestamp": r[0], "capital": r[1], "drawdown": r[2], "chart_url": r[3]} for r in rows])
        else:
            cursor.execute("SELECT timestamp, capital, drawdown FROM performance ORDER BY id DESC LIMIT 100")
            rows = cursor.fetchall()
            conn.close()
            return jsonify([{"timestamp": r[0], "capital": r[1], "drawdown": r[2]} for r in rows])
    except Exception as e:
        logger.error(f"Performance fetch error: {e}")
        return jsonify([])

if __name__ == '__main__':
    logger.info("=== QUANTUM WHALE TERMINAL v5.0 STARTING ===")
    logger.info(f"Initial Capital: ${INITIAL_CAPITAL}")
    logger.info(f"Symbols: {SYMBOLS}")
    logger.info(f"📝 Trading Mode: {'PAPER' if PAPER_TRADING_MODE else 'REAL'}")
    logger.info("📊 Multi-Timeframe Analysis: Enabled (15m, 1H, 4H)")
    logger.info("🛡️ Circuit Breaker: 20% Drawdown Limit")
    logger.info("🔄 Auto-Start: Enabled (24/7)")
    logger.info(f"🌐 Primary Exchange: {PRIMARY_EXCHANGE.upper()} (Execution)")
    logger.info("👀 Monitoring: Binance, Bybit, OKX (Price Reference Only)")
    logger.info(f"☁️ Cloudinary: {'ENABLED' if CLOUDINARY_ENABLED else 'DISABLED'}")
    
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    logger.info("✅ Bot started automatically!")
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
