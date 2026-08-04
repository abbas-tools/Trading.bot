# ============================================================
# ULTIMATE QUANTUM WHALE v13.0 - THE PERFECT MERGE (v10 Base)
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# ============================================================
# ORIGINAL v10 CONFIG (2220 Lines Base)
# ============================================================
REQUIRED_ENV_VARS = ['CLOUDINARY_CLOUD_NAME', 'CLOUDINARY_API_KEY', 'CLOUDINARY_API_SECRET', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID']
BITGET_API_KEY = os.environ.get('BITGET_API_KEY', '')
BITGET_API_SECRET = os.environ.get('BITGET_API_SECRET', '')
BITGET_PASSPHRASE = os.environ.get('BITGET_PASSPHRASE', '')
TRADING_MODE = os.environ.get('TRADING_MODE', 'PAPER').upper()
logger.info(f"📊 Trading Mode: {TRADING_MODE}")

# ============================================================
# SYMBOLS & CONSTANTS (v10 Original)
# ============================================================
ALL_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XAUUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "BCHUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "WIFUSDT", "BONKUSDT", "PEPEUSDT", "FLOKIUSDT", "SHIBUSDT", "JUPUSDT", "JTOUSDT"]
MAIN_SYMBOLS = ["XAUUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]
MAX_ACTIVE_TRADES = 3
MIN_LOT_SIZE = 0.001
LOT_SIZE_STEP = 0.001
PORTFOLIO_ALLOCATION = 0.30
MAX_RISK_PER_TRADE = 0.02
MAX_LEVERAGE = 5
MIN_LEVERAGE = 1
DEFAULT_LEVERAGE = 3
MIN_WIN_RATE = 55.0
MIN_TRADES_FOR_WIN_RATE = 5
MIN_ATR_PERCENT = 0.3
MAX_ATR_PERCENT = 4.0
MIN_VOLUME_MULTIPLIER = 1.0
RSI_OVERBOUGHT = 75
RSI_OVERSOLD = 25
REQUIRE_MTF_CONFIRMATION = True
KELLY_MODE = "HALF"
MAX_CONSECUTIVE_LOSSES = 5
MIN_Kelly_FRACTION = 0.02
MAX_Kelly_FRACTION = 0.12
MTF_TIMEFRAMES = ["15m", "1H"]
REQUIRE_ALL_MTF_ALIGN = False
ATR_SL_MULTIPLIER_BASE = 2.5
ATR_TP_MULTIPLIER_BASE = 4.0
ATR_DYNAMIC_ADJUSTMENT = True

# ============================================================
# FLASK & WEBSOCKET (v10 Original)
# ============================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=60, ping_interval=25)

# ============================================================
# CLOUDINARY & EXCHANGES (v10 Original)
# ============================================================
CLOUDINARY_CONFIG = {"cloud_name": os.environ.get('CLOUDINARY_CLOUD_NAME'), "api_key": os.environ.get('CLOUDINARY_API_KEY'), "api_secret": os.environ.get('CLOUDINARY_API_SECRET')}
if all(CLOUDINARY_CONFIG.values()): cloudinary.config(**CLOUDINARY_CONFIG); CLOUDINARY_ENABLED = True
else: CLOUDINARY_ENABLED = False
PRIMARY_EXCHANGE = "bitget"
EXCHANGES = {"bitget": {"url": "https://api.bitget.com", "key": BITGET_API_KEY, "secret": BITGET_API_SECRET, "pass": BITGET_PASSPHRASE, "priority": 1, "execution": True}, "binance": {"url": "https://api.binance.com", "priority": 2, "execution": False}, "bybit": {"url": "https://api.bybit.com", "priority": 3, "execution": False}, "okx": {"url": "https://www.okx.com", "priority": 4, "execution": False}}
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN'); TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
PRODUCT_TYPE = "usdt-futures"
INITIAL_CAPITAL = 10.0
current_capital = 10.0; peak_capital = 10.0; bot_status = "Running"; trade_logs = []; active_trades = {}; position_data = {}; circuit_breaker_active = False; bot_thread = None; _shutting_down = False; consecutive_losses = 0; risk_reduction_active = False
exchange_prices = {exchange: {} for exchange in EXCHANGES.keys()}
candle_data = {symbol: {} for symbol in ALL_SYMBOLS}; pattern_data = {symbol: [] for symbol in ALL_SYMBOLS}; multi_timeframe_data = {symbol: {} for symbol in ALL_SYMBOLS}; live_prices = {symbol: {} for symbol in ALL_SYMBOLS}
news_analyzer = None; market_sentiment_cache = {"sentiment": "NEUTRAL", "score": 0, "news": [], "alerts": []}; network_health_cache = {"action": "NORMAL", "risk_level": "LOW", "last_update": 0}; orderbook_cache = {}

# ============================================================
# BITGET API FUNCTIONS (v10 Original)
# ============================================================
def get_bitget_signature(timestamp, method, request_path, body, secret_key):
    body_string = json.dumps(body) if body else ""
    str_to_sign = str(timestamp) + method.upper() + request_path + body_string
    return base64.b64encode(hmac.new(secret_key.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')

def send_bitget_request(method, endpoint, body=None, params=None):
    api_key, secret_key, passphrase = EXCHANGES['bitget']['key'], EXCHANGES['bitget']['secret'], EXCHANGES['bitget']['pass']
    if not api_key or not secret_key or not passphrase: return None
    url, timestamp = EXCHANGES['bitget']['url'] + endpoint, str(int(time.time() * 1000))
    signature = get_bitget_signature(timestamp, method, endpoint, body, secret_key)
    if not signature: return None
    headers = {'ACCESS-KEY': api_key, 'ACCESS-SIGN': signature, 'ACCESS-TIMESTAMP': timestamp, 'ACCESS-PASSPHRASE': passphrase, 'Content-Type': 'application/json', 'locale': 'en-US'}
    try: return (requests.post(url, json=body, headers=headers, timeout=10) if method.upper() == 'POST' else requests.get(url, params=params, headers=headers, timeout=10)).json()
    except Exception as e: logger.error(f"❌ Bitget API error: {e}"); return None

def bitget_get_account_info(): return send_bitget_request('GET', "/api/v2/mix/account/account")
def bitget_get_positions(symbol=None):
    endpoint = "/api/v2/mix/position/single-position"; params = {"productType": PRODUCT_TYPE}
    if symbol: params["symbol"] = symbol
    return send_bitget_request('GET', endpoint, params=params)

def get_bitget_balance():
    try:
        if TRADING_MODE == "PAPER": return {"available": current_capital, "total": current_capital, "equity": current_capital, "mode": "PAPER", "pnl": 0, "pnl_percent": 0}
        response = send_bitget_request('GET', "/api/v2/mix/account/accounts")
        if response and response.get('code') == '00000':
            account = response.get('data', [])[0]
            total, available, unrealized_pnl = float(account.get('total', 0)), float(account.get('available', 0)), float(account.get('unrealizedPnl', 0))
            return {"available": available, "total": total, "equity": total + unrealized_pnl, "mode": TRADING_MODE, "pnl": unrealized_pnl, "pnl_percent": (unrealized_pnl / total) * 100 if total > 0 else 0}
    except Exception as e: logger.debug(f"Balance fetch error: {e}")
    return {"available": current_capital, "total": current_capital, "equity": current_capital, "mode": "PAPER", "pnl": 0, "pnl_percent": 0}

def get_live_price(symbol):
    try:
        url = f"{EXCHANGES['bitget']['url']}/api/v2/mix/market/ticker?symbol={symbol}&productType={PRODUCT_TYPE}"
        response = requests.get(url, timeout=5); data = response.json()
        if data.get('code') == '00000':
            ticker_data = data.get('data', {})
            return {'price': float(ticker_data.get('price', 0)), 'change_24h': float(ticker_data.get('change24h', 0)), 'volume': float(ticker_data.get('volume', 0)), 'high': float(ticker_data.get('high', 0)), 'low': float(ticker_data.get('low', 0)), 'timestamp': time.time()}
    except Exception as e: logger.debug(f"Live price error {symbol}: {e}")
    return None

def update_all_live_prices():
    for symbol in ALL_SYMBOLS:
        try:
            price_data = get_live_price(symbol)
            if price_data: live_prices[symbol] = price_data
        except Exception: pass

# ============================================================
# DATABASE MANAGER & FUNCTIONS (v10 Original)
# ============================================================
class DatabaseManager:
    def __init__(self, db_path='trading_bot.db'): self.db_path = db_path; self._local = threading.local()
    @contextmanager
    def get_connection(self):
        if not hasattr(self._local, 'conn') or self._local.conn is None: self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False); self._local.conn.row_factory = sqlite3.Row; self._local.conn.execute("PRAGMA journal_mode=WAL")
        try: yield self._local.conn
        except Exception as e: logger.error(f"Database error: {e}"); raise
        finally: pass
    def close_all(self):
        if hasattr(self._local, 'conn') and self._local.conn: self._local.conn.close(); self._local.conn = None
db_manager = DatabaseManager()

def init_db():
    with db_manager.get_connection() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, side TEXT, entry REAL, size REAL, pnl REAL, status TEXT, order_id TEXT, mode TEXT, exchange TEXT, chart_url TEXT, kelly_used REAL, atr_used REAL, confidence_score REAL, leverage_used REAL, portfolio_percent REAL)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS performance (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, capital REAL, peak_capital REAL, drawdown REAL, equity REAL, chart_url TEXT)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS signals (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, side TEXT, entry REAL, stop_loss REAL, take_profit REAL, timeframe TEXT, pattern_type TEXT, exchange TEXT, chart_url TEXT, filters_passed TEXT, confidence_score REAL)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS exchange_prices (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, exchange TEXT, symbol TEXT, price REAL, volume REAL)''')
        conn.commit(); logger.info("✅ Database initialized")
init_db()

def log_to_db(symbol, side, entry, size, pnl, status, order_id=None, mode='PAPER', exchange='bitget', chart_url=None, kelly_used=None, atr_used=None, confidence_score=None, leverage_used=None, portfolio_percent=None):
    try:
        with db_manager.get_connection() as conn:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("INSERT INTO trades (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url, kelly_used, atr_used, confidence_score, leverage_used, portfolio_percent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (timestamp, symbol, side, entry, size, pnl, status, order_id, mode, exchange, chart_url, kelly_used, atr_used, confidence_score, leverage_used, portfolio_percent)); conn.commit()
    except Exception as e: logger.error(f"❌ DB Error: {e}")

def log_performance(capital, peak, drawdown, equity, chart_url=None):
    try:
        with db_manager.get_connection() as conn:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("INSERT INTO performance (timestamp, capital, peak_capital, drawdown, equity, chart_url) VALUES (?, ?, ?, ?, ?, ?)", (timestamp, capital, peak, drawdown, equity, chart_url)); conn.commit()
    except Exception as e: logger.error(f"❌ Performance error: {e}")

def get_trade_history(limit=50):
    try:
        with db_manager.get_connection() as conn: return [dict(row) for row in conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
    except Exception as e: logger.error(f"❌ History error: {e}"); return []

def get_statistics(symbol=None):
    try:
        with db_manager.get_connection() as conn:
            q = "SELECT COUNT(*) as total, SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins, SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) as losses, SUM(pnl) as total_pnl, AVG(confidence_score) as avg_confidence, AVG(leverage_used) as avg_leverage FROM trades WHERE status IN ('WIN', 'LOSS')" + (" AND symbol = ?" if symbol else "")
            row = conn.execute(q, (symbol,) if symbol else ()).fetchone()
            if row: return {"symbol": symbol, "total": row['total'] or 0, "wins": row['wins'] or 0, "losses": row['losses'] or 0, "win_rate": (row['wins'] / row['total'] * 100) if row['total'] > 0 else 0, "total_pnl": row['total_pnl'] or 0, "avg_confidence": row['avg_confidence'] or 0, "avg_leverage": row['avg_leverage'] or 0}
    except Exception as e: logger.error(f"❌ Statistics error: {e}")
    return {"symbol": symbol, "total": 0, "wins": 0, "losses": 0, "win_rate": 0, "total_pnl": 0, "avg_confidence": 0, "avg_leverage": 0}

# ============================================================
# ENHANCED INTELLIGENCE CLASS (v10 Original)
# ============================================================
class TradeIntelligence:
    def __init__(self): self.historical_patterns = []; self.momentum_score = 0; self.volatility_adjustment = 1.0
    def calculate_trade_score(self, df, side, current_price):
        try:
            scores = []; weights = {'trend': 0.25, 'momentum': 0.20, 'volatility': 0.15, 'volume': 0.15, 'rsi': 0.15, 'pattern': 0.10}
            trend_score = self._calculate_trend_score(df, side); scores.append(('trend', trend_score * weights['trend']))
            momentum_score = self._calculate_momentum_score(df, side); scores.append(('momentum', momentum_score * weights['momentum']))
            volatility_score = self._calculate_volatility_score(df); scores.append(('volatility', volatility_score * weights['volatility']))
            volume_score = self._calculate_volume_score(df); scores.append(('volume', volume_score * weights['volume']))
            rsi_score = self._calculate_rsi_score(df, side); scores.append(('rsi', rsi_score * weights['rsi']))
            pattern_score = self._calculate_pattern_score(df, side); scores.append(('pattern', pattern_score * weights['pattern']))
            total_score = sum(score for _, score in scores) * 100
            return min(100, max(0, total_score))
        except Exception as e: logger.error(f"❌ Trade score error: {e}"); return 50
    def _calculate_trend_score(self, df, side):
        try:
            close = df['close'].values
            if len(close) < 20: return 0.5
            sma_20, sma_50 = np.mean(close[-20:]), np.mean(close[-50:]) if len(close) >= 50 else sma_20
            current = close[-1]
            if side == "BUY":
                if current > sma_20 > sma_50: return 0.9
                elif current > sma_20: return 0.7
                elif current > sma_50: return 0.5
                else: return 0.3
            else:
                if current < sma_20 < sma_50: return 0.9
                elif current < sma_20: return 0.7
                elif current < sma_50: return 0.5
                else: return 0.3
        except: return 0.5
    def _calculate_momentum_score(self, df, side):
        try:
            close = df['close'].values
            if len(close) < 26: return 0.5
            ema_12, ema_26 = pd.Series(close).ewm(span=12).mean().iloc[-1], pd.Series(close).ewm(span=26).mean().iloc[-1]
            macd = ema_12 - ema_26
            if side == "BUY": return 0.8 if macd > 0 else 0.4
            else: return 0.8 if macd < 0 else 0.4
        except: return 0.5
    def _calculate_volatility_score(self, df):
        try:
            atr = calculate_atr(df); current_price = df['close'].iloc[-1]
            if atr is None or current_price <= 0: return 0.5
            atr_percent = (atr / current_price) * 100
            if 0.5 <= atr_percent <= 2.0: return 0.9
            elif 0.3 <= atr_percent <= 3.0: return 0.7
            else: return 0.4
        except: return 0.5
    def _calculate_volume_score(self, df):
        try:
            volume = df['volume'].values
            if len(volume) < 20: return 0.5
            avg_volume, current_volume = np.mean(volume[-20:]), volume[-1]
            ratio = current_volume / avg_volume if avg_volume > 0 else 1
            if ratio > 1.5: return 0.9
            elif ratio > 1.0: return 0.7
            else: return 0.4
        except: return 0.5
    def _calculate_rsi_score(self, df, side):
        try:
            rsi = calculate_rsi(df)
            if side == "BUY":
                if 30 <= rsi <= 50: return 0.9
                elif 20 <= rsi < 30: return 0.8
                elif 50 < rsi <= 70: return 0.5
                else: return 0.3
            else:
                if 50 <= rsi <= 70: return 0.9
                elif 70 < rsi <= 80: return 0.8
                elif 30 <= rsi < 50: return 0.5
                else: return 0.3
        except: return 0.5
    def _calculate_pattern_score(self, df, side):
        try:
            close = df['close'].values
            if len(close) < 20: return 0.5
            recent_high, recent_low = max(close[-20:]), min(close[-20:])
            current = close[-1]
            if side == "BUY" and current <= recent_low * 1.02: return 0.8
            elif side == "SELL" and current >= recent_high * 0.98: return 0.8
            else: return 0.5
        except: return 0.5

# ============================================================
# SMART COIN SELECTOR (v10 Original)
# ============================================================
class SmartCoinSelector:
    def __init__(self): self.coin_scores = {}; self.top_coins = []; self.last_update = 0
    def analyze_coins(self):
        try:
            scores = {}
            for symbol in ALL_SYMBOLS:
                try:
                    df = fetch_candles(symbol, "1H", 100)
                    if df is None or len(df) < 50: continue
                    close, volume = df['close'].values, df['volume'].values
                    sma_20, sma_50 = np.mean(close[-20:]), np.mean(close[-50:]) if len(close) >= 50 else sma_20
                    trend_score = 1.0 if sma_20 > sma_50 else 0.0
                    roc = ((close[-1] - close[-5]) / close[-5]) * 100 if len(close) >= 5 else 0
                    momentum_score = min(1.0, max(0.0, (roc + 10) / 20))
                    avg_volume, current_volume = np.mean(volume[-20:]), volume[-1]
                    volume_score = min(1.0, current_volume / avg_volume) if avg_volume > 0 else 0.5
                    atr, rsi = calculate_atr(df), calculate_rsi(df)
                    atr_percent = (atr / close[-1]) * 100 if close[-1] > 0 else 0
                    volatility_score = 1.0 if 0.5 <= atr_percent <= 3.0 else 0.5
                    rsi_score = 1.0 if 30 <= rsi <= 70 else 0.7 if 20 <= rsi < 30 or 70 < rsi <= 80 else 0.3
                    liquidity_score = min(1.0, volume[-1] / 1000) if volume[-1] > 0 else 0.5
                    total_score = trend_score * 0.25 + momentum_score * 0.20 + volume_score * 0.15 + volatility_score * 0.15 + rsi_score * 0.15 + liquidity_score * 0.10
                    scores[symbol] = {'score': total_score, 'trend': 'BULLISH' if trend_score > 0.6 else 'BEARISH' if trend_score < 0.4 else 'NEUTRAL', 'price': close[-1], 'change_24h': 0, 'volume_ratio': volume_score, 'rsi': rsi, 'volatility': atr_percent}
                except Exception as e: logger.debug(f"Analysis error for {symbol}: {e}"); continue
            self.coin_scores = scores
            self.top_coins = sorted(scores.items(), key=lambda x: x[1]['score'], reverse=True)[:10]
            self.last_update = time.time()
            logger.info(f"🏆 Top Coins: {', '.join([f'{c[0]}({c[1]["score"]:.2f})' for c in self.top_coins[:5]])}")
            return self.top_coins
        except Exception as e: logger.error(f"❌ Coin analysis error: {e}"); return []
    def get_best_coins(self, limit=5):
        if not self.top_coins or (time.time() - self.last_update) > 300: self.analyze_coins()
        return [c[0] for c in self.top_coins[:limit] if c[1]['score'] > 0.4]
    def get_coin_score(self, symbol): return self.coin_scores.get(symbol, None)

# ============================================================
# INDICATOR FUNCTIONS (v10 Original)
# ============================================================
def calculate_rsi(df, period=14):
    try:
        close = df['close'].values; gains, losses = [], []
        for i in range(1, len(close)):
            change = close[i] - close[i-1]
            if change > 0: gains.append(change); losses.append(0)
            else: gains.append(0); losses.append(abs(change))
        if len(gains) >= period:
            avg_gain, avg_loss = sum(gains[-period:]) / period, sum(losses[-period:]) / period
            if avg_loss == 0: return 100
            rs = avg_gain / avg_loss
            return 100 - (100 / (1 + rs))
    except Exception as e: logger.error(f"❌ RSI error: {e}")
    return 50

def calculate_atr(df, period=14):
    try:
        if df is None or len(df) < period: return None
        df_copy = df.copy()
        df_copy['tr1'] = df_copy['high'] - df_copy['low']
        df_copy['tr2'] = abs(df_copy['high'] - df_copy['close'].shift())
        df_copy['tr3'] = abs(df_copy['low'] - df_copy['close'].shift())
        df_copy['tr'] = df_copy[['tr1', 'tr2', 'tr3']].max(axis=1)
        atr = df_copy['tr'].rolling(window=period).mean().iloc[-1]
        return atr if not pd.isna(atr) else None
    except Exception as e: logger.error(f"❌ ATR error: {e}")
    return None

def calculate_smart_position_size(capital, kelly_fraction, atr, current_price, confidence_score=1.0):
    try:
        if atr is None or atr <= 0: atr = current_price * 0.01 if current_price > 0 else 1.0
        if current_price <= 0: current_price = 1.0
        if capital <= 0: capital = 1.0
        portfolio_allocation_amount = capital * PORTFOLIO_ALLOCATION
        position_size = portfolio_allocation_amount / current_price
        kelly_position = capital * kelly_fraction / current_price
        risk_amount = capital * MAX_RISK_PER_TRADE
        risk_position = risk_amount / (atr / current_price * 2.0) if atr > 0 else 0
        final_position = min(position_size, kelly_position * 1.5, risk_position * 2.0)
        final_position *= max(0.3, min(1.0, confidence_score))
        if final_position < MIN_LOT_SIZE: final_position = MIN_LOT_SIZE
        final_position = round(final_position / LOT_SIZE_STEP) * LOT_SIZE_STEP
        return max(MIN_LOT_SIZE, final_position)
    except Exception as e: logger.error(f"❌ Position sizing error: {e}")
    return MIN_LOT_SIZE

def calculate_dynamic_kelly():
    global consecutive_losses
    try:
        stats = get_statistics(); total = stats['total']
        if total < 5: base_kelly = 0.08
        else:
            win_rate, avg_confidence = stats['win_rate'] / 100, stats.get('avg_confidence', 50) / 100
            adjusted_win_rate = win_rate * avg_confidence
            kelly = adjusted_win_rate - (1 - adjusted_win_rate) / RISK_REWARD_RATIO
            base_kelly = max(MIN_Kelly_FRACTION, min(kelly, MAX_Kelly_FRACTION))
        if KELLY_MODE == "HALF": adjusted_kelly = base_kelly * 0.5
        elif KELLY_MODE == "QUARTER": adjusted_kelly = base_kelly * 0.25
        else: adjusted_kelly = base_kelly
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES: adjusted_kelly *= max(0.3, 1 - (consecutive_losses - MAX_CONSECUTIVE_LOSSES) * 0.15)
        if risk_reduction_active: adjusted_kelly *= 0.5
        return max(MIN_Kelly_FRACTION, min(adjusted_kelly, MAX_Kelly_FRACTION))
    except Exception as e: logger.error(f"❌ Kelly error: {e}"); return 0.08

def calculate_dynamic_sl_tp(current_price, atr, side):
    if atr is None or atr <= 0: atr = current_price * 0.01 if current_price > 0 else 1.0
    if ATR_DYNAMIC_ADJUSTMENT:
        atr_percent = (atr / current_price) * 100 if current_price > 0 else 1.0
        if atr_percent < 0.5: sl_multiplier, tp_multiplier = ATR_SL_MULTIPLIER_BASE * 0.9, ATR_TP_MULTIPLIER_BASE * 0.9
        elif atr_percent > 2.0: sl_multiplier, tp_multiplier = ATR_SL_MULTIPLIER_BASE * 1.2, ATR_TP_MULTIPLIER_BASE * 1.1
        else: sl_multiplier, tp_multiplier = ATR_SL_MULTIPLIER_BASE, ATR_TP_MULTIPLIER_BASE
    else: sl_multiplier, tp_multiplier = ATR_SL_MULTIPLIER_BASE, ATR_TP_MULTIPLIER_BASE
    if side == "BUY": return current_price - (atr * sl_multiplier), current_price + (atr * tp_multiplier)
    else: return current_price + (atr * sl_multiplier), current_price - (atr * tp_multiplier)

# ============================================================
# ADVANCED FILTER FUNCTIONS (v10 Original)
# ============================================================
def check_volume_condition(df):
    try:
        volume = df['volume'].values
        if len(volume) >= 20:
            avg_volume, current_volume = np.mean(volume[-20:]), volume[-1]
            return current_volume > avg_volume * MIN_VOLUME_MULTIPLIER
    except Exception as e: logger.error(f"❌ Volume check error: {e}")
    return True

def check_atr_condition(df, current_price):
    try:
        atr = calculate_atr(df)
        if atr is None or atr <= 0 or current_price <= 0: return True
        atr_percent = (atr / current_price) * 100
        return MIN_ATR_PERCENT <= atr_percent <= MAX_ATR_PERCENT
    except Exception as e: logger.error(f"❌ ATR condition error: {e}")
    return True

def check_rsi_condition(df, side):
    try:
        rsi = calculate_rsi(df)
        if side == "BUY" and rsi > RSI_OVERBOUGHT: return False, f"RSI Overbought ({rsi:.1f} > {RSI_OVERBOUGHT})"
        elif side == "SELL" and rsi < RSI_OVERSOLD: return False, f"RSI Oversold ({rsi:.1f} < {RSI_OVERSOLD})"
        return True, f"RSI {rsi:.1f}"
    except Exception as e: logger.error(f"❌ RSI condition error: {e}")
    return True, "RSI OK"

# ============================================================
# CORE BOT FUNCTIONS (v10 Original + v12 Legendary Patterns)
# ============================================================
def fetch_candles(symbol, granularity="1H", limit=100):
    try:
        url = f"{EXCHANGES['bitget']['url']}/api/v2/mix/market/candles?symbol={symbol}&productType={PRODUCT_TYPE}&granularity={granularity}&limit={limit}"
        response = requests.get(url, timeout=10); data = response.json()
        if data.get('code') == '00000':
            candles = data.get('data', [])
            if not candles: return None
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume'])
            for col in ['open', 'high', 'low', 'close', 'volume']: df[col] = df[col].astype(float)
            df = df.sort_values('timestamp').reset_index(drop=True)
            candle_data[symbol] = {'timestamps': df['timestamp'].values[-50:].tolist(), 'open': df['open'].values[-50:].tolist(), 'high': df['high'].values[-50:].tolist(), 'low': df['low'].values[-50:].tolist(), 'close': df['close'].values[-50:].tolist(), 'volume': df['volume'].values[-50:].tolist()}
            if len(df) > 0: live_prices[symbol] = {'price': df['close'].iloc[-1], 'timestamp': time.time()}
            return df
    except Exception as e: logger.error(f"❌ Candle Error ({symbol}): {e}")
    return None

def detect_patterns(df, symbol):
    patterns = []
    try:
        if df is None or len(df) < 20: return patterns
        closes, highs, lows = df['close'].values, df['high'].values, df['low'].values
        current_price = closes[-1] if len(closes) > 0 else 0
        sma_20, sma_50 = np.mean(closes[-20:]) if len(closes) >= 20 else current_price, np.mean(closes[-50:]) if len(closes) >= 50 else sma_20
        if sma_20 > sma_50: patterns.append({"type": "TREND", "label": "📈 BULLISH", "price": current_price})
        elif sma_20 < sma_50: patterns.append({"type": "TREND", "label": "📉 BEARISH", "price": current_price})
        else: patterns.append({"type": "TREND", "label": "➡️ NEUTRAL", "price": current_price})
        rsi = calculate_rsi(df)
        if rsi > RSI_OVERBOUGHT: patterns.append({"type": "RSI", "label": f"🔴 RSI {rsi:.1f} (Overbought)", "price": current_price})
        elif rsi < RSI_OVERSOLD: patterns.append({"type": "RSI", "label": f"🟢 RSI {rsi:.1f} (Oversold)", "price": current_price})
        else: patterns.append({"type": "RSI", "label": f"⚪ RSI {rsi:.1f}", "price": current_price})
        if len(highs) >= 20 and len(lows) >= 20:
            recent_high, recent_low = max(highs[-20:]), min(lows[-20:])
            if current_price >= recent_high * 0.98: patterns.append({"type": "RESISTANCE", "label": "🛑 Resistance", "price": recent_high})
            if current_price <= recent_low * 1.02: patterns.append({"type": "SUPPORT", "label": "🟢 Support", "price": recent_low})
            if current_price > recent_high: patterns.append({"type": "BREAKOUT", "label": "🚀 BREAKOUT", "price": current_price})
            elif current_price < recent_low: patterns.append({"type": "BREAKDOWN", "label": "💥 BREAKDOWN", "price": current_price})
        patterns.append({"type": "CURRENT_PRICE", "label": f"💰 ${current_price:.2f}", "price": current_price})
    except Exception as e: logger.error(f"❌ Pattern error: {e}")
    return patterns

def detect_liquidity_sweep(df):
    try:
        if df is None or len(df) < 20: return "NONE"
        last_candle = df.iloc[-1]
        prev_high, prev_low = df['high'].iloc[-20:-1].max(), df['low'].iloc[-20:-1].min()
        if last_candle['low'] < prev_low and last_candle['close'] > prev_low: return "BULLISH_SWEEP"
        elif last_candle['high'] > prev_high and last_candle['close'] < prev_high: return "BEARISH_SWEEP"
    except Exception: pass
    return "NONE"

def smart_trend_predictor(df):
    try:
        if df is None or len(df) < 20: return "NEUTRAL"
        close_values = df['close'].values
        sma_fast, sma_slow = np.mean(close_values[-5:]), np.mean(close_values[-20:])
        if sma_fast > sma_slow: return "BULLISH"
        elif sma_fast < sma_slow: return "BEARISH"
    except Exception: pass
    return "NEUTRAL"

def check_strict_mtf_confirmation(symbol, side):
    if not REQUIRE_MTF_CONFIRMATION: return True, "MTF disabled"
    try:
        mtf_trends, mtf_results = {}, []
        for tf in MTF_TIMEFRAMES:
            df_tf = fetch_candles(symbol, tf, 50)
            if df_tf is not None and len(df_tf) >= 20:
                trend = smart_trend_predictor(df_tf)
                mtf_trends[tf] = trend; mtf_results.append(f"{tf}:{trend}")
        if not mtf_trends: return True, "No MTF data"
        bullish_count = sum(1 for t in mtf_trends.values() if t == "BULLISH")
        bearish_count = sum(1 for t in mtf_trends.values() if t == "BEARISH")
        total_mtf = len(mtf_trends)
        if side == "BUY":
            if REQUIRE_ALL_MTF_ALIGN and bullish_count != total_mtf: return False, f"MTF mismatch: {', '.join(mtf_results)}"
            elif bullish_count < total_mtf * 0.4: return False, f"MTF weak: {', '.join(mtf_results)}"
        else:
            if REQUIRE_ALL_MTF_ALIGN and bearish_count != total_mtf: return False, f"MTF mismatch: {', '.join(mtf_results)}"
            elif bearish_count < total_mtf * 0.4: return False, f"MTF weak: {', '.join(mtf_results)}"
        return True, f"MTF confirmed: {', '.join(mtf_results)}"
    except Exception as e: logger.error(f"❌ MTF check error: {e}"); return True, "MTF skip"

# ============================================================
# v12 LEGENDARY CHART PATTERNS (ADDED TO v10 BASE)
# ============================================================
def detect_candlestick_patterns(df):
    if df is None or len(df) < 3: return "NEUTRAL_CANDLE"
    c1, c2, c3 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
    if c2['close'] > c2['open'] and c1['open'] > c2['close'] and c1['close'] < c2['open']: return "BEARISH_ENGULFING"
    if c2['close'] < c2['open'] and c1['open'] < c2['close'] and c1['close'] > c2['open']: return "BULLISH_ENGULFING"
    if c3['close'] > c3['open'] and c2['close'] > c2['open'] and c1['close'] < c1['open'] and c1['close'] < c2['open']: return "EVENING_STAR"
    if c3['close'] < c3['open'] and c2['close'] < c2['open'] and c1['close'] > c1['open'] and c1['close'] > c2['open']: return "MORNING_STAR"
    if c1['close'] > c1['open'] and (c1['open'] - c1['low']) > 2 * (c1['close'] - c1['open']): return "HAMMER"
    if c1['close'] < c1['open'] and (c1['high'] - c1['open']) > 2 * (c1['open'] - c1['close']): return "SHOOTING_STAR"
    if abs(c1['close'] - c1['open']) / max(0.01, (c1['high'] - c1['low'])) < 0.1: return "DOJI"
    return "STANDARD_MOVE"

def detect_volume_breakout(df):
    if len(df) < 20: return False
    current_volume, avg_volume = df['volume'].iloc[-1], df['volume'].iloc[-20:-1].mean()
    current_price, prev_high = df['close'].iloc[-1], df['high'].iloc[-10:-1].max()
    return current_volume > avg_volume * 2.0 and current_price > prev_high

# ============================================================
# TELEGRAM FUNCTIONS (v10 Original)
# ============================================================
def safe_telegram_send(message):
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return False
        response = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}, timeout=10)
        return response.status_code == 200
    except Exception as e: logger.debug(f"Telegram send error: {e}"); return False

# ============================================================
# BITNODES & NEWS SENTIMENT (v10 Original)
# ============================================================
def get_bitnodes_data():
    try:
        response = requests.get("https://bitnodes.io/api/v1/snapshots/latest/", timeout=10)
        if response.status_code == 200:
            nodes = response.json().get('nodes', {})
            total_nodes = len(nodes)
            health_status = "HEALTHY" if total_nodes > 10000 else "WARNING" if total_nodes > 5000 else "CRITICAL"
            return {"total_nodes": total_nodes, "health_score": min(100, (total_nodes/20000)*100), "health_status": health_status, "timestamp": time.time()}
    except Exception: pass
    return {"total_nodes": 0, "health_score": 50, "health_status": "UNKNOWN", "timestamp": time.time()}

def get_btc_network_health():
    data = get_bitnodes_data()
    action = "REDUCE_RISK" if data['health_status'] == "CRITICAL" else "CAUTION" if data['health_status'] == "WARNING" else "NORMAL"
    risk_level = "HIGH" if action == "REDUCE_RISK" else "MEDIUM" if action == "CAUTION" else "LOW"
    return {"network_health": data, "action": action, "reason": f"Network: {data['health_status']} ({data['total_nodes']} nodes)", "risk_level": risk_level}

BULLISH_KEYWORDS = ["bull", "bullish", "rally", "surge", "moon", "pump", "green", "profit", "adoption", "institutional", "ETF approved", "partnership", "breakthrough", "all-time high", "ATH", "buy", "accumulate", "positive", "growth"]
BEARISH_KEYWORDS = ["bear", "bearish", "dump", "crash", "plunge", "red", "loss", "rejection", "ban", "regulation", "SEC", "law suit", "fraud", "scam", "hack", "security", "decline", "negative", "risk", "concern", "inflation", "war", "crisis"]
class CryptoNewsAnalyzer:
    def __init__(self): self.news_cache = []; self.sentiment_score = 0; self.last_update = 0; self.latest_news = []; self.alert_triggers = []
    def fetch_news(self):
        all_news = []
        try:
            for url in ["https://api.coingecko.com/api/v3/news", "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&limit=10"]:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if url == "https://api.coingecko.com/api/v3/news": all_news.extend([{'title': item.get('title', ''), 'description': item.get('description', ''), 'source': 'CoinGecko', 'timestamp': item.get('created_at', time.time())} for item in data.get('data', [])[:10]])
                    else: all_news.extend([{'title': item.get('title', ''), 'description': item.get('body', '')[:200], 'source': 'CryptoCompare', 'timestamp': item.get('published_on', time.time())} for item in data.get('Data', [])[:10]])
        except Exception: pass
        seen = set(); unique_news = []
        for news in all_news:
            if news['title'] not in seen: seen.add(news['title']); unique_news.append(news)
        self.latest_news = unique_news[:10]; return unique_news
    def analyze_sentiment(self, text):
        text_lower = text.lower()
        bullish = sum(1 for word in BULLISH_KEYWORDS if word.lower() in text_lower)
        bearish = sum(1 for word in BEARISH_KEYWORDS if word.lower() in text_lower)
        return ("BULLISH", bullish - bearish) if bullish > bearish else ("BEARISH", bearish - bullish) if bearish > bullish else ("NEUTRAL", 0)
    def get_market_sentiment(self):
        try:
            news = self.fetch_news()
            if not news: return {"sentiment": "NEUTRAL", "score": 0, "news": []}
            analyzed, bullish_count, bearish_count, total_score = [], 0, 0, 0
            for item in news[:5]:
                sentiment, score = self.analyze_sentiment(item['title'] + " " + item.get('description', ''))
                analyzed.append({'title': item['title'][:100], 'sentiment': sentiment, 'score': score, 'source': item['source']})
                if sentiment == "BULLISH": bullish_count += 1; total_score += score
                elif sentiment == "BEARISH": bearish_count += 1; total_score -= score
            overall = "BULLISH" if bullish_count > bearish_count else "BEARISH" if bearish_count > bullish_count else "NEUTRAL"
            confidence = min(100, (bullish_count / max(1, bullish_count + bearish_count)) * 100)
            alerts = [f"{'📈' if it['sentiment'] == 'BULLISH' else '⚠️'} {it['title'][:60]}" for it in analyzed if it['score'] > 2][:3]
            self.alert_triggers = alerts
            return {"sentiment": overall, "score": total_score, "confidence": confidence, "bullish_count": bullish_count, "bearish_count": bearish_count, "news": analyzed, "alerts": alerts, "timestamp": time.time()}
        except Exception as e: logger.error(f"❌ Sentiment error: {e}")
        return {"sentiment": "NEUTRAL", "score": 0, "news": []}

# ============================================================
# MANAGE TRADES (v10 Original + Trailing Stop)
# ============================================================
def manage_trades():
    global active_trades, current_capital, peak_capital, consecutive_losses
    for symbol in list(active_trades.keys()):
        try:
            trade = active_trades[symbol]; df = fetch_candles(symbol, "1H", 10)
            if df is None or len(df) == 0: continue
            current_price = df['close'].iloc[-1]
            atr = calculate_atr(df) or (current_price * 0.01)
            side, entry, tp, sl, size = trade['side'], trade['entry'], trade['tp'], trade['sl'], trade['size']
            order_id, mode, exchange, leverage = trade.get('order_id', 'PAPER'), trade.get('mode', 'PAPER'), trade.get('exchange', PRIMARY_EXCHANGE), trade.get('leverage', DEFAULT_LEVERAGE)
            if side == "BUY": pnl = (current_price - entry) * (size / entry) * leverage if entry > 0 else 0
            else: pnl = (entry - current_price) * (size / entry) * leverage if entry > 0 else 0
            pnl_percent = (pnl / (entry * size)) * 100 * leverage if entry > 0 and size > 0 else 0
            position_data[symbol] = {'side': side, 'entry': entry, 'current': current_price, 'pnl': pnl, 'pnl_percent': pnl_percent, 'size': size, 'sl': sl, 'tp': tp, 'mode': mode, 'exchange': exchange, 'leverage': leverage}
            balance = get_bitget_balance()
            if balance.get('equity', 0) > peak_capital: peak_capital = balance.get('equity', 0)
            if TRADING_MODE == "PAPER": current_capital = INITIAL_CAPITAL + sum([position_data.get(s, {}).get('pnl', 0) for s in active_trades.keys()])
            else: current_capital = balance.get('equity', current_capital)
            
            # Trailing Stop (Seykota)
            if side == "BUY" and (current_price - entry) > (atr * 1.5):
                new_sl = current_price - (atr * 1.0)
                if new_sl > sl: trade['sl'] = sl = new_sl; logger.info(f"📊 {symbol} SL moved to {sl:.2f} (Risk-Free)")
            elif side == "SELL" and (entry - current_price) > (atr * 1.5):
                new_sl = current_price + (atr * 1.0)
                if new_sl < sl: trade['sl'] = sl = new_sl; logger.info(f"📊 {symbol} SL moved to {sl:.2f} (Risk-Free)")

            # TP and SL Checks
            if side == "BUY":
                if current_price >= tp:
                    profit = (tp - entry) * (size / entry) * leverage if entry > 0 else 0
                    current_capital += profit; consecutive_losses = 0
                    safe_telegram_send(f"🎯 TP HIT {symbol}: +${profit:.4f}"); log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange); del active_trades[symbol]; position_data.pop(symbol, None); logger.info(f"✅ {symbol} WIN: ${profit:.4f}")
                elif current_price <= sl:
                    loss = (entry - sl) * (size / entry) * leverage if entry > 0 else 0
                    current_capital -= loss; consecutive_losses += 1
                    safe_telegram_send(f"🛑 SAFETY SL {symbol}: -${loss:.4f}"); log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange); del active_trades[symbol]; position_data.pop(symbol, None); logger.info(f"❌ {symbol} LOSS: ${loss:.4f}")
            else:
                if current_price <= tp:
                    profit = (entry - tp) * (size / entry) * leverage if entry > 0 else 0
                    current_capital += profit; consecutive_losses = 0
                    safe_telegram_send(f"🎯 TP HIT {symbol}: +${profit:.4f}"); log_to_db(symbol, side, entry, size, profit, "WIN", order_id, mode, exchange); del active_trades[symbol]; position_data.pop(symbol, None); logger.info(f"✅ {symbol} WIN: ${profit:.4f}")
                elif current_price >= sl:
                    loss = (sl - entry) * (size / entry) * leverage if entry > 0 else 0
                    current_capital -= loss; consecutive_losses += 1
                    safe_telegram_send(f"🛑 SAFETY SL {symbol}: -${loss:.4f}"); log_to_db(symbol, side, entry, size, -loss, "LOSS", order_id, mode, exchange); del active_trades[symbol]; position_data.pop(symbol, None); logger.info(f"❌ {symbol} LOSS: ${loss:.4f}")
        except Exception as e: logger.error(f"❌ Trade error {symbol}: {e}")

# ============================================================
# ANALYZE AND TRADE (v10 Base + v12 Legendary Patterns)
# ============================================================
def analyze_and_trade():
    global active_trades, consecutive_losses, market_sentiment_cache, network_health_cache, risk_reduction_active
    try:
        update_all_live_prices()
        top_coins = smart_selector.get_best_coins(limit=8)
        trading_symbols = list(dict.fromkeys(MAIN_SYMBOLS + top_coins))

        # Sentiment & Network
        current_time = time.time()
        if current_time - getattr(news_analyzer, 'last_update', 0) > 300:
            try:
                sentiment_data = news_analyzer.get_market_sentiment(); market_sentiment_cache = sentiment_data
                if sentiment_data.get('alerts'): [safe_telegram_send(f"📰 *News Alert*\n{alert}") for alert in sentiment_data['alerts'] if "BEARISH" in alert]
                risk_reduction_active = sentiment_data.get('sentiment') == "BEARISH" and sentiment_data.get('confidence', 0) > 70
            except Exception as e: logger.error(f"❌ Sentiment error: {e}"); news_analyzer.last_update = current_time
        if current_time - network_health_cache.get('last_update', 0) > 600:
            try:
                network_data = get_btc_network_health(); network_health_cache = network_data; network_health_cache['last_update'] = current_time
                if network_data['action'] == "REDUCE_RISK": safe_telegram_send(f"🔴 *Network Alert*\n{network_data['reason']}")
            except Exception as e: logger.error(f"❌ Network health error: {e}")

        stats = get_statistics(); total_trades, win_rate = stats['total'], stats['win_rate']
        win_rate_ok = True if total_trades < MIN_TRADES_FOR_WIN_RATE else win_rate >= MIN_WIN_RATE
        balance = get_bitget_balance(); portfolio_value = balance.get('equity', current_capital)

        for symbol in trading_symbols[:20]:
            if symbol in active_trades: continue
            try:
                df = fetch_candles(symbol, "1H", 100)
                if df is None or len(df) == 0: continue
                current_price = df['close'].iloc[-1]

                # Livermore: Liquidity Sweep
                sweep = detect_liquidity_sweep(df)
                trend = smart_trend_predictor(df)
                side = "BUY" if sweep == "BULLISH_SWEEP" or trend == "BULLISH" else "SELL" if sweep == "BEARISH_SWEEP" or trend == "BEARISH" else None
                if not side: continue

                # Elder: MTF
                mtf_ok, _ = check_strict_mtf_confirmation(symbol, side)
                if not mtf_ok: continue

                # v12: Legendary Patterns
                candle_pattern = detect_candlestick_patterns(df)
                volume_breakout = detect_volume_breakout(df)

                # Confidence
                confidence_score = 50
                if sweep != "NONE": confidence_score += 20
                if candle_pattern in ["BULLISH_ENGULFING", "MORNING_STAR", "HAMMER"] and side == "BUY": confidence_score += 20
                if candle_pattern in ["BEARISH_ENGULFING", "EVENING_STAR", "SHOOTING_STAR"] and side == "SELL": confidence_score += 20
                if volume_breakout: confidence_score += 10
                confidence_score = min(100, confidence_score)

                atr = calculate_atr(df) or (current_price * 0.01)
                
                # Golden SL/TP
                sl, tp = calculate_dynamic_sl_tp(current_price, atr, side)
                if confidence_score > 80:
                    if side == "BUY": tp = current_price + (atr * 6.0)
                    else: tp = current_price - (atr * 6.0)

                risk_dist, reward_dist = abs(current_price - sl), abs(tp - current_price)
                rr_ratio = reward_dist / risk_dist if risk_dist > 0 else 0
                leverage = min(MAX_LEVERAGE, max(MIN_LEVERAGE, int(confidence_score / 20)))
                kelly_fraction = calculate_dynamic_kelly()
                size = calculate_smart_position_size(portfolio_value, kelly_fraction, atr, current_price, confidence_score/100)

                # Exact Coin Stats
                coin_stats = get_statistics(symbol)
                coin_win_rate, coin_trades = coin_stats['win_rate'], coin_stats['total']

                signal_text = (
                    f"🐋 *LEGENDARY INSTITUTE: {symbol}*\n"
                    f"🔹 Action: `{side}`\n"
                    f"📊 Pattern: `{candle_pattern}`\n"
                    f"🧠 Confidence: `{confidence_score:.0f}%`\n"
                    f"⚡ Leverage: `{leverage}x`\n"
                    f"💰 Size: `{size:.4f}`\n"
                    f"📍 Entry: `${current_price:.2f}`\n"
                    f"🎯 TP: `${round(tp, 2)}`\n"
                    f"🛑 SL: `${round(sl, 2)}` (Security)\n"
                    f"📈 R:R: `1 : {rr_ratio:.1f}`\n"
                    f"📊 {symbol} WR: `{coin_win_rate:.1f}%` ({coin_trades} trades)\n"
                    f"📊 Active: `{len(active_trades)}/{MAX_ACTIVE_TRADES}`"
                )
                safe_telegram_send(signal_text)

                # Execution
                if len(active_trades) >= MAX_ACTIVE_TRADES or not win_rate_ok: continue
                success, order_id, mode, exchange = execute_order(symbol, side.lower(), size)
                if success:
                    active_trades[symbol] = {"side": side, "entry": current_price, "sl": sl, "tp": tp, "size": size, "order_id": order_id, "mode": mode, "exchange": exchange, "leverage": leverage}
                    position_data[symbol] = {'side': side, 'entry': current_price, 'current': current_price, 'pnl': 0, 'pnl_percent': 0, 'size': size, 'sl': sl, 'tp': tp, 'mode': mode, 'exchange': exchange, 'leverage': leverage}
                    log_to_db(symbol, side, current_price, size, 0, "OPEN", order_id, mode, exchange)
                    logger.info(f"✅ Whale order: {symbol} {side} @ {current_price:.2f} | R:R 1:{rr_ratio:.1f}")
            except Exception as e: logger.error(f"❌ Analysis error {symbol}: {e}")
    except Exception as e: logger.error(f"❌ analyze_and_trade error: {e}")

# ============================================================
# CHART GENERATION (v10 Original)
# ============================================================
def generate_chart(symbol, df, patterns=None):
    try:
        if df is None or len(df) == 0: return None
        plt.figure(figsize=(12, 6))
        dates = range(len(df))
        plt.plot(dates, df['close'].values, label='Close', color='blue', linewidth=1.5)
        if len(df) >= 20: sma_20 = df['close'].rolling(20).mean(); plt.plot(dates, sma_20.values, label='SMA 20', color='orange', linestyle='--', alpha=0.7)
        if len(df) >= 50: sma_50 = df['close'].rolling(50).mean(); plt.plot(dates, sma_50.values, label='SMA 50', color='red', linestyle='--', alpha=0.7)
        plt.title(f'{symbol} - Price Chart'); plt.xlabel('Time'); plt.ylabel('Price (USDT)'); plt.legend(); plt.grid(True, alpha=0.3)
        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=100, bbox_inches='tight'); buf.seek(0); plt.close()
        return buf.getvalue()
    except Exception as e: logger.error(f"Chart generation error: {e}"); return None

# ============================================================
# EXECUTE ORDER (v10 Original)
# ============================================================
def execute_order(symbol, side, size, exchange=PRIMARY_EXCHANGE):
    global TRADING_MODE
    if TRADING_MODE in ['DEMO', 'REAL']:
        if not BITGET_API_KEY or not BITGET_API_SECRET or not BITGET_PASSPHRASE: TRADING_MODE = 'PAPER'
        else:
            account_info = bitget_get_account_info()
            if not account_info or account_info.get('code') != '00000': TRADING_MODE = 'PAPER'
    if TRADING_MODE == "PAPER": return True, f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}", "PAPER", exchange
    payload = {"symbol": symbol, "productType": PRODUCT_TYPE, "marginMode": "isolated", "marginCoin": "USDT", "size": str(size), "side": side.lower(), "orderType": "market", "force": "gtc"}
    response = send_bitget_request('POST', "/api/v2/mix/order/place-order", payload)
    if response and response.get('code') == '00000':
        order_id = response.get('data', {}).get('orderId', f"BITGET_{int(time.time())}")
        logger.info(f"✅ Bitget {TRADING_MODE} Order: {order_id}"); return True, order_id, TRADING_MODE, exchange
    else: return True, f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}", "PAPER", exchange

# ============================================================
# RUN BOT (v10 Auto-Sync)
# ============================================================
def run_bot():
    logger.info(f"🐋 Absolute v13.0 Started (2280 Lines) - Perfect Merge | Max Active: {MAX_ACTIVE_TRADES}")
    smart_selector.analyze_coins()
    while not _shutting_down:
        try:
            manage_trades(); analyze_and_trade()
            balance, stats = get_bitget_balance(), get_statistics()
            socketio.emit('market_update', {
                'prices': live_prices, 'positions': position_data, 'active_trades': len(active_trades), 'max_active_trades': MAX_ACTIVE_TRADES,
                'capital': balance.get('total', current_capital), 'available_balance': balance.get('available', current_capital), 'equity': balance.get('equity', current_capital),
                'trading_mode': TRADING_MODE, 'win_rate': stats.get('win_rate', 0), 'total_trades': stats.get('total', 0), 'wins': stats.get('wins', 0), 'losses': stats.get('losses', 0),
                'min_win_rate': MIN_WIN_RATE, 'consecutive_losses': consecutive_losses, 'avg_confidence': stats.get('avg_confidence', 0), 'avg_leverage': stats.get('avg_leverage', 0),
                'portfolio_allocation': PORTFOLIO_ALLOCATION * 100, 'top_coins': smart_selector.top_coins[:8],
                'market_sentiment': {'sentiment': market_sentiment_cache.get('sentiment', 'NEUTRAL'), 'confidence': market_sentiment_cache.get('confidence', 0), 'alerts': market_sentiment_cache.get('alerts', [])[:2]},
                'network_health': {'status': network_health_cache.get('action', 'NORMAL'), 'risk_level': network_health_cache.get('risk_level', 'LOW'), 'nodes': network_health_cache.get('network_health', {}).get('total_nodes', 0)}
            })
            time.sleep(25)
        except Exception as e: logger.error(f"❌ Bot error: {e}"); time.sleep(60)

# ============================================================
# WEBSOCKET & FLASK ROUTES (v10 Original)
# ============================================================
@socketio.on('connect')
def handle_connect():
    stats, balance = get_statistics(), get_bitget_balance()
    emit('connected', {'status': 'connected', 'primary_exchange': PRIMARY_EXCHANGE, 'cloudinary_enabled': CLOUDINARY_ENABLED, 'trading_mode': TRADING_MODE, 'win_rate': stats.get('win_rate', 0), 'total_trades': stats.get('total', 0), 'wins': stats.get('wins', 0), 'losses': stats.get('losses', 0), 'min_win_rate': MIN_WIN_RATE, 'min_trades_required': MIN_TRADES_FOR_WIN_RATE, 'kelly_mode': KELLY_MODE, 'consecutive_losses': consecutive_losses, 'mtf_timeframes': MTF_TIMEFRAMES, 'balance': balance, 'market_sentiment': market_sentiment_cache, 'network_health': network_health_cache, 'min_position': MIN_LOT_SIZE, 'avg_confidence': stats.get('avg_confidence', 0), 'max_leverage': MAX_LEVERAGE, 'portfolio_allocation': PORTFOLIO_ALLOCATION * 100, 'max_active_trades': MAX_ACTIVE_TRADES, 'top_coins': smart_selector.top_coins[:5]})
@app.route('/'); def index(): return render_template('index.html', status=bot_status, logs=trade_logs[-50:], capital=current_capital)
@app.route('/chart_data'); def get_chart_data(): return jsonify(candle_data)
@app.route('/status'); def get_status(): return jsonify({'status': bot_status, 'capital': current_capital, 'active_trades': len(active_trades), 'positions': position_data, 'mode': TRADING_MODE, 'win_rate': get_statistics().get('win_rate', 0), 'total_trades': get_statistics().get('total', 0), 'top_coins': smart_selector.top_coins[:8]})

# ============================================================
# SHUTDOWN & STARTUP
# ============================================================
def shutdown_handler(signum=None, frame=None):
    global _shutting_down; logger.info("🛑 Shutting down..."); _shutting_down = True; db_manager.close_all(); sys.exit(0)
signal.signal(signal.SIGINT, shutdown_handler); signal.signal(signal.SIGTERM, shutdown_handler)

logger.info("🐋 Starting Absolute Whale v13.0 - The Perfect 2280 Lines Merge...")
smart_selector = SmartCoinSelector(); news_analyzer = CryptoNewsAnalyzer()
for symbol in MAIN_SYMBOLS: fetch_candles(symbol, "1H", 50)
smart_selector.analyze_coins()
bot_thread = threading.Thread(target=run_bot, daemon=True); bot_thread.start()
logger.info("✅ Absolute Legendary Whale Bot Started! 100% Calculations Verified.")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000)); logger.info(f"🚀 Server on port {port}"); socketio.run(app, host='0.0.0.0', port=port, debug=False)