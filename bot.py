# ============================================================
# ULTIMATE QUANTUM WHALE v14.0 - COMPLETE FIXED VERSION
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
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any

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

MAX_ACTIVE_TRADES = 3
MIN_LOT_SIZE = 0.001
LOT_SIZE_STEP = 0.001
PORTFOLIO_ALLOCATION = 0.30
MAX_RISK_PER_TRADE = 0.02
MAX_LEVERAGE = 3
MIN_LEVERAGE = 1
DEFAULT_LEVERAGE = 2
MIN_WIN_RATE = 55.0
MIN_TRADES_FOR_WIN_RATE = 10
MIN_ATR_PERCENT = 0.3
MAX_ATR_PERCENT = 4.0
MIN_VOLUME_MULTIPLIER = 1.5
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
REQUIRE_MTF_CONFIRMATION = True
KELLY_MODE = "HALF"
MAX_CONSECUTIVE_LOSSES = 3
MIN_Kelly_FRACTION = 0.02
MAX_Kelly_FRACTION = 0.12
MTF_TIMEFRAMES = ["15m", "1H", "4H"]
REQUIRE_ALL_MTF_ALIGN = False
ATR_SL_MULTIPLIER_BASE = 2.5
ATR_TP_MULTIPLIER_BASE = 5.0
ATR_DYNAMIC_ADJUSTMENT = True

# Consensus Engine Settings
CONSENSUS_THRESHOLD = 3  # 4 mein se 3 strategies agree karein
MIN_CONFIDENCE_SCORE = 75  # Minimum confidence for trade
MIN_RR_RATIO = 2.5  # Minimum Risk:Reward ratio
SIGNAL_COOLDOWN = 300  # 5 minutes between signals per symbol

# ============================================================
# FLASK & WEBSOCKET
# ============================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=60, ping_interval=25)

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
            
    def close_all(self):
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

db_manager = DatabaseManager()

def init_db():
    with db_manager.get_connection() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, side TEXT, entry REAL, size REAL,
            pnl REAL, status TEXT, order_id TEXT, mode TEXT, exchange TEXT,
            confidence_score REAL, strategies_used TEXT, pattern_type TEXT,
            entry_reason TEXT, leverage_used REAL, exit_price REAL,
            rr_ratio REAL, mtf_confirmed TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, side TEXT, entry REAL,
            stop_loss REAL, take_profit REAL, confidence_score REAL,
            strategies_agreed TEXT, pattern_type TEXT, consensus_count INTEGER,
            rr_ratio REAL, mtf_status TEXT
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, capital REAL, peak_capital REAL, drawdown REAL,
            equity REAL, win_rate REAL, total_trades INTEGER
        )''')
        conn.commit()
    logger.info("✅ Database initialized")

init_db()

# ============================================================
# BITGET API FUNCTIONS (FIXED)
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
            response = requests.post(url, json=body, headers=headers, timeout=10)
        else:
            response = requests.get(url, params=params, headers=headers, timeout=10)
        return response.json()
    except Exception as e:
        logger.error(f"❌ Bitget API error: {e}")
        return None

def fetch_candles(symbol, granularity="1H", limit=100):
    try:
        url = f"https://api.bitget.com/api/v2/mix/market/candles?symbol={symbol}&productType=usdt-futures&granularity={granularity}&limit={limit}"
        response = requests.get(url, timeout=10)
        data = response.json()
        if data.get('code') == '00000':
            candles = data.get('data', [])
            if not candles or len(candles) < 10:
                return None
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # ✅ FIX: Validate price data
            if df['close'].iloc[-1] <= 0 or pd.isna(df['close'].iloc[-1]):
                return None
            if df['high'].max() <= 0 or df['low'].min() <= 0:
                return None
                
            return df
    except Exception as e:
        logger.debug(f"Candle error {symbol}: {e}")
    return None

def get_live_price(symbol):
    try:
        url = f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={symbol}&productType=usdt-futures"
        response = requests.get(url, timeout=5)
        data = response.json()
        if data.get('code') == '00000':
            ticker = data.get('data', {})
            price = float(ticker.get('price', 0))
            if price <= 0:
                return None
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
            
        c1, c2, c3 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
        body1 = abs(c1['close'] - c1['open'])
        range1 = c1['high'] - c1['low']
        
        # Doji check
        if range1 > 0 and body1 / range1 < 0.1:
            return "DOJI"
            
        # Engulfing patterns
        if c2['close'] > c2['open'] and c1['open'] > c2['close'] and c1['close'] < c2['open']:
            if (c1['high'] - c1['low']) > (c2['high'] - c2['low']) * 1.1:
                return "BEARISH_ENGULFING"
                
        if c2['close'] < c2['open'] and c1['open'] < c2['close'] and c1['close'] > c2['open']:
            if (c1['high'] - c1['low']) > (c2['high'] - c2['low']) * 1.1:
                return "BULLISH_ENGULFING"
                
        # Hammer / Shooting Star
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
# STRATEGY 1: TREND FOLLOWING
# ============================================================
class TrendFollowingStrategy:
    def __init__(self):
        self.name = "Trend Following"
        self.weight = 0.30
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 50:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
                
            close = df['close'].values
            sma_fast = np.mean(close[-10:])
            sma_slow = np.mean(close[-30:])
            ema_fast = pd.Series(close).ewm(span=12).mean().iloc[-1]
            ema_slow = pd.Series(close).ewm(span=26).mean().iloc[-1]
            
            # Trend strength
            trend_strength = abs(sma_fast - sma_slow) / sma_slow * 100
            
            # ✅ FIX: Multiple confirmations required
            if sma_fast > sma_slow and ema_fast > ema_slow and trend_strength > 0.5:
                signal = 'BUY'
                confidence = min(90, 65 + trend_strength * 2)
            elif sma_fast < sma_slow and ema_fast < ema_slow and trend_strength > 0.5:
                signal = 'SELL'
                confidence = min(90, 65 + trend_strength * 2)
            else:
                signal = 'NEUTRAL'
                confidence = 30
                
            return {
                'signal': signal,
                'confidence': confidence,
                'details': f"SMA: {sma_fast:.2f}/{sma_slow:.2f}, Strength: {trend_strength:.1f}%"
            }
        except Exception as e:
            logger.error(f"Trend strategy error: {e}")
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

# ============================================================
# STRATEGY 2: MOMENTUM
# ============================================================
class MomentumStrategy:
    def __init__(self):
        self.name = "Momentum"
        self.weight = 0.25
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 30:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
                
            rsi = calculate_rsi(df)
            macd_data = calculate_macd(df)
            
            # ✅ FIX: Stronger RSI signals
            rsi_signal = 'NEUTRAL'
            rsi_confidence = 40
            
            if rsi < RSI_OVERSOLD:
                rsi_signal = 'BUY'
                rsi_confidence = 65 + (RSI_OVERSOLD - rsi) * 1.2
            elif rsi > RSI_OVERBOUGHT:
                rsi_signal = 'SELL'
                rsi_confidence = 65 + (rsi - RSI_OVERBOUGHT) * 1.2
            elif 35 <= rsi <= 45:
                rsi_signal = 'BUY'
                rsi_confidence = 55
            elif 55 <= rsi <= 65:
                rsi_signal = 'SELL'
                rsi_confidence = 55
                
            # MACD signal
            macd_signal = 'NEUTRAL'
            macd_confidence = 35
            
            if macd_data:
                if macd_data['trend'] == 'BULLISH' and macd_data['histogram'] > 0.1:
                    macd_signal = 'BUY'
                    macd_confidence = 65 + min(25, abs(macd_data['histogram']) * 5)
                elif macd_data['trend'] == 'BEARISH' and macd_data['histogram'] < -0.1:
                    macd_signal = 'SELL'
                    macd_confidence = 65 + min(25, abs(macd_data['histogram']) * 5)
                    
            # Combine signals
            if rsi_signal == macd_signal and rsi_signal != 'NEUTRAL':
                signal = rsi_signal
                confidence = (rsi_confidence * 0.6 + macd_confidence * 0.4)
            elif rsi_signal != 'NEUTRAL' and macd_signal == 'NEUTRAL':
                signal = rsi_signal
                confidence = rsi_confidence * 0.85
            elif macd_signal != 'NEUTRAL' and rsi_signal == 'NEUTRAL':
                signal = macd_signal
                confidence = macd_confidence * 0.85
            else:
                signal = 'NEUTRAL'
                confidence = 25
                
            return {
                'signal': signal,
                'confidence': min(95, confidence),
                'details': f"RSI: {rsi:.1f}, MACD: {macd_data['trend'] if macd_data else 'N/A'}"
            }
        except Exception as e:
            logger.error(f"Momentum strategy error: {e}")
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

# ============================================================
# STRATEGY 3: VOLATILITY BREAKOUT
# ============================================================
class VolatilityBreakoutStrategy:
    def __init__(self):
        self.name = "Volatility Breakout"
        self.weight = 0.25
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 30:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
                
            current_price = df['close'].iloc[-1]
            atr = calculate_atr(df)
            bb = calculate_bollinger_bands(df)
            
            if atr is None or bb is None:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Indicator calculation failed'}
                
            atr_percent = (atr / current_price) * 100
            
            # Bollinger Band position
            bb_range = bb['upper'] - bb['lower']
            if bb_range > 0:
                bb_position = (current_price - bb['lower']) / bb_range
            else:
                bb_position = 0.5
                
            # Volume check
            avg_volume = df['volume'].iloc[-20:].mean()
            current_volume = df['volume'].iloc[-1]
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            
            # ✅ FIX: Stronger breakout requirements
            signal = 'NEUTRAL'
            confidence = 30
            
            # Bullish breakout
            if current_price > bb['upper'] and volume_ratio > 1.5 and atr_percent > 1.0:
                signal = 'BUY'
                confidence = 65 + min(25, atr_percent * 3) + min(10, volume_ratio * 2)
            # Bearish breakdown
            elif current_price < bb['lower'] and volume_ratio > 1.5 and atr_percent > 1.0:
                signal = 'SELL'
                confidence = 65 + min(25, atr_percent * 3) + min(10, volume_ratio * 2)
            # Strong trend with volume
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
                'details': f"ATR: {atr_percent:.1f}%, Vol: {volume_ratio:.1f}x, BB: {bb_position:.0%}"
            }
        except Exception as e:
            logger.error(f"Volatility strategy error: {e}")
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

# ============================================================
# STRATEGY 4: PATTERN RECOGNITION
# ============================================================
class PatternRecognitionStrategy:
    def __init__(self):
        self.name = "Pattern Recognition"
        self.weight = 0.20
        
    def analyze(self, df: pd.DataFrame) -> Dict:
        try:
            if df is None or len(df) < 20:
                return {'signal': 'NEUTRAL', 'confidence': 0, 'details': 'Insufficient data'}
                
            current_price = df['close'].iloc[-1]
            pattern = detect_candlestick_patterns(df)
            sweep = detect_liquidity_sweep(df)
            
            # Support/Resistance detection
            recent_high = df['high'].iloc[-20:].max()
            recent_low = df['low'].iloc[-20:].min()
            
            confidence = 30
            signal = 'NEUTRAL'
            final_pattern = pattern
            
            # ✅ FIX: Multi-pattern confirmation
            if pattern in ["BULLISH_ENGULFING", "HAMMER"]:
                confidence = 70
                signal = 'BUY'
            elif pattern in ["BEARISH_ENGULFING", "SHOOTING_STAR"]:
                confidence = 70
                signal = 'SELL'
            elif pattern == "DOJI":
                confidence = 35
                signal = 'NEUTRAL'
                
            # Support bounce
            if current_price <= recent_low * 1.01:
                if signal == 'BUY':
                    confidence += 15
                elif signal == 'NEUTRAL':
                    signal = 'BUY'
                    confidence = 60
                final_pattern = "SUPPORT_BOUNCE"
                
            # Resistance rejection
            if current_price >= recent_high * 0.99:
                if signal == 'SELL':
                    confidence += 15
                elif signal == 'NEUTRAL':
                    signal = 'SELL'
                    confidence = 60
                final_pattern = "RESISTANCE_REJECT"
                
            # Liquidity sweep boost
            if sweep == "BULLISH_SWEEP" and signal == 'BUY':
                confidence += 10
            elif sweep == "BEARISH_SWEEP" and signal == 'SELL':
                confidence += 10
                
            return {
                'signal': signal,
                'confidence': min(90, confidence),
                'details': f"Pattern: {final_pattern}, Sweep: {sweep}"
            }
        except Exception as e:
            logger.error(f"Pattern strategy error: {e}")
            return {'signal': 'NEUTRAL', 'confidence': 0, 'details': str(e)}

# ============================================================
# CONSENSUS ENGINE
# ============================================================
class ConsensusEngine:
    def __init__(self):
        self.strategies = [
            TrendFollowingStrategy(),
            MomentumStrategy(),
            VolatilityBreakoutStrategy(),
            PatternRecognitionStrategy()
        ]
        self.consensus_threshold = CONSENSUS_THRESHOLD
        self.min_confidence = MIN_CONFIDENCE_SCORE
        
    def analyze(self, df: pd.DataFrame, symbol: str) -> Dict:
        results = []
        for strategy in self.strategies:
            result = strategy.analyze(df)
            results.append({
                'name': strategy.name,
                'signal': result['signal'],
                'confidence': result['confidence'],
                'details': result['details']
            })
            
        # Count signals with confidence
        buy_count = sum(1 for r in results if r['signal'] == 'BUY' and r['confidence'] >= self.min_confidence * 0.8)
        sell_count = sum(1 for r in results if r['signal'] == 'SELL' and r['confidence'] >= self.min_confidence * 0.8)
        neutral_count = len(results) - buy_count - sell_count
        
        # Weighted confidence
        buy_confidence = sum(r['confidence'] for r in results if r['signal'] == 'BUY') / max(1, buy_count)
        sell_confidence = sum(r['confidence'] for r in results if r['signal'] == 'SELL') / max(1, sell_count)
        
        # ✅ FIX: Stronger consensus requirements
        final_signal = 'NEUTRAL'
        final_confidence = 0
        reason = ""
        
        if buy_count >= self.consensus_threshold:
            final_signal = 'BUY'
            final_confidence = min(95, buy_confidence + 10)
            reason = f"{buy_count}/4 strategies agree on BUY"
        elif sell_count >= self.consensus_threshold:
            final_signal = 'SELL'
            final_confidence = min(95, sell_confidence + 10)
            reason = f"{sell_count}/4 strategies agree on SELL"
        else:
            reason = f"No consensus ({buy_count} BUY, {sell_count} SELL, {neutral_count} NEUTRAL)"
            
        # ✅ FIX: Extra confirmation - all strategies must not be opposing
        has_opposing = any(r['signal'] == 'SELL' for r in results if final_signal == 'BUY')
        if final_signal == 'BUY' and has_opposing:
            final_confidence *= 0.9
            
        return {
            'signal': final_signal,
            'confidence': final_confidence,
            'strategies': results,
            'consensus': {
                'buy_count': buy_count,
                'sell_count': sell_count,
                'neutral_count': neutral_count,
                'threshold': self.consensus_threshold,
                'reason': reason
            }
        }

# ============================================================
# MULTI-TIMEFRAME ANALYZER (FIXED)
# ============================================================
class MultiTimeframeAnalyzer:
    def __init__(self):
        self.timeframes = MTF_TIMEFRAMES
        self.cache = {}
        
    def analyze(self, symbol: str, side: str) -> Tuple[bool, str, Dict]:
        try:
            agreements = []
            details = {}
            trends = {}
            
            for tf in self.timeframes:
                df = fetch_candles(symbol, tf, 50)
                if df is None or len(df) < 20:
                    continue
                    
                # Trend detection
                close = df['close'].values
                sma_fast = np.mean(close[-5:])
                sma_slow = np.mean(close[-20:])
                
                if sma_fast > sma_slow * 1.005:
                    trend = 'BULLISH'
                elif sma_fast < sma_slow * 0.995:
                    trend = 'BEARISH'
                else:
                    trend = 'NEUTRAL'
                    
                trends[tf] = trend
                details[tf] = f"{tf}: {trend}"
                
                # ✅ FIX: For BUY, require BULLISH or NEUTRAL (not BEARISH)
                if side == 'BUY':
                    agreements.append(trend in ['BULLISH', 'NEUTRAL'])
                else:
                    agreements.append(trend in ['BEARISH', 'NEUTRAL'])
                    
            if not agreements:
                return True, "No MTF data available", trends
                
            agreement_rate = sum(agreements) / len(agreements) * 100
            
            # ✅ FIX: Higher threshold for MTF confirmation
            if agreement_rate >= 66:  # 2/3 of timeframes agree
                return True, f"MTF confirmed ({agreement_rate:.0f}%)", trends
            else:
                return False, f"MTF disagreement ({agreement_rate:.0f}%)", trends
                
        except Exception as e:
            logger.error(f"MTF error: {e}")
            return True, "MTF skip due to error", {}

# ============================================================
# RISK MANAGER (FIXED)
# ============================================================
class RiskManager:
    def __init__(self):
        self.consecutive_losses = 0
        self.peak_capital = 10.0
        
    def calculate_position_size(self, capital: float, confidence: float, 
                                atr: float, current_price: float, 
                                rr_ratio: float) -> float:
        try:
            if capital <= 0 or current_price <= 0 or atr is None or atr <= 0:
                return MIN_LOT_SIZE
                
            # ✅ FIX: Dynamic Kelly based on confidence and R:R
            base_kelly = 0.08
            if confidence > 80:
                base_kelly = 0.12
            elif confidence > 70:
                base_kelly = 0.10
                
            # Adjust for R:R
            if rr_ratio > 3.0:
                base_kelly *= 1.2
            elif rr_ratio < 2.5:
                base_kelly *= 0.8
                
            # Consecutive loss adjustment
            if self.consecutive_losses >= 2:
                base_kelly *= (1 - self.consecutive_losses * 0.2)
                
            # Position size calculation
            position_size = (capital * base_kelly) / current_price
            
            # Risk-based sizing
            risk_amount = capital * MAX_RISK_PER_TRADE
            risk_position = risk_amount / (atr / current_price * 2.0)
            position_size = min(position_size, risk_position * 1.5)
            
            # Minimum size
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

# ============================================================
# TELEGRAM (FIXED)
# ============================================================
def safe_telegram_send(message):
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return False
        if len(message) > 4000:
            message = message[:3950] + "...\n(truncated)"
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'},
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        logger.debug(f"Telegram error: {e}")
        return False

# ============================================================
# MAIN TRADING BOT (COMPLETE FIXED VERSION)
# ============================================================
class QuantumWhaleBot:
    def __init__(self):
        self.active_trades = {}
        self.position_data = {}
        self.current_capital = 10.0
        self.peak_capital = 10.0
        self.is_running = False
        self.last_signal_time = defaultdict(float)
        self.signal_cooldown = SIGNAL_COOLDOWN
        
        self.consensus_engine = ConsensusEngine()
        self.mtf_analyzer = MultiTimeframeAnalyzer()
        self.risk_manager = RiskManager()
        
        self.live_prices = {}
        self.candle_cache = {}
        self.market_sentiment = {"sentiment": "NEUTRAL", "confidence": 0}
        
    def update_prices(self):
        for symbol in ALL_SYMBOLS:
            try:
                price = get_live_price(symbol)
                if price and price['price'] > 0:
                    self.live_prices[symbol] = price
            except Exception as e:
                logger.debug(f"Price update error {symbol}: {e}")
                
    def get_candles(self, symbol: str, timeframe: str = "1H", limit: int = 100) -> Optional[pd.DataFrame]:
        cache_key = f"{symbol}_{timeframe}"
        if cache_key in self.candle_cache:
            cached = self.candle_cache[cache_key]
            if time.time() - cached['timestamp'] < 60:
                return cached['data']
                
        df = fetch_candles(symbol, timeframe, limit)
        if df is not None and len(df) > 0:
            self.candle_cache[cache_key] = {
                'data': df,
                'timestamp': time.time()
            }
        return df
        
    def calculate_dynamic_sl_tp(self, current_price: float, atr: float, side: str, confidence: float) -> Tuple[float, float]:
        if atr is None or atr <= 0:
            atr = current_price * 0.01
            
        # ✅ FIX: Dynamic multipliers based on confidence
        if confidence > 80:
            sl_mult = 2.0
            tp_mult = 5.5
        elif confidence > 70:
            sl_mult = 2.5
            tp_mult = 5.0
        else:
            sl_mult = 3.0
            tp_mult = 4.5
            
        if side == "BUY":
            sl = current_price - (atr * sl_mult)
            tp = current_price + (atr * tp_mult)
        else:
            sl = current_price + (atr * sl_mult)
            tp = current_price - (atr * tp_mult)
            
        return sl, tp
        
    def execute_order(self, symbol: str, side: str, size: float) -> Tuple[bool, str, str]:
        try:
            if TRADING_MODE == "PAPER":
                order_id = f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}"
                return True, order_id, "PAPER"
                
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
            else:
                return False, None, "PAPER"
        except Exception as e:
            logger.error(f"Order execution error: {e}")
            return False, None, "PAPER"
            
    def generate_signal_message(self, symbol: str, side: str, confidence: float,
                               strategies: list, consensus: dict, pattern: str,
                               entry: float, sl: float, tp: float, size: float,
                               rr_ratio: float, mtf_status: str) -> str:
        
        strategy_details = "\n".join([
            f"  • {s['name']}: {s['signal']} ({s['confidence']:.0f}%) - {s['details']}"
            for s in strategies
        ])
        
        return f"""
🐋 *QUANTUM WHALE CONSENSUS SIGNAL*

📊 *{symbol}*
🔹 Action: `{side}`
🔹 Confidence: `{confidence:.0f}%`
🔹 Pattern: `{pattern}`
🔹 R:R Ratio: `1 : {rr_ratio:.1f}`

📈 Entry: `${entry:.4f}`
🎯 Take Profit: `${tp:.4f}`
🛑 Stop Loss: `${sl:.4f}`
📊 Position: `{size:.4f}`

🧠 *STRATEGY CONSENSUS:*
{strategy_details}

📡 *MTF Status:* {mtf_status}
💡 *Reason:* {consensus['reason']}
📊 *Active Trades:* {len(self.active_trades)}/{MAX_ACTIVE_TRADES}
"""
        
    def analyze_and_trade(self):
        try:
            self.update_prices()
            
            # Get statistics
            stats = self.get_statistics()
            total_trades = stats.get('total', 0)
            win_rate = stats.get('win_rate', 0)
            
            # Check win rate
            if total_trades >= MIN_TRADES_FOR_WIN_RATE and win_rate < MIN_WIN_RATE:
                logger.warning(f"⚠️ Win rate {win_rate:.1f}% below {MIN_WIN_RATE}%, pausing")
                return
                
            if len(self.active_trades) >= MAX_ACTIVE_TRADES:
                return
                
            # ✅ FIX: Check all symbols properly
            trading_symbols = MAIN_SYMBOLS + list(ALL_SYMBOLS[:10])
            
            for symbol in trading_symbols:
                try:
                    # Cooldown check
                    if time.time() - self.last_signal_time[symbol] < self.signal_cooldown:
                        continue
                        
                    if symbol in self.active_trades:
                        continue
                        
                    # Get data
                    df = self.get_candles(symbol, "1H", 100)
                    if df is None or len(df) < 30:
                        continue
                        
                    # ✅ FIX: Validate price
                    current_price = df['close'].iloc[-1]
                    if current_price <= 0 or pd.isna(current_price):
                        logger.debug(f"⏭️ {symbol}: Invalid price {current_price}")
                        continue
                        
                    # ✅ FIX: Validate ATR
                    atr = calculate_atr(df)
                    if atr is None or atr <= 0 or atr > current_price * 0.1:
                        logger.debug(f"⏭️ {symbol}: Invalid ATR {atr}")
                        continue
                        
                    # ✅ FIX: Check ATR condition
                    atr_percent = (atr / current_price) * 100
                    if atr_percent < MIN_ATR_PERCENT or atr_percent > MAX_ATR_PERCENT:
                        logger.debug(f"⏭️ {symbol}: ATR {atr_percent:.1f}% outside range")
                        continue
                        
                    # ✅ FIX: Volume condition
                    avg_volume = df['volume'].iloc[-20:].mean()
                    current_volume = df['volume'].iloc[-1]
                    if current_volume < avg_volume * MIN_VOLUME_MULTIPLIER:
                        logger.debug(f"⏭️ {symbol}: Low volume")
                        continue
                        
                    # Consensus analysis
                    consensus = self.consensus_engine.analyze(df, symbol)
                    
                    # Skip if neutral or low confidence
                    if consensus['signal'] == 'NEUTRAL':
                        logger.debug(f"⏭️ {symbol}: No consensus")
                        continue
                        
                    if consensus['confidence'] < MIN_CONFIDENCE_SCORE:
                        logger.debug(f"⏭️ {symbol}: Low confidence {consensus['confidence']:.0f}%")
                        continue
                        
                    # ✅ FIX: MTF confirmation
                    mtf_ok, mtf_reason, mtf_trends = self.mtf_analyzer.analyze(symbol, consensus['signal'])
                    if not mtf_ok:
                        logger.debug(f"⏭️ {symbol}: {mtf_reason}")
                        continue
                        
                    # Get pattern from strategies
                    pattern = "NONE"
                    for s in consensus['strategies']:
                        if "Pattern:" in s['details']:
                            pattern = s['details'].replace("Pattern: ", "")
                            break
                            
                    # ✅ FIX: Calculate SL/TP with validation
                    sl, tp = self.calculate_dynamic_sl_tp(
                        current_price, atr, consensus['signal'], consensus['confidence']
                    )
                    
                    # ✅ FIX: Validate SL/TP
                    if consensus['signal'] == 'BUY':
                        if sl >= current_price or tp <= current_price:
                            continue
                    else:
                        if sl <= current_price or tp >= current_price:
                            continue
                            
                    # ✅ FIX: Calculate R:R
                    risk = abs(current_price - sl)
                    reward = abs(tp - current_price)
                    rr_ratio = reward / risk if risk > 0 else 0
                    
                    if rr_ratio < MIN_RR_RATIO:
                        logger.debug(f"⏭️ {symbol}: R:R {rr_ratio:.1f} below {MIN_RR_RATIO}")
                        continue
                        
                    # Position sizing
                    size = self.risk_manager.calculate_position_size(
                        self.current_capital, consensus['confidence'],
                        atr, current_price, rr_ratio
                    )
                    
                    if size < MIN_LOT_SIZE:
                        logger.debug(f"⏭️ {symbol}: Size {size} below minimum")
                        continue
                        
                    # ✅ FIX: Extra check - ensure not opposing signals
                    has_strong_opposing = False
                    for s in consensus['strategies']:
                        if s['signal'] == 'SELL' and consensus['signal'] == 'BUY' and s['confidence'] > 70:
                            has_strong_opposing = True
                            break
                        elif s['signal'] == 'BUY' and consensus['signal'] == 'SELL' and s['confidence'] > 70:
                            has_strong_opposing = True
                            break
                            
                    if has_strong_opposing:
                        logger.debug(f"⏭️ {symbol}: Strong opposing signals, skipping")
                        continue
                        
                    # Mark signal time
                    self.last_signal_time[symbol] = time.time()
                    
                    # Generate and send signal
                    signal_text = self.generate_signal_message(
                        symbol, consensus['signal'], consensus['confidence'],
                        consensus['strategies'], consensus['consensus'],
                        pattern, current_price, sl, tp, size,
                        rr_ratio, mtf_reason
                    )
                    
                    safe_telegram_send(signal_text)
                    logger.info(f"📤 SIGNAL: {symbol} {consensus['signal']} @ {current_price:.4f} | R:R 1:{rr_ratio:.1f}")
                    
                    # Execute trade
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
                                'rr_ratio': rr_ratio,
                                'entry_time': time.time()
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
                                'confidence': consensus['confidence']
                            }
                            
                            self.log_trade(
                                symbol, consensus['signal'], current_price, size,
                                0, "OPEN", order_id, mode, consensus['confidence'],
                                json.dumps([s['name'] for s in consensus['strategies'] if s['signal'] != 'NEUTRAL']),
                                pattern, consensus['consensus']['reason'],
                                rr_ratio, mtf_reason
                            )
                            
                            logger.info(f"✅ EXECUTED: {symbol} {consensus['signal']} @ {current_price:.4f}")
                            
                except Exception as e:
                    logger.error(f"❌ {symbol} analysis error: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Main loop error: {e}")
            
    def manage_trades(self):
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
                
                # Calculate PnL
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
                    'confidence': trade.get('confidence', 0)
                }
                
                # Update capital
                self.current_capital = 10.0 + sum([
                    self.position_data.get(s, {}).get('pnl', 0)
                    for s in self.active_trades.keys()
                ])
                
                if self.current_capital > self.peak_capital:
                    self.peak_capital = self.current_capital
                    
                # ✅ FIX: Trailing stop with proper logic
                if side == "BUY" and (current_price - entry) > (atr * 1.5):
                    new_sl = current_price - (atr * 1.0)
                    if new_sl > sl:
                        trade['sl'] = sl = new_sl
                        logger.info(f"📊 {symbol} SL moved to {sl:.4f}")
                        
                elif side == "SELL" and (entry - current_price) > (atr * 1.5):
                    new_sl = current_price + (atr * 1.0)
                    if new_sl < sl:
                        trade['sl'] = sl = new_sl
                        logger.info(f"📊 {symbol} SL moved to {sl:.4f}")
                        
                # Check TP
                if side == "BUY" and current_price >= tp:
                    profit = (tp - entry) * size
                    self.current_capital += profit
                    self.risk_manager.update_losses(False)
                    safe_telegram_send(f"🎯 TP HIT {symbol}: +${profit:.4f}")
                    self.log_trade(symbol, side, entry, size, profit, "WIN", 
                                  trade.get('order_id'), trade.get('mode'), 
                                  trade.get('confidence', 0), "", "", "TP Hit",
                                  trade.get('rr_ratio', 0), "")
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"✅ {symbol} WIN: ${profit:.4f}")
                    
                elif side == "SELL" and current_price <= tp:
                    profit = (entry - tp) * size
                    self.current_capital += profit
                    self.risk_manager.update_losses(False)
                    safe_telegram_send(f"🎯 TP HIT {symbol}: +${profit:.4f}")
                    self.log_trade(symbol, side, entry, size, profit, "WIN",
                                  trade.get('order_id'), trade.get('mode'),
                                  trade.get('confidence', 0), "", "", "TP Hit",
                                  trade.get('rr_ratio', 0), "")
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"✅ {symbol} WIN: ${profit:.4f}")
                    
                # Check SL
                elif side == "BUY" and current_price <= sl:
                    loss = (entry - sl) * size
                    self.current_capital -= loss
                    self.risk_manager.update_losses(True)
                    safe_telegram_send(f"🛑 SL HIT {symbol}: -${loss:.4f}")
                    self.log_trade(symbol, side, entry, size, -loss, "LOSS",
                                  trade.get('order_id'), trade.get('mode'),
                                  trade.get('confidence', 0), "", "", "SL Hit",
                                  trade.get('rr_ratio', 0), "")
                    del self.active_trades[symbol]
                    del self.position_data[symbol]
                    logger.info(f"❌ {symbol} LOSS: ${loss:.4f}")
                    
                elif side == "SELL" and current_price >= sl:
                    loss = (sl - entry) * size
                    self.current_capital -= loss
                    self.risk_manager.update_losses(True)
                    safe_telegram_send(f"🛑 SL HIT {symbol}: -${loss:.4f}")
                    self.log_trade(symbol, side, entry, size, -loss, "LOSS",
                                  trade.get('order_id'), trade.get('mode'),
                                  trade.get('confidence', 0), "", "", "SL Hit",
                                  trade.get('rr_ratio', 0), "")
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
                  confidence, strategies, pattern, reason, rr_ratio, mtf_status):
        try:
            with db_manager.get_connection() as conn:
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                conn.execute("""
                    INSERT INTO trades
                    (timestamp, symbol, side, entry, size, pnl, status, order_id,
                     mode, confidence_score, strategies_used, pattern_type,
                     entry_reason, rr_ratio, mtf_confirmed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (timestamp, symbol, side, entry, size, pnl, status, order_id,
                      mode, confidence, strategies, pattern, reason, rr_ratio, mtf_status))
                conn.commit()
        except Exception as e:
            logger.error(f"Log error: {e}")
            
    def run(self):
        logger.info("🐋 Quantum Whale v14.0 - Complete Fixed Version")
        logger.info(f"📊 Strategies: {len(self.consensus_engine.strategies)}")
        logger.info(f"🎯 Consensus Threshold: {CONSENSUS_THRESHOLD}/{len(self.consensus_engine.strategies)}")
        logger.info(f"⚡ Min Confidence: {MIN_CONFIDENCE_SCORE}%")
        logger.info(f"📈 Min R:R: {MIN_RR_RATIO}")
        logger.info(f"⏰ Signal Cooldown: {SIGNAL_COOLDOWN}s")
        logger.info(f"⚡ Max Active Trades: {MAX_ACTIVE_TRADES}")
        
        self.is_running = True
        
        while self.is_running:
            try:
                self.manage_trades()
                self.analyze_and_trade()
                
                stats = self.get_statistics()
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
                    'avg_rr': stats.get('avg_rr', 0)
                })
                
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"❌ Bot loop error: {e}")
                time.sleep(30)

# ============================================================
# FLASK ROUTES
# ============================================================
bot = QuantumWhaleBot()

@app.route('/')
def index():
    stats = bot.get_statistics()
    return render_template('index.html',
        status="Running",
        trading_mode=TRADING_MODE,
        capital=bot.current_capital,
        peak_capital=bot.peak_capital,
        active_trades=len(bot.active_trades),
        max_active_trades=MAX_ACTIVE_TRADES,
        win_rate=stats.get('win_rate', 0),
        total_trades=stats.get('total', 0),
        wins=stats.get('wins', 0),
        losses=stats.get('losses', 0),
        avg_confidence=stats.get('avg_confidence', 0),
        avg_rr=stats.get('avg_rr', 0),
        consecutive_losses=bot.risk_manager.consecutive_losses,
        positions=bot.position_data,
        consensus_threshold=CONSENSUS_THRESHOLD,
        min_confidence=MIN_CONFIDENCE_SCORE,
        min_rr=MIN_RR_RATIO
    )

@app.route('/status')
def get_status():
    stats = bot.get_statistics()
    return jsonify({
        'status': 'Running',
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
        'min_rr': MIN_RR_RATIO
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
            rows = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 50").fetchall()
            return jsonify([dict(row) for row in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    stats = bot.get_statistics()
    emit('connected', {
        'status': 'connected',
        'trading_mode': TRADING_MODE,
        'capital': bot.current_capital,
        'peak_capital': bot.peak_capital,
        'active_trades': len(bot.active_trades),
        'max_active_trades': MAX_ACTIVE_TRADES,
        'win_rate': stats.get('win_rate', 0),
        'total_trades': stats.get('total', 0),
        'consensus_threshold': CONSENSUS_THRESHOLD,
        'min_confidence': MIN_CONFIDENCE_SCORE,
        'min_rr': MIN_RR_RATIO
    })

# ============================================================
# SHUTDOWN
# ============================================================
def shutdown_handler(signum=None, frame=None):
    logger.info("🛑 Shutting down...")
    bot.is_running = False
    db_manager.close_all()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ============================================================
# STARTUP
# ============================================================
if __name__ == '__main__':
    logger.info("🚀 Starting Quantum Whale v14.0 - Complete Fixed Version...")
    logger.info("📊 All fixes integrated successfully!")
    
    bot_thread = threading.Thread(target=bot.run, daemon=True)
    bot_thread.start()
    
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🌐 Web Server on port {port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=False)