# forecast.py
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import json
import aiohttp
from dataclasses import dataclass, asdict
from enum import Enum
from collections import deque
from aiohttp import web
import time
import yaml
import os
from aiohttp import ClientTimeout
from aiohttp.connector import TCPConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrendDirection(Enum):
BULLISH = "bullish"
BEARISH = "bearish"
NEUTRAL = "neutral"
HIGH_VOLATILITY = "high_volatility"

class SignalStrength(Enum):
WEAK = "weak"
MODERATE = "moderate"
STRONG = "strong"
VERY_STRONG = "very_strong"

@dataclass
class ArbitrageOpportunity:
symbol: str
buy_exchange: str
sell_exchange: str
buy_price: float
sell_price: float
spread_abs: float
spread_percent: float
potential_profit: float
confidence: float
timestamp: str

@dataclass
class PriceForecast:
symbol: str
direction: TrendDirection
confidence: float
target_price: float
stop_loss: float
timeframe: str
signal_strength: SignalStrength
reasoning: List[str]
timestamp: str

@dataclass
class MarketAnalysis:
symbol: str
current_volatility: float
volume_trend: str
support_level: float
resistance_level: float
rsi_signal: str
momentum: float
market_sentiment: str

class ForecastEngine:
def __init__(self, config_path: str = "config/forecast_config.yaml"):
self.config_path = config_path
self.config = self.load_config()
self.collector_url = self.config['collector_url']

self.analysis_cache = {}
self.historical_data = {}
self.arbitrage_history = deque(maxlen=self.config.get('max_history_size', 1000))
self.forecast_history = deque(maxlen=self.config.get('max_history_size', 1000))

self.session = None
self._forecast_task = None
self._is_running = False
self._cleanup_task = None

# Лимиты соединений
self.connector = TCPConnector(limit=self.config.get('max_connections', 10),
limit_per_host=self.config.get('max_connections_per_host', 5))
self.timeout = ClientTimeout(total=self.config.get('request_timeout', 30))

def load_config(self) -> Dict:
"""Загрузка конфигурации из YAML файла или переменных окружения"""
default_config = {
'collector_url': os.getenv('COLLECTOR_URL', 'http://localhost:8080'),
'host': os.getenv('FORECAST_HOST', '0.0.0.0'),
'port': int(os.getenv('FORECAST_PORT', '8081')),
'min_arbitrage_spread': 0.02,
'max_position_size': 1000,
'volatility_threshold': 5.0,
'confidence_threshold': 0.7,
'update_interval': 5,
'max_retries': 3,
'retry_delay': 5,
'max_connections': 10,
'max_connections_per_host': 5,
'request_timeout': 30,
'max_history_size': 1000,
'data_cleanup_interval': 3600, # 1 hour
'max_historical_points': 500
}

# Пытаемся загрузить из YAML
try:
if os.path.exists(self.config_path):
with open(self.config_path, 'r') as f:
yaml_config = yaml.safe_load(f) or {}
# Обновляем дефолтные значения из YAML
default_config.update(yaml_config)
logger.info(f"Configuration loaded from {self.config_path}")
except Exception as e:
logger.warning(f"Failed to load YAML config: {e}. Using defaults.")

# Переменные окружения имеют высший приоритет
for key in default_config:
env_key = f"FORECAST_{key.upper()}"
if env_key in os.environ:
if isinstance(default_config[key], int):
default_config[key] = int(os.environ[env_key])
elif isinstance(default_config[key], float):
default_config[key] = float(os.environ[env_key])
else:
default_config[key] = os.environ[env_key]

logger.info(f"Final configuration: { {k: v for k, v in default_config.items() if 'url' not in k.lower()} }")
return default_config

async def init_session(self):
"""Инициализация aiohttp сессии с ограничениями"""
if not self.session:
self.session = aiohttp.ClientSession(
connector=self.connector,
timeout=self.timeout
)

async def cleanup_old_data(self):
"""Периодическая очистка устаревших исторических данных"""
while self._is_running:
try:
current_time = time.time()
cleanup_threshold = current_time - self.config.get('data_cleanup_interval', 3600)

# Очищаем исторические данные, которые не обновлялись давно
symbols_to_remove = []
for symbol, data_deque in self.historical_data.items():
# Если данных мало или они старые - очищаем
if len(data_deque) < 10:
symbols_to_remove.append(symbol)

for symbol in symbols_to_remove:
del self.historical_data[symbol]
logger.info(f"Cleaned up historical data for {symbol}")

await asyncio.sleep(self.config.get('data_cleanup_interval', 3600))

except Exception as e:
logger.error(f"Error in data cleanup: {e}")
await asyncio.sleep(300) # Retry after 5 minutes

async def fetch_market_data(self) -> Dict:
"""Получение данных от коллектора с повторными попытками"""
for attempt in range(self.config['max_retries']):
try:
async with self.session.get(f"{self.collector_url}/prices") as response:
if response.status == 200:
data = await response.json()
logger.debug(f"Successfully fetched {len(data)} market data points")
return data
else:
logger.warning(f"Attempt {attempt + 1}: Failed to fetch market data: {response.status}")
except asyncio.TimeoutError:
logger.warning(f"Attempt {attempt + 1}: Request timeout")
except Exception as e:
logger.warning(f"Attempt {attempt + 1}: Error fetching market data: {e}")

if attempt < self.config['max_retries'] - 1:
await asyncio.sleep(self.config['retry_delay'])

logger.error("All attempts to fetch market data failed")
return {}

def calculate_volatility(self, symbol: str, prices: List[float]) -> float:
"""Расчет волатильности на основе исторических данных"""
if len(prices) < 2:
return 0.0

returns = np.diff(prices) / prices[:-1]
volatility = np.std(returns) * 100 # В процентах
return float(volatility)

def detect_trend(self, symbol: str, prices: List[float]) -> TrendDirection:
"""Определение тренда на основе скользящих средних"""
if len(prices) < 20:
return TrendDirection.NEUTRAL

short_ma = np.mean(prices[-10:]) # Короткая MA
long_ma = np.mean(prices[-20:]) # Длинная MA
volatility = self.calculate_volatility(symbol, prices[-20:])

trend_strength = (short_ma - long_ma) / long_ma * 100

if volatility > self.config['volatility_threshold']:
return TrendDirection.HIGH_VOLATILITY
elif trend_strength > 0.1:
return TrendDirection.BULLISH
elif trend_strength < -0.1:
return TrendDirection.BEARISH
else:
return TrendDirection.NEUTRAL

def calculate_support_resistance(self, prices: List[float]) -> tuple:
"""Расчет уровней поддержки и сопротивления"""
if len(prices) < 10:
return 0.0, 0.0

support = np.percentile(prices, 25)
resistance = np.percentile(prices, 75)

return float(support), float(resistance)

def analyze_momentum(self, prices: List[float]) -> float:
"""Анализ импульса цены"""
if len(prices) < 5:
return 0.0

recent_prices = prices[-5:]
momentum = (recent_prices[-1] - recent_prices[0]) / recent_prices[0] * 100
return float(momentum)

def find_arbitrage_opportunities(self, market_data: Dict) -> List[ArbitrageOpportunity]:
"""Поиск арбитражных возможностей между биржами"""
opportunities = []

# Группируем данные по символам
symbol_data = {}
for key, data in market_data.items():
symbol = data['symbol']
if symbol not in symbol_data:
symbol_data[symbol] = []
symbol_data[symbol].append(data)

for symbol, exchanges_data in symbol_data.items():
if len(exchanges_data) < 2:
continue

# Сортируем по лучшим ценам покупки и продажи
buy_candidates = sorted(exchanges_data, key=lambda x: x['ask'])
sell_candidates = sorted(exchanges_data, key=lambda x: x['bid'], reverse=True)

for buy_data in buy_candidates:
for sell_data in sell_candidates:
if buy_data['exchange'] == sell_data['exchange']:
continue

spread_abs = sell_data['bid'] - buy_data['ask']
spread_percent = (spread_abs / buy_data['ask']) * 100

# Учитываем комиссии (примерно 0.1% на биржу)
effective_spread = spread_percent - 0.2

if effective_spread > self.config['min_arbitrage_spread']:
opportunity = ArbitrageOpportunity(
symbol=symbol,
buy_exchange=buy_data['exchange'],
sell_exchange=sell_data['exchange'],
buy_price=buy_data['ask'],
sell_price=sell_data['bid'],
spread_abs=round(spread_abs, 2),
spread_percent=round(spread_percent, 4),
potential_profit=round(spread_abs * self.config['max_position_size'], 2),
confidence=min(0.95, effective_spread / 1.0),
timestamp=datetime.now().isoformat()
)
opportunities.append(opportunity)

# Сортируем по потенциальной прибыли
opportunities.sort(key=lambda x: x.potential_profit, reverse=True)
return opportunities[:10]

def generate_forecast(self, symbol: str, market_data: Dict) -> Optional[PriceForecast]:
"""Генерация прогноза для символа"""
symbol_data = [data for data in market_data.values() if data['symbol'] == symbol]

if not symbol_data:
return None

# Собираем исторические цены
current_prices = [data['last'] for data in symbol_data]

# Обновляем исторические данные с ограничением размера
if symbol not in self.historical_data:
self.historical_data[symbol] = deque(maxlen=self.config.get('max_historical_points', 500))

self.historical_data[symbol].extend(current_prices)

# Анализируем тренд
prices_list = list(self.historical_data[symbol])
if len(prices_list) < 10:
return None

trend = self.detect_trend(symbol, prices_list)
volatility = self.calculate_volatility(symbol, prices_list[-50:])
support, resistance = self.calculate_support_resistance(prices_list[-100:])
momentum = self.analyze_momentum(prices_list[-10:])

# Рассчитываем уверенность
confidence_factors = []

# Фактор волатильности
vol_confidence = max(0.1, 1.0 - (volatility / 20.0))
confidence_factors.append(vol_confidence)

# Фактор объема данных
data_confidence = min(1.0, len(prices_list) / 100.0)
confidence_factors.append(data_confidence)

# Фактор согласованности бирж
exchange_consensus = len(symbol_data) / 6.0
confidence_factors.append(exchange_consensus)

confidence = np.mean(confidence_factors)

# Определяем силу сигнала
if confidence > 0.8:
signal_strength = SignalStrength.VERY_STRONG
elif confidence > 0.7:
signal_strength = SignalStrength.STRONG
elif confidence > 0.6:
signal_strength = SignalStrength.MODERATE
else:
signal_strength = SignalStrength.WEAK

# Формируем целевые уровни
current_price = np.mean(current_prices)
if trend == TrendDirection.BULLISH:
target_price = current_price * 1.02
stop_loss = current_price * 0.98
elif trend == TrendDirection.BEARISH:
target_price = current_price * 0.98
stop_loss = current_price * 1.02
else:
target_price = current_price
stop_loss = current_price * 0.99

# Формируем обоснование
reasoning = []
if trend != TrendDirection.NEUTRAL:
reasoning.append(f"{trend.value.capitalize()} trend detected")
if volatility < 2.0:
reasoning.append("Low volatility environment")
elif volatility > 8.0:
reasoning.append("High volatility - caution advised")
if len(symbol_data) >= 4:
reasoning.append("Strong exchange consensus")

forecast = PriceForecast(
symbol=symbol,
direction=trend,
confidence=round(confidence, 3),
target_price=round(target_price, 2),
stop_loss=round(stop_loss, 2),
timeframe="15m",
signal_strength=signal_strength,
reasoning=reasoning,
timestamp=datetime.now().isoformat()
)

return forecast

async def get_market_analysis(self, symbol: str) -> Optional[MarketAnalysis]:
"""Полный анализ рынка для символа"""
market_data = await self.fetch_market_data()
symbol_data = [data for data in market_data.values() if data['symbol'] == symbol]

if not symbol_data:
return None

prices = [data['last'] for data in symbol_data]
volatility = self.calculate_volatility(symbol, prices)
support, resistance = self.calculate_support_resistance(prices)
momentum = self.analyze_momentum(prices)

# Анализ объема (упрощенный)
volume_trend = "stable"
if len(prices) > 10:
recent_vol = np.mean(prices[-5:])
older_vol = np.mean(prices[-10:-5])
if recent_vol > older_vol * 1.1:
volume_trend = "increasing"
elif recent_vol < older_vol * 0.9:
volume_trend = "decreasing"

# RSI-like сигнал (упрощенный)
rsi_signal = "neutral"
if len(prices) >= 14:
gains = [max(0, prices[i] - prices[i-1]) for i in range(1, len(prices))]
losses = [max(0, prices[i-1] - prices[i]) for i in range(1, len(prices))]

avg_gain = np.mean(gains[-14:])
avg_loss = np.mean(losses[-14:])

if avg_loss == 0:
rsi = 100
else:
rs = avg_gain / avg_loss
rsi = 100 - (100 / (1 + rs))

if rsi > 70:
rsi_signal = "overbought"
elif rsi < 30:
rsi_signal = "oversold"

# Общая sentiment оценка
bullish_count = len([p for p in prices if p > prices[0]])
bearish_count = len(prices) - bullish_count
market_sentiment = "bullish" if bullish_count > bearish_count else "bearish"

analysis = MarketAnalysis(
symbol=symbol,
current_volatility=round(volatility, 2),
volume_trend=volume_trend,
support_level=round(support, 2),
resistance_level=round(resistance, 2),
rsi_signal=rsi_signal,
momentum=round(momentum, 2),
market_sentiment=market_sentiment
)

return analysis

async def generate_all_forecasts(self) -> Dict:
"""Генерация всех прогнозов и арбитражных возможностей"""
market_data = await self.fetch_market_data()

if not market_data:
return {"error": "No market data available", "timestamp": datetime.now().isoformat()}

# Арбитражные возможности
arbitrage_ops = self.find_arbitrage_opportunities(market_data)

# Прогнозы по символам
symbols = set(data['symbol'] for data in market_data.values())
forecasts = {}
analyses = {}

for symbol in symbols:
forecast = self.generate_forecast(symbol, market_data)
if forecast:
forecasts[symbol] = forecast

analysis = await self.get_market_analysis(symbol)
if analysis:
analyses[symbol] = analysis

# Сохраняем в историю
self.arbitrage_history.extend(arbitrage_ops)
for forecast in forecasts.values():
self.forecast_history.append(forecast)

result = {
"timestamp": datetime.now().isoformat(),
"arbitrage_opportunities": [
{
"symbol": op.symbol,
"buy_exchange": op.buy_exchange,
"sell_exchange": op.sell_exchange,
"buy_price": op.buy_price,
"sell_price": op.sell_price,
"spread_abs": op.spread_abs,
"spread_percent": op.spread_percent,
"potential_profit": op.potential_profit,
"confidence": op.confidence
}
for op in arbitrage_ops
],
"price_forecasts": {
symbol: {
"direction": forecast.direction.value,
"confidence": forecast.confidence,
"target_price": forecast.target_price,
"stop_loss": forecast.stop_loss,
"timeframe": forecast.timeframe,
"signal_strength": forecast.signal_strength.value,
"reasoning": forecast.reasoning
}
for symbol, forecast in forecasts.items()
},
"market_analysis": {
symbol: {
"current_volatility": analysis.current_volatility,
"volume_trend": analysis.volume_trend,
"support_level": analysis.support_level,
"resistance_level": analysis.resistance_level,
"rsi_signal": analysis.rsi_signal,
"momentum": analysis.momentum,
"market_sentiment": analysis.market_sentiment
}
for symbol, analysis in analyses.items()
}
}

return result

async def continuous_forecasting(self):
"""Непрерывное обновление прогнозов в фоне"""
self._is_running = True
await self.init_session()

while self._is_running:
try:
start_time = time.time()

# Генерируем прогнозы
forecasts = await self.generate_all_forecasts()
self.latest_forecasts = forecasts

processing_time = time.time() - start_time
logger.info(f"Forecasts updated in {processing_time:.2f}s")

# Ждем перед следующим обновлением
await asyncio.sleep(self.config['update_interval'])

except Exception as e:
logger.error(f"Error in continuous forecasting: {e}")
await asyncio.sleep(self.config['retry_delay'])

async def start_forecasting(self):
"""Запуск фоновой задачи прогнозирования"""
if not self._forecast_task or self._forecast_task.done():
self._forecast_task = asyncio.create_task(self.continuous_forecasting())
self._cleanup_task = asyncio.create_task(self.cleanup_old_data())
logger.info("Forecast engine started")

async def stop_forecasting(self):
"""Остановка фоновой задачи прогнозирования"""
self._is_running = False
if self._forecast_task:
self._forecast_task.cancel()
try:
await self._forecast_task
except asyncio.CancelledError:
pass
if self._cleanup_task:
self._cleanup_task.cancel()
try:
await self._cleanup_task
except asyncio.CancelledError:
pass
logger.info("Forecast engine stopped")

async def close(self):
"""Корректное закрытие ресурсов"""
await self.stop_forecasting()
if self.session:
await self.session.close()

# Глобальный экземпляр Forecast Engine
forecast_engine = ForecastEngine()

# HTTP handlers для REST API
async def forecast_handler(request):
"""Эндпоинт для получения всех прогнозов"""
try:
if hasattr(forecast_engine, 'latest_forecasts'):
return web.json_response(forecast_engine.latest_forecasts)
else:
forecasts = await forecast_engine.generate_all_forecasts()
return web.json_response(forecasts)
except Exception as e:
logger.error(f"Error in forecast handler: {e}")
return web.json_response({"error": "Failed to generate forecasts"}, status=500)

async def arbitrage_handler(request):
"""Эндпоинт для получения арбитражных возможностей"""
try:
market_data = await forecast_engine.fetch_market_data()
arbitrage_ops = forecast_engine.find_arbitrage_opportunities(market_data)

return web.json_response({
"timestamp": datetime.now().isoformat(),
"opportunities": [
{
"symbol": op.symbol,
"buy_exchange": op.buy_exchange,
"sell_exchange": op.sell_exchange,
"buy_price": op.buy_price,
"sell_price": op.sell_price,
"spread_abs": op.spread_abs,
"spread_percent": op.spread_percent,
"potential_profit": op.potential_profit,
"confidence": op.confidence
}
for op in arbitrage_ops
]
})
except Exception as e:
logger.error(f"Error in arbitrage handler: {e}")
return web.json_response({"error": "Failed to generate arbitrage opportunities"}, status=500)

async def analysis_handler(request):
"""Эндпоинт для анализа конкретного символа"""
symbol = request.query.get('symbol', 'BTCUSDT').upper()

try:
analysis = await forecast_engine.get_market_analysis(symbol)
if analysis:
return web.json_response(asdict(analysis))
else:
return web.json_response({"error": f"No analysis available for {symbol}"}, status=404)
except Exception as e:
logger.error(f"Error in analysis handler: {e}")
return web.json_response({"error": "Failed to generate analysis"}, status=500)

async def health_handler(request):
"""Эндпоинт для проверки здоровья Forecast Engine"""
status = {
"status": "running" if forecast_engine._is_running else "stopped",
"timestamp": datetime.now().isoformat(),
"historical_data_points": sum(len(data) for data in forecast_engine.historical_data.values()),
"arbitrage_history_size": len(forecast_engine.arbitrage_history),
"forecast_history_size": len(forecast_engine.forecast_history),
"symbols_tracked": list(forecast_engine.historical_data.keys()),
"config": {
"max_connections": forecast_engine.config.get('max_connections'),
"max_historical_points": forecast_engine.config.get('max_historical_points'),
"update_interval": forecast_engine.config.get('update_interval')
}
}
return web.json_response(status)

# Создание и настройка приложения
forecast_app = web.Application()
forecast_app.router.add_get('/forecast', forecast_handler)
forecast_app.router.add_get('/arbitrage', arbitrage_handler)
forecast_app.router.add_get('/analysis', analysis_handler)
forecast_app.router.add_get('/health', health_handler)

async def start_forecast_background(app):
"""Запуск фоновых задач прогнозирования"""
await forecast_engine.start_forecasting()

async def cleanup_forecast_background(app):
"""Остановка фоновых задач прогнозирования"""
await forecast_engine.close()

forecast_app.on_startup.append(start_forecast_background)
forecast_app.on_cleanup.append(cleanup_forecast_background)

if __name__ == '__main__':
host = forecast_engine.config.get('host', '0.0.0.0')
port = forecast_engine.config.get('port', 8081)
web.run_app(forecast_app, host=host, port=port)
