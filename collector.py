# collector.py
import asyncio
import websockets
import aiohttp
from aiohttp import web
import json
from datetime import datetime
import logging
from typing import Dict, List
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UnifiedMarketDataCollector:
def __init__(self):
self.market_data = {}
self.last_update = {}
self.config = self.load_config()
self.session = None
self._cleanup_task = None

def load_config(self):
return {
'exchanges': {
'binance': {
'ws_url': 'wss://stream.binance.com:9443/ws/{}@ticker',
'rest_url': 'https://api.binance.com/api/v3/ticker/bookTicker?symbol={}',
'symbol_mapping': {'BTCUSDT': 'btcusdt', 'ETHUSDT': 'ethusdt'},
'reconnect_delay': 5
},
'okx': {
'ws_url': 'wss://ws.okx.com:8443/ws/v5/public',
'rest_url': 'https://www.okx.com/api/v5/market/ticker?instId={}',
'symbol_mapping': {'BTCUSDT': 'BTC-USDT', 'ETHUSDT': 'ETH-USDT'},
'reconnect_delay': 5
},
'bybit': {
'ws_url': 'wss://stream.bybit.com/v5/public/spot',
'rest_url': 'https://api.bybit.com/v5/market/tickers?category=spot&symbol={}',
'symbol_mapping': {'BTCUSDT': 'BTCUSDT', 'ETHUSDT': 'ETHUSDT'},
'reconnect_delay': 5
},
'kraken': {
'ws_url': 'wss://ws.kraken.com/',
'rest_url': 'https://api.kraken.com/0/public/Ticker?pair={}',
'symbol_mapping': {'BTCUSDT': 'XBTUSDT', 'ETHUSDT': 'ETHUSDT'},
'reconnect_delay': 5
},
'coinbase': {
'ws_url': 'wss://advanced-trade-ws.coinbase.com',
'rest_url': 'https://api.exchange.coinbase.com/products/{}/ticker',
'symbol_mapping': {'BTCUSDT': 'BTC-USD', 'ETHUSDT': 'ETH-USD'},
'reconnect_delay': 5
},
'bitstamp': {
'ws_url': 'wss://ws.bitstamp.net',
'rest_url': 'https://www.bitstamp.net/api/v2/ticker/{}/',
'symbol_mapping': {'BTCUSDT': 'btcusd', 'ETHUSDT': 'ethusd'},
'reconnect_delay': 5
}
},
'symbols': ['BTCUSDT', 'ETHUSDT'],
'max_data_age': 30, # seconds
'cleanup_interval': 60 # seconds
}

async def init_session(self):
"""Инициализация aiohttp сессии"""
self.session = aiohttp.ClientSession()

async def cleanup_old_data(self):
"""Очистка устаревших данных"""
while True:
try:
current_time = time.time()
expired_keys = []

for key, data in self.market_data.items():
if current_time - data.get('_timestamp', 0) > self.config['max_data_age']:
expired_keys.append(key)

for key in expired_keys:
del self.market_data[key]
logger.info(f"Removed expired data: {key}")

await asyncio.sleep(self.config['cleanup_interval'])
except Exception as e:
logger.error(f"Cleanup error: {e}")
await asyncio.sleep(30)

def _create_unified_record(self, exchange: str, symbol: str, bid: float, ask: float, last: float, source_timestamp: int = None):
"""Создание унифицированной записи с метриками"""
current_time = time.time()
latency = None

if source_timestamp:
latency = current_time - (source_timestamp / 1000) # Convert ms to seconds

return {
'exchange': exchange,
'symbol': symbol,
'bid': bid,
'ask': ask,
'last': last,
'timestamp': datetime.now().isoformat(),
'source_timestamp': source_timestamp,
'source_latency': latency,
'_timestamp': current_time # Internal timestamp for cleanup
}

async def binance_handler(self, symbol: str):
exchange_config = self.config['exchanges']['binance']
ws_symbol = exchange_config['symbol_mapping'][symbol]
uri = exchange_config['ws_url'].format(ws_symbol)

while True:
try:
async with websockets.connect(uri) as websocket:
logger.info(f"Binance WebSocket connected for {symbol}")

async for message in websocket:
data = json.loads(message)

# Skip non-ticker messages
if 'e' in data and data['e'] != '24hrTicker':
continue

self.market_data[f"binance_{symbol}"] = self._create_unified_record(
exchange='binance',
symbol=symbol,
bid=float(data.get('b', 0)),
ask=float(data.get('a', 0)),
last=float(data.get('c', 0)),
source_timestamp=data.get('E')
)

except Exception as e:
logger.error(f"Binance error for {symbol}: {e}")
await asyncio.sleep(exchange_config['reconnect_delay'])

async def okx_handler(self, symbol: str):
exchange_config = self.config['exchanges']['okx']
ws_symbol = exchange_config['symbol_mapping'][symbol]
subscribe_msg = {
"op": "subscribe",
"args": [{"channel": "tickers", "instId": ws_symbol}]
}

while True:
try:
async with websockets.connect(exchange_config['ws_url']) as websocket:
await websocket.send(json.dumps(subscribe_msg))
logger.info(f"OKX WebSocket connected for {symbol}")

async for message in websocket:
data = json.loads(message)

# Skip non-ticker messages and heartbeats
if data.get('event') in ['subscribe', 'error']:
continue
if not data.get('data'):
continue

ticker_data = data.get('data', [{}])[0]
self.market_data[f"okx_{symbol}"] = self._create_unified_record(
exchange='okx',
symbol=symbol,
bid=float(ticker_data.get('bidPx', 0)),
ask=float(ticker_data.get('askPx', 0)),
last=float(ticker_data.get('last', 0)),
source_timestamp=ticker_data.get('ts')
)

except Exception as e:
logger.error(f"OKX error for {symbol}: {e}")
await asyncio.sleep(exchange_config['reconnect_delay'])

async def bybit_handler(self, symbol: str):
exchange_config = self.config['exchanges']['bybit']
subscribe_msg = {
"op": "subscribe",
"args": [f"tickers.{symbol}"]
}

while True:
try:
async with websockets.connect(exchange_config['ws_url']) as websocket:
await websocket.send(json.dumps(subscribe_msg))
logger.info(f"Bybit WebSocket connected for {symbol}")

async for message in websocket:
data = json.loads(message)

# Skip non-ticker messages
if data.get('topic', '').startswith('tickers.'):
ticker_data = data.get('data', {})
self.market_data[f"bybit_{symbol}"] = self._create_unified_record(
exchange='bybit',
symbol=symbol,
bid=float(ticker_data.get('bid1Price', 0)),
ask=float(ticker_data.get('ask1Price', 0)),
last=float(ticker_data.get('lastPrice', 0)),
source_timestamp=ticker_data.get('ts')
)

except Exception as e:
logger.error(f"Bybit error for {symbol}: {e}")
await asyncio.sleep(exchange_config['reconnect_delay'])

async def kraken_handler(self, symbol: str):
exchange_config = self.config['exchanges']['kraken']
ws_symbol = exchange_config['symbol_mapping'][symbol]
subscribe_msg = {
"event": "subscribe",
"pair": [ws_symbol],
"subscription": {"name": "ticker"}
}

while True:
try:
async with websockets.connect(exchange_config['ws_url']) as websocket:
await websocket.send(json.dumps(subscribe_msg))
logger.info(f"Kraken WebSocket connected for {symbol}")

async for message in websocket:
data = json.loads(message)

# Skip system messages and non-ticker data
if not isinstance(data, list) or len(data) < 2:
continue

ticker_data = data[1]
if 'b' in ticker_data and 'a' in ticker_data:
self.market_data[f"kraken_{symbol}"] = self._create_unified_record(
exchange='kraken',
symbol=symbol,
bid=float(ticker_data['b'][0]),
ask=float(ticker_data['a'][0]),
last=float(ticker_data.get('c', [0])[0])
)

except Exception as e:
logger.error(f"Kraken error for {symbol}: {e}")
await asyncio.sleep(exchange_config['reconnect_delay'])

async def coinbase_handler(self, symbol: str):
exchange_config = self.config['exchanges']['coinbase']
ws_symbol = exchange_config['symbol_mapping'][symbol]
subscribe_msg = {
"type": "subscribe",
"product_ids": [ws_symbol],
"channels": ["ticker"]
}

while True:
try:
async with websockets.connect(exchange_config['ws_url']) as websocket:
await websocket.send(json.dumps(subscribe_msg))
logger.info(f"Coinbase WebSocket connected for {symbol}")

async for message in websocket:
data = json.loads(message)

# Skip non-ticker messages
if data.get('channel') != 'ticker':
continue

self.market_data[f"coinbase_{symbol}"] = self._create_unified_record(
exchange='coinbase',
symbol=symbol,
bid=float(data.get('best_bid', 0)),
ask=float(data.get('best_ask', 0)),
last=float(data.get('price', 0)),
source_timestamp=data.get('time')
)

except Exception as e:
logger.error(f"Coinbase error for {symbol}: {e}")
await asyncio.sleep(exchange_config['reconnect_delay'])

async def bitstamp_handler(self, symbol: str):
exchange_config = self.config['exchanges']['bitstamp']
ws_symbol = exchange_config['symbol_mapping'][symbol]
subscribe_msg = {
"event": "bts:subscribe",
"data": {"channel": f"live_trades_{ws_symbol}"}
}

while True:
try:
async with websockets.connect(exchange_config['ws_url']) as websocket:
await websocket.send(json.dumps(subscribe_msg))
logger.info(f"Bitstamp WebSocket connected for {symbol}")

async for message in websocket:
data = json.loads(message)

# Skip non-trade messages
if data.get('event') != 'trade':
continue

trade_data = data.get('data', {})
price = float(trade_data.get('price', 0))
self.market_data[f"bitstamp_{symbol}"] = self._create_unified_record(
exchange='bitstamp',
symbol=symbol,
bid=price,
ask=price,
last=price,
source_timestamp=trade_data.get('timestamp')
)

except Exception as e:
logger.error(f"Bitstamp error for {symbol}: {e}")
await asyncio.sleep(exchange_config['reconnect_delay'])

async def start_all_collectors(self):
"""Запуск всех коллекторов параллельно"""
await self.init_session()
self._cleanup_task = asyncio.create_task(self.cleanup_old_data())

tasks = []

for symbol in self.config['symbols']:
tasks.extend([
self.binance_handler(symbol),
self.okx_handler(symbol),
self.bybit_handler(symbol),
self.kraken_handler(symbol),
self.coinbase_handler(symbol),
self.bitstamp_handler(symbol)
])

logger.info(f"Starting {len(tasks)} collector tasks...")
await asyncio.gather(*tasks, return_exceptions=True)

async def close(self):
"""Корректное закрытие ресурсов"""
if self.session:
await self.session.close()
if self._cleanup_task:
self._cleanup_task.cancel()

# Глобальный экземпляр коллектора
collector = UnifiedMarketDataCollector()

# HTTP handlers
async def prices_handler(request):
"""Эндпоинт для получения текущих цен"""
return web.json_response(collector.market_data)

async def health_handler(request):
"""Эндпоинт для проверки здоровья системы"""
current_time = time.time()
healthy_data = {}
unhealthy_data = {}

for key, data in collector.market_data.items():
if current_time - data.get('_timestamp', 0) < collector.config['max_data_age']:
healthy_data[key] = data
else:
unhealthy_data[key] = data

return web.json_response({
'status': 'healthy' if len(healthy_data) > 0 else 'unhealthy',
'timestamp': datetime.now().isoformat(),
'total_data_points': len(collector.market_data),
'healthy_streams': len(healthy_data),
'unhealthy_streams': len(unhealthy_data),
'active_exchanges': list(set([data['exchange'] for data in healthy_data.values()]))
})

async def metrics_handler(request):
"""Эндпоинт для метрик системы"""
current_time = time.time()
metrics = {
'total_streams': len(collector.market_data),
'streams_by_exchange': {},
'average_latency_by_exchange': {},
'data_freshness': {}
}

for key, data in collector.market_data.items():
exchange = data['exchange']

# Count by exchange
metrics['streams_by_exchange'][exchange] = metrics['streams_by_exchange'].get(exchange, 0) + 1

# Calculate average latency
if data.get('source_latency'):
if exchange not in metrics['average_latency_by_exchange']:
metrics['average_latency_by_exchange'][exchange] = []
metrics['average_latency_by_exchange'][exchange].append(data['source_latency'])

# Data freshness
data_age = current_time - data.get('_timestamp', 0)
if exchange not in metrics['data_freshness']:
metrics['data_freshness'][exchange] = []
metrics['data_freshness'][exchange].append(data_age)

# Calculate averages
for exchange in metrics['average_latency_by_exchange']:
metrics['average_latency_by_exchange'][exchange] = sum(metrics['average_latency_by_exchange'][exchange]) / len(metrics['average_latency_by_exchange'][exchange])

for exchange in metrics['data_freshness']:
metrics['data_freshness'][exchange] = sum(metrics['data_freshness'][exchange]) / len(metrics['data_freshness'][exchange])

return web.json_response(metrics)

# Создание и настройка приложения
app = web.Application()
app.router.add_get('/prices', prices_handler)
app.router.add_get('/health', health_handler)
app.router.add_get('/metrics', metrics_handler)

async def start_background_tasks(app):
"""Запуск фоновых задач"""
app['collector_task'] = asyncio.create_task(collector.start_all_collectors())

async def cleanup_background_tasks(app):
"""Остановка фоновых задач"""
await collector.close()
app['collector_task'].cancel()
await app['collector_task']

app.on_startup.append(start_background_tasks)
app.on_cleanup.append(cleanup_background_tasks)

if __name__ == '__main__':
web.run_app(app, host='0.0.0.0', port=8080)

