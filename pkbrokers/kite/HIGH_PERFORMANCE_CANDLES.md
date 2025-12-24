# High-Performance Candle Data System

This document describes the high-performance, in-memory candle data system for PKBrokers that provides instant access to OHLCV candles across all supported timeframes without database dependency.

## Overview

The system consists of three main components:

1. **InMemoryCandleStore** - Core in-memory storage for real-time candle data
2. **TickProcessor** - Bridges WebSocket ticks to the candle store
3. **HighPerformanceDataProvider** - Convenience API for PKScreener integration

## Supported Timeframes

| Interval | Description | Max Candles Stored |
|----------|-------------|-------------------|
| `1m` | 1 minute | 390 (full trading day) |
| `2m` | 2 minutes | 195 |
| `3m` | 3 minutes | 130 |
| `4m` | 4 minutes | 98 |
| `5m` | 5 minutes | 78 |
| `10m` | 10 minutes | 39 |
| `15m` | 15 minutes | 26 |
| `30m` | 30 minutes | 13 |
| `60m` | 60 minutes (1 hour) | 7 |
| `day` | Daily candles | 365 (1 year) |

## Quick Start

### Basic Usage

```python
from pkbrokers.kite import get_candle_store, HighPerformanceDataProvider

# Get the singleton candle store
store = get_candle_store()

# Or use the high-level data provider
provider = HighPerformanceDataProvider()

# Get 5-minute candles for RELIANCE
df = provider.get_stock_data("RELIANCE", interval="5m", count=50)

# Get current day's OHLCV
ohlcv = provider.get_current_ohlcv("RELIANCE")
print(f"Open: {ohlcv['open']}, High: {ohlcv['high']}, Low: {ohlcv['low']}, Close: {ohlcv['close']}")
```

### With Real-Time Tick Data

```python
from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher

# Initialize and start the watcher
watcher = KiteTokenWatcher()
watcher.watch()

# Access candle data in real-time
candles = watcher.get_candles(symbol="RELIANCE", interval="5m", count=50)
df = watcher.get_candles_df(symbol="RELIANCE", interval="15m")

# Get all instruments' day OHLCV
all_ohlcv = watcher.get_all_day_ohlcv()
```

## API Reference

### InMemoryCandleStore

The core singleton class that maintains all candle data in memory.

#### Methods

```python
# Get candles as list of dicts
candles = store.get_candles(
    instrument_token=256265,  # or
    trading_symbol="RELIANCE",
    interval='5m',
    count=50,
    include_current=True  # Include forming candle
)

# Get candles as DataFrame
df = store.get_ohlcv_dataframe(
    trading_symbol="RELIANCE",
    interval='15m',
    count=100
)

# Get current (forming) candle
current = store.get_current_candle(trading_symbol="RELIANCE", interval='5m')

# Get latest price
price = store.get_latest_price(trading_symbol="RELIANCE")

# Get today's OHLCV
day_ohlcv = store.get_day_ohlcv(trading_symbol="RELIANCE")

# Get all instruments' OHLCV
all_ohlcv = store.get_all_instruments_ohlcv(interval='day')

# Process a tick manually
store.process_tick({
    'instrument_token': 256265,
    'last_price': 2500.50,
    'day_volume': 1000000,
    'oi': 0,
    'exchange_timestamp': 1703505600,
    'trading_symbol': 'RELIANCE',
    'type': 'tick'
})

# Export to pickle format (compatible with InstrumentDataManager)
pickle_data = store.export_to_pickle_format()

# Export to ticks.json format
store.save_ticks_json('/path/to/ticks.json')

# Get statistics
stats = store.get_stats()
```

### HighPerformanceDataProvider

A convenience class for PKScreener integration.

```python
from pkbrokers.kite.tickProcessor import HighPerformanceDataProvider

provider = HighPerformanceDataProvider()

# Get stock data as DataFrame
df = provider.get_stock_data("RELIANCE", interval="5m", count=100)

# Get multiple stocks
data = provider.get_stocks_data(["RELIANCE", "TCS", "INFY"], interval="day")

# Get intraday data
intraday = provider.get_intraday_data("RELIANCE", interval="5m")

# Get current price
price = provider.get_current_price("RELIANCE")

# Get all market data
market_data = provider.get_market_data()

# Check data availability
if provider.is_data_available("RELIANCE"):
    df = provider.get_stock_data("RELIANCE")

# Get available symbols
symbols = provider.get_available_symbols()
```

### CandleAggregator

Utility class for aggregating candles between timeframes.

```python
from pkbrokers.kite.candleAggregator import CandleAggregator

# Aggregate 1-minute candles to 5-minute
df_5min = CandleAggregator.aggregate_candles(df_1min, '5m')

# Aggregate ticks to 1-minute candles
df_1min = CandleAggregator.aggregate_ticks(ticks_df, '1m')

# Resample to multiple timeframes at once
results = CandleAggregator.resample_to_multiple_timeframes(df_1min)
df_5min = results['5m']
df_15min = results['15m']
df_1hour = results['1h']

# Validate OHLCV data
is_valid = CandleAggregator.validate_ohlcv(df)
```

## Integration with PKScreener

### Using in Scan Routines

```python
from pkbrokers.kite.tickProcessor import HighPerformanceDataProvider

class MyScanRoutine:
    def __init__(self):
        self.data_provider = HighPerformanceDataProvider()
    
    def scan(self, symbol):
        # Get real-time candle data
        df = self.data_provider.get_stock_data(symbol, interval="5m", count=50)
        
        if df.empty:
            # Fall back to other data source
            return None
        
        # Perform technical analysis
        close = df['close']
        sma_20 = close.rolling(20).mean()
        
        if close.iloc[-1] > sma_20.iloc[-1]:
            return "BUY"
        return "HOLD"
```

### Real-Time Market Monitoring

```python
from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher
import threading
import time

def monitor_market():
    watcher = KiteTokenWatcher()
    
    # Start in background thread
    watcher_thread = threading.Thread(target=watcher.watch, daemon=True)
    watcher_thread.start()
    
    # Give it time to connect
    time.sleep(10)
    
    while True:
        # Get all day OHLCV
        all_ohlcv = watcher.get_all_day_ohlcv()
        
        for token, data in all_ohlcv.items():
            symbol = data.get('trading_symbol', '')
            change = ((data['close'] - data['open']) / data['open']) * 100
            print(f"{symbol}: {change:.2f}%")
        
        time.sleep(30)

monitor_market()
```

## Data Persistence

The system automatically persists data to:

1. **candle_store.pkl** - Full candle store state (every 5 minutes by default)
2. **ticks.json** - Current day's OHLCV in JSON format

### Manual Persistence

```python
from pkbrokers.kite import get_candle_store

store = get_candle_store()

# Save immediately
store._persist_to_disk()

# Export to specific file
store.save_ticks_json('/custom/path/ticks.json')
```

## Performance Characteristics

- **O(1) access** to any candle for any instrument/interval
- **Thread-safe** operations with RLock
- **Memory-efficient** using deques with fixed max sizes
- **Automatic cleanup** of old candles beyond max window
- **No database dependency** for real-time access
- **Automatic recovery** from disk on startup

## Memory Usage

Estimated memory usage per instrument:
- ~100 bytes per candle
- ~50KB per instrument (all intervals, full day)
- ~100MB for 2000 instruments (full market)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Zerodha WebSocket API                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ZerodhaWebSocketClient                         │
│  - Manages multiple WebSocket connections                       │
│  - Parses binary tick data                                      │
│  - Puts ticks into data queue                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    KiteTokenWatcher                             │
│  - Processes tick batches                                       │
│  - Updates InMemoryCandleStore ◄──── High Performance Path      │
│  - Sends to JSON writer                                         │
│  - Sends to database (optional)                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐  ┌───────────────┐  ┌─────────────────┐
│ InMemoryCandleS │  │ JSONFileWrit  │  │ ThreadSafeDB    │
│     tore        │  │    er         │  │   (optional)    │
│                 │  │               │  │                 │
│ - All intervals │  │ - ticks.json  │  │ - Turso/SQLite  │
│ - O(1) access   │  │               │  │                 │
└─────────────────┘  └───────────────┘  └─────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│              HighPerformanceDataProvider                        │
│  - Convenient API for PKScreener                                │
│  - Symbol-based access                                          │
│  - DataFrame output                                             │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                     PKScreener Scans                            │
│  - Real-time technical analysis                                 │
│  - No database latency                                          │
└─────────────────────────────────────────────────────────────────┘
```

## Troubleshooting

### No Data Available

```python
# Check if store has data
stats = store.get_stats()
print(f"Instruments: {stats['instrument_count']}")
print(f"Ticks processed: {stats['ticks_processed']}")

# List available symbols
symbols = list(store.symbol_to_token.keys())
print(f"Available symbols: {symbols[:10]}...")
```

### Memory Issues

```python
# Clear old data
store.clear()

# Check memory usage
stats = store.get_stats()
print(f"Memory: {stats['memory_mb']:.2f} MB")
```

### Persistence Issues

```python
# Force save
store._persist_to_disk()

# Check persistence file
import os
from PKDevTools.classes import Archiver

path = os.path.join(Archiver.get_user_data_dir(), "candle_store.pkl")
if os.path.exists(path):
    print(f"Store file size: {os.path.getsize(path) / 1024:.2f} KB")
```


## Local SQLite Database (Offline Support)

For scenarios where the Turso remote database is unavailable (quota exceeded, network issues),
the `LocalCandleDatabase` module provides local SQLite storage.

### Features

- **Daily Candles**: Stores 1-year historical OHLCV data in `candles_daily_YYMMDD.db`
- **Intraday Candles**: Stores 1-minute candles in `candles_YYMMDD_intraday.db`
- **Turso Sync**: Automatically syncs from Turso when available
- **Tick Fallback**: Falls back to InMemoryCandleStore tick aggregation
- **Pickle Export**: Exports to PKScreener-compatible pickle format

### Usage

```python
from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase

# Initialize database
db = LocalCandleDatabase(base_path='/path/to/data')

# Try to sync from Turso
if not db.sync_from_turso():
    # Fall back to tick data
    from pkbrokers.kite.inMemoryCandleStore import get_candle_store
    db.update_from_ticks(get_candle_store())

# Export for PKScreener
daily_pkl, intraday_pkl = db.export_to_pickle()

# Get statistics
stats = db.get_stats()
print(f"Daily: {stats['daily']['symbols']} symbols")
print(f"Intraday: {stats['intraday']['symbols']} symbols")

db.close()
```

### Database Schema

**Daily Candles Table:**
```sql
CREATE TABLE daily_candles (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    updated_at TEXT,
    PRIMARY KEY (symbol, date)
);
```

**Intraday Candles Table:**
```sql
CREATE TABLE intraday_candles (
    symbol TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    interval TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    updated_at TEXT,
    PRIMARY KEY (symbol, timestamp, interval)
);
```

### GitHub Workflow Integration

The `w-local-candle-sync.yml` workflow in PKScreener:
1. Runs during market hours (every 30 min) and after market close
2. Syncs from Turso or uses existing pickle data
3. Commits SQLite databases to the repository
4. Enables offline scan support
