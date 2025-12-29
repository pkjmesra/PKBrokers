# PKBrokers Architecture

This document provides a detailed technical overview of PKBrokers architecture for developers who want to contribute or integrate with the library.

## Table of Contents

1. [System Overview](#system-overview)
2. [Data Flow](#data-flow)
3. [Component Details](#component-details)
4. [Design Patterns](#design-patterns)
5. [Thread Safety](#thread-safety)
6. [Extension Points](#extension-points)

---

## System Overview

PKBrokers is designed as a layered system with clear separation between data acquisition, processing, storage, and distribution.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Data Consumers                                 │
│                    (PKScreener, Custom Applications)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                    High-Level Data Providers                       │     │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │     │
│  │  │HighPerformance   │  │InstrumentData    │  │LocalCandle       │  │     │
│  │  │DataProvider      │  │Manager           │  │Database          │  │     │
│  │  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │     │
│  └───────────┼─────────────────────┼─────────────────────┼────────────┘     │
│              │                     │                     │                  │
│  ┌───────────▼─────────────────────▼─────────────────────▼────────────┐     │
│  │                      InMemoryCandleStore                           │     │
│  │                   (Central Data Repository)                        │     │
│  └────────────────────────────────┬───────────────────────────────────┘     │
│                                   │                                         │
│  ┌────────────────────────────────▼───────────────────────────────────┐     │
│  │                      Processing Layer                              │     │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │     │
│  │  │CandleAggregator  │  │TickProcessor    │  │JSONWriter         │  │     │
│  │  │(Tick → Candle)   │  │(Parse/Validate) │  │(Persistence)      │  │     │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │     │
│  └────────────────────────────────┬───────────────────────────────────┘     │
│                                   │                                         │
│  ┌────────────────────────────────▼───────────────────────────────────┐     │
│  │                      WebSocket Layer                               │     │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │     │
│  │  │KiteTokenWatcher  │  │ZerodhaWebSocket  │  │WebSocketParser   │  │     │
│  │  │(Orchestrator)    │  │Client            │  │(Binary → Dict)   │  │     │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │     │
│  └────────────────────────────────┬───────────────────────────────────┘     │
│                                   │                                         │
│  ┌────────────────────────────────▼───────────────────────────────────┐     │
│  │                      Infrastructure                                │     │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │     │
│  │  │KiteInstruments   │  │Authenticator     │  │PKTickBot         │  │     │
│  │  │(Symbols/Tokens)  │  │(TOTP Login)      │  │(Telegram)        │  │     │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Tick Processing Flow

```
┌─────────────────┐
│ Kite WebSocket  │
│ (Binary Ticks)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ WebSocketParser │ ─── Parse binary to dict
│ (zerodhaWeb     │
│  SocketParser)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ TickProcessor   │ ─── Validate and normalize
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ InMemoryCandle  │ ─── Aggregate into candles
│ Store           │
└────────┬────────┘
         │
    ┌────┴────┬──────────────┐
    ▼         ▼              ▼
┌───────┐ ┌───────┐   ┌────────────┐
│ticks  │ │SQLite │   │Consumers   │
│.json  │ │DB     │   │(API calls) │
└───────┘ └───────┘   └────────────┘
```

### Candle Aggregation

```
Tick arrives at 09:16:45 (price=100, volume=500)
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│         Determine candle boundaries             │
│                                                 │
│  1m candle: 09:16:00 - 09:16:59                 │
│  5m candle: 09:15:00 - 09:19:59                 │
│  15m candle: 09:15:00 - 09:29:59                │
│  ...                                            │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│         Update candle for each interval         │
│                                                 │
│  current_candle.high = max(high, price)         │
│  current_candle.low = min(low, price)           │
│  current_candle.close = price                   │
│  current_candle.volume += volume                │
│  current_candle.tick_count += 1                 │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│    If candle complete (time boundary crossed)   │
│                                                 │
│  1. Move current → completed deque              │
│  2. Create new current candle                   │
│  3. Trim deque if > max_candles                 │
└─────────────────────────────────────────────────┘
```

---

## Component Details

### InMemoryCandleStore

The central data repository for real-time candle data.

**Data Structure**:
```python
instruments = {
    256265: InstrumentCandles(  # NIFTY 50
        current_candle = {
            '1m': OHLCVCandle(...),
            '5m': OHLCVCandle(...),
            ...
        },
        completed_candles = {
            '1m': deque([OHLCVCandle, ...], maxlen=375),
            '5m': deque([OHLCVCandle, ...], maxlen=75),
            ...
        }
    ),
    ...
}
```

**Key Operations**:
- `process_tick(tick)`: O(1) update across all intervals
- `get_candles(token, interval, count)`: O(count) retrieval
- `get_current_candle(token, interval)`: O(1) lookup

---

### KiteTokenWatcher

Orchestrates WebSocket connection and tick processing.

**Lifecycle**:
1. Authenticate with Kite Connect
2. Fetch instruments from database/API
3. Subscribe to instrument tokens
4. Process incoming ticks
5. Handle reconnection on disconnect

**Configuration**:
```python
MAX_BATCHES = 10           # Max concurrent subscriptions
BATCH_SIZE = 300           # Tokens per batch
JSON_WRITE_INTERVAL = 60   # Seconds between JSON saves
```

---

### InstrumentDataManager

Multi-source data manager with priority-based fetching.

**Source Priority (Market Hours)**:
1. Local SQLite database
2. InMemoryCandleStore
3. Kite API (if authenticated)
4. GitHub ticks.json

**Source Priority (After Hours)**:
1. Local pickle files
2. Remote GitHub pickle files

---

### LocalCandleDatabase

SQLite-based persistent storage.

**Schema**:
```sql
-- Daily candles
CREATE TABLE daily_candles (
    symbol TEXT,
    date DATE,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    PRIMARY KEY (symbol, date)
);

-- Intraday candles
CREATE TABLE intraday_candles (
    symbol TEXT,
    timestamp DATETIME,
    interval TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    PRIMARY KEY (symbol, timestamp, interval)
);
```

---

## Design Patterns

### Singleton Pattern

Used for global state management.

```python
# InMemoryCandleStore singleton
_candle_store: Optional[InMemoryCandleStore] = None

def get_candle_store() -> InMemoryCandleStore:
    global _candle_store
    if _candle_store is None:
        _candle_store = InMemoryCandleStore()
    return _candle_store
```

### Observer Pattern

Tick distribution to multiple handlers.

```python
class KiteTokenWatcher:
    def __init__(self):
        self._handlers = [
            self._candle_store.process_tick,
            self._json_writer.queue_tick,
            self._db_writer.queue_tick,
        ]
    
    def on_ticks(self, ticks):
        for tick in ticks:
            for handler in self._handlers:
                handler(tick)
```

### Strategy Pattern

Data source selection in InstrumentDataManager.

```python
class InstrumentDataManager:
    def execute(self):
        if self._is_market_hours():
            return self._load_market_hours_data()
        else:
            return self._load_pickle_data()
```

### Producer-Consumer Pattern

Queue-based async processing.

```python
class JSONWriter:
    def __init__(self):
        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._writer_loop)
    
    def queue_tick(self, tick):
        self._queue.put(tick)
    
    def _writer_loop(self):
        while self._running:
            tick = self._queue.get(timeout=1)
            self._process(tick)
```

---

## Thread Safety

### Lock Hierarchy

```
InMemoryCandleStore.lock      # Main store lock
    └── InstrumentCandles.lock # Per-instrument lock
            └── CandleAggregator.lock # Per-aggregator lock
```

### Safe Patterns

```python
# Correct: Use context manager
with self.lock:
    self._update_candle(tick)

# Correct: Copy before return
def get_candles(self, ...):
    with self.lock:
        return list(self.completed_candles[interval])
```

### Queue-Based Thread Safety

```python
# Producer (main thread)
self._write_queue.put(data)

# Consumer (background thread)
def _writer_loop(self):
    while True:
        data = self._write_queue.get()
        self._write_to_file(data)
```

---

## Extension Points

### Adding a New Data Source

1. Create source class:
```python
class MyDataSource:
    def fetch_data(self, symbols: List[str], start_date, end_date):
        """Fetch data from source"""
        pass
    
    def is_available(self) -> bool:
        """Check if source is accessible"""
        pass
```

2. Register in InstrumentDataManager:
```python
def _load_market_hours_data(self):
    # Add to source chain
    sources = [
        self._fetch_from_sqlite,
        self._fetch_from_candle_store,
        self._fetch_from_my_source,  # New source
        self._fetch_from_github,
    ]
```

### Adding a New Interval

1. Add to SUPPORTED_INTERVALS:
```python
SUPPORTED_INTERVALS = {
    '1m': 60,
    '5m': 300,
    '2h': 7200,  # New 2-hour interval
    ...
}
```

2. Add max candles:
```python
MAX_CANDLES = {
    '1m': 375,
    '5m': 75,
    '2h': 4,  # ~4 candles per day
    ...
}
```

### Adding a New Bot Command

```python
class PKTickBot:
    def _register_handlers(self):
        dispatcher = self.updater.dispatcher
        dispatcher.add_handler(CommandHandler("mycommand", self.my_command))
    
    def my_command(self, update: Update, context: CallbackContext):
        update.message.reply_text("My custom command response")
```

---

## Performance Considerations

### Memory Usage

- ~50 bytes per candle
- ~100MB for 2000 instruments × 10 intervals × 100 candles

### Optimization Techniques

1. **Deque with maxlen**: Auto-truncation
2. **NumPy arrays**: Efficient OHLCV storage
3. **Dict lookup**: O(1) instrument access
4. **Batch processing**: Reduce lock contention

### Monitoring

```python
stats = store.get_stats()
print(f"Instruments: {stats['instrument_count']}")
print(f"Ticks processed: {stats['ticks_processed']}")
print(f"Memory (MB): {stats['memory_mb']:.2f}")
print(f"Uptime: {stats['uptime_seconds']:.0f}s")
```

---

## Error Handling

### WebSocket Reconnection

```python
def on_close(self, ws, close_code, close_message):
    if self._should_reconnect:
        time.sleep(self._reconnect_delay)
        self._connect()
```

### Database Failover

```python
try:
    result = self._query_turso(query)
except Exception as e:
    if "BLOCKED" in str(e):
        # Turso quota exceeded, use local
        result = self._query_sqlite(query)
```

### Graceful Shutdown

```python
def stop(self):
    self._running = False
    self._save_state()
    self._flush_queues()
    self._close_connections()
```

---

For more details, see the source code or open an issue on [GitHub](https://github.com/pkjmesra/pkbrokers).




