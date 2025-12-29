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

---

## GitHub Actions Workflows

PKBrokers includes automated GitHub Actions workflows for data collection and synchronization. The primary workflow is `w1-workflow-history-data-child.yml`.

### w1-workflow-history-data-child.yml

This workflow fetches historical OHLCV (Open, High, Low, Close, Volume) data from Zerodha's Kite Connect API and saves it to pkl files in the [PKScreener actions-data-download branch](https://github.com/pkjmesra/PKScreener/tree/actions-data-download).

#### Workflow Inputs

| Input | Description | Default | Type |
|-------|-------------|---------|------|
| `period` | Time interval for data fetching | `day` | string |
| `logLevel` | Log verbosity level | `20` | number |
| `kiteToken` | Kite API authentication token | From secrets | string |
| `pastoffset` | Days of historical data to fetch | `0` | number |

#### Supported Period Values

| Period | Description | Data Limit Per Request |
|--------|-------------|----------------------|
| `day` | Daily candles | 2000 days |
| `minute` | 1-minute candles | 60 days |
| `5minute` | 5-minute candles | 100 days |
| `10minute` | 10-minute candles | 100 days |
| `30minute` | 30-minute candles | 200 days |
| `60minute` | 60-minute candles | 400 days |

### How `--history=day` Works

When the workflow is triggered with `period=day`, the following sequence occurs:

#### 1. Environment Setup

```yaml
# Checkout PKBrokers code
uses: actions/checkout@v4
with:
  repository: pkjmesra/PKBrokers
  ref: main

# Install dependencies
run: pip install -e . kiteconnect
```

#### 2. Authentication

The workflow creates a `.env.dev` file with all necessary secrets:
- `KTOKEN` - Kite authentication token
- `KUSER/KPWD/KTOTP` - Kite credentials
- `TURSO_TOKEN` - Remote database token
- Telegram bot credentials for notifications

#### 3. Historical Data Fetch

```bash
python pkkite.py --history=day --pastoffset=0 --verbose
```

This triggers `kite_history()` in `pkkite.py`, which:

1. **Initializes KiteInstruments**: Fetches all NSE instrument tokens (~2000 stocks)
2. **Creates KiteTickerHistory**: Connects to Kite API with authentication
3. **Fetches Historical Data**: For each instrument:
   - Calls Kite API: `https://kite.zerodha.com/oms/instruments/historical/{token}/day`
   - Applies rate limiting (3 requests/second)
   - Saves to SQLite database (`instrument_history` table)

```python
# From instrumentHistory.py
history.get_multiple_instruments_history(
    instruments=tokens,
    interval="day",
    forceFetch=True,
    insertOnly=True
)
```

#### 4. Export to PKL Files (When `period=day`)

After fetching, the workflow exports data to pickle files:

```python
# Embedded Python script in workflow
# 1. Find the history database
db_path = 'instrument_history.db'

# 2. Query daily candles
query = """
SELECT instrument_token, timestamp, open, high, low, close, volume
FROM instrument_history
WHERE interval = 'day'
ORDER BY instrument_token, timestamp
"""

# 3. Map tokens to symbols using KiteInstruments
token_to_symbol = {}
for inst in instruments.get_or_fetch_instrument_tokens(all_columns=True):
    token_to_symbol[inst['instrument_token']] = inst['tradingsymbol']

# 4. Create stock_data dictionary
stock_data = {}
for token, group in df.groupby('instrument_token'):
    symbol = token_to_symbol.get(token, str(token))
    stock_data[symbol] = group[['Open', 'High', 'Low', 'Close', 'Volume']]

# 5. Save to pkl
today = datetime.now().strftime('%d%m%Y')
with open(f"stock_data_{today}.pkl", 'wb') as f:
    pickle.dump(stock_data, f, protocol=pickle.HIGHEST_PROTOCOL)
```

**Output Files**:
- `stock_data_DDMMYYYY.pkl` - Date-suffixed daily candles
- `daily_candles.pkl` - Latest daily candles (same content)

#### 5. Commit to PKScreener actions-data-download Branch

The workflow clones PKScreener's `actions-data-download` branch and commits the pkl files:

```bash
# Clone PKScreener actions-data-download branch
git clone --single-branch --branch actions-data-download \
  https://x-access-token:$CI_PAT@github.com/pkjmesra/PKScreener.git

# Copy pkl files
cp daily_candles.pkl actions-data-download/stock_data_${TODAY}.pkl
cp daily_candles.pkl actions-data-download/daily_candles.pkl

# Commit and push
git add -f actions-data-download/
git commit -m "Update pkl files from history download - $TODAY [period: day]"
git push
```

**Target Directories in PKScreener**:
- [`actions-data-download/actions-data-download/`](https://github.com/pkjmesra/PKScreener/tree/actions-data-download/actions-data-download) - Primary pkl storage
- [`actions-data-download/results/Data/`](https://github.com/pkjmesra/PKScreener/tree/actions-data-download/results/Data) - Secondary data location

### Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     w1-workflow-history-data-child.yml                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────┐                                                      │
│  │   GitHub Actions   │                                                      │
│  │   (Trigger: Manual │                                                      │
│  │    or Scheduled)   │                                                      │
│  └─────────┬──────────┘                                                      │
│            │                                                                 │
│            ▼                                                                 │
│  ┌────────────────────┐     ┌────────────────────┐                           │
│  │  pkkite.py         │     │  KiteInstruments   │                           │
│  │  --history=day     │────▶│  (Fetch tokens)    │                           │
│  └─────────┬──────────┘     └────────────────────┘                           │
│            │                                                                 │
│            ▼                                                                 │
│  ┌────────────────────┐     ┌────────────────────┐                           │
│  │  KiteTickerHistory │────▶│  Kite Connect API  │                           │
│  │  (Rate-limited)    │◀────│  /historical/{tok} │                           │
│  └─────────┬──────────┘     └────────────────────┘                           │
│            │                                                                 │
│            ▼                                                                 │
│  ┌────────────────────┐                                                      │
│  │  SQLite Database   │                                                      │
│  │  instrument_history│                                                      │
│  │  .db               │                                                      │
│  └─────────┬──────────┘                                                      │
│            │                                                                 │
│            ▼                                                                 │
│  ┌────────────────────┐     ┌────────────────────┐                           │
│  │  Export to PKL     │────▶│  stock_data_       │                           │
│  │  (Python script)   │     │  DDMMYYYY.pkl      │                           │
│  └─────────┬──────────┘     └────────────────────┘                           │
│            │                                                                 │
│            ▼                                                                 │
│  ┌────────────────────┐                                                      │
│  │  git commit/push   │                                                      │
│  │  to PKScreener     │                                                      │
│  │  actions-data-     │                                                      │
│  │  download branch   │                                                      │
│  └────────────────────┘                                                      │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Starting from Existing PKL Files

When the workflow runs, it can leverage previously saved pkl files from:
- [actions-data-download/actions-data-download/](https://github.com/pkjmesra/PKScreener/tree/actions-data-download/actions-data-download)
- [actions-data-download/results/Data/](https://github.com/pkjmesra/PKScreener/tree/actions-data-download/results/Data)

The `DataSharingManager` class in `pkbrokers/bot/dataSharingManager.py` handles this:

```python
def download_from_github(self, file_type: str = "daily") -> Tuple[bool, Optional[str]]:
    """
    Download pkl file from GitHub actions-data-download branch.
    Searches multiple locations and date formats:
    - actions-data-download/stock_data_DDMMYYYY.pkl
    - results/Data/stock_data_DDMMYYYY.pkl
    - Tries recent dates going back up to 10 days
    """
    urls_to_try = []
    for days_ago in range(0, 10):
        check_date = today - timedelta(days=days_ago)
        date_str = check_date.strftime('%d%m%Y')
        
        # Location 1: actions-data-download/actions-data-download/
        urls_to_try.append(
            f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/actions-data-download/stock_data_{date_str}.pkl"
        )
        # Location 2: actions-data-download/results/Data/
        urls_to_try.append(
            f"{PKSCREENER_RAW_BASE}/{ACTIONS_DATA_BRANCH}/results/Data/stock_data_{date_str}.pkl"
        )
```

### Merging New Data with Existing PKL

The `export_daily_candles_to_pkl()` method merges today's data with historical:

```python
def export_daily_candles_to_pkl(self, candle_store, merge_with_historical: bool = True):
    """
    Export daily candles from InMemoryCandleStore to pkl file.
    Merges with historical data from GitHub to create complete ~35MB+ pkl files.
    """
    # 1. Download existing historical pkl from GitHub
    success, historical_path = self.download_from_github(file_type="daily")
    
    if success:
        with open(historical_path, 'rb') as f:
            historical_data = pickle.load(f)
    
    # 2. Add today's candles from candle store
    for token, instrument in candle_store.instruments.items():
        symbol = candle_store.instrument_symbols.get(token)
        day_candles = instrument.candles.get('day', [])
        
        # 3. Merge with historical data
        if symbol in data:
            combined = pd.concat([existing_df, new_df])
            combined = combined[~combined.index.duplicated(keep='last')]
            data[symbol] = combined
    
    # 4. Save merged pkl
    with open(output_path, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
```

### Chained Workflow Execution

After `period=day` completes, the workflow automatically triggers the next period:

```
day → minute → 5minute → 10minute → 30minute → 60minute → (complete)
```

Each period is triggered via GitHub API:

```bash
curl -X POST \
  -H "Authorization: token $GITHUB_TOKEN" \
  "https://api.github.com/repos/$REPO/actions/workflows/w1-workflow-history-data-child.yml/dispatches" \
  -d '{"ref":"main","inputs":{"period":"minute","kiteToken":"..."}}'
```

### Triggering the Workflow Programmatically

From `DataSharingManager.trigger_history_download_workflow()`:

```python
def trigger_history_download_workflow(self, past_offset: int = 1) -> bool:
    """
    Trigger the w1-workflow-history-data-child.yml workflow to download missing OHLCV data.
    """
    url = "https://api.github.com/repos/pkjmesra/PKBrokers/actions/workflows/w1-workflow-history-data-child.yml/dispatches"
    
    payload = {
        "ref": "main",
        "inputs": {
            "period": "day",
            "pastoffset": str(past_offset),
            "logLevel": "20"
        }
    }
    
    response = requests.post(url, headers=headers, json=payload)
    return response.status_code == 204
```

### Parent Workflow (w1-workflow-history-data-parent.yml)

The parent workflow handles:
1. **Holiday Detection**: Checks NSE holiday calendar before running
2. **Token Acquisition**: Gets fresh Kite token via Telegram bot
3. **Token Validation**: Validates base64-encoded token
4. **Child Triggering**: Dispatches child workflows for each period

Scheduled to run: `59 9 * * 1-6` (3:30 PM IST, Monday-Saturday)

### File Locations Summary

| File Type | Location in PKBrokers | Location in PKScreener |
|-----------|----------------------|----------------------|
| SQLite DB | `~/.PKDevTools_userdata/instrument_history.db` | - |
| Daily PKL | `~/.PKDevTools_userdata/daily_candles.pkl` | `actions-data-download/stock_data_DDMMYYYY.pkl` |
| Intraday PKL | `~/.PKDevTools_userdata/intraday_1m_candles.pkl` | `actions-data-download/intraday_stock_data_DDMMYYYY.pkl` |

---

For more details, see the source code or open an issue on [GitHub](https://github.com/pkjmesra/pkbrokers).





