
# PKBrokers

[![MADE-IN-INDIA][MADE-IN-INDIA-badge]][MADE-IN-INDIA] [![GitHub release (latest by date)][GitHub release (latest by date)-badge]][GitHub release (latest by date)] [![Downloads][Downloads-badge]][Downloads] ![latest download][Latest-Downloads-badge] [![Docker Pulls][Docker Pulls-badge]][Docker Status]

| Platforms | [![Windows][Windows-badge]][Windows] | [![Linux(x64)][Linux-badge_x64]][Linux_x64] [![Linux(arm64)][Linux-badge_arm64]][Linux_arm64] | [![Mac OS(x64)][Mac OS-badge_x64]][Mac OS_x64] [![Mac OS(arm64)][Mac OS-badge_arm64]][Mac OS_arm64] | [![Docker Status][Docker Status-badge]][Docker Status] |
| :-------------: | :-----------------: | :-----------------: | :-----------------: | :-----------------: |
| Package / Docs | [![Documentation][Documentation-badge]][Documentation] [![OpenSSF Best Practices][OpenSSF-Badge]][OpenSSF-pkbrokers] | [![PyPI][pypi-badge]][pypi] | [![is wheel][wheel-badge]][pypi] | ![github license][github-license] |
| Tests/Code-Quality | [![CodeFactor][Codefactor-badge]][Codefactor] | [![Coverage Status][Coverage-Status-badge]][Coverage-Status] | [![codecov][codecov-badge]][codecov] | [![After Market][After Market-badge]][After Market] |

---

## Table of Contents

- [What is PKBrokers?](#what-is-pkbrokers)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture Overview](#architecture-overview)
- [Core Modules](#core-modules)
  - [In-Memory Candle Store](#1-in-memory-candle-store)
  - [Data Manager](#2-data-manager)
  - [Kite Instruments](#3-kite-instruments)
  - [Tick Watcher](#4-tick-watcher)
  - [Local Candle Database](#5-local-candle-database)
  - [Telegram Bots](#6-telegram-bots)
  - [Authentication](#7-authentication)
- [API Reference](#api-reference)
- [Environment Variables](#environment-variables)
- [Contributing](#contributing)
- [Related Projects](#related-projects)

---

## What is PKBrokers?

**PKBrokers** is a high-performance Python library for connecting to stock brokers (primarily Zerodha's Kite Connect) to fetch real-time market data, instruments, and ticks. Key features include:

- 🚀 **High-Performance Candle Store** - O(1) access to OHLCV candles across 10 timeframes
- 📊 **Real-Time Tick Processing** - WebSocket-based tick aggregation
- 💾 **Multi-Source Data Management** - SQLite, Turso, pickle files, and Kite API
- 🤖 **Telegram Bot Integration** - Distribute tick data via Telegram
- 🔐 **Automated Authentication** - TOTP-based Kite login
- 📦 **24/7 Data Availability** - GitHub-based data persistence

This library is part of the [PKScreener](https://github.com/pkjmesra/PKScreener) ecosystem.

---

## Installation

### From PyPI

```bash
pip install pkbrokers
```

### From Source

```bash
git clone https://github.com/pkjmesra/pkbrokers.git
cd pkbrokers
pip install -r requirements.txt
pip install -e .
```

### Requirements

- Python 3.9+
- Zerodha Kite Connect account (for real-time data)
- See `requirements.txt` for dependencies

---

## Quick Start

### High-Performance Data Provider

```python
from pkbrokers.kite import get_candle_store, HighPerformanceDataProvider

# Get singleton candle store
store = get_candle_store()

# Or use high-level data provider
provider = HighPerformanceDataProvider()

# Get 5-minute candles for any stock
df = provider.get_stock_data("RELIANCE", interval="5m", count=50)

# Get current day's OHLCV
ohlcv = provider.get_current_ohlcv("TCS")
print(f"Open: {ohlcv['open']}, High: {ohlcv['high']}, Low: {ohlcv['low']}, Close: {ohlcv['close']}")
```

### Data Manager (Multi-Source)

```python
from pkbrokers.kite.datamanager import InstrumentDataManager

# Initialize manager
manager = InstrumentDataManager()

# Execute data synchronization
success = manager.execute()

if success:
    # Access stock data
    reliance = manager.pickle_data["RELIANCE"]
    df = pd.DataFrame(
        data=reliance['data'],
        columns=reliance['columns'],
        index=reliance['index']
    )
    print(f"Shape: {df.shape}")
```

### Kite Authentication

```python
from pkbrokers.kite.examples.externals import kite_auth

# Authenticate and get access token
# Requires KUSER, KPWD, KTOTP environment variables
kite_auth()

# Token is now available as KTOKEN
from PKDevTools.classes.Environment import PKEnvironment
token = PKEnvironment().KTOKEN
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            PKBrokers Architecture                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │                     Application Layer                          │         │
│  │            PKScreener | Custom Applications                    │         │
│  └─────────────────────────────┬──────────────────────────────────┘         │
│                                │                                            │
│  ┌─────────────────────────────▼──────────────────────────────────┐         │
│  │                      Data Provider API                         │         │
│  │   HighPerformanceDataProvider | InstrumentDataManager          │         │
│  └─────────────────────────────┬──────────────────────────────────┘         │
│                                │                                            │
│       ┌────────────────────────┼────────────────────────┐                   │
│       │                        │                        │                   │
│  ┌────▼────┐           ┌───────▼───────┐        ┌───────▼───────┐           │
│  │InMemory │           │  Local SQLite │        │ Remote Data   │           │
│  │Candle   │           │  Database     │        │ (GitHub/Turso)│           │
│  │Store    │           │               │        │               │           │
│  └────┬────┘           └───────────────┘        └───────────────┘           │
│       │                                                                     │
│  ┌────▼──────────────────────────────────────────────────────────-┐         │
│  │                    Tick Processing Layer                       │         │
│  │   KiteTokenWatcher | CandleAggregator | TickProcessor          │         │
│  └────────────────────────────┬───────────────────────────────────┘         │
│                               │                                             │
│  ┌────────────────────────────▼───────────────────────────────────┐         │
│  │                     WebSocket Layer                            │         │
│  │          ZerodhaWebSocketClient | KiteTicker                   │         │
│  └────────────────────────────┬───────────────────────────────────┘         │
│                               │                                             │
│  ┌────────────────────────────▼───────────────────────────────────┐         │
│  │                  Kite Connect API / Authentication             │         │
│  │            Authenticator | KiteInstruments                     │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────┐         │
│  │                      Bot Layer (Telegram)                      │         │
│  │         PKTickBot | Orchestrator | Consumer                    │         │
│  └────────────────────────────────────────────────────────────────┘         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```
See more details on [Architecture](https://github.com/pkjmesra/PKBrokers/blob/main/docs/ARCHITECTURE.md)
---

## Core Modules

### 1. In-Memory Candle Store

High-performance, in-memory OHLCV storage with O(1) access to all timeframes.

```python
from pkbrokers.kite.inMemoryCandleStore import InMemoryCandleStore, get_candle_store

# Get singleton instance
store = get_candle_store()

# Process incoming tick
store.process_tick({
    'instrument_token': 256265,
    'last_price': 21500.50,
    'volume': 1000000,
    'timestamp': datetime.now()
})

# Get completed candles
candles = store.get_candles(
    instrument_token=256265,
    interval='5m',
    count=50
)

# Get current forming candle
current = store.get_current_candle(
    instrument_token=256265,
    interval='5m'
)

# Export to ticks.json
store.save_ticks_json("/path/to/ticks.json")

# Get statistics
stats = store.get_stats()
print(f"Instruments: {stats['instrument_count']}")
print(f"Ticks processed: {stats['ticks_processed']}")
```

#### Supported Timeframes

| Interval | Description | Max Candles Stored |
|----------|-------------|-------------------|
| `1m` | 1 minute | 375 (full day) |
| `2m` | 2 minutes | 188 |
| `3m` | 3 minutes | 125 |
| `4m` | 4 minutes | 94 |
| `5m` | 5 minutes | 75 |
| `10m` | 10 minutes | 38 |
| `15m` | 15 minutes | 25 |
| `30m` | 30 minutes | 13 |
| `60m` | 60 minutes | 7 |
| `day` | Daily | 1 |

#### Features

- **O(1) Access**: Instant lookup via hash-based indexing
- **No Rate Limits**: Unlike Yahoo Finance
- **Auto-Persistence**: Saves to disk every 5 minutes
- **Memory Efficient**: ~100MB for 2000 instruments
- **Thread-Safe**: Lock-protected operations

---

### 2. Data Manager

Comprehensive data synchronization from multiple sources.

```python
from pkbrokers.kite.datamanager import InstrumentDataManager

manager = InstrumentDataManager()

# Set specific stocks (optional)
manager.list_stock_codes = ["RELIANCE", "TCS", "INFY"]

# Execute synchronization
# Priority: SQLite → InMemoryCandleStore → Kite API → Pickle files
success = manager.execute()

# Access data
if success:
    for symbol, data in manager.pickle_data.items():
        df = pd.DataFrame(
            data=data['data'],
            columns=data['columns'],
            index=data['index']
        )
        print(f"{symbol}: {len(df)} rows")
```

#### Data Source Priority

1. **During Market Hours**:
   - Local SQLite database
   - InMemoryCandleStore (real-time ticks)
   - Kite API (authenticated)
   - GitHub ticks.json

2. **After Market Hours**:
   - Local pickle files
   - Remote GitHub pickle files

---

### 3. Kite Instruments

Manage instrument data from Kite Connect API.

```python
from pkbrokers.kite.instruments import KiteInstruments, Instrument

# Initialize with credentials
kite = KiteInstruments(
    api_key="your_api_key",
    access_token="your_access_token"
)

# Sync instruments from Kite API
kite.sync_instruments(force_fetch=True)

# Get instrument count
count = kite.get_instrument_count()
print(f"Total instruments: {count}")

# Get NSE stocks only
equities = kite.get_equities(only_nse_stocks=True)

# Get instrument tokens for subscription
tokens = kite.get_instrument_tokens(equities)

# Fetch instrument by token
instrument = kite.get_instrument(256265)  # NIFTY 50
print(f"Symbol: {instrument.tradingsymbol}")
```

#### Instrument Model

```python
@dataclass
class Instrument:
    instrument_token: int      # Unique identifier
    exchange_token: str        # Exchange-specific token
    tradingsymbol: str         # Trading symbol (e.g., 'RELIANCE')
    name: Optional[str]        # Full name
    last_price: Optional[float]
    expiry: Optional[str]      # For derivatives
    strike: Optional[float]    # For options
    tick_size: float
    lot_size: int
    instrument_type: str       # EQ, FUT, OPT, INDEX
    segment: str               # NSE, BSE
    exchange: str
    last_updated: str
    nse_stock: bool
```

---

### 4. Tick Watcher

WebSocket-based real-time tick processing.

```python
from pkbrokers.kite.kiteTokenWatcher import KiteTokenWatcher

# Initialize watcher
watcher = KiteTokenWatcher()

# Start watching (blocking)
try:
    watcher.watch(test_mode=False)
except KeyboardInterrupt:
    watcher.stop()
```

#### Command-Line Usage

```bash
# Start tick watcher
pkkite --ticks

# Test mode (3 minutes)
pkkite --ticks --test

# Authenticate first
pkkite --auth

# Fetch historical data
pkkite --history=5minute
```

---

### 5. Local Candle Database

SQLite-based candle storage for persistence.

```python
from pkbrokers.kite.localCandleDatabase import LocalCandleDatabase

# Initialize database
db = LocalCandleDatabase()

# Save daily candle
db.save_daily_candle(
    symbol="RELIANCE",
    date=date.today(),
    open_price=2500.0,
    high_price=2550.0,
    low_price=2480.0,
    close_price=2530.0,
    volume=1000000
)

# Load candles
candles = db.load_daily_candles("RELIANCE", days=30)

# Save intraday candles
db.save_intraday_candle(
    symbol="RELIANCE",
    timestamp=datetime.now(),
    interval="5m",
    open_price=2500.0,
    high_price=2510.0,
    low_price=2495.0,
    close_price=2505.0,
    volume=50000
)
```

---

### 6. Telegram Bots

#### PKTickBot

Telegram bot for distributing tick data.

```python
from pkbrokers.bot.tickbot import PKTickBot

bot = PKTickBot(
    bot_token="your_bot_token",
    ticks_file_path="/path/to/ticks.json",
    chat_id="-1001234567890"
)

# Start bot (blocking)
bot.run()
```

**Available Commands**:
| Command | Description |
|---------|-------------|
| `/ticks` | Get zipped ticks.json file |
| `/db` | Get local SQLite database |
| `/status` | Check bot and data status |
| `/top` | Get top 20 ticking symbols |
| `/token` | Get current KTOKEN |
| `/refresh_token` | Generate new KTOKEN |
| `/restart` | Refresh token and restart watcher |
| `/test_ticks` | Start 3-minute tick test |
| `/help` | Show help message |

#### Orchestrator

Multi-process orchestrator for bot and data management.

```python
from pkbrokers.bot.orchestrator import Orchestrator

orchestrator = Orchestrator()

# Check if market is open
if orchestrator.should_run_kite_process():
    orchestrator.start_kite_process()
```

---

### 7. Authentication

Automated Kite Connect authentication using TOTP.

```python
from pkbrokers.kite.authenticator import KiteAuthenticator

auth = KiteAuthenticator(
    user_id="your_user_id",
    password="your_password",
    totp_secret="your_totp_secret",
    api_key="your_api_key"
)

# Get access token
access_token = auth.authenticate()

# Token is automatically saved to environment
```

**Environment Variables Required**:
- `KUSER`: Kite user ID
- `KPWD`: Kite password
- `KTOTP`: TOTP secret key
- `KAPI`: Kite API key

---

### 8. PKL Generator Script

Reliable script for generating pkl files from ticks.json with historical data merge.

```python
# Command line usage
python pkbrokers/scripts/generate_pkl_from_ticks.py --data-dir results/Data --verbose

# Programmatic usage
from pkbrokers.scripts.generate_pkl_from_ticks import (
    download_historical_pkl,
    download_ticks_json,
    convert_ticks_to_candles,
    merge_candles,
    save_pkl_files
)

# Download and merge data
historical = download_historical_pkl()  # ~37MB from GitHub
ticks = download_ticks_json()           # Today's ticks
candles = convert_ticks_to_candles(ticks)
merged = merge_candles(historical, candles)
save_pkl_files(merged, "results/Data")
```

**What it does**:
1. Downloads ticks.json (local or from GitHub)
2. Downloads historical pkl (~37MB) from GitHub
3. Converts ticks to candle format
4. Merges today's data with historical
5. Saves both intraday and daily pkl files

**Output Files**:
| File | Description |
|------|-------------|
| `stock_data_DDMMYYYY.pkl` | Daily candles merged with historical |
| `daily_candles.pkl` | Same as above (generic name) |
| `intraday_stock_data_DDMMYYYY.pkl` | Today's intraday data only |
| `intraday_1m_candles.pkl` | Same as above (generic name) |

---

## API Reference

### Main Exports

```python
from pkbrokers.kite import (
    # Candle Store
    InMemoryCandleStore,
    get_candle_store,
    
    # Data Providers
    HighPerformanceDataProvider,
    InstrumentDataManager,
    
    # Instruments
    KiteInstruments,
    Instrument,
    
    # Tick Processing
    KiteTokenWatcher,
    CandleAggregator,
    
    # Database
    LocalCandleDatabase,
    
    # Authentication
    KiteAuthenticator,
)

from pkbrokers.bot import (
    PKTickBot,
    Orchestrator,
)
```

### Module Structure

```
pkbrokers/
├── __init__.py
├── bot/
│   ├── __init__.py
│   ├── consumer.py          # Data consumer
│   ├── orchestrator.py      # Multi-process orchestrator
│   └── tickbot.py           # Telegram tick bot
├── kite/
│   ├── __init__.py
│   ├── authenticator.py     # Kite authentication
│   ├── candleAggregator.py  # Tick → Candle aggregation
│   ├── datamanager.py       # Multi-source data manager
│   ├── databasewriter.py    # Database writer
│   ├── inMemoryCandleStore.py  # In-memory candle store
│   ├── instrumentHistory.py # Historical data
│   ├── instruments.py       # Instrument management
│   ├── kiteTokenWatcher.py  # WebSocket tick watcher
│   ├── localCandleDatabase.py  # SQLite candle storage
│   ├── tickProcessor.py     # Tick processing
│   ├── ticks.py             # Tick utilities
│   ├── trader.py            # Trading operations
│   ├── zerodhaWebSocketClient.py  # WebSocket client
│   └── examples/
│       ├── externals.py     # External helpers
│       └── pkkite.py        # CLI entry point
└── scripts/
    └── publish_candle_data.py  # Data publishing
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KUSER` | Yes* | Kite user ID |
| `KPWD` | Yes* | Kite password |
| `KTOTP` | Yes* | TOTP secret for 2FA |
| `KAPI` | Yes* | Kite API key |
| `KTOKEN` | Auto | Access token (auto-generated) |
| `TOKEN` | Yes** | Telegram bot token |
| `CHAT_ID` | Yes** | Default Telegram chat ID |
| `TURSO_DB_URL` | No | Turso database URL |
| `TURSO_DB_AUTH_TOKEN` | No | Turso auth token |

*Required for Kite Connect features  
**Required for Telegram bot features

---

## Contributing

### Development Setup

```bash
git clone https://github.com/pkjmesra/pkbrokers.git
cd pkbrokers
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

### Running Tests

```bash
pytest test/
pytest --cov=pkbrokers test/
```

### Code Style

```bash
ruff check pkbrokers/
ruff format pkbrokers/
```

---

## Related Projects

- [PKScreener](https://github.com/pkjmesra/PKScreener) - Stock screening application
- [PKDevTools](https://github.com/pkjmesra/PKDevTools) - Common development tools
- [PKNSETools](https://github.com/pkjmesra/PKNSETools) - NSE market data tools

---

## License

MIT License - see [LICENSE](LICENSE) file.

---

[![Star History Chart](https://api.star-history.com/svg?repos=pkjmesra/pkbrokers&type=Date)](https://star-history.com/#pkjmesra/pkbrokers&Date)

[MADE-IN-INDIA-badge]: https://img.shields.io/badge/MADE%20WITH%20%E2%9D%A4%20IN-INDIA-orange
[MADE-IN-INDIA]: https://en.wikipedia.org/wiki/India
[Windows-badge]: https://img.shields.io/badge/Windows-0078D6?logo=windows&logoColor=white
[Windows]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250914.18/pkkite.exe
[Linux-badge_x64]: https://img.shields.io/badge/Linux(x64)-FCC624?logo=linux&logoColor=black
[Linux_x64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250914.18/pkkite_x64.bin
[Linux-badge_arm64]: https://img.shields.io/badge/Linux(arm64)-FCC624?logo=linux&logoColor=black
[Linux_arm64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250914.18/pkkite_arm64.bin
[Mac OS-badge_x64]: https://img.shields.io/badge/mac%20os(x64)-D3D3D3?logo=apple&logoColor=000000
[Mac OS_x64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250914.18/pkkite_x64.run
[Mac OS-badge_arm64]: https://img.shields.io/badge/mac%20os(arm64)-D3D3D3?logo=apple&logoColor=000000
[Mac OS_arm64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250914.18/pkkite_arm64.run
[GitHub release (latest by date)-badge]: https://img.shields.io/github/v/release/pkjmesra/pkbrokers
[GitHub release (latest by date)]: https://github.com/pkjmesra/pkbrokers/releases/latest
[pypi-badge]: https://img.shields.io/pypi/v/pkbrokers.svg?style=flat-square
[pypi]: https://pypi.python.org/pypi/pkbrokers
[wheel-badge]: https://img.shields.io/pypi/wheel/pkbrokers.svg?style=flat-square
[github-license]: https://img.shields.io/github/license/pkjmesra/pkbrokers
[Downloads-badge]: https://static.pepy.tech/personalized-badge/pkbrokers?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=Total%20Downloads
[Downloads]: https://pepy.tech/project/pkbrokers
[Latest-Downloads-badge]: https://img.shields.io/github/downloads-pre/pkjmesra/pkbrokers/latest/total?logo=github
[Coverage-Status-badge]: https://coveralls.io/repos/github/pkjmesra/pkbrokers/badge.svg?kill_cache=1
[Coverage-Status]: https://coveralls.io/github/pkjmesra/pkbrokers?branch=main
[codecov-badge]: https://codecov.io/gh/pkjmesra/pkbrokers/branch/main/graph/badge.svg
[codecov]: https://codecov.io/gh/pkjmesra/pkbrokers
[Documentation-badge]: https://readthedocs.org/projects/pkbrokers/badge/?version=latest
[Documentation]: https://pkbrokers.readthedocs.io/en/latest/?badge=latest
[Docker Status-badge]: https://img.shields.io/docker/automated/pkjmesra/pkbrokers.svg
[Docker Status]: https://hub.docker.com/repository/docker/pkjmesra/pkbrokers
[Docker Pulls-badge]: https://img.shields.io/docker/pulls/pkjmesra/pkbrokers.svg
[Codefactor-badge]: https://www.codefactor.io/repository/github/pkjmesra/pkbrokers/badge
[Codefactor]: https://www.codefactor.io/repository/github/pkjmesra/pkbrokers
[After Market-badge]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w9-workflow-download-data.yml/badge.svg
[After Market]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w9-workflow-download-data.yml
[OpenSSF-Badge]: https://www.bestpractices.dev/projects/10011/badge
[OpenSSF-pkbrokers]: https://www.bestpractices.dev/projects/10011
