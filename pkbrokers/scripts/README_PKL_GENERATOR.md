# PKL Generator Documentation

## Overview

The generate_pkl_from_ticks.py script is a unified PKL generator that creates processed stock data files for the PKBrokers trading system. It handles multiple data sources, converts tick data to candle format, merges with historical data, and produces both daily aggregated and intraday 1-minute candle files.

## Architecture

INPUT SOURCES:
- GitHub Historical PKL (last 30 days, ~37MB)
- Local/Remote ticks.json (real-time tick data)
- SQLite Database (instrument_history.db)
- InMemoryCandleStore (real-time aggregated candles)

PROCESSING PIPELINE:
1. Download/Load historical base data from GitHub
2. Retrieve current day's aggregated data from InMemoryCandleStore
3. Load/Download fresh ticks.json data
4. Convert ticks to 1-minute OHLCV candles
5. Merge with historical data (deduplication by timestamp)
6. Trim daily data to 251 rows per stock (1 trading year)
7. Save as daily_candles.pkl & stock_data_<DDMMYYYY>.pkl
8. Save intraday_1m_candles.pkl & intraday_stock_data_<DDMMYYYY>.pkl

## Key Features

Intelligent Freshness Detection:
- Checks actual data timestamps, not just filenames
- Determines if updates are needed based on trading dates vs. current date, market hours (9:15 AM - 3:30 PM IST), and non-trading days (weekends/holidays)

Data Quality Validation:
- Validates each stock has at least 248 rows (approx 1 year of data)
- Rejects stale data (more than 20 trading days behind)
- Checks file size (greater than 1MB minimum)

Safe File Operations:
- Atomic writes using temporary files + rename
- Verification after write to prevent corruption
- Backup of previous files when applicable

Timezone Normalization:
- All timestamps converted to Asia/Kolkata (IST)
- Handles both timezone-aware and naive indices
- Consistent merging across data sources

## Installation

Prerequisites:
pip install pandas numpy requests pytz

Dependencies:
- PKDevTools - Date utilities and environment management
- pkbrokers - Kite broker integration and instrument mapping

## Usage

Basic Usage:

# Generate PKL files from ticks.json (default)
python generate_pkl_from_ticks.py

# Specify output directory
python generate_pkl_from_ticks.py --data-dir /path/to/output

# Load from SQLite database instead
python generate_pkl_from_ticks.py --from-db

# Trigger historical data download if stale
python generate_pkl_from_ticks.py --trigger-history

# Quiet mode (minimal output)
python generate_pkl_from_ticks.py --verbose False

Command Line Arguments:

--data-dir PATH        Output directory for PKL files (default: results/Data)
--from-db              Load from SQLite instead of ticks.json (default: False)
--db-path PATH         Specific SQLite database path (auto-detected if not specified)
--trigger-history      Trigger GitHub workflow if data stale (default: False)
--past-offset DAYS     Days to look back for PKL files (default: 30)
--verbose, -v          Verbose output (default: True)

## Data Formats

Daily PKL Format (stock_data_<DDMMYYYY>.pkl):
{
    "RELIANCE": pd.DataFrame with columns ['Open', 'High', 'Low', 'Close', 'Volume'],
    "TCS": pd.DataFrame,
    "HDFCBANK": pd.DataFrame,
    ... (100+ stocks)
}

Intraday PKL Format (intraday_stock_data_<DDMMYYYY>.pkl):
{
    "RELIANCE": pd.DataFrame with 1-minute OHLCV candles,
    "TCS": pd.DataFrame,
    ... (100+ stocks)
}

Ticks.json Format (Input):
{
    "instrument_token": {
        "trading_symbol": "RELIANCE",
        "last_updated": "2026-05-09T15:29:00",
        "ohlcv": {
            "open": 2450.5,
            "high": 2460.0,
            "low": 2445.0,
            "close": 2455.5,
            "volume": 1234567,
            "timestamp": "2026-05-09T15:29:00"
        }
    }
}

## Behavior by Scenario

Historical PKL Scenarios:

Scenario: File doesn't exist (404)
Behavior: Falls back to next date, continues trying up to past_offset days

Scenario: File exists but empty
Behavior: Rejected (min 100 instruments, 1MB size)

Scenario: Data from 1 month ago
Behavior: Accepted but missing_days > 0, workflow triggered if requested

Scenario: Data from 10 days ago
Behavior: Accepted, missing_days = approx 7, triggers history download

Scenario: Data until yesterday
Behavior: Accepted, missing_days = 0, attempts to merge today's ticks

Scenario: Data from 9:14 AM today
Behavior: Accepted, considered current, will merge new ticks if available

Scenario: Data from 9:20 AM today
Behavior: Same as above

Scenario: Data from 12:30 PM today
Behavior: Same as above

Scenario: Data from 3:29 PM today
Behavior: Accepted, approx 1 minute before market close

Scenario: Data from 3:30 PM today
Behavior: Accepted, market close data (complete day)

Ticks.json Scenarios:

Scenario: Not available on GitHub
Behavior: Returns None, falls back to historical only

Scenario: Data only until yesterday
Behavior: Rejected as stale (date < today's trading date)

Scenario: Data until 9:30 AM, current time 3:20 PM
Behavior: Accepted (same date), will merge partial day data

Scenario: Data until 2:44 PM today
Behavior: Accepted, partial day data

Scenario: Data until 3:29 PM today
Behavior: Accepted, nearly complete day

Scenario: Local file stale
Behavior: Falls back to GitHub download

Scenario: Local file fresh
Behavior: Used directly, skips download

## Output Files

Generated Files:
1. stock_data_<DDMMYYYY>.pkl - Daily candles with date in filename
2. daily_candles.pkl - Daily candles with generic name (overwritten)
3. intraday_stock_data_<DDMMYYYY>.pkl - Intraday 1-minute candles
4. intraday_1m_candles.pkl - Intraday candles with generic name

File Characteristics:
- Size: ~37-40 MB for daily data
- Instruments: 100+ NSE stocks
- Rows per stock: <= 251 (approx 1 trading year)
- Compression: Pickle with HIGHEST_PROTOCOL

## Error Handling

Graceful Degradation:
If primary data source fails:
1. Falls back to secondary source (GitHub -> Local -> DB)
2. Uses historical data only if new data unavailable
3. Skips intraday files if insufficient new data
4. Continues with partial data rather than failing

Retry Logic:
- 30-second timeout for HTTP requests
- Multiple URL attempts for each resource
- 30-day lookback for historical PKL files

## Testing

Running Tests:

# Run all tests
python test_generate_pkl.py

# Run specific test class
python -c "from test_generate_pkl import TestHistoricalPKLScenarios; TestHistoricalPKLScenarios().run()"

# With unittest discovery
python -m unittest discover -p "test_*.py" -v

Test Coverage:
The test suite covers:
- All historical PKL scenarios (12 cases)
- All ticks.json scenarios (8 cases)
- Data conversion functions
- Merge logic (including duplicates)
- File save operations (atomic writes)
- Data quality validation
- Integration end-to-end flows

## Troubleshooting

Common Issues:

Issue: "No historical pkl found on GitHub"
Cause: All PKL files in last 30 days are invalid or missing
Solution: Run with --trigger-history to force download

Issue: "Downloaded ticks.json is stale"
Cause: The latest data is from a previous trading day
Solution: Wait for market hours or run with --from-db

Issue: "Insufficient historical data" warning
Cause: Stock has <248 rows (less than 1 year of data)
Solution: Acceptable for newer stocks; no action needed

Issue: Pickle save fails
Cause: Disk full, permission denied, or data corruption
Solution: Check disk space, run with sudo if needed

Logging Levels:
- Default (verbose=True): Shows all steps with timestamps
- Quiet (verbose=False): Only errors and critical warnings

## Performance

Benchmarks:
- Download: ~37MB PKL file in 2-5 seconds
- Conversion: ~0.5-1 second for 100 stocks
- Merge: ~0.5-1 second
- Save: ~0.5-1 second per file

Optimization Tips:
1. Use local ticks.json when possible to avoid downloads
2. Set --past-offset lower (e.g., 15) for faster historical lookup
3. Run outside market hours when possible
4. Use --from-db for batch historical processing

## Version History

Version 2.0.0 (2026-05-09): Added InMemoryCandleStore integration, 251-row trim
Version 1.5.0 (2026-04-15): Added SQLite support, atomic writes
Version 1.0.0 (2026-03-01): Initial release

## License

Proprietary - PKBrokers Trading System

## Support

For issues or questions:
- GitHub Issues: PKBrokers Repository
- Documentation: Internal Wiki
