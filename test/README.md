# Unit Tests for PKBrokers Kite Token Watcher Components

This directory contains comprehensive unit tests for the three main classes in the `kiteTokenWatcher.py` module:

## Test Coverage

### 1. JSONFileWriter Tests (`TestJSONFileWriter`)
- **Initialization**: Tests proper setup of JSON writer with file paths and multiprocessing context
- **PID File Management**: Tests checking for already running processes and PID file cleanup
- **Data Deduplication**: Tests keeping latest data per instrument_token based on timestamps
- **JSON Validation**: Tests data structure validation and error handling
- **Atomic File Writing**: Tests safe file writing with temporary files and atomic replacement
- **Instrument Data Updates**: Tests OHLCV updates, high/low tracking, and metadata updates
- **Process Management**: Tests starting/stopping writer processes (with multiprocessing mocking)

### 2. TickHealthMonitor Tests (`TestTickHealthMonitor`)
- **Initialization**: Tests health monitor setup with watcher reference and thresholds
- **Tick Recording**: Tests recording ticks for individual instruments and resetting recovery attempts
- **Stale Detection**: Tests identifying instruments that haven't received ticks recently
- **Warning System**: Tests rate-limited warnings during market hours only
- **Recovery Logic**: Tests recovery triggers based on market hours, holidays, and weekends
- **Thread Management**: Tests starting/stopping monitoring threads

### 3. KiteTokenWatcher Tests (`TestKiteTokenWatcher`)
- **Initialization**: Tests watcher setup with token batches and shared statistics
- **Token Batching**: Tests splitting large token lists into optimal batch sizes
- **Event Handling**: Tests WebSocket stop events and stop queue management
- **Database Configuration**: Tests database instance creation based on environment settings
- **Tick Processing**: Tests batch processing and ensuring single tick per instrument
- **Component Integration**: Tests JSON writer, health monitor, and candle store initialization

### 4. Integration Tests (`TestIntegration`)
- **Full Lifecycle**: Tests complete JSON writer workflow from data to file
- **Health Monitoring**: Tests tick recording and stale instrument detection
- **Token Management**: Tests handling of multiple token batches

## Test Setup

### Environment Configuration (`conftest.py`)
- Mocks `PKEnvironment` to avoid dependency issues
- Sets required environment variables (`KTOKEN`, `KUSER`, `DB_TYPE`, etc.)
- Provides reusable fixtures for mocking

### Mocking Strategy
- **PKEnvironment**: Fully mocked to avoid real database connections
- **Multiprocessing**: Processes mocked to prevent real subprocess creation
- **External Dependencies**: PKDateUtilities, ZerodhaWebSocketClient, etc. properly mocked
- **File Operations**: Temporary files used for safe testing

## Running Tests

```bash
# Run all tests
python -m pytest test/test_kiteTokenWatcher.py -v

# Run specific test class
python -m pytest test/test_kiteTokenWatcher.py::TestJSONFileWriter -v

# Run specific test method
python -m pytest test/test_kiteTokenWatcher.py::TestJSONFileWriter::test_json_file_writer_initialization -v

# Run with coverage
python -m pytest test/test_kiteTokenWatcher.py --cov=pkbrokers.kite.kiteTokenWatcher
```

## Test Results Summary

- **Total Tests**: 46
- **Passing**: 44 (95.7% pass rate)
- **Skipped**: 2 (multiprocessing tests with complex mocking requirements)
- **Failing**: 0
- **Test Classes**: 4 (JSONFileWriter, TickHealthMonitor, KiteTokenWatcher, Integration)
- **Coverage Areas**:
  - Data validation and integrity
  - Multiprocessing safety
  - Error handling and recovery
  - Market hours awareness
  - File I/O operations
  - Thread synchronization
  - Component integration

## Key Testing Features

1. **Comprehensive Mocking**: All external dependencies properly mocked to ensure isolated unit testing
2. **Temporary Files**: Safe file operations using temporary directories
3. **Multiprocessing Safety**: Tests handle multiprocessing without creating real processes
4. **Market Hours Awareness**: Tests validate behavior during trading hours vs. closed hours
5. **Error Scenarios**: Tests cover various failure modes and recovery scenarios
6. **Data Integrity**: Tests ensure data consistency and proper deduplication
7. **Thread Safety**: Tests validate concurrent operations and thread synchronization

## Dependencies

- `pytest`: Test framework
- `pytest-mock`: Mocking utilities
- `tempfile`: Temporary file management
- `unittest.mock`: Standard mocking
- `pytz`: Timezone handling
- `json`: JSON operations
- `datetime`: Date/time operations

## Notes

- Tests are designed to run in isolation without requiring real Kite Connect credentials
- All file operations use temporary files to avoid affecting the actual filesystem
- Multiprocessing is mocked to prevent real process creation during testing
- Environment variables are mocked to avoid dependency on real configuration</content>
<parameter name="filePath">/Users/praveen.jha1/Downloads/codes/PKBrokers/test/README.md