[
  {
    "type": "table",
    "name": "users",
    "tbl_name": "users",
    "rootpage": 2,
    "sql": "CREATE TABLE users (userid INTEGER PRIMARY KEY,username TEXT,name TEXT,email TEXT,mobile INTEGER,otpvaliduntil TEXT,totptoken TEXT NOT NULL,subscriptionmodel TEXT, lastotp TEXT)"
  },
  {
    "type": "table",
    "name": "scannerjobs",
    "tbl_name": "scannerjobs",
    "rootpage": 9,
    "sql": "CREATE TABLE `scannerjobs` (`scannerId` text, `users` integer)"
  },
  {
    "type": "table",
    "name": "alertssummary",
    "tbl_name": "alertssummary",
    "rootpage": 14,
    "sql": "CREATE TABLE alertssummary (\n    id INTEGER PRIMARY KEY AUTOINCREMENT,\n    userId INTEGER NOT NULL,\n    scannerId TEXT NOT NULL,\n    timestamp TEXT NOT NULL\n)"
  },
  {
    "type": "table",
    "name": "sqlite_sequence",
    "tbl_name": "sqlite_sequence",
    "rootpage": 15,
    "sql": "CREATE TABLE sqlite_sequence(name,seq)"
  },
  {
    "type": "table",
    "name": "alertsubscriptions",
    "tbl_name": "alertsubscriptions",
    "rootpage": 8,
    "sql": "CREATE TABLE alertsubscriptions (userId INTEGER, balance REAL, scannerJobs text)"
  },
  {
    "type": "table",
    "name": "instruments",
    "tbl_name": "instruments",
    "rootpage": 10,
    "sql": "CREATE TABLE instruments (\n                    instrument_token INTEGER,\n                    exchange_token TEXT,\n                    tradingsymbol TEXT NOT NULL,\n                    name TEXT,\n                    last_price REAL,\n                    expiry TEXT,\n                    strike REAL,\n                    tick_size REAL NOT NULL CHECK(tick_size >= 0),\n                    lot_size INTEGER NOT NULL CHECK(lot_size >= 0),\n                    instrument_type TEXT NOT NULL,\n                    segment TEXT NOT NULL,\n                    exchange TEXT NOT NULL,\n                    last_updated TEXT DEFAULT (datetime('now')),\n                    PRIMARY KEY (exchange, tradingsymbol, instrument_type)\n                ) STRICT"
  },
  {
    "type": "index",
    "name": "sqlite_autoindex_instruments_1",
    "tbl_name": "instruments",
    "rootpage": 11,
    "sql": null
  },
  {
    "type": "index",
    "name": "idx_instrument_token",
    "tbl_name": "instruments",
    "rootpage": 12,
    "sql": "CREATE INDEX idx_instrument_token \n                ON instruments(instrument_token)"
  },
  {
    "type": "index",
    "name": "idx_tradingsymbol_segment",
    "tbl_name": "instruments",
    "rootpage": 17,
    "sql": "CREATE INDEX idx_tradingsymbol_segment \n                ON instruments(tradingsymbol, segment)"
  },
  {
    "type": "table",
    "name": "ticks",
    "tbl_name": "ticks",
    "rootpage": 13,
    "sql": "CREATE TABLE ticks (\n                    instrument_token INTEGER,\n                    timestamp DATETIME, -- Will use registered converter\n                    last_price REAL,\n                    day_volume INTEGER,\n                    oi INTEGER,\n                    buy_quantity INTEGER,\n                    sell_quantity INTEGER,\n                    high_price REAL,\n                    low_price REAL,\n                    open_price REAL,\n                    prev_day_close REAL,\n                    PRIMARY KEY (instrument_token)\n                ) WITHOUT ROWID"
  },
  {
    "type": "table",
    "name": "market_depth",
    "tbl_name": "market_depth",
    "rootpage": 19,
    "sql": "CREATE TABLE market_depth (\n                    instrument_token INTEGER,\n                    timestamp DATETIME, -- Will use registered converter\n                    depth_type TEXT CHECK(depth_type IN ('bid', 'ask')),\n                    position INTEGER CHECK(position BETWEEN 1 AND 5),\n                    price REAL,\n                    quantity INTEGER,\n                    orders INTEGER,\n                    PRIMARY KEY (instrument_token, depth_type, position),\n                    FOREIGN KEY (instrument_token) \n                        REFERENCES ticks(instrument_token)\n                        ON DELETE CASCADE\n                ) WITHOUT ROWID"
  },
  {
    "type": "index",
    "name": "idx_depth_main",
    "tbl_name": "market_depth",
    "rootpage": 20,
    "sql": "CREATE INDEX idx_depth_main \n                ON market_depth(instrument_token)"
  },
  {
    "type": "index",
    "name": "idx_timestamp",
    "tbl_name": "ticks",
    "rootpage": 21,
    "sql": "CREATE INDEX idx_timestamp ON ticks(timestamp)"
  },
  {
    "type": "index",
    "name": "idx_instrument",
    "tbl_name": "ticks",
    "rootpage": 22,
    "sql": "CREATE INDEX idx_instrument ON ticks(instrument_token)"
  },
  {
    "type": "table",
    "name": "instrument_last_update",
    "tbl_name": "instrument_last_update",
    "rootpage": 23,
    "sql": "CREATE TABLE instrument_last_update (\n                    instrument_token INTEGER PRIMARY KEY,\n                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n                )"
  },
  {
    "type": "trigger",
    "name": "update_timestamp_insert",
    "tbl_name": "ticks",
    "rootpage": 0,
    "sql": "CREATE TRIGGER update_timestamp_insert\n                AFTER INSERT ON ticks\n                FOR EACH ROW\n                BEGIN\n                    INSERT INTO instrument_last_update (instrument_token, last_updated)\n                    VALUES (NEW.instrument_token, CURRENT_TIMESTAMP)\n                    ON CONFLICT(instrument_token) DO UPDATE \n                    SET last_updated = CURRENT_TIMESTAMP;\n                END"
  },
  {
    "type": "trigger",
    "name": "update_timestamp_update",
    "tbl_name": "ticks",
    "rootpage": 0,
    "sql": "CREATE TRIGGER update_timestamp_update\n                AFTER UPDATE ON ticks\n                FOR EACH ROW\n                BEGIN\n                    INSERT INTO instrument_last_update (instrument_token, last_updated)\n                    VALUES (NEW.instrument_token, CURRENT_TIMESTAMP)\n                    ON CONFLICT(instrument_token) DO UPDATE \n                    SET last_updated = CURRENT_TIMESTAMP;\n                END"
  },
  {
    "type": "index",
    "name": "idx_Userid",
    "tbl_name": "users",
    "rootpage": 26,
    "sql": "CREATE UNIQUE INDEX idx_Userid ON users(userId)"
  },
  {
    "type": "index",
    "name": "idx_alertsUserid",
    "tbl_name": "alertsubscriptions",
    "rootpage": 29,
    "sql": "CREATE UNIQUE INDEX idx_alertsUserid ON alertsubscriptions(userId)"
  },
  {
    "type": "index",
    "name": "idx_scanid",
    "tbl_name": "scannerjobs",
    "rootpage": 30,
    "sql": "CREATE UNIQUE INDEX idx_scanid ON ScannerJobs(ScannerID)"
  },
  {
    "type": "table",
    "name": "instrument_history",
    "tbl_name": "instrument_history",
    "rootpage": 31,
    "sql": "CREATE TABLE instrument_history (\n    instrument_token INTEGER NOT NULL,\n    timestamp TEXT NOT NULL,  -- ISO 8601 format with timezone\n    open REAL NOT NULL,\n    high REAL NOT NULL,\n    low REAL NOT NULL,\n    close REAL NOT NULL,\n    volume INTEGER NOT NULL,\n    oi INTEGER NULL, date TEXT \nGENERATED ALWAYS AS (substr(timestamp, 1, 10)) STORED,     -- Open Interest\n    PRIMARY KEY (instrument_token, timestamp)\n)"
  },
  {
    "type": "index",
    "name": "sqlite_autoindex_instrument_history_1",
    "tbl_name": "instrument_history",
    "rootpage": 32,
    "sql": null
  },
  {
    "type": "index",
    "name": "idx_instrument_history_timestamp",
    "tbl_name": "instrument_history",
    "rootpage": 33,
    "sql": "CREATE INDEX idx_instrument_history_timestamp \nON instrument_history (timestamp)"
  },
  {
    "type": "index",
    "name": "idx_instrument_history_token",
    "tbl_name": "instrument_history",
    "rootpage": 34,
    "sql": "CREATE INDEX idx_instrument_history_token \nON instrument_history (instrument_token)"
  },
  {
    "type": "index",
    "name": "idx_instrument_history_token_timestamp",
    "tbl_name": "instrument_history",
    "rootpage": 35,
    "sql": "CREATE INDEX idx_instrument_history_token_timestamp \nON instrument_history (instrument_token, timestamp)"
  },
  {
    "type": "index",
    "name": "idx_instrument_history_date",
    "tbl_name": "instrument_history",
    "rootpage": 36,
    "sql": "CREATE INDEX idx_instrument_history_date ON instrument_history(date)"
  }
]