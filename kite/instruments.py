# -*- coding: utf-8 -*-
# """
#     The MIT License (MIT)

#     Copyright (c) 2023 pkjmesra

#     Permission is hereby granted, free of charge, to any person obtaining a copy
#     of this software and associated documentation files (the "Software"), to deal
#     in the Software without restriction, including without limitation the rights
#     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#     copies of the Software, and to permit persons to whom the Software is
#     furnished to do so, subject to the following conditions:

#     The above copyright notice and this permission notice shall be included in all
#     copies or substantial portions of the Software.

#     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#     IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#     FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#     LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#     OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#     SOFTWARE.

# """
import os
import sqlite3
import requests
import csv
from datetime import datetime
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('instrument_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NSEInstrumentSync:
    def __init__(self, api_key: str, access_token: str, db_path: str = 'instruments.db'):
        self.api_key = api_key
        self.access_token = access_token
        self.db_path = db_path
        self.base_url = "https://api.kite.trade"
        self.headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {self.api_key}:{self.access_token}"
        }

    def _init_db(self):
        """Initialize SQLite database with TEXT type for dates"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS instruments")
            # Create instruments table with TEXT for dates
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS instruments (
                    instrument_token INTEGER,
                    exchange_token TEXT,
                    tradingsymbol TEXT NOT NULL,
                    name TEXT,
                    last_price REAL,
                    expiry TEXT,  -- Changed from DATE to TEXT
                    strike REAL,
                    tick_size REAL NOT NULL,
                    lot_size INTEGER NOT NULL,
                    instrument_type TEXT NOT NULL CHECK(instrument_type IN ('EQ', 'FUT', 'CE', 'PE')),
                    segment TEXT NOT NULL,
                    exchange TEXT NOT NULL CHECK(exchange = 'NSE'),
                    last_updated TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (exchange, tradingsymbol, expiry, strike, instrument_type)
                )
            ''')
            
            # Create index for faster lookups
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_instrument_token 
                ON instruments(instrument_token)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tradingsymbol 
                ON instruments(tradingsymbol)
            ''')
            
            conn.commit()

    def _normalize_expiry(self, expiry_str: str | None) -> str | None:
        """Convert expiry date to YYYY-MM-DD format or None"""
        if not expiry_str:
            return None
        try:
            # Parse and reformat to ensure consistent format
            return datetime.strptime(expiry_str, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            logger.warning(f"Invalid expiry date format: {expiry_str}")
            return None

    def fetch_instruments(self) -> list[dict]:
        """Fetch NSE instruments from Kite API"""
        url = f"{self.base_url}/instruments/NSE"
        logger.info(f"Fetching instruments from {url}")
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            # Decompress gzipped response
            decompressed = response.content.decode('utf-8')
            
            # Parse CSV
            return list(csv.DictReader(decompressed.splitlines()))
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch instruments: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error processing instruments: {str(e)}")
            raise

    def store_instruments(self, instruments: list[dict]):
        """Upsert instruments into SQLite database with proper type handling"""
        if not instruments:
            logger.warning("No instruments to store")
            return
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            
            cursor = conn.cursor()
            
            # Prepare data with type conversion
            data = []
            for inst in instruments:
                try:
                    data.append((
                        int(inst['instrument_token']),
                        inst['exchange_token'],
                        inst['tradingsymbol'].strip(),
                        inst['name'].strip() if inst['name'] else None,
                        float(inst['last_price']) if inst['last_price'] else None,
                        self._normalize_expiry(inst['expiry']),
                        float(inst['strike']) if inst['strike'] else None,
                        float(inst['tick_size']),
                        int(inst['lot_size']),
                        inst['instrument_type'].strip(),
                        inst['segment'].strip(),
                        inst['exchange'].strip()
                    ))
                except (ValueError, KeyError) as e:
                    logger.warning(f"Skipping malformed instrument {inst.get('tradingsymbol')}: {str(e)}")
                    continue
            
            # Upsert with conflict resolution
            cursor.executemany('''
                INSERT INTO instruments (
                    instrument_token, exchange_token, tradingsymbol, name,
                    last_price, expiry, strike, tick_size, lot_size,
                    instrument_type, segment, exchange
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, tradingsymbol, expiry, strike, instrument_type) 
                DO UPDATE SET
                    instrument_token = excluded.instrument_token,
                    exchange_token = excluded.exchange_token,
                    name = excluded.name,
                    last_price = excluded.last_price,
                    tick_size = excluded.tick_size,
                    lot_size = excluded.lot_size,
                    segment = excluded.segment,
                    last_updated = datetime('now')
            ''', data)
            
            conn.commit()
            logger.info(f"Stored/updated {len(data)} instruments (skipped {len(instruments)-len(data)})")

    def sync_instruments(self, instruments, force_fetch=False):
        """Full sync workflow with error handling"""
        try:
            logger.info("Starting NSE instruments sync")
            if force_fetch:
                instruments = self.fetch_instruments()
            self.store_instruments(instruments)
            
            # Verify sync
            count = self.get_instrument_count()
            logger.info(f"Sync completed successfully. Total instruments: {count}")
            return True
        except Exception as e:
            logger.error(f"Sync failed: {str(e)}", exc_info=True)
            return False

    def get_instrument_count(self) -> int:
        """Get current instrument count in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(1) FROM instruments')
            return cursor.fetchone()[0]

    def get_active_nse_equities(self, segment='NSE') -> list[dict]: # INDICES
        """Get NSE equities with additional business rules"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            base_query = """
                SELECT instrument_token,tradingsymbol,name FROM instruments
                WHERE 
                    exchange = 'NSE' AND
                    segment = ? AND
                    instrument_type = 'EQ'
                """
            if segment == 'NSE':
                base_query = base_query + """
                        AND
                        tradingsymbol NOT LIKE '%ETF%' AND  -- Exclude ETFs
                        name IS NOT NULL AND
                        tradingsymbol NOT LIKE '%-%' AND  -- Exclude preferred stocks
                        lot_size BETWEEN 1 AND 100 AND
                        tradingsymbol GLOB '[A-Z]*'  -- Starts with uppercase letter
                    ORDER BY tradingsymbol
                """
            cursor.execute(base_query, (segment,))
            
            return [dict(zip([col[0] for col in cursor.description], row))
                    for row in cursor.fetchall()]

# Example usage
if __name__ == "__main__":
    # Configuration - load from environment in production
    from dotenv import dotenv_values
    local_secrets = dotenv_values(".env.dev")
    
    API_KEY = "kitefront"
    ACCESS_TOKEN = os.environ.get("KTOKEN",local_secrets.get("KTOKEN","You need your Kite token")),
    
    # Initialize sync
    sync = NSEInstrumentSync(api_key=API_KEY, access_token=ACCESS_TOKEN)
    
    instruments = sync.fetch_instruments()
    if len(instruments) > 2000:
        sync._init_db()
    # Run sync
    if sync.sync_instruments(instruments):
        print(f"Current instrument count: {sync.get_instrument_count()}")
    else:
        print("Sync failed - check logs for details")
    print(sync.get_active_nse_equities(segment='INDICES'))