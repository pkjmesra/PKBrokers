from PKDevTools.classes import Archiver
import os
from datetime import datetime
import pytz

_, file_name = Archiver.afterMarketStockDataExists()
if file_name is not None and len(file_name) > 0:
    date_str = file_name.replace(".pkl", "").replace("stock_data_", "")
    with open(os.environ['GITHUB_ENV'], 'a') as f:
        f.write(f"ARCHIVE_DATE_STR={date_str}\n")
else:
    # Fallback to today's date if file doesn't exist
    KOLKATA_TZ = pytz.timezone("Asia/Kolkata")
    date_str = datetime.now(KOLKATA_TZ).strftime('%d%m%Y')
    with open(os.environ['GITHUB_ENV'], 'a') as f:
        f.write(f"ARCHIVE_DATE_STR={date_str}\n")
