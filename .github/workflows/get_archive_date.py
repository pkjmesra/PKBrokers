from PKDevTools.classes import Archiver
import os
from datetime import datetime
import pytz

try:
    _, file_name = Archiver.afterMarketStockDataExists()
    if file_name is not None and len(file_name) > 0:
        date_str = file_name.replace(".pkl", "").replace("stock_data_", "")
        print(f"Using date from cache file: {date_str}")
    else:
        # Fallback to today's date if file doesn't exist
        KOLKATA_TZ = pytz.timezone("Asia/Kolkata")
        date_str = datetime.now(KOLKATA_TZ).strftime('%d%m%Y')
        print(f"Using fallback today's date: {date_str}")
except Exception as e:
    # Ultimate fallback
    print(f"Error getting date: {e}")
    KOLKATA_TZ = pytz.timezone("Asia/Kolkata")
    date_str = datetime.now(KOLKATA_TZ).strftime('%d%m%Y')
    print(f"Using error fallback date: {date_str}")

# Write to GITHUB_ENV
with open(os.environ['GITHUB_ENV'], 'a') as f:
    f.write(f"ARCHIVE_DATE_STR={date_str}\n")

# Write to GITHUB_OUTPUT for step outputs
github_output = os.environ.get('GITHUB_OUTPUT')
if github_output:
    with open(github_output, 'a') as f:
        f.write(f"date_str={date_str}\n")

print(f"Final archive date string: {date_str}")