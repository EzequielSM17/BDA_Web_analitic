import datetime


DAY = datetime.date.today().isoformat()
BRONZE_DIR = "data/drops/"
SILVER_DIR = "output/silver"
GOLD_DIR = "output/gold"
QUARANTINE_DIR = "output/quarantine"
REPORT_DIR = "output/reports"
SESSION_TIMEOUT_MIN = 30
FILE_SILVER_NAME = "events_silver.parquet"
FILE_GOLD_NAME = "events_gold.parquet"
FILE_BRONZE_NAME = "events.ndjson"
