# YouTube → PySpark → Local Parquet

Small, practical pipeline that pulls YouTube “most popular” (trending) videos for a region,
flattens the JSON with PySpark, and writes out partitioned Parquet locally. There’s also a
tiny aggregation job that ranks top channels by total views and by video count.

## Why this exists
I wanted a clean demo that’s easy to run on Windows, uses a real API, and shows:
- Fetch → land raw JSON
- Spark transform → tidy tabular schema
- Partitioned Parquet output
- A simple aggregation job (“top channels”)

## Quick start (Windows)

### 0) Prereqs
- Python 3.11+ (I used 3.11)
- Java (JDK 11 or 17 recommended)
- (Windows) Hadoop winutils present in `C:\hadoop\bin\winutils.exe` and `JAVA_TOOL_OPTIONS=-Djava.library.path=C:\hadoop\bin`

### 1) Create env + install
```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

2) Configure

Copy .env.example → .env and set your YouTube API key:

YOUTUBE_API_KEY=your_key_here
YOUTUBE_REGION=PL
SPARK_APP_NAME=YouTubeIngestion
OUTPUT_FORMAT=parquet

3) Run ingestion
.\.venv\Scripts\python.exe .\src\main.py trending --region PL --max-results 50


Outputs:

Parquet: data/processed/youtube/trending/dt=YYYY-MM-DD/

JSONL (debug): data/processed/youtube/trending_txt/dt=YYYY-MM-DD/

4) Run aggregation (top channels)
.\.venv\Scripts\python.exe .\src\aggregate_top_channels.py --top 10 --save


Saves results to:

data/analytics/top_channels/dt=YYYY-MM-DD/
  ├─ by_views/ (parquet) + by_views_csv/
  └─ by_count/ (parquet) + by_count_csv/

Notes

This is local-only. Swap the writer to S3 later if needed.

The code is intentionally small and readable.