import argparse
from config import CONFIG
from youtube_client import fetch_trending, dump_raw
from transform import build_spark, json_to_videos_df
from writer import local_target_uri, write_parquet, local_txt_uri, write_txt

def run_trending(region: str, max_results: int):
    api_key = CONFIG["yt_api_key"]
    if not api_key or api_key.strip().upper() in {"REPLACE_ME", "YOUR_YOUTUBE_API_KEY", "YOUR_YT_KEY"}:
        raise RuntimeError(
            "YOUTUBE_API_KEY is not set (or is a placeholder). "
            "Open .env and set YOUTUBE_API_KEY to your real key."
        )

    # 1) Fetch raw JSON
    raw = fetch_trending(api_key=api_key, region_code=region, max_results=max_results)
    raw_path = dump_raw(raw, folder="data/raw", prefix=f"trending_{region}")

    # 2) Build Spark + transform
    spark = build_spark(CONFIG["spark_app_name"])
    df = json_to_videos_df(spark, raw_path)

    # 3a) Parquet (partitioned by date)
    out_uri = local_target_uri(prefix="youtube/trending")
    write_parquet(df, out_uri, mode="append")
    print(f"Wrote {df.count()} records to: {out_uri}")

    # 3b) JSONL (for quick inspection)
    out_txt_uri = local_txt_uri(prefix="youtube/trending_txt")
    write_txt(df, out_txt_uri, mode="overwrite")
    print(f"Also wrote TXT dump to: {out_txt_uri}")

    spark.stop()

def parse_args():
    p = argparse.ArgumentParser(description="YouTube → PySpark → Local Parquet pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("trending", help="Ingest trending videos")
    t.add_argument("--region", default=CONFIG["yt_region"])
    t.add_argument("--max-results", type=int, default=50)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.cmd == "trending":
        run_trending(region=args.region, max_results=args.max_results)
