import os
import argparse
from pyspark.sql import functions as F
from transform import build_spark

BASE = os.path.join("data", "processed", "youtube", "trending")

def latest_dt(base: str) -> str:
    if not os.path.isdir(base):
        raise FileNotFoundError(f"No processed data at: {base}")
    dts = [d for d in os.listdir(base) if d.startswith("dt=")]
    if not dts:
        raise FileNotFoundError(f"No partitions under: {base}")
    dts.sort(key=lambda x: x.split("=",1)[1])
    return dts[-1].split("=",1)[1]

def run(date_str: str | None, top_n: int, save: bool):
    spark = build_spark("YouTubeTopChannels")
    if not date_str:
        date_str = latest_dt(BASE)

    part_path = os.path.join(BASE, f"dt={date_str}")
    df = spark.read.parquet(part_path)

    required = {"channel_title", "views"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing} in {part_path}")

    by_views = (
        df.groupBy("channel_title")
          .agg(F.sum("views").alias("total_views"),
               F.count("*").alias("video_count"))
          .orderBy(F.desc("total_views"))
    )

    by_count = (
        df.groupBy("channel_title")
          .agg(F.count("*").alias("video_count"))
          .orderBy(F.desc("video_count"))
    )

    print(f"\nTop {top_n} channels by TOTAL VIEWS on {date_str}")
    by_views.show(top_n, truncate=False)

    print(f"\nTop {top_n} channels by VIDEO COUNT on {date_str}")
    by_count.show(top_n, truncate=False)

    if save:
        out_dir = os.path.join("data", "analytics", "top_channels", f"dt={date_str}")
        (by_views.coalesce(1).write.mode("overwrite").parquet(os.path.join(out_dir, "by_views")))
        (by_count.coalesce(1).write.mode("overwrite").parquet(os.path.join(out_dir, "by_count")))
        (by_views.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(out_dir, "by_views_csv")))
        (by_count.coalesce(1).write.mode("overwrite").option("header", True).csv(os.path.join(out_dir, "by_count_csv")))
        print(f"\nSaved results to: {out_dir}")

    spark.stop()

def parse_args():
    p = argparse.ArgumentParser(description="Top YouTube channels from trending dataset")
    p.add_argument("--date", help="Partition date (YYYY-MM-DD). If omitted, uses latest.")
    p.add_argument("--top", type=int, default=10, help="How many rows to show")
    p.add_argument("--save", action="store_true", help="Save results (Parquet + CSV)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(args.date, args.top, args.save)
