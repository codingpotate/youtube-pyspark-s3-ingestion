import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, to_timestamp, regexp_replace

def build_spark(app_name: str) -> SparkSession:
    spark_tmp = r"C:\tmp\spark"
    hive_tmp = r"C:\tmp\hive"
    os.makedirs(spark_tmp, exist_ok=True)
    os.makedirs(hive_tmp, exist_ok=True)

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", spark_tmp)
        .config("hive.exec.scratchdir", hive_tmp)
        .getOrCreate()
    )

def json_to_videos_df(spark: SparkSession, raw_path: str) -> DataFrame:
    df_raw = spark.read.json(raw_path)
    items = df_raw.select(explode("items").alias("item"))

    df = items.select(
        col("item.id").alias("video_id"),
        col("item.snippet.publishedAt").alias("published_at"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.channelId").alias("channel_id"),
        col("item.snippet.channelTitle").alias("channel_title"),
        col("item.snippet.categoryId").alias("category_id"),
        col("item.contentDetails.duration").alias("duration_iso8601"),
        col("item.statistics.viewCount").cast("long").alias("views"),
        col("item.statistics.likeCount").cast("long").alias("likes"),
        col("item.statistics.commentCount").cast("long").alias("comments")
    ).withColumn(
        "published_at_ts", to_timestamp("published_at")
    ).withColumn(
        "title_clean", regexp_replace("title", r"\s+", " ")
    )

    return df
