import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json

def _dt_path() -> str:
    return datetime.utcnow().strftime("dt=%Y-%m-%d")

def local_target_uri(prefix: str = "youtube/trending") -> str:
    return os.path.join("data", "processed", prefix, _dt_path())

def local_txt_uri(prefix: str = "youtube/trending_txt") -> str:
    return os.path.join("data", "processed", prefix, _dt_path())

def write_parquet(df: DataFrame, uri: str, mode: str = "append"):
    os.makedirs(uri, exist_ok=True)
    (df.repartition(1).write.mode(mode).parquet(uri))

def write_txt(df: DataFrame, uri: str, mode: str = "overwrite"):
    os.makedirs(uri, exist_ok=True)
    lines = df.select(to_json(struct(*[col(c) for c in df.columns])).alias("value"))
    (lines.coalesce(1).write.mode(mode).text(uri))
