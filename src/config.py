import os
from dotenv import load_dotenv

load_dotenv()

def env(key, default=None):
    v = os.getenv(key)
    return v if v not in (None, "") else default

CONFIG = {
    "yt_api_key": env("YOUTUBE_API_KEY"),
    "yt_region": env("YOUTUBE_REGION", "US"),
    "aws_region": env("AWS_DEFAULT_REGION", "eu-central-1"),
    "s3_bucket": env("S3_BUCKET"),
    "s3_prefix": env("S3_PREFIX", "youtube/trending"),
    "spark_app_name": env("SPARK_APP_NAME", "YouTubeIngestion"),
    "output_format": env("OUTPUT_FORMAT", "parquet"),
}
