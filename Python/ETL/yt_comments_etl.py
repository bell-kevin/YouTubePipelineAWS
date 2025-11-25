import sys
import os
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    explode_outer,
    regexp_extract,
    input_file_name,
    coalesce
)

# ---------------------------------------------------------------------------
# 1. Glue boilerplate + arguments
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_S3_PATH", "CURATED_S3_PATH"]
)

raw_s3_path = args["RAW_S3_PATH"]
curated_s3_path = os.path.join(args["CURATED_S3_PATH"],"comments")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# 2. Read raw JSON comment data
# ---------------------------------------------------------------------------

raw_df = (
    spark.read
         .option("recursiveFileLookup", "true")
         .json(raw_s3_path)
)

print("=== RAW DF SCHEMA ===")
raw_df.printSchema()

# ---------------------------------------------------------------------------
# 3. Flatten to one row per comment
#    Schema:
#      videoId   (string)
#      comments[]: struct<authorDisplayName, likeCount, publishedAt, textDisplay, videoId>
# ---------------------------------------------------------------------------

comments_df = (
    raw_df
        .select(
            col("videoId").alias("top_video_id"),
            explode_outer("comments").alias("comment")
        )
)

print("=== COMMENTS DF SCHEMA ===")
comments_df.printSchema()

flattened_df = (
    comments_df
        .select(
            # Prefer the comment-level videoId if present, otherwise fallback to top-level
            coalesce(col("comment.videoId"), col("top_video_id")).alias("video_id"),

            col("comment.authorDisplayName").alias("author_display_name"),
            col("comment.textDisplay").alias("comment_text"),
            col("comment.likeCount").alias("like_count"),
            col("comment.publishedAt").alias("published_at")
        )
)

print("=== FLATTENED DF SCHEMA ===")
flattened_df.printSchema()

# ---------------------------------------------------------------------------
# 4. Derive region / ingest_date from S3 path
#    Example path:
#    s3://yt-analytics-cs6705-data/raw/comments/region=US/date=2025-11-20/part-0000.json
# ---------------------------------------------------------------------------

path_col = input_file_name()

with_partitions_df = (
    flattened_df
        .withColumn(
            "region",
            regexp_extract(path_col, r"region=([^/]+)", 1)
        )
        .withColumn(
            "ingest_date",
            regexp_extract(path_col, r"date=([^/]+)", 1)
        )
)

print("=== FINAL DF SCHEMA (WITH PARTITIONS) ===")
with_partitions_df.printSchema()

# ---------------------------------------------------------------------------
# 5. Write curated Parquet, partitioned (region, ingest_date)
# ---------------------------------------------------------------------------

(
    with_partitions_df
        .write
        .mode("append")                     # change to "overwrite" if you want to replace
        .partitionBy("region", "ingest_date")
        .parquet(curated_s3_path)
)

job.commit()
