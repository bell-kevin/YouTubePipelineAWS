import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------- Job args ---------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_S3_PATH",
        "CURATED_S3_PATH"
    ]
)

raw_s3_path = args["RAW_S3_PATH"]
curated_s3_path = args["CURATED_S3_PATH"]

# --------- Glue / Spark bootstrap ---------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Optional: ignore missing/corrupt files for robustness
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# --------- 1. Read RAW JSON from S3 ---------
# Example: s3://.../raw/trending/region=US/date=2025-11-19/part-*.json
print(f"Reading raw trending videos from: {raw_s3_path}")

raw_videos = (
    spark.read
         .option("multiLine", "true")
         .json(raw_s3_path)
)

print("Raw schema:")
raw_videos.printSchema()

# --------- 2. Explode + flatten + region/trending_date from path ---------
videos_exploded = (
    raw_videos
    .select(
        F.explode("items").alias("item"),
        F.input_file_name().alias("source_file")
    )
)

# Flatten nested fields
videos_flat = (
    videos_exploded
    .select(
        # Identity
        F.col("item.id").alias("video_id"),
        F.col("item.snippet.channelId").alias("channel_id"),
        F.col("item.snippet.categoryId").alias("category_id"),

        # Descriptive
        F.col("item.snippet.title").alias("title"),
        F.col("item.snippet.description").alias("description"),
        F.col("item.snippet.tags").alias("tags"),
        F.col("item.snippet.defaultLanguage").alias("default_lang"),

        # Time
        F.col("item.snippet.publishedAt").alias("published_at_raw"),

        # Metrics (raw strings)
        F.col("item.statistics.viewCount").alias("view_count_raw"),
        F.col("item.statistics.likeCount").alias("like_count_raw"),
        F.col("item.statistics.commentCount").alias("comment_count_raw"),

        # Technical
        F.col("item.contentDetails.duration").alias("duration_iso"),
        F.col("item.contentDetails.dimension").alias("dimension"),
        F.col("item.contentDetails.definition").alias("definition"),
        F.col("item.contentDetails.caption").alias("caption"),

        # Source file for region & date extraction
        F.col("source_file")
    )
)

# Extract region and trending_date from path
# Expects paths like: .../region=US/date=2025-11-19/part-0000.json
videos_flat = (
    videos_flat
    .withColumn(
        "region",
        F.regexp_extract(F.col("source_file"), r"region=([^/]+)", 1)
    )
    .withColumn(
        "trending_date_raw",
        F.regexp_extract(F.col("source_file"), r"date=([^/]+)", 1)
    )
)

# --------- 3. Type casting & basic cleaning ---------
videos_typed = (
    videos_flat
    .withColumn("view_count",    F.col("view_count_raw").cast("bigint"))
    .withColumn("like_count",    F.col("like_count_raw").cast("bigint"))
    .withColumn("comment_count", F.col("comment_count_raw").cast("bigint"))
    .drop("view_count_raw", "like_count_raw", "comment_count_raw")
)

videos_clean = (
    videos_typed
    .filter(F.col("video_id").isNotNull())
    .filter(F.col("title").isNotNull())
    .filter(F.col("published_at_raw").isNotNull())
)

# --------- 4. Deduplicate by video_id (keep highest view_count) ---------
w = Window.partitionBy("video_id").orderBy(F.col("view_count").desc_nulls_last())

videos_dedup = (
    videos_clean
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# --------- 5. Timestamp normalization & date features ---------
videos_dates = (
    videos_dedup
    .withColumn("published_at", F.to_timestamp("published_at_raw"))
    .drop("published_at_raw")
    .withColumn("published_date", F.to_date("published_at"))
    .withColumn("published_hour", F.hour("published_at"))
    .withColumn("published_year", F.year("published_at"))
    .withColumn("published_week", F.weekofyear("published_at").cast("int"))
    .withColumn("trending_date", F.to_date("trending_date_raw"))
)

# --------- 6. Optional text cleaning ---------
videos_text = (
    videos_dates
    .withColumn("title_clean", F.lower(F.col("title")))
    .withColumn(
        "title_clean",
        F.regexp_replace("title_clean", r"http[s]?://\S+", "")
    )
    .withColumn(
        "title_clean",
        F.regexp_replace("title_clean", r"\s+", " ")
    )
)

df_curated = videos_text

# --------- 7. Write curated parquet to S3 ---------
print(f"Writing curated Parquet to: {curated_s3_path}")

(
    df_curated
    .write
    .mode("overwrite")  # later: 'append' for incremental
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("region", "trending_date")
    .format("parquet")
    .save(curated_s3_path)
)

print("Write complete.")

job.commit()
