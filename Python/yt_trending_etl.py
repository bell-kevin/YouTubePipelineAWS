from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==== CONFIG – CHANGE THESE FOR YOUR ENVIRONMENT ====
USE_GLUE_TABLE = False   # True = use Glue Catalog table, False = read JSON from S3 paths

# S3 buckets / prefixes
RAW_S3_PATH      = "s3://yt-analytics-cs6705-data/raw/trending/region=*/date=*/*.json"
CURATED_S3_PATH  = "s3://yt-analytics-cs6705-data/curated/trending/"

# Glue catalog (if using)
GLUE_DB          = "`youtube-analytics`"
RAW_VIDEOS_TABLE = "`raw_trending`"
CURATED_TABLE    = "`curated_trending`"  # logical name; actual Glue table is via crawler or CREATE TABLE

print("Config loaded.")

if USE_GLUE_TABLE:
    print(f"Reading from Glue table: {GLUE_DB}.{RAW_VIDEOS_TABLE}")
    raw_videos = spark.table(f"{GLUE_DB}.{RAW_VIDEOS_TABLE}")
else:
    print(f"Reading JSON from S3: {RAW_S3_PATH}")
    raw_videos = spark.read.json(RAW_S3_PATH)

raw_videos.printSchema()
raw_videos.show(5, truncate=False)
print(f"Raw count: {raw_videos.count()}")

from pyspark.sql import functions as F

# 1) Explode the items array so we get one row per video, and capture the source file path
videos_exploded = (
    raw_videos
    .select(
        F.explode("items").alias("item"),
        F.input_file_name().alias("source_file")
    )
)

videos_exploded.printSchema()
videos_exploded.show(3, truncate=False)

# 2) Flatten fields from the item struct
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

        # Times (raw string)
        F.col("item.snippet.publishedAt").alias("published_at_raw"),

        # Performance metrics (raw strings)
        F.col("item.statistics.viewCount").alias("view_count_raw"),
        F.col("item.statistics.likeCount").alias("like_count_raw"),
        F.col("item.statistics.commentCount").alias("comment_count_raw"),

        # Technical
        F.col("item.contentDetails.duration").alias("duration_iso"),
        F.col("item.contentDetails.dimension").alias("dimension"),
        F.col("item.contentDetails.definition").alias("definition"),
        F.col("item.contentDetails.caption").alias("caption"),

        # Source file (for region/date extraction)
        F.col("source_file")
    )
)

# 3) Extract region and trending_date from the S3 path, e.g.
# s3://bucket/raw/youtube/trending/region=US/date=2025-11-18/part-0000.json
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

videos_flat.printSchema()
videos_flat.select("video_id", "region", "trending_date_raw", "source_file").show(10, truncate=False)

# Cast numeric metrics
videos_typed = (
    videos_flat
    .withColumn("view_count",    F.col("view_count_raw").cast("bigint"))
    .withColumn("like_count",    F.col("like_count_raw").cast("bigint"))
    .withColumn("comment_count", F.col("comment_count_raw").cast("bigint"))
    .drop("view_count_raw", "like_count_raw", "comment_count_raw")
)

# Drop rows with critical nulls (video_id, title, published_at_raw)
videos_clean = (
    videos_typed
    .filter(F.col("video_id").isNotNull())
    .filter(F.col("title").isNotNull())
    .filter(F.col("published_at_raw").isNotNull())
)

print("After cleaning (null filters):", videos_clean.count())
videos_clean.select("video_id", "title", "region", "trending_date_raw").show(10, truncate=False)

from pyspark.sql.window import Window

w = Window.partitionBy("video_id").orderBy(F.col("view_count").desc_nulls_last())

videos_dedup = (
    videos_clean
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("After dedup on video_id:", videos_dedup.count())
videos_dedup.select(
    "video_id", "view_count", "region", "trending_date_raw"
).show(10, truncate=False)

videos_dates = (
    videos_dedup
    .withColumn("published_at", F.to_timestamp("published_at_raw"))
    .drop("published_at_raw")
    .withColumn("published_date", F.to_date("published_at"))
    .withColumn("published_hour", F.hour("published_at"))
    .withColumn("published_year", F.year("published_at"))
    .withColumn("published_week", F.weekofyear("published_at"))  # integer 1–53
    .withColumn("trending_date", F.to_date("trending_date_raw"))
)

videos_dates.select(
    "video_id", "region", "trending_date", "published_at", "published_date"
).show(10, truncate=False)

videos_text = (
    videos_dates
    # lowercase title
    .withColumn("title_clean", F.lower(F.col("title")))
    # remove URLs
    .withColumn(
        "title_clean",
        F.regexp_replace("title_clean", r"http[s]?://\\S+", "")
    )
    # normalize whitespace
    .withColumn(
        "title_clean",
        F.regexp_replace("title_clean", r"\\s+", " ")
    )
)

videos_text.select("video_id", "title", "title_clean").show(10, truncate=False)

df_curated = videos_text  # or videos_dates if you skip text cleaning

print("Writing curated Parquet to:", CURATED_S3_PATH)

(
    df_curated
    .write
    .mode("overwrite")  # change to "append" once your pipeline is stable
    .partitionBy("region", "trending_date")
    .format("parquet")
    .save(CURATED_S3_PATH)
)

print("Write complete.")

job.commit()