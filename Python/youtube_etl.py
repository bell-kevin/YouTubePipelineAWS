from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("YouTubeETL").getOrCreate()

# Replace these:
raw_path = "s3://yt-analytics-cs6705-data/raw/trending/"
processed_path = "s3://yt-analytics-cs6705-data/curated/trending/"

# Read raw JSON
df = spark.read.json(raw_path)

# Curate fields
df2 = (
    df.select(
        col("videoId").alias("video_id"),
        col("title"),
        col("channelTitle").alias("channel_title"),
        col("categoryId").alias("category_id"),
        col("publishedAt").alias("published_at"),
        col("regionCode").alias("region_code"),
        col("viewCount").cast("bigint").alias("view_count"),
        col("likeCount").cast("bigint").alias("like_count"),
        col("commentCount").cast("bigint").alias("comment_count")
    )
    .withColumn("published_date", to_date(col("published_at")))
)

(
    df2.repartition("region_code", "published_date")
    .write
    .mode("overwrite")
    .partitionBy("region_code", "published_date")
    .parquet(processed_path)
)

spark.stop()
