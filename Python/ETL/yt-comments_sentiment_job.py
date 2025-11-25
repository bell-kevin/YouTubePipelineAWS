import sys
import os
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, Row, DateType

# ---------- Glue boilerplate ----------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_S3_PATH", "CURATED_S3_PATH"]
)

raw_s3_path = args["RAW_S3_PATH"]
curated_s3_path = os.path.join(args["CURATED_S3_PATH"],"comments_sentiment")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------- 1) Read raw JSON + capture file path ----------
from pyspark.sql import functions as F

raw_df = (
    spark.read.json(raw_s3_path)
    .withColumn("file_path", F.input_file_name())
)

raw_df.printSchema()
raw_df.show(2, truncate=False)

# Example file_path:
# s3://yt-analytics-cs6705-data/raw/comments/region=US/date=2025-11-25/part-0000.json

# ---------- 2) Extract region/date from path + explode comments ----------

comments_df = (
    raw_df
    # extract region from .../region=XX/...
    .withColumn(
        "region",
        F.regexp_extract("file_path", r"region=([^/]+)", 1)
    )
    # extract date string from .../date=YYYY-MM-DD/...
    .withColumn(
        "ingest_date",
        F.to_date(
            F.regexp_extract("file_path", r"date=([^/]+)", 1),
            "yyyy-MM-dd"
        )
    )
    # explode comments array
    .withColumn("comment", F.explode("comments"))
    .select(
        F.col("videoId").alias("video_id"),
        F.col("region"),
        F.col("ingest_date"),
        F.col("comment.textDisplay").alias("comment_text")
    )
    .where(F.col("comment_text").isNotNull())
)


# ---------- 3) Comprehend scoring ----------
def sentiment_partition(iter_rows):
    client = boto3.client("comprehend", region_name="us-west-2")
    for r in iter_rows:
        try:
            resp = client.detect_sentiment(
                Text=r.comment_text[:4500],
                LanguageCode="en"
            )
            s = resp["SentimentScore"]
            yield Row(
                video_id=r.video_id,
                region=r.region,
                ingest_date=r.ingest_date,
                sentiment_pos=float(s["Positive"]),
                sentiment_neg=float(s["Negative"]),
                sentiment_neu=float(s["Neutral"]),
                sentiment_mix=float(s["Mixed"])
            )
        except Exception:
            yield Row(
                video_id=r.video_id,
                region=r.region,
                ingest_date=r.ingest_date,
                sentiment_pos=0.0,
                sentiment_neg=0.0,
                sentiment_neu=1.0,
                sentiment_mix=0.0
            )

sentiment_rdd = comments_df.rdd.mapPartitions(sentiment_partition)

schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("ingest_date", DateType(), True),
    StructField("sentiment_pos", DoubleType(), True),
    StructField("sentiment_neg", DoubleType(), True),
    StructField("sentiment_neu", DoubleType(), True),
    StructField("sentiment_mix", DoubleType(), True),
])

sentiment_scored_df = spark.createDataFrame(sentiment_rdd, schema=schema)

# ---------- 4) Aggregate ----------
agg_df = (
    sentiment_scored_df
    .groupBy("video_id", "region", "ingest_date")
    .agg(
        F.avg("sentiment_pos").alias("avg_comment_sent_pos"),
        F.avg("sentiment_neg").alias("avg_comment_sent_neg"),
        F.avg("sentiment_neu").alias("avg_comment_sent_neu"),
        F.avg("sentiment_mix").alias("avg_comment_sent_mix"),
        (F.avg("sentiment_pos") - F.avg("sentiment_neg")).alias("net_comment_sentiment"),
        F.count("*").alias("comment_count_scored"),
    )
)

# ---------- 5) Write ----------
(
    agg_df.write
        .mode("overwrite")
        .partitionBy("region", "ingest_date")
        .parquet(curated_s3_path)
)

print("Wrote sentiment aggregates to:", curated_s3_path)
job.commit()
