import sys
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, Row, DateType

# ---------- Glue boilerplate ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_COMMENTS_PATH = "s3://yt-analytics-cs6705-data/raw/comments/"
CURATED_SENTIMENT_PATH = "s3://yt-analytics-cs6705-data/curated/comments_sentiment/"

# ---------- 1) Read raw JSON ----------
raw_df = spark.read.json(RAW_COMMENTS_PATH)

raw_df.printSchema()
raw_df.show(2, truncate=False)

# Expect schema:
# videoId  string
# date     string
# region   string
# comments array<struct<textDisplay:string,...>>

# ---------- 2) Explode comment array ----------
comments_df = (
    raw_df
    .withColumn("comment", F.explode("comments"))
    .select(
        F.col("videoId").alias("video_id"),
        F.col("region"),
        F.to_date("date").alias("ingest_date"),   # <-- cast to date
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
        .parquet(CURATED_SENTIMENT_PATH)
)

print("Wrote sentiment aggregates to:", CURATED_SENTIMENT_PATH)
job.commit()
