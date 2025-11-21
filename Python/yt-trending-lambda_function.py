import json
import os
from datetime import datetime, timezone
import urllib.parse
import urllib.request

import boto3

secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
glue = boto3.client("glue")

# Cache the key across invocations
YOUTUBE_API_KEY = None


def get_api_key():
    global YOUTUBE_API_KEY
    if YOUTUBE_API_KEY is not None:
        return YOUTUBE_API_KEY

    secret_name = "yt/youtube-api-key"
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = resp["SecretString"]
    data = json.loads(secret_str)
    YOUTUBE_API_KEY = data["YOUTUBE_API_KEY"]
    return YOUTUBE_API_KEY


def fetch_trending(region_code, max_results=50):
    api_key = get_api_key()

    base_url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region_code,
        "maxResults": max_results,
        "key": api_key,
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    with urllib.request.urlopen(url) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def lambda_handler(event, context):
    # Environment variables
    bucket = os.environ["BUCKET_NAME"]          # e.g. yt-analytics-cs6705-data
    regions_str = os.environ.get("REGIONS", "US")
    regions = [r.strip() for r in regions_str.split(",") if r.strip()]

    # Optionally make curated path configurable too:
    curated_trending_path = os.environ.get(
        "CURATED_TRENDING_PATH",
        f"s3://{bucket}/curated/trending/"
    )

    # Current UTC date/time
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")

    results = {}
    glue_runs = []

    for region in regions:
        print(f"Fetching trending videos for region={region}")
        data = fetch_trending(region)

        # Raw JSON path for this region/date
        key = f"raw/trending/region={region}/date={date_str}/trending_{region}_{ts_str}.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json",
        )

        results[region] = {
            "s3_key": key,
            "item_count": len(data.get("items", []))
        }

        # Start Glue ETL job for this region + date
        raw_trending_path = f"s3://{bucket}/raw/trending/region={region}/date={date_str}"

        response = glue.start_workflow_run(
            Name="YouTubeTrendingWorkflow",
            RunProperties={
                "RAW_S3_PATH": raw_trending_path,
                "CURATED_S3_PATH": curated_trending_path,
            }
        )

        glue_runs.append({
            "region": region,
            "job_run_id": response["JobRunId"],
            "raw_path": raw_trending_path,
        })

        print(f"Started Glue job yt_trending_etl_job for region={region}, "
              f"JobRunId={response['JobRunId']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Trending harvest + ETL trigger complete",
            "results": results,
            "glue_runs": glue_runs,
        }),
    }
