import json
import os
import datetime
import urllib.parse
import urllib.request

import boto3

secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

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
    bucket = os.environ["BUCKET_NAME"]
    regions_str = os.environ.get("REGIONS", "US")
    regions = [r.strip() for r in regions_str.split(",") if r.strip()]

    now = datetime.datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")

    results = {}

    for region in regions:
        print(f"Fetching trending videos for region={region}")
        data = fetch_trending(region)
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

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Trending harvest complete",
            "results": results,
        }),
    }
