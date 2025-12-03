# Presentation Script – YouTube Trending Data Analytics Pipeline on AWS

## Slide 1 — Title Slide
Hello everyone, my name is Kevin Bell, and this is my team’s final project for CS 6705.  
Today I’ll be presenting our Cloud-Based YouTube Analytics Pipeline, built on AWS.  
Our goal was to collect trending-video metadata and comments, process them at scale, and generate useful descriptive analytics and next-day trending predictions. Let’s walk through our architecture, AWS components, and results.

## Slide 2 — Project Goal
Our main goal was to design and implement a fully automated AWS pipeline that pulls YouTube trending data daily, stores and processes it, and then runs ETL and ML steps to generate predictions.

Specifically, the pipeline gathers:
- Trending-video metadata  
- Comment text for those videos  
- Sentiment associated with comments  
- Features for prediction  
- Predicted trending probability for the next day

All of this is orchestrated through AWS so the entire system runs without manual intervention.

## Slide 3 — Project Flow Diagram
This diagram shows the high-level flow of our system.  
EventBridge triggers a Lambda function daily at 6 AM.  
Lambda pulls trending and comment data through the YouTube Data API.  
Raw data lands in S3, triggering the Glue Workflow with a chain of ETL jobs.

The ETL operations clean, flatten, and transform the data into curated Parquet files, which are then queryable in Athena and used for analytics and ML.

## Slide 4 — Network Infrastructure
We built a dedicated AWS VPC with public and private subnets.  
The public subnet routes through an Internet Gateway, while private components use NAT Gateway for secure outbound access.

## Slide 5 — AWS VPC Details
- Public subnet for components needing internet access  
- Private subnet for Glue and internal data processing  
- Security groups controlling communication  
- NAT Gateway for secure API/library access

## Slide 6 — EventBridge
EventBridge automatically schedules the ingestion every day at 6 AM, ensuring consistent and fresh trending-video data.

## Slide 7 — AWS Lambda
Lambda handles ingestion:
1. Retrieves trending videos  
2. Retrieves comments  
3. Reads API credentials from Secrets Manager  
4. Stores raw JSON into S3  
5. Triggers the Glue Workflow

## Slide 8 — Amazon S3
S3 acts as the data lake, containing:
- Raw JSON  
- Curated Parquet  
- Prediction outputs  
- Glue script storage

## Slide 9 — AWS Glue Workflow
The workflow includes six ETL jobs:
1. Trending ETL  
2. Comments ETL  
3. Comment Sentiment ETL  
4. Feature Label ETL  
5. Train Model ETL  
6. Predictions ETL  

It guarantees stage ordering and dependency correctness.

## Slide 10–15 — Glue ETL Jobs Details
### 1. Trending ETL Job  
Flattens trending-video metadata.

### 2. Comments ETL Job  
Processes comment JSON.

### 3. Comment Sentiment ETL Job  
Extracts sentiment polarity.

### 4. Feature Label ETL Job  
Builds features: view count, like ratio, sentiment, history.

### 5. Train Model ETL Job  
Retrains models as new data arrives.

### 6. Predictions ETL Job  
Generates next-day trending predictions.

## Slide 16 — AWS Glue Crawlers
Crawlers create Athena-ready schemas from the curated Parquet datasets.

## Slide 17 — Glue ETL Notebook
Using Plotly and Pandas, the notebook visualizes:
- Top trending categories  
- Comment sentiment  
- Time-series patterns  

## Slide 18 — Athena Queries
Athena validates ETL outputs and lets us run SQL to verify data consistency.

## Slide 19 — Final Results
We successfully delivered:
- Automated pipeline  
- Daily ingestion  
- Curated Parquet datasets  
- Extracted sentiment features  
- ML predictions  
- Dashboards via Glue notebook  
- Approx. $180 total AWS cost

## Slide 20 — Conclusion
Thank you for your time.  
This project provided hands-on experience with event-driven architecture, serverless ingestion, data lakes, ETL, ML workflows, and visualization.  
I'm happy to answer any questions.
