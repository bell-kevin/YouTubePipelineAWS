# Cloud-Based YouTube Trending Analytics Pipeline on AWS

**Team:** Kevin Bell, Jacob Child  \
**Course:** CS 6705 (Fall 2025)  \
**Submission Date:** December 10, 2025

## 1. Problem Statement and Scope

YouTube generates millions of daily interactions that signal which videos trend across regions. Content creators, advertisers, and platform operators need timely insight into which videos will continue trending so they can optimize promotion, moderation, and infrastructure. Our project builds an end-to-end cloud pipeline that automatically ingests trending-video metadata and comments, curates reliable analytics datasets, applies sentiment analysis, and produces next-day trending predictions. The scope covers daily ingestion, scalable ETL, feature engineering, model training, and prediction publishing—all orchestrated on AWS using serverless and managed data services. We target low-ops automation with cost control and reproducible artifacts (code, diagrams, and notebooks).

## 2. Background and Related Work

Prior research on video virality highlights the importance of early engagement velocity (views, likes, comments) and contextual signals like category, title language, and audience sentiment. Industrial systems often use event-driven ingestion, data lakes for raw and curated layers, and distributed processing (Spark/Glue) for feature computation. Sentiment-aware features can improve short-term popularity forecasting by capturing viewer reception. Our design follows the modern medallion architecture: raw JSON lands in S3, curated Parquet layers standardize schema, and downstream ML consumes labeled feature sets. Compared with single-node notebooks or cron-based scripts, the AWS Glue + EventBridge + Lambda stack provides resiliency, schema evolution via crawlers, and horizontal scalability for both batch and near–real-time updates.

## 3. System Architecture and Contributions

### 3.1 End-to-End Flow

A 6:00 AM EventBridge rule triggers two Lambda functions to fetch trending videos and related comments via the YouTube Data API. Raw JSON files are written to S3 under partitioned folders by region and ingest date. The trending-ingest Lambda then launches a Glue Workflow that chains six ETL jobs, ensuring dependency ordering and eliminating manual coordination ([Notes.md, lines 1–18](Notes.md#L1-L18)).

### 3.2 Trending ETL

The `yt_trending_etl.py` Glue job flattens nested API responses, extracts region and trending date from S3 paths, casts metrics, deduplicates by video ID, and enriches timestamps with date, hour, week, and cleaned titles. It writes curated Parquet partitioned by region and trending_date, enabling efficient Athena queries and downstream feature computation ([Python/ETL/yt_trending_etl.py, lines 12–119](Python/ETL/yt_trending_etl.py#L12-L119); [lines 121–168](Python/ETL/yt_trending_etl.py#L121-L168)).

### 3.3 Comments ETL

The comments job normalizes one row per comment, keeps author, text, likes, and published timestamp, and derives region/ingest_date from file names. Curated Parquet is appended partitioned by region and ingest_date, ready for sentiment analysis and engagement statistics ([Python/ETL/yt_comments_etl.py, lines 8–110](Python/ETL/yt_comments_etl.py#L8-L110)).

### 3.4 Sentiment and Feature Labeling

A subsequent Glue job (notebook-linked) scores each comment with AWS Comprehend to compute per-video averages of positive, negative, neutral, and mixed sentiment. The one-time backfill script `yt-onetimebatch.py` joins curated trending data with sentiment, constructs time-window lags and ratios (view/like/comment velocities, growth ratios, engagement per view), derives labels for next-day view growth and continued trending presence, and writes labeled feature sets partitioned by region and ingest date ([Notes.md, lines 51–86](Notes.md#L51-L86); [Python/ETL/yt-onetimebatch.py, lines 1–157](Python/ETL/yt-onetimebatch.py#L1-L157); [lines 159–209](Python/ETL/yt-onetimebatch.py#L159-L209)).

### 3.5 Modeling and Predictions

Within the Glue workflow, training and prediction steps (scripts in `ETL/`) retrain models on updated labeled features and emit next-day forecasts. Outputs include predicted view counts and probability a video will remain trending, stored as curated Parquet for dashboarding and ad hoc analytics ([Notes.md, lines 88–119](Notes.md#L88-L119)).

### 3.6 Infrastructure and Security

A dedicated VPC isolates Glue resources, with public subnets for internet-facing components and private subnets using a NAT Gateway for outbound API/library access. S3 serves as the data lake for raw, curated, and model outputs. Secrets Manager supplies API credentials to Lambda. The architecture diagrams (`FlowDiagram.png`, `NetworkInfrastructure.png`) document connectivity and dependency ordering ([PresentationScript.md, lines 12–75](PresentationScript.md#L12-L75)).

### 3.7 Collaboration & Project Management

We adhered to weekly milestones captured in the proposal and TODO lists, iterating on ETL robustness, schema definitions, and notebook visualizations. Code and notebooks are organized under `Python/`, `Notebooks/`, and `Diagrams/` to keep raw ingestion, ETL, ML, and presentation assets separated.

## 4. Results and Analysis

### 4.1 Data Quality and Curated Schemas

Curated trending and comments schemas capture identifiers, metrics, time fields, and text fields with consistent typing, enabling Athena validation and reproducible analytics ([Notes.md, lines 20–50](Notes.md#L20-L50)). Deduplication by video ID with highest view count removes API duplicates, while partitioning by region and date keeps cost predictable. Title cleaning strips URLs and normalizes whitespace for later NLP.

### 4.2 Sentiment Insights

Sentiment aggregation surfaces audience reception per video and date. Example rows show mixed sentiment distributions, with negative shares ranging from ~0.03 to ~0.27 and positive shares often above 0.45, offering discriminative signals for trend persistence ([Notes.md, lines 52–79](Notes.md#L52-L79)). These features are joined to trending metrics to capture both engagement magnitude and tone.

### 4.3 Feature Engineering and Labels

Lag-based velocities and growth ratios quantify momentum, while engagement-per-view and log-count transforms stabilize variance across orders of magnitude. Labels track whether a video remains in the trending set on the next ingest date and estimate logarithmic view growth; this framing suits classification for stay-trending probability and regression for view-count prediction ([Python/ETL/yt-onetimebatch.py, lines 67–209](Python/ETL/yt-onetimebatch.py#L67-L209)).

### 4.4 Model Outputs

Predicted outputs include next-day view counts and probabilities of staying in the trending set, written to curated S3 paths. Example rows illustrate high-confidence predictions (probability ~1.0) for diverse videos, validating pipeline plumbing from ingestion through ML inference ([Notes.md, lines 88–119](Notes.md#L88-L119)).

### 4.5 Cost and Operational Considerations

Using serverless ingestion and managed Spark (Glue) minimized operational burden. Most spend comes from Glue job runtime and NAT egress during package downloads; our final run-rate stayed near the planned ~$180 budget. Partition pruning and Parquet compression reduced Athena costs. The workflow’s dependency graph lowered failure domains by sequencing ETL stages.

## 5. Team Contributions

- **Kevin Bell:** VPC/network design, and Lambda + EventBridge integration; implemented trending ETL, curated schema definitions, and presentation materials documenting the workflow and infrastructure ([PresentationScript.md, lines 3–119](PresentationScript.md#L3-L119); [Python/ETL/yt_trending_etl.py, lines 12–168](Python/ETL/yt_trending_etl.py#L12-L168)).
- **Jacob Child:** Led architecture, focused on comments ingestion, sentiment processing, and feature/label engineering; authored the backfill job that joins sentiment with trending metrics and produces ML-ready datasets; supported model training and prediction stages ([Python/ETL/yt_comments_etl.py, lines 8–110](Python/ETL/yt_comments_etl.py#L8-L110); [Python/ETL/yt-onetimebatch.py, lines 1–209](Python/ETL/yt-onetimebatch.py#L1-L209); [Notes.md, lines 51–119](Notes.md#L51-L119)).

Workload was split evenly, with paired code reviews on each ETL milestone and shared notebook-based data validation.

## 6. Conclusion and Future Work

We delivered an automated, cloud-native YouTube analytics pipeline that ingests daily trending data, curates clean datasets, enriches them with sentiment, engineers predictive features, and outputs next-day trending predictions. The architecture balances scalability and cost, and the modular ETL jobs simplify maintenance. Future enhancements include:

- Incremental model retraining with drift detection for changing audience behavior.
- Additional NLP features from titles/descriptions (e.g., embeddings) and toxicity detection on comments.
- Near–real-time micro-batching using Kinesis or Kafka for faster trend detection.
- Expanded dashboards (QuickSight or Streamlit) for stakeholder-facing monitoring.

Data and code are bundled in this repository; raw and curated datasets reside in the team’s AWS S3 bucket as referenced in the ETL scripts. This project demonstrates practical application of event-driven data engineering and ML operations on AWS at modest cost.
