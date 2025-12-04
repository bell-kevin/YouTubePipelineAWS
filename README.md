<a name="readme-top"></a>

# CS 6705 Final Project
<!--
Source - https://stackoverflow.com/a
Posted by Tieme, modified by community. See post 'Timeline' for change history
Retrieved 2025-11-20, License - CC BY-SA 4.0
-->

<a href="https://youtube.com"><img src="Resources/yt_logo_fullcolor_white_digital.png" alt="YouTube Logo" width="400"/></a>
<a href="https://aws.amazon.com/what-is-cloud-computing"><img src="https://d0.awsstatic.com/logos/powered-by-aws-white.png" alt="Powered by AWS Cloud Computing" width="400"></a>

## Overview

This repository contains the artifacts for **Cloud-Based YouTube Trending Analytics Pipeline on AWS**, a serverless data and ML workflow that ingests daily trending-video metadata and comments, curates analytics-ready datasets, and produces next-day trending predictions. The system emphasizes managed AWS services to minimize operations overhead while keeping costs predictable.

### Key Capabilities
- **Daily ingestion**: EventBridge triggers Lambda functions that call the YouTube Data API and land raw JSON into partitioned S3 buckets by region and ingest date.
- **Curated ETL layers**: AWS Glue jobs flatten, type-cast, deduplicate, and partition trending and comment data into Parquet, enabling efficient Athena queries and downstream feature computation.
- **Sentiment + feature engineering**: Comments are scored with AWS Comprehend; trending metrics are joined with sentiment aggregates to build labeled feature sets capturing engagement momentum and audience tone.
- **Model training and predictions**: Glue jobs retrain models on updated features and emit next-day view-growth estimates and stay-trending probabilities for analytics dashboards.
- **Secure, modular infrastructure**: A dedicated VPC, Secrets Manager–backed credentials, and clearly separated scripts/notebooks keep ingestion, ETL, ML, and presentation assets organized.

## Repository Map
- `Python/ETL/`: Glue scripts for trending ingestion, comments processing, sentiment integration, and feature labeling used throughout the workflow.
- `Notebooks/`: Exploratory analysis, validation, and visualization notebooks.
- `Diagrams/`: Architecture and network diagrams referenced in the presentation materials.
- `Resources/`: Branding assets displayed in this README.
- `FinalReport.md`: Full project write-up covering scope, architecture, results, and cost considerations.
- `PresentationScript.md` and `Presentation` assets: Slide narrative outlining workflow stages and infrastructure.
- `Notes.md` and `TODO.md`: Working design notes, schemas, and milestone tracking.

## Quick Links
- [Final Report](FinalReport.md)
- [Project Proposal](ProjectProposal.md)
- [Notes](Notes.md)
- [TODO](TODO.md)
- [Presentation Script](PresentationScript.md)

## How to Use This Repo
1. Review `FinalReport.md` for the full architecture description, operational flow, and future work ideas.
2. Browse `Python/ETL/` to see Glue job implementations for trending ingestion, comments curation, and sentiment/feature engineering.
3. Open `Notebooks/` for exploratory analyses and validation steps used during model development.
4. Check `Diagrams/` alongside `PresentationScript.md` for visual references to the pipeline and network layout.
5. Track outstanding tasks or design decisions in `TODO.md` and `Notes.md` when iterating on the pipeline.

https://docs.google.com/document/d/1DZMIBqV4MBL5r5YGobFmRfblJUcRrJ7MrwSeqJXpMOs/edit?tab=t.0#heading=h.abprv9xacjmy

<p align="right"><a href="#readme-top">back to top</a></p>
