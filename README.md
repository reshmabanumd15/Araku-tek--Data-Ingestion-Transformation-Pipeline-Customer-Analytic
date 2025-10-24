# Data Ingestion & Transformation Pipeline – Customer Analytics (Pro Edition)

This repository is a **production-style** scaffold for an analytics pipeline with realistic volume and moving parts.

**Highlights**
- Partitioned **raw** and **processed** datasets with ~**350k+ rows** across multiple ingest dates
- **Python ingestion** (batched local → S3)
- **AWS Lambda** S3 trigger for metadata logging
- **AWS Glue** jobs (batch + incremental) CSV→Parquet with partitioning
- **Glue Crawler** JSON config
- **Amazon Redshift** DDL, COPY, MERGE (upsert) & star schema
- **Data Quality (DQ)** checks (lightweight Python + pytest)
- **Airflow DAG** (orchestration, optional)
- **CI** via GitHub Actions (lint + tests)
- **Terraform stubs** to provision baseline AWS resources
- **Docs**: setup, monitoring, incremental refresh (PBI/Athena), runbooks

> ⚠️ Names/roles/ARNs are placeholders—replace with your own before deploy.
