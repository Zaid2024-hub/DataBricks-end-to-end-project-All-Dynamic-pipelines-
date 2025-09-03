# End-to-End Data Engineering Project with Databricks & DBT

📌 Project Overview

This project demonstrates an end-to-end dynamic data engineering pipeline built on Databricks, leveraging Apache Spark, Delta Lake, and DBT.

It follows the Medallion Architecture (Bronze → Silver → Gold) and is designed to be 100% dynamic:

No hardcoded table names, schemas, or paths.

Pipelines are fully parameterized using Databricks widgets and configs.

Capable of handling multiple datasets and incremental loads with minimal changes.

The pipeline ingests, processes, and transforms raw data into high-quality, analytics-ready datasets for BI and reporting.

🛠️ Tech Stack

Databricks – Unified platform for data engineering & analytics

Apache Spark (PySpark/SQL) – Distributed data processing

Delta Lake – Storage layer with ACID transactions & schema evolution

DBT (Data Build Tool) – SQL-based modular transformations

Medallion Architecture – Bronze, Silver, Gold layered design

Dynamic Parameterization – CDC columns, keys, refresh type

📂 Project Architecture
            Raw Data (Source Systems)
                      │
                      ▼
             Bronze Layer (Dynamic Ingestion)
                      │
            +-----------------------------+
            |  Schema Inference,          |
            |  Config-driven ingestion    |
            +-----------------------------+
                      │
                      ▼
             Silver Layer (Dynamic Processing)
                      │
            +-----------------------------+
            |  Deduplication, Joins,      |
            |  CDC Logic, Backdated Load  |
            +-----------------------------+
                      │
                      ▼
             Gold Layer (Dynamic Business Models)
                      │
            +-----------------------------+
            |  Aggregations, KPIs,        |
            |  Reusable DBT Models        |
            +-----------------------------+
                      │
                      ▼
              BI Tools / Analytics

⚙️ Key Dynamic Features

✅ Config & Widget Driven: Pipeline execution controlled via Databricks widgets (keycols, cdc_col, backdated_refresh).

✅ Dynamic Ingestion: Automatically adapts to new tables/datasets without code changes.

✅ Dynamic CDC Logic: Handles incremental loads & change data capture across multiple datasets.

✅ Dynamic Backdated Refresh: Ability to reprocess data for specific dates without impacting the whole pipeline.

✅ Reusable Notebooks: Modularized notebooks for ingestion, transformation, and aggregation.

🧪 Example Use Case

Load flight booking data dynamically into Bronze (no hardcoded schema).

Apply CDC logic and deduplication dynamically in Silver.

Build Gold-level KPIs (e.g., revenue trends, top destinations) using DBT models.

Re-run pipeline dynamically for any chosen date range using backdated refresh.

🚦 How to Run

Clone this repository:

git clone https://github.com/your-username/databricks-dynamic-etl.git


Import notebooks into Databricks Workspace.

Configure widgets dynamically:

keycols → Primary key columns

cdc_col → Change Data Capture column

backdated_refresh → Date or flag for reprocessing

Run Bronze → Silver → Gold pipelines.

(Optional) Use DBT for modular transformations and documentation.

📊 Results

Built a fully dynamic, end-to-end data pipeline.

Successfully implemented CDC and backdated refresh without hardcoding.

Delivered scalable, reusable, and production-ready pipeline design.

📌 Skills Demonstrated

Dynamic Data Pipelines

Databricks (Spark, SQL, Delta Lake)

DBT for Data Modeling

Medallion Architecture

CDC & Incremental Loading

Modular, Config-driven ETL

📎 References

Databricks Documentation

DBT Documentation

Inspired by Anshul Kalma’s tutorial

