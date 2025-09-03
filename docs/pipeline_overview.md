# Data Pipeline Overview

## 1. Data Source
- Sample flight bookings dataset (`data/sample_flight_bookings.csv`).
- Ingested dynamically using JSON configs (`configs/sample_config.json`).

## 2. Bronze Layer
- Raw ingestion into Delta format.
- Minimal transformations, schema preserved.

## 3. Silver Layer
- Data cleaning, casting, and incremental CDC applied.
- Ensures high-quality, reliable data.

## 4. Gold Layer
- Business-level aggregations (e.g., revenue per destination).
- Ready for analytics and reporting.

## 5. DBT Integration
- Bronze → Silver → Gold modeled in DBT.
- Materializations: Bronze (table), Silver (incremental), Gold (table).

## 6. Dynamic Configs
- Paths, key columns, CDC column, partitions are all parameterized.
- No static/hardcoded values.

---

This architecture ensures:
- **Scalability** (new sources can be added via config).
- **Maintainability** (clean medallion design).
- **Transparency** (clear lineage from raw → gold).
