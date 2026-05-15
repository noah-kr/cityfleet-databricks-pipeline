# CityFleet Taxi Demand, Revenue & Fare Quality Control Tower

This project was created for Assignment 2 in Engineering of Data Analysis. It prototypes a Databricks lakehouse pipeline using a Medallion Architecture from Bronze to Silver to Gold.

The business case is a hypothetical New York taxi fleet operator called CityFleet. The company wants to use trip-level data to improve dispatch decisions, weekly fleet planning, and fare-quality monitoring. The same curated data also supports self-service exploration through Databricks Genie.

## Business Process

CityFleet receives taxi trip data with vehicle IDs, driver IDs, pickup and dropoff times, trip distance, GPS coordinates, payment type, fare, tip, tolls, and total amount. The process is realistic because these data points are directly connected to fleet operations:

- Dispatch teams need hourly demand hotspots to decide where drivers should be repositioned.
- Fleet planners need weekly KPIs to understand usage, revenue, payment mix, and vehicle productivity.
- Finance and audit teams need a review queue for suspicious fares or low-quality trip records.
- Business users need a curated table that can be queried through natural language in Databricks Genie.

## Data Source

The source data is the provided NYC taxi trip dataset. The project uses a sample with 1,999,999 rows covering trips from 2013-01-01 to 2013-01-20.

To demonstrate incremental processing, the static dataset was split into three time-ordered compressed CSV batches:

| Batch | Date range | Rows | Role |
|---|---:|---:|---|
| `batch_001.csv.gz` | 2013-01-01 to 2013-01-04 | 864,187 | Initial load |
| `batch_002.csv.gz` | 2013-01-05 to 2013-01-14 | 599,783 | Refresh iteration 1 |
| `batch_003.csv.gz` | 2013-01-15 to 2013-01-20 | 536,029 | Refresh iteration 2 |

The files are stored in `sample_data/taxi_batches/`.

## Medallion Architecture

### Bronze

The Bronze table is `bronze_taxi_raw`. It keeps the source data as close to the original files as possible. The raw columns are stored as strings, and the pipeline adds metadata such as source file, ingestion batch, ingestion timestamp, and row hash.

Purpose: preserve the raw landing history and make every later transformation traceable.

### Silver

The Silver table is `silver_taxi_trips`. It converts raw strings into usable business data:

- timestamps and numeric fields are cast into proper types
- invalid or suspicious records are flagged
- pickup and dropoff coordinates are validated
- trips are assigned to approximate grid cells
- a deterministic `trip_id` is created for deduplication
- quality flags are stored instead of silently dropping every imperfect record

Purpose: create a typed, validated, deduplicated trip-level source of truth.

### Gold

The Gold layer contains business-level tables:

| Table | Main user |
|---|---|
| `gold_hourly_demand_by_cell` | Dispatch / operations |
| `gold_weekly_fleet_kpis` | Fleet planning / management |
| `gold_fare_anomaly_review` | Finance / audit |
| `gold_genie_taxi_summary` | Business analysts using Genie |

Purpose: serve specific analytical needs instead of exposing raw trip data to every user.

## Databricks Implementation

The Databricks prototype was built in the `workspace.default` schema. The uploaded batch files are stored in the Unity Catalog volume:

```text
/Volumes/workspace/default/cityfleet_taxi_project/landing/
```

The validation run processed all three batches successfully. After the final refresh:

- Bronze contained 1,999,999 raw rows.
- Silver contained 1,999,999 deduplicated trip records.
- Silver marked 1,955,237 records as clean trips.
- The operational hourly demand Gold table contained 314,394 rows.
- The fare anomaly review Gold table contained 45,382 rows.

## Project Files

```text
notebooks/
  01_setup_volume.ipynb
  02_incremental_refresh.ipynb
  03_dashboard_and_genie_queries.ipynb
  99_reset_project_tables.ipynb
  cityfleet_databricks_validation.py

sample_data/
  taxi_batches/
    batch_001.csv.gz
    batch_002.csv.gz
    batch_003.csv.gz
    manifest.csv
    manifest.json

report/
  report.tex

report_assets/
  databricks_gold_kpi_sample.png
  databricks_gold_hourly_sample.png
  genie_answer.png

report_draft.md
```

The notebook `cityfleet_databricks_validation.py` was used as the final Databricks validation run because it executes the full demonstration in one place: reset, batch processing, Silver validation, Gold table creation, and output checks.
