# CityFleet Taxi Demand, Revenue & Fare Quality Control Tower

This project was created for Assignment 2 in Engineering of Data Analysis. It prototypes an end-to-end Databricks Free Edition pipeline using a Medallion Architecture from Bronze to Silver to Gold.

The business case is a hypothetical New York taxi fleet operator called CityFleet. The company uses trip-level taxi data to improve hourly dispatch decisions, weekly fleet planning, fare-quality monitoring, and self-service analysis through Databricks Genie.

## Business Process

CityFleet receives taxi trip data with vehicle IDs, driver IDs, pickup and dropoff times, trip distance, GPS coordinates, payment type, fare, tip, tolls, and total amount. These fields support several consumers from the same data source:

- Dispatch teams use hourly pickup demand hotspots to reposition vehicles.
- Fleet planners use weekly KPIs to evaluate usage, revenue, payment mix, and vehicle productivity.
- Finance and audit teams use an anomaly queue to review suspicious fares and low-quality trip records.
- Business analysts use Databricks Genie over a curated Gold table with business-readable column names.

## Data Source and Incremental Arrival

The source data is the instructor-provided NYC taxi trip dataset. The project uses a sample with 1,999,999 rows covering trips from 2013-01-01 to 2013-01-20.

The original dataset is static, so incremental arrival is simulated by splitting it into three time-ordered compressed CSV batches:

| Batch | Date range | Rows | Role |
|---|---:|---:|---|
| `batch_001.csv.gz` | 2013-01-01 to 2013-01-04 | 864,187 | Initial load |
| `batch_002.csv.gz` | 2013-01-05 to 2013-01-14 | 599,783 | Refresh iteration 1 |
| `batch_003.csv.gz` | 2013-01-15 to 2013-01-20 | 536,029 | Refresh iteration 2 |

The batch files are stored in `sample_data/taxi_batches/`, together with `manifest.csv` and `manifest.json`.

## Databricks Environment

The prototype uses these default Databricks names:

| Item | Value |
|---|---|
| Catalog | `workspace` |
| Schema | `default` |
| Volume | `cityfleet_taxi_project` |
| Landing folder | `/Volumes/workspace/default/cityfleet_taxi_project/landing/` |

The notebooks expose these values as widgets where relevant, so the same code can be adapted if the workspace uses different names.

## Reproducibility Run Order

The grader can replay the project in Databricks Free Edition using the notebooks and sample data included in this repository.

1. Import the notebooks from `notebooks/` into Databricks.
2. Run `01_setup_volume.ipynb` to create the volume folders.
3. Upload the three compressed batch files from `sample_data/taxi_batches/` into `/Volumes/workspace/default/cityfleet_taxi_project/landing/`.
4. Run `02_incremental_refresh.ipynb` with `process_mode = next`. This processes `batch_001`.
5. Run `02_incremental_refresh.ipynb` again with `process_mode = next`. This processes `batch_002` and demonstrates the first refresh.
6. Run `02_incremental_refresh.ipynb` a third time with `process_mode = next`. This processes `batch_003` and demonstrates the second refresh.
7. Run `03_dashboard_and_genie_queries.ipynb` to display the Gold-table queries used for dashboard and Genie validation.
8. If a complete one-notebook validation is preferred, `cityfleet_databricks_validation.py` replays reset, batch processing, Silver validation, Gold creation, and final output checks in one Databricks notebook/script.

The optional `99_reset_project_tables.ipynb` drops the project tables so the demonstration can be replayed from the beginning without deleting the uploaded batch files.

## Medallion Architecture

### Bronze

The Bronze table is `bronze_taxi_raw`. It stores the 17 source columns as strings and adds `source_file`, `ingestion_batch_id`, `ingestion_timestamp`, and `raw_row_hash`.

Purpose: preserve the raw landing history and make every later transformation traceable.

### Silver

The Silver table is `silver_taxi_trips`. It converts raw strings into usable analytical data:

- timestamps and numeric fields are cast into proper types
- invalid or suspicious records are flagged
- pickup and dropoff coordinates are validated
- trips are assigned to approximate grid cells
- a deterministic `trip_id` is created for deduplication
- quality flags are stored instead of silently dropping every imperfect record

Purpose: create a typed, validated, deduplicated trip-level source of truth.

### Gold

The Gold layer contains business-level tables:

| Table | Main user | Purpose |
|---|---|---|
| `gold_hourly_demand_by_cell` | Dispatch / operations | Hourly demand hotspots, vehicle activity, revenue, and payment mix by pickup grid cell. |
| `gold_weekly_fleet_kpis` | Fleet planning / management | Weekly trips, revenue, active vehicles, active drivers, payment mix, and productivity. |
| `gold_fare_anomaly_review` | Finance / audit | Review queue for high fares, zero-distance paid trips, high tip ratios, nonpositive durations, and invalid coordinates. |
| `gold_genie_taxi_summary` | Business analysts | Business-readable hourly summary for Databricks Genie self-service questions. |

## Expected Final Output

After all three batches have been processed, the expected final counts are:

| Layer / output | Expected count |
|---|---:|
| Bronze raw rows | 1,999,999 |
| Silver trip records | 1,999,999 |
| Silver clean trips | 1,955,237 |
| Gold hourly demand rows | 314,394 |
| Gold fare anomaly review rows | 45,382 |

The implemented consumer output is a Databricks Genie space connected to Gold tables, supported by dashboard-ready Gold queries for operational hotspots, hourly trends, weekly KPIs, and fare anomaly review.


