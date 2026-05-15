# CityFleet Taxi Demand, Revenue & Fare Quality Control Tower

This folder contains a Databricks Free Edition prototype for Assignment 2, Engineering of Data Analysis.

The project uses the instructor-provided NYC taxi trip dataset to simulate an incremental lakehouse pipeline. The organisational process is taxi fleet operations: dispatch managers use hourly demand tables, fleet planners use weekly KPIs, finance/audit users review fare anomalies, and analysts use Databricks Genie over curated Gold tables.

## Folder Contents

- `notebooks/01_setup_volume.ipynb` — creates the Unity Catalog volume folders.
- `notebooks/02_incremental_refresh.ipynb` — processes the next unprocessed batch: Bronze append, Silver merge, Gold recompute.
- `notebooks/03_dashboard_and_genie_queries.ipynb` — dashboard SQL queries and Genie test questions.
- `notebooks/99_reset_project_tables.ipynb` — optional reset notebook for replay.
- `sample_data/taxi_batches/` — three time-ordered compressed CSV arrival batches.
- `report_draft.md` — draft text for the report.

## Databricks Names Used

Default values:

- Catalog: `workspace`
- Schema: `default`
- Volume: `cityfleet_taxi_project`
- Landing folder: `/Volumes/workspace/default/cityfleet_taxi_project/landing/`

You may change the notebook widgets if your Databricks workspace uses different names.

## Run Order

1. Import all notebooks in `notebooks/` into Databricks Free Edition.
2. Run `01_setup_volume.ipynb`.
3. Upload these files into `/Volumes/workspace/default/cityfleet_taxi_project/landing/`:
   - `sample_data/taxi_batches/batch_001.csv.gz`
   - `sample_data/taxi_batches/batch_002.csv.gz`
   - `sample_data/taxi_batches/batch_003.csv.gz`
4. Run `02_incremental_refresh.ipynb` with `process_mode = next`.
   - This processes `batch_001`.
   - Record the final row-count table for the report.
5. Run `02_incremental_refresh.ipynb` again with `process_mode = next`.
   - This processes `batch_002`.
   - Record that Bronze grew, Silver merged, and Gold changed.
6. Run `02_incremental_refresh.ipynb` a third time with `process_mode = next`.
   - This processes `batch_003`.
   - This gives two refresh iterations after the initial load.
7. Run `03_dashboard_and_genie_queries.ipynb`.
8. In Databricks SQL, create dashboard visualisations from the SQL queries:
   - Latest hour demand hotspots.
   - Hourly demand trend.
   - Weekly fleet KPIs.
   - Fare anomaly review queue.
9. Create a Databricks Genie space using:
   - `gold_genie_taxi_summary`
   - `gold_weekly_fleet_kpis`
   - `gold_fare_anomaly_review`

## Expected Batch Sizes

| Batch | Date range | Rows |
|---|---:|---:|
| `batch_001` | 2013-01-01 to 2013-01-04 | 864,187 |
| `batch_002` | 2013-01-05 to 2013-01-14 | 599,783 |
| `batch_003` | 2013-01-15 to 2013-01-20 | 536,029 |

After all three runs, Bronze should contain 1,999,999 raw rows before any Silver quality filtering.

## Tables Created

### Bronze

- `bronze_taxi_raw`

Append-only table containing source columns as strings plus:

- `source_file`
- `ingestion_batch_id`
- `ingestion_timestamp`
- `raw_row_hash`

### Silver

- `silver_taxi_trips`

Typed, validated, deduplicated trip-level table. It includes:

- timestamp, numeric fare, distance, and coordinate fields
- pickup/dropoff grid cells
- quality booleans
- `clean_trip`
- `quality_issues`
- `trip_id`

### Gold

- `gold_hourly_demand_by_cell` — operational dashboard table.
- `gold_weekly_fleet_kpis` — strategic fleet planning table.
- `gold_fare_anomaly_review` — finance/audit action queue.
- `gold_genie_taxi_summary` — business-readable table for Genie.

## What to Screenshot for the Report

Capture these after each refresh:

- Final summary table from `02_incremental_refresh.ipynb`.
- Processed batch IDs in Bronze.
- Silver quality summary by `ingestion_batch_id`.

Capture these after running dashboard queries:

- Hotspot table or map from `gold_genie_taxi_summary`.
- Hourly demand trend visualisation.
- Weekly KPI table.
- Fare anomaly queue.

For Genie:

- Screenshot at least two successful questions.
- Screenshot or describe one question that did not work perfectly, then explain that table comments and business-readable column names helped.

## Production Discussion Points

In production, simulated file upload would be replaced by Auto Loader or cloud file-arrival triggers from object storage. If events arrived continuously, Kafka/Event Hubs plus Structured Streaming could land events into Bronze. Compute would move from Free Edition serverless quotas to scheduled jobs or multi-node clusters. Storage would move from workspace volumes to S3/Azure Blob/GCS with Unity Catalog governance, table permissions, lineage, monitoring, and alerting.

