# Databricks notebook source
# CityFleet Taxi Project — full validation run
# This notebook assumes batch_001.csv.gz, batch_002.csv.gz, and batch_003.csv.gz are uploaded to:
# /Volumes/workspace/default/cityfleet_taxi_project/landing/

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "cityfleet_taxi_project"
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
landing_dir = f"{volume_path}/landing"

BRONZE = f"`{CATALOG}`.`{SCHEMA}`.`bronze_taxi_raw`"
SILVER = f"`{CATALOG}`.`{SCHEMA}`.`silver_taxi_trips`"
GOLD_DEMAND = f"`{CATALOG}`.`{SCHEMA}`.`gold_hourly_demand_by_cell`"
GOLD_KPIS = f"`{CATALOG}`.`{SCHEMA}`.`gold_weekly_fleet_kpis`"
GOLD_ANOM = f"`{CATALOG}`.`{SCHEMA}`.`gold_fare_anomaly_review`"
GOLD_GENIE = f"`{CATALOG}`.`{SCHEMA}`.`gold_genie_taxi_summary`"

BRONZE_PLAIN = f"{CATALOG}.{SCHEMA}.bronze_taxi_raw"
SILVER_PLAIN = f"{CATALOG}.{SCHEMA}.silver_taxi_trips"

print("Landing dir:", landing_dir)
display(dbutils.fs.ls(landing_dir))

# COMMAND ----------

# Reset tables so the demonstration is replayable.
for table in ["bronze_taxi_raw", "silver_taxi_trips", "gold_hourly_demand_by_cell", "gold_weekly_fleet_kpis", "gold_fare_anomaly_review", "gold_genie_taxi_summary"]:
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.`{table}`")
print("Project tables reset.")

# COMMAND ----------

raw_columns = [
    "medallion_raw", "hack_license_raw", "pickup_datetime_raw", "dropoff_datetime_raw",
    "trip_time_in_secs_raw", "trip_distance_raw", "pickup_longitude_raw", "pickup_latitude_raw",
    "dropoff_longitude_raw", "dropoff_latitude_raw", "payment_type_raw", "fare_amount_raw",
    "surcharge_raw", "mta_tax_raw", "tip_amount_raw", "tolls_amount_raw", "total_amount_raw"
]
raw_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in raw_columns])

lat_step = F.lit(0.0013474668)
lon_step = F.lit(0.0017958)
north_lat = F.lit(40.95)
east_lon = F.lit(-74.2)

def table_exists(plain_name):
    try:
        return spark.catalog.tableExists(plain_name)
    except Exception:
        return False

def add_grid_cells(df):
    return (
        df
        .withColumn("pickup_cell_lat", north_lat - F.floor((north_lat - F.col("pickup_latitude")) / lat_step) * lat_step - lat_step / 2)
        .withColumn("pickup_cell_lon", F.floor((F.col("pickup_longitude") - east_lon) / lon_step) * lon_step + east_lon + lon_step / 2)
        .withColumn("dropoff_cell_lat", north_lat - F.floor((north_lat - F.col("dropoff_latitude")) / lat_step) * lat_step - lat_step / 2)
        .withColumn("dropoff_cell_lon", F.floor((F.col("dropoff_longitude") - east_lon) / lon_step) * lon_step + east_lon + lon_step / 2)
    )

def recompute_gold():
    silver = spark.table(SILVER_PLAIN)
    clean = silver.where(F.col("clean_trip") == True)

    hourly_demand = (
        clean.groupBy("pickup_date", "pickup_hour", "pickup_cell_lat", "pickup_cell_lon")
        .agg(
            F.count("*").alias("trip_count"),
            F.countDistinct("medallion").alias("active_vehicles"),
            F.countDistinct("driver_id").alias("active_drivers"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("trip_distance_miles").alias("avg_trip_distance_miles"),
            F.avg("trip_time_seconds").alias("avg_trip_time_seconds"),
            F.sum(F.when(F.col("payment_type") == "CRD", 1).otherwise(0)).alias("card_trips"),
            F.sum(F.when(F.col("payment_type") == "CSH", 1).otherwise(0)).alias("cash_trips")
        )
        .withColumn("card_share", F.col("card_trips") / F.col("trip_count"))
        .withColumn("cash_share", F.col("cash_trips") / F.col("trip_count"))
        .withColumn("demand_score", F.col("trip_count") + F.col("total_revenue") / F.lit(1000.0))
    )
    hourly_demand.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DEMAND)

    weekly_kpis = (
        clean.withColumn("week_start", F.date_trunc("week", "pickup_datetime").cast("date"))
        .groupBy("week_start")
        .agg(
            F.count("*").alias("total_clean_trips"),
            F.countDistinct("medallion").alias("active_vehicles"),
            F.countDistinct("driver_id").alias("active_drivers"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_revenue_per_trip"),
            F.avg("trip_distance_miles").alias("avg_trip_distance_miles"),
            F.avg("trip_time_seconds").alias("avg_trip_time_seconds"),
            F.sum(F.when(F.col("payment_type") == "CRD", 1).otherwise(0)).alias("card_trips"),
            F.sum(F.when(F.col("payment_type") == "CSH", 1).otherwise(0)).alias("cash_trips")
        )
        .withColumn("card_share", F.col("card_trips") / F.col("total_clean_trips"))
        .withColumn("cash_share", F.col("cash_trips") / F.col("total_clean_trips"))
        .withColumn("trips_per_vehicle", F.col("total_clean_trips") / F.col("active_vehicles"))
        .withColumn("revenue_per_vehicle", F.col("total_revenue") / F.col("active_vehicles"))
    )
    weekly_kpis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_KPIS)

    anom = (
        silver.withColumn("tip_ratio", F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")))
        .withColumn("anomaly_reason", F.concat_ws(", ",
            F.when(F.col("total_amount") > 100, F.lit("high_total_amount")),
            F.when((F.col("trip_distance_miles") <= 0) & (F.col("total_amount") > 20), F.lit("zero_distance_paid_trip")),
            F.when(F.col("tip_ratio") > 0.8, F.lit("high_tip_ratio")),
            F.when(F.col("trip_time_seconds") <= 0, F.lit("nonpositive_duration")),
            F.when(~F.col("coordinates_valid"), F.lit("invalid_coordinates"))
        ))
        .withColumn("anomaly_score",
            F.when(F.col("total_amount") > 100, 3).otherwise(0) +
            F.when((F.col("trip_distance_miles") <= 0) & (F.col("total_amount") > 20), 3).otherwise(0) +
            F.when(F.col("tip_ratio") > 0.8, 2).otherwise(0) +
            F.when(F.col("trip_time_seconds") <= 0, 2).otherwise(0) +
            F.when(~F.col("coordinates_valid"), 1).otherwise(0)
        )
        .where(F.length("anomaly_reason") > 0)
        .select("trip_id", "pickup_datetime", "pickup_date", "pickup_hour", "medallion", "driver_id", "trip_distance_miles", "trip_time_seconds", "pickup_latitude", "pickup_longitude", "fare_amount", "tip_amount", "tip_ratio", "total_amount", "payment_type", "anomaly_reason", "anomaly_score", "ingestion_batch_id")
    )
    anom.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_ANOM)

    hourly_demand.select(
        F.col("pickup_date").alias("service_date"),
        F.col("pickup_hour").alias("service_hour"),
        F.col("pickup_cell_lat").alias("pickup_area_latitude"),
        F.col("pickup_cell_lon").alias("pickup_area_longitude"),
        "trip_count", "active_vehicles", "active_drivers", "total_revenue", "avg_total_amount", "avg_trip_distance_miles", "card_share", "cash_share", "demand_score"
    ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_GENIE)

# COMMAND ----------

def process_batch(batch_id, file_path):
    print(f"=== Processing {batch_id} ===")
    raw = (
        spark.read.schema(raw_schema).option("header", False).csv(file_path)
        .withColumn("source_file", F.lit(file_path))
        .withColumn("ingestion_batch_id", F.lit(batch_id))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("raw_row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in raw_columns]), 256))
    )
    raw.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(BRONZE)

    typed = raw.select(
        F.col("medallion_raw").alias("medallion"), F.col("hack_license_raw").alias("driver_id"),
        F.to_timestamp("pickup_datetime_raw").alias("pickup_datetime"), F.to_timestamp("dropoff_datetime_raw").alias("dropoff_datetime"),
        F.col("trip_time_in_secs_raw").cast("int").alias("trip_time_seconds"), F.col("trip_distance_raw").cast("double").alias("trip_distance_miles"),
        F.col("pickup_longitude_raw").cast("double").alias("pickup_longitude"), F.col("pickup_latitude_raw").cast("double").alias("pickup_latitude"),
        F.col("dropoff_longitude_raw").cast("double").alias("dropoff_longitude"), F.col("dropoff_latitude_raw").cast("double").alias("dropoff_latitude"),
        F.upper(F.trim("payment_type_raw")).alias("payment_type"), F.col("fare_amount_raw").cast("double").alias("fare_amount"),
        F.col("surcharge_raw").cast("double").alias("surcharge"), F.col("mta_tax_raw").cast("double").alias("mta_tax"),
        F.col("tip_amount_raw").cast("double").alias("tip_amount"), F.col("tolls_amount_raw").cast("double").alias("tolls_amount"),
        F.col("total_amount_raw").cast("double").alias("total_amount"), "source_file", "ingestion_batch_id", "ingestion_timestamp", "raw_row_hash"
    ).withColumn("pickup_date", F.to_date("pickup_datetime")).withColumn("pickup_hour", F.hour("pickup_datetime"))

    typed = add_grid_cells(typed)
    coords_valid = F.col("pickup_latitude").between(40.0, 41.0) & F.col("pickup_longitude").between(-75.0, -73.0) & F.col("dropoff_latitude").between(40.0, 41.0) & F.col("dropoff_longitude").between(-75.0, -73.0)
    time_valid = F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull() & (F.col("dropoff_datetime") >= F.col("pickup_datetime")) & (F.col("trip_time_seconds") > 0)
    distance_valid = F.col("trip_distance_miles") > 0
    amount_valid = (F.col("fare_amount") >= 0) & (F.col("tip_amount") >= 0) & (F.col("total_amount") > 0)

    stage = (typed.withColumn("trip_id", F.sha2(F.concat_ws("||", F.coalesce(F.col("medallion"), F.lit("")), F.coalesce(F.col("driver_id"), F.lit("")), F.coalesce(F.col("pickup_datetime").cast("string"), F.lit("")), F.coalesce(F.col("dropoff_datetime").cast("string"), F.lit("")), F.coalesce(F.col("trip_distance_miles").cast("string"), F.lit("")), F.coalesce(F.col("total_amount").cast("string"), F.lit(""))), 256))
        .withColumn("coordinates_valid", coords_valid).withColumn("time_valid", time_valid).withColumn("distance_valid", distance_valid).withColumn("amount_valid", amount_valid)
        .withColumn("clean_trip", coords_valid & time_valid & distance_valid & amount_valid)
        .withColumn("quality_issues", F.concat_ws(", ", F.when(~coords_valid, F.lit("bad_coordinates")), F.when(~time_valid, F.lit("bad_time")), F.when(~distance_valid, F.lit("bad_distance")), F.when(~amount_valid, F.lit("bad_amount"))))
    )
    silver_cols = ["trip_id", "medallion", "driver_id", "pickup_datetime", "dropoff_datetime", "pickup_date", "pickup_hour", "trip_time_seconds", "trip_distance_miles", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "pickup_cell_lat", "pickup_cell_lon", "dropoff_cell_lat", "dropoff_cell_lon", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount", "coordinates_valid", "time_valid", "distance_valid", "amount_valid", "clean_trip", "quality_issues", "source_file", "ingestion_batch_id", "ingestion_timestamp", "raw_row_hash"]
    w = Window.partitionBy("trip_id").orderBy(F.col("ingestion_timestamp").desc())
    stage = stage.select(*silver_cols).withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
    if not table_exists(SILVER_PLAIN):
        stage.limit(0).write.format("delta").mode("overwrite").saveAsTable(SILVER)
    stage.createOrReplaceTempView("silver_stage")
    spark.sql(f"MERGE INTO {SILVER} AS target USING silver_stage AS source ON target.trip_id = source.trip_id WHEN NOT MATCHED THEN INSERT *")
    recompute_gold()

    return (batch_id, spark.table(BRONZE_PLAIN).count(), spark.table(SILVER_PLAIN).count(), spark.table(SILVER_PLAIN).where("clean_trip").count(), spark.table(f"{CATALOG}.{SCHEMA}.gold_hourly_demand_by_cell").count(), spark.table(f"{CATALOG}.{SCHEMA}.gold_fare_anomaly_review").count())

# COMMAND ----------

files = sorted([f.path for f in dbutils.fs.ls(landing_dir) if f.name.startswith("batch_") and f.name.endswith(".csv.gz")])
assert len(files) == 3, f"Expected 3 batch files in {landing_dir}, found {len(files)}: {files}"
results = []
for path in files:
    batch_id = path.split('/')[-1].replace('.csv.gz', '')
    results.append(process_batch(batch_id, path))

summary = spark.createDataFrame(results, ["processed_batch", "bronze_rows", "silver_rows", "silver_clean_rows", "gold_demand_rows", "gold_anomaly_rows"])
display(summary)

# COMMAND ----------

print("Bronze batches")
display(spark.table(BRONZE_PLAIN).groupBy("ingestion_batch_id").count().orderBy("ingestion_batch_id"))
print("Silver quality by batch")
display(spark.table(SILVER_PLAIN).groupBy("ingestion_batch_id", "clean_trip").count().orderBy("ingestion_batch_id", "clean_trip"))
print("Weekly KPIs")
display(spark.table(f"{CATALOG}.{SCHEMA}.gold_weekly_fleet_kpis").orderBy("week_start"))
print("Top operational hotspots")
display(spark.sql(f"""
SELECT service_date, service_hour, pickup_area_latitude, pickup_area_longitude, trip_count, active_vehicles, ROUND(total_revenue, 2) AS total_revenue, ROUND(demand_score, 2) AS demand_score
FROM {CATALOG}.{SCHEMA}.gold_genie_taxi_summary
ORDER BY demand_score DESC
LIMIT 25
"""))
print("Top fare anomalies")
display(spark.table(f"{CATALOG}.{SCHEMA}.gold_fare_anomaly_review").orderBy(F.desc("anomaly_score"), F.desc("total_amount")).limit(50))
