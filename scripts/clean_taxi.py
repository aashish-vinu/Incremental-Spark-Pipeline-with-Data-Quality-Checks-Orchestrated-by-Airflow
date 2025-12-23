# scripts/clean_taxi.py

import sys
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, hour,
    unix_timestamp, when, isnan, isnull
)

# Paths – mounted from host
INPUT_DIR = "/opt/airflow/data/input"
OUTPUT_DIR = "/opt/airflow/data/output/cleaned"
CHECKPOINT_FILE = "/opt/airflow/data/checkpoints/processed_months.json"
STATS_FILE = "/opt/airflow/data/stats.json"

def load_processed_months():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip()
            return json.loads(content) if content else []
    return []

def save_processed_months(processed):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(processed), f)

def get_month_key(file_name):
    # Extract YYYY-MM from filename like yellow_tripdata_2025-10.parquet
    base = os.path.basename(file_name)
    if "yellow_tripdata_" in base and ".parquet" in base:
        return base.replace("yellow_tripdata_", "").replace(".parquet", "")
    return None

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("NYC Taxi Incremental Clean") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Load already processed months
processed_months = load_processed_months()
print(f"Already processed months: {processed_months}")

# Find all parquet files
files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if f.endswith(".parquet")]
print(f"Found {len(files)} input files")

# Determine which ones are new
new_files = []
new_months = []
for file_path in files:
    month_key = get_month_key(file_path)
    if month_key and month_key not in processed_months:
        new_files.append(file_path)
        new_months.append(month_key)

if not new_files:
    print("No new months to process. Exiting.")
    stats = {"processed_months": [], "records_before": 0, "records_after": 0, "rejected": 0}
else:
    print(f"Processing new months: {new_months}")

    # Read only new files
    df = spark.read.parquet(*new_files)

    initial_count = df.count()
    print(f"Initial record count: {initial_count}")

    # Data cleaning and transformations
    cleaned_df = df.filter(
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0) &
        col("PULocationID").isNotNull() &
        col("DOLocationID").isNotNull() &
        (col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
    )

    cleaned_df = cleaned_df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    ).withColumn(
        "avg_speed_mph",
        when(col("trip_duration_minutes") > 0,
             col("trip_distance") / (col("trip_duration_minutes") / 60)
        ).otherwise(0)
    )

    # Add year/month columns for partitioning
    cleaned_df = cleaned_df.withColumn("year", year("tpep_pickup_datetime")) \
                           .withColumn("month", month("tpep_pickup_datetime").cast("string"))

    final_count = cleaned_df.count()
    rejected = initial_count - final_count

    print(f"After cleaning: {final_count} records (rejected: {rejected})")

    # Write partitioned by year/month
    (cleaned_df.write
     .mode("append")  # Important for incremental
     .partitionBy("year", "month")
     .parquet(OUTPUT_DIR))

    # Update checkpoint
    all_processed = list(set(processed_months + new_months))
    save_processed_months(all_processed)

    stats = {
        "processed_months": new_months,
        "records_before": initial_count,
        "records_after": final_count,
        "rejected": rejected
    }

# Always write stats (used by next task via XCom and file)
os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
with open(STATS_FILE, "w") as f:
    json.dump(stats, f, indent=2)

print("Stats written to", STATS_FILE)
print(json.dumps(stats, indent=2))

# Stop Spark
spark.stop()
