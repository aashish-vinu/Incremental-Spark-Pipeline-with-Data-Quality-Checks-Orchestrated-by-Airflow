import sys
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, hour,
    unix_timestamp, when, isnan, isnull
)


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
    base = os.path.basename(file_name)
    return base.replace("yellow_tripdata_", "").replace(".parquet", "")

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("NYC Taxi Incremental Clean") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

processed_months = load_processed_months()
files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if f.endswith('parquet')]
new_files = []
new_months = []

for file_path in files:
    month_key = get_month_key(file_path)
    if month_key and month_key not in processed_months:
        new_files.append(file_path)
        new_months.append(month_key)

if not new_files:
    stats = {"processed_months": [], "records_before": 0, "records_after": 0, "rejected": 0}
else:
    df = spark.read.parquet(*new_files)
    initial_count = df.count()
    
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

    cleaned_df = cleaned_df.withColumn("year", year("tpep_pickup_datetime")) \
                           .withColumn("month", month("tpep_pickup_datetime").cast("string"))

    final_count = cleaned_df.count()
    rejected = initial_count - final_count

    print(f"After cleaning: {final_count} records (rejected: {rejected})")

    (cleaned_df.write
     .mode("append")  
     .partitionBy("year", "month")
     .parquet(OUTPUT_DIR))


    all_processed = list(set(processed_months + new_months))
    save_processed_months(all_processed)

    stats = {
        "processed_months": new_months,
        "records_before": initial_count,
        "records_after": final_count,
        "rejected": rejected
    }

os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
with open(STATS_FILE, "w") as f:
    json.dump(stats, f, indent=2)

spark.stop()
