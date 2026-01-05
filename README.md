# NYC Taxi Data Cleaning Pipeline with Incremental Processing and Data Quality Checks

This project implements a monthly data cleaning pipeline for New York City Yellow Taxi trip data using **Apache Spark**, orchestrated with **Apache Airflow**. The pipeline has been enhanced to process data incrementally, add basic data quality checks, and handle multiple months of data.

## Dataset

The pipeline processes **NYC Yellow Taxi Trip Records** in Parquet format.
Example file: `yellow_tripdata_2025-10.parquet`

These datasets are publicly available from the NYC Taxi & Limousine Commission (TLC) and contain detailed trip information such as pickup/dropoff times, locations, distances, fares, passenger count, etc.

Source: [NYC Taxi & Limousine Commission Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Task Enhancements

### Incremental Data Processing

- The pipeline now processes **multiple months** of data incrementally, ensuring that only new data (i.e., records for new months) is processed.
- A checkpoint or state file is maintained to track which months have already been processed, preventing duplicate work.

### Data Transformations

The Spark job (scripts/clean_taxi.py) performs the following additional transformations:

- Filters out invalid records:
  - Removes rows where `tpep_pickup_datetime` is null.
  - Removes rows where `trip_distance <= 0` or `fare_amount <= 0`.
- Adds derived columns:
  - **Trip duration**: `dropoff_time - pickup_time`.
  - **Average speed**: `distance / duration`.
- Partitions the output data by **pickup date** (or month) for better scalability.
- Writes the cleaned DataFrame back to Parquet format.

### Data Quality Checks

- A new **data quality check** step has been added:
  - Ensures that **record counts** are greater than zero.
  - Verifies that **key columns** (e.g., `pickup_datetime`, `trip_distance`, `fare_amount`) do not contain null values after cleaning.
- If any of the data quality checks fail, the job will be marked as failed.

### Statistics and Logging

- After cleaning, statistics are saved in a `stats.json` file, which contains:
  - Number of records read.
  - Number of records written (after filtering).
  - Data quality validation results.
- Meaningful logs are included to show:
  - Which months are being processed.
  - Record counts before and after filtering.
  - Results of the data quality checks.

## Airflow DAG

The Airflow DAG `taxi_clean_pipeline` orchestrates the following tasks:

1. **run_spark_clean**:
   - Submits a Spark job using the `SparkSubmitOperator` to clean the taxi data.
   - Processes data for new months, ensuring that only new data is processed.
   
2. **run_data_quality_checks**:
   - Runs data quality checks to validate the processed data.
   - If the checks fail, the pipeline is stopped.
   
3. **push_stats_to_xcom**:
   - Reads the generated `stats.json` file.
   - Pushes key metrics (records processed, records rejected, data quality validation) to **XCom** for downstream tasks to use.

### Monitoring and Debugging

- You can monitor the execution of each task in the **Airflow UI**.
- Logs for the Spark job are also available within the Airflow task logs.

## Local Setup

Run everything locally on your laptop:
- **Spark** in local mode.
- **Airflow** using the `LocalExecutor` or `SequentialExecutor`.

### Steps for Local Setup

1. **Create the `logs` directory:**

    ```bash
    mkdir logs
    sudo chown -R 50000:0 logs
    sudo chmod -R 777 data
    ```

2. **Build and run Docker containers:**

    ```bash
    docker compose up -d --build
    ```

3. **Set up Airflow connection:**
   - Go to Airflow UI at `http://localhost:8080`.
   - Navigate to **Admin > Connections**.
   - Create a new connection with the following details:
     - **Connection Id**: `spark_default` (default used by Spark operators)
     - **Connection Type**: `Spark`
     - **Host**: `local[*]` (runs Spark in local mode using all available CPU cores)

## How to Run

### 1. Run the Spark Job Directly (Standalone)

- Access the webserver (or any airflow) container:

    ```bash
    docker compose exec airflow-webserver bash
    ```

- Run the Spark job manually:

    ```bash
    spark-submit /opt/airflow/scripts/clean_taxi.py \
      --input /opt/airflow/data/input/yellow_tripdata_2025-10.parquet \
      --output /opt/airflow/data/output/cleaned \
      --stats /opt/airflow/data/stats.json
    ```

### 2. Run via Airflow DAG

- Access the **Airflow UI** at: `http://localhost:8080`
  - **Username**: `admin`
  - **Password**: `admin`
- In the **Airflow UI**:
  - Find the DAG named **taxi_clean_pipeline**.
  - Toggle the DAG to "On".
  - Trigger the DAG manually or wait for the next scheduled run.

### What the DAG Does

The **taxi_clean_pipeline** DAG performs the following key steps:

1. **run_spark_clean**:
   - Submits a Spark job to clean the taxi data.
   
2. **run_data_quality_checks**:
   - Runs checks on the data quality (e.g., record counts, nulls).
   
3. **push_stats_to_xcom**:
   - Pushes key metrics (e.g., records processed, records rejected) to XCom.

### Triggering and Re-runs

- The DAG can be triggered manually from the Airflow UI.
- The DAG will re-run safely without reprocessing already processed data by tracking the last processed month in a checkpoint file.

## Docker Compose Configuration

Ensure the following services are defined in your `docker-compose.yml` file:

- **Airflow Webserver**: Access Airflow UI for DAG management.
- **Airflow Scheduler**: Schedules the DAGs.
- **Airflow Worker**: Executes the tasks in the DAG.

## Logging and Debugging

- Ensure that your Spark job includes meaningful logs, such as:
  - The months being processed.
  - Record counts before and after filtering.
  - Results of the data quality checks.
- Logs can be accessed from the Airflow task logs for troubleshooting.
  
## Screenshots

Below are the screenshots for Airflow DAG execution:

<img width="1848" height="737" alt="Screenshot 2026-01-05 160118" src="https://github.com/user-attachments/assets/3173bdb1-9b9c-493c-b296-40d39281ca58" />
<img width="1325" height="425" alt="Screenshot 2026-01-05 160132" src="https://github.com/user-attachments/assets/ea37d7b8-a49b-4517-95fe-37b00bbf9754" />
