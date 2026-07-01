# Incremental Orders Pipeline with Delta Lake MERGE

## Project Overview

This project implements an incremental data engineering pipeline using **Databricks Free Edition**, **Apache Spark**, **PySpark**, **Spark SQL**, and **Delta Lake**.

The pipeline simulates an e-commerce order processing system where order events arrive in multiple batches. Some events represent new orders, while others represent updates to existing orders.

The main goal of this project is to demonstrate how to build an incremental pipeline using **Delta Lake `MERGE INTO`** to maintain a reliable current-state table without duplicating records.

This project focuses on key data engineering concepts such as:

* Incremental loading
* Delta Lake `MERGE INTO`
* Upserts
* Deduplication
* Current-state tables
* Bronze, Silver, and Gold architecture
* Business KPI generation
* Delta table history
* Idempotent pipeline behavior

---

## Business Problem

An e-commerce company receives order events from its operational system.

An order can change status over time:

```text
pending → shipped → delivered
```

or:

```text
pending → cancelled
```

The challenge is that the source system sends multiple events for the same `order_id`.

For example:

| order_id | order_status | event_ts            |
| -------- | ------------ | ------------------- |
| 1001     | pending      | 2026-06-01 10:00:00 |
| 1001     | shipped      | 2026-06-01 12:00:00 |
| 1001     | delivered    | 2026-06-02 09:00:00 |

The business needs two different views of the data:

1. A full historical event log.
2. A current-state table with only the latest status of each order.

This project solves that problem using a Medallion-style architecture with Delta Lake.

---

## Architecture

```text
Synthetic Order Batches
        |
        v
Bronze Layer
Raw order events stored with append logic
        |
        v
Silver Layer
Current-state orders table using MERGE INTO
        |
        v
Gold Layer
Business metrics and executive summaries
```

---

## Pipeline Flow

```text
source_orders_batch_1
source_orders_batch_2
source_orders_batch_3
        |
        v
orders_project.bronze_orders_raw
        |
        v
orders_project.silver_orders_current
        |
        v
orders_project.gold_orders_by_status
orders_project.gold_daily_sales
orders_project.gold_order_summary
```

---

## Technologies Used

* Databricks Free Edition
* Apache Spark
* PySpark
* Spark SQL
* Delta Lake
* Delta Lake `MERGE INTO`
* GitHub
* Databricks Git Folders

---

## Repository Structure

```text
databricks-incremental-orders-pipeline/
│
├── README.md
│
├── notebooks/
│   ├── 01_create_sample_batches.ipynb
│   ├── 02_bronze_ingestion.ipynb
│   ├── 03_silver_merge_upsert.ipynb
│   └── 04_gold_business_metrics.ipynb
│
├── images/
│   ├── show_tables.png
│   ├── table_counts.png
│   ├── bronze_events.png
│   ├── silver_current_orders.png
│   ├── gold_orders_by_status.png
│   ├── gold_daily_sales.png
│   ├── gold_order_summary.png
│   └── silver_delta_history.png
│
└── docs/
    └── architecture.md
```

---

## Data Model

The simulated order events contain the following fields:

| Column          | Description                                            |
| --------------- | ------------------------------------------------------ |
| `batch_id`      | Identifier of the incoming batch                       |
| `order_id`      | Unique order identifier                                |
| `customer_id`   | Customer identifier                                    |
| `order_status`  | Current status reported by the event                   |
| `order_amount`  | Order amount in USD                                    |
| `event_ts`      | Timestamp when the event happened in the source system |
| `source_system` | Name of the source system                              |

---

## Sample Batches

The project simulates three batches of order events.

### Batch 1

Initial new orders:

| order_id | status  |
| -------- | ------- |
| 1001     | pending |
| 1002     | pending |
| 1003     | pending |

---

### Batch 2

New orders and updates:

| order_id | status    |
| -------- | --------- |
| 1001     | shipped   |
| 1002     | cancelled |
| 1004     | pending   |

---

### Batch 3

More updates and a new order:

| order_id | status    |
| -------- | --------- |
| 1001     | delivered |
| 1003     | shipped   |
| 1004     | delivered |
| 1005     | pending   |

---

## Expected Final Current State

After processing all three batches, the Silver table should keep only the latest status per order:

| order_id | final_status |
| -------- | ------------ |
| 1001     | delivered    |
| 1002     | cancelled    |
| 1003     | shipped      |
| 1004     | delivered    |
| 1005     | pending      |

Bronze stores **10 total events**.

Silver stores **5 unique current orders**.

---

# Medallion Architecture

This project follows a Medallion-style architecture:

```text
Bronze → Silver → Gold
```

Each layer has a specific responsibility.

---

## Bronze Layer — Raw Event History

### Table

```text
orders_project.bronze_orders_raw
```

### Purpose

The Bronze layer stores all incoming order events.

This layer is append-only and keeps the full event history. It does not decide which event is the latest or which status is correct.

### Main Responsibilities

* Read source batch tables.
* Add ingestion metadata.
* Append raw events to the Bronze Delta table.
* Preserve full historical event data.
* Avoid re-ingesting the same batch twice.

### Key Columns

| Column                | Description                                     |
| --------------------- | ----------------------------------------------- |
| `batch_id`            | Batch number                                    |
| `order_id`            | Order identifier                                |
| `customer_id`         | Customer identifier                             |
| `order_status`        | Status from the source event                    |
| `order_amount`        | Order amount                                    |
| `event_ts`            | Source event timestamp                          |
| `source_system`       | Source system name                              |
| `bronze_ingestion_ts` | Timestamp when the event was loaded into Bronze |
| `source_batch_table`  | Source table used for ingestion                 |

### Write Pattern

Bronze uses append logic:

```python
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("orders_project.bronze_orders_raw")
```

### Why Append?

Bronze should preserve every incoming event.

For example, if order `1001` changes from `pending` to `shipped` to `delivered`, Bronze keeps all three events.

This is important for:

* Auditing
* Debugging
* Historical analysis
* Reprocessing
* Data lineage

---

## Silver Layer — Current-State Orders

### Table

```text
orders_project.silver_orders_current
```

### Purpose

The Silver layer stores the latest known state of each order.

Unlike Bronze, Silver does not keep all historical events. It keeps only one row per `order_id`, representing the most recent event based on `event_ts`.

### Main Responsibilities

* Read incremental batch events from Bronze.
* Deduplicate records within each batch.
* Keep the latest event per order.
* Use Delta Lake `MERGE INTO` to update or insert records.
* Maintain a reliable current-state table.

---

## Delta Lake MERGE INTO

The main concept in this project is `MERGE INTO`.

`MERGE INTO` allows the pipeline to perform an upsert:

```text
UPSERT = UPDATE + INSERT
```

The logic is:

```text
If order_id already exists in Silver:
    update the record only if the incoming event is newer.

If order_id does not exist in Silver:
    insert the new order.
```

### MERGE Logic

```sql
MERGE INTO orders_project.silver_orders_current AS target
USING silver_updates_temp AS source
ON target.order_id = source.order_id

WHEN MATCHED AND source.last_event_ts > target.last_event_ts THEN
  UPDATE SET
    target.customer_id = source.customer_id,
    target.order_status = source.order_status,
    target.order_amount = source.order_amount,
    target.last_event_ts = source.last_event_ts,
    target.last_batch_id = source.last_batch_id,
    target.source_system = source.source_system,
    target.source_batch_table = source.source_batch_table,
    target.bronze_ingestion_ts = source.bronze_ingestion_ts,
    target.silver_processed_ts = source.silver_processed_ts

WHEN NOT MATCHED THEN
  INSERT (
    order_id,
    customer_id,
    order_status,
    order_amount,
    last_event_ts,
    last_batch_id,
    source_system,
    source_batch_table,
    bronze_ingestion_ts,
    silver_processed_ts
  )
  VALUES (
    source.order_id,
    source.customer_id,
    source.order_status,
    source.order_amount,
    source.last_event_ts,
    source.last_batch_id,
    source.source_system,
    source.source_batch_table,
    source.bronze_ingestion_ts,
    source.silver_processed_ts
  )
```

---

## Why Use MERGE Instead of Overwrite?

Using `overwrite` would rebuild the entire Silver table every time.

Using `MERGE INTO` allows the pipeline to process only the new incoming batch and update the current-state table incrementally.

This is closer to how real-world data pipelines work when handling operational data.

---

## Deduplication Logic

Before merging into Silver, each batch is deduplicated by `order_id`.

If multiple events for the same order arrive in the same batch, the pipeline keeps only the latest event using `event_ts`.

```python
dedup_window = Window.partitionBy("order_id").orderBy(col("event_ts").desc())

silver_updates_df = (
    bronze_batch_df
    .withColumn("row_num", row_number().over(dedup_window))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

This prevents multiple updates for the same `order_id` from conflicting inside the same batch.

---

## Gold Layer — Business Metrics

The Gold layer creates analytics-ready tables from the Silver current-state table.

Gold answers business questions such as:

* How many current orders exist by status?
* How much revenue has been recognized?
* What is the cancellation rate?
* What is the average order value?
* How much order amount is still active?
* How many orders are pending, shipped, delivered, or cancelled?

---

## Gold Table 1 — Orders by Status

### Table

```text
orders_project.gold_orders_by_status
```

### Purpose

Aggregates current orders by their latest status.

### Metrics

| Metric                 | Description                              |
| ---------------------- | ---------------------------------------- |
| `order_status`         | Current order status                     |
| `total_orders`         | Number of orders in that status          |
| `total_amount_usd`     | Total amount for orders in that status   |
| `avg_order_amount_usd` | Average amount for orders in that status |
| `first_event_ts`       | Earliest event timestamp in that status  |
| `latest_event_ts`      | Latest event timestamp in that status    |
| `gold_processed_ts`    | Gold processing timestamp                |

---

## Gold Table 2 — Daily Sales

### Table

```text
orders_project.gold_daily_sales
```

### Purpose

Aggregates current orders by the date of their latest event.

### Metrics

| Metric                           | Description                                 |
| -------------------------------- | ------------------------------------------- |
| `order_activity_date`            | Date of the latest order event              |
| `total_current_orders`           | Total current orders for that activity date |
| `gross_current_order_amount_usd` | Total amount of current orders              |
| `recognized_revenue_usd`         | Revenue from delivered orders               |
| `cancelled_amount_usd`           | Amount associated with cancelled orders     |
| `pending_orders`                 | Count of pending orders                     |
| `shipped_orders`                 | Count of shipped orders                     |
| `delivered_orders`               | Count of delivered orders                   |
| `cancelled_orders`               | Count of cancelled orders                   |

---

## Gold Table 3 — Executive Order Summary

### Table

```text
orders_project.gold_order_summary
```

### Purpose

Provides a one-row executive summary of the current order state.

### Metrics

| Metric                           | Description                       |
| -------------------------------- | --------------------------------- |
| `total_current_orders`           | Total unique current orders       |
| `total_customers`                | Total unique customers            |
| `gross_current_order_amount_usd` | Gross value of all current orders |
| `recognized_revenue_usd`         | Revenue from delivered orders     |
| `active_order_amount_usd`        | Amount from non-cancelled orders  |
| `cancelled_amount_usd`           | Amount from cancelled orders      |
| `pending_orders`                 | Number of pending orders          |
| `shipped_orders`                 | Number of shipped orders          |
| `delivered_orders`               | Number of delivered orders        |
| `cancelled_orders`               | Number of cancelled orders        |
| `avg_order_value_usd`            | Average order value               |
| `delivered_rate_pct`             | Percentage of orders delivered    |
| `cancellation_rate_pct`          | Percentage of orders cancelled    |
| `gold_processed_ts`              | Gold processing timestamp         |

---

## Final Tables

| Layer  | Table                                  | Purpose                      |
| ------ | -------------------------------------- | ---------------------------- |
| Source | `orders_project.source_orders_batch_1` | First sample batch           |
| Source | `orders_project.source_orders_batch_2` | Second sample batch          |
| Source | `orders_project.source_orders_batch_3` | Third sample batch           |
| Bronze | `orders_project.bronze_orders_raw`     | Full historical order events |
| Silver | `orders_project.silver_orders_current` | Current state of each order  |
| Gold   | `orders_project.gold_orders_by_status` | Orders grouped by status     |
| Gold   | `orders_project.gold_daily_sales`      | Daily sales metrics          |
| Gold   | `orders_project.gold_order_summary`    | Executive summary            |

---

## Validation

The pipeline was validated using table existence checks and row count checks.

### Show Tables

```sql
SHOW TABLES IN orders_project;
```

### Row Count Validation

```sql
SELECT 'bronze_orders_raw' AS table_name, COUNT(*) AS total_records
FROM orders_project.bronze_orders_raw

UNION ALL

SELECT 'silver_orders_current' AS table_name, COUNT(*) AS total_records
FROM orders_project.silver_orders_current

UNION ALL

SELECT 'gold_orders_by_status' AS table_name, COUNT(*) AS total_records
FROM orders_project.gold_orders_by_status

UNION ALL

SELECT 'gold_daily_sales' AS table_name, COUNT(*) AS total_records
FROM orders_project.gold_daily_sales

UNION ALL

SELECT 'gold_order_summary' AS table_name, COUNT(*) AS total_records
FROM orders_project.gold_order_summary;
```

### Expected Counts

| Table                   | Expected Records |
| ----------------------- | ---------------: |
| `bronze_orders_raw`     |               10 |
| `silver_orders_current` |                5 |
| `gold_orders_by_status` |                4 |
| `gold_daily_sales`      |                2 |
| `gold_order_summary`    |                1 |

---

## Screenshots

### Project Tables

![Show Tables](images/show_tables.png)

### Table Counts

![Table Counts](images/table_counts.png)

### Bronze Events

![Bronze Events](images/bronze_events.png)

### Silver Current Orders

![Silver Current Orders](images/silver_current_orders.png)

### Gold Orders by Status

![Gold Orders by Status](images/gold_orders_by_status.png)

### Gold Daily Sales

![Gold Daily Sales](images/gold_daily_sales.png)

### Gold Order Summary

![Gold Order Summary](images/gold_order_summary.png)

### Silver Delta History

![Silver Delta History](images/silver_delta_history.png)

---

## Notebook Execution Order

Run the notebooks in this order:

```text
1. notebooks/01_create_sample_batches.ipynb
2. notebooks/02_bronze_ingestion.ipynb
3. notebooks/03_silver_merge_upsert.ipynb
4. notebooks/04_gold_business_metrics.ipynb
```

Important execution notes:

* Run `02_bronze_ingestion` once per batch: batch 1, batch 2, and batch 3.
* Run `03_silver_merge_upsert` once per batch: batch 1, batch 2, and batch 3.
* Run `04_gold_business_metrics` after Silver has processed all batches.

---

## Key Technical Decisions

### 1. Why Keep Bronze Append-Only?

Bronze stores all incoming events exactly as they arrive.

This allows the pipeline to preserve history, audit changes, and reprocess downstream layers if needed.

---

### 2. Why Use MERGE in Silver?

Silver represents the latest state of each order.

`MERGE INTO` allows the pipeline to update existing orders and insert new orders without rebuilding the full table.

---

### 3. Why Compare Event Timestamps?

The condition:

```sql
source.last_event_ts > target.last_event_ts
```

prevents older events from overwriting newer order states.

This protects the Silver table from late-arriving or out-of-order records.

---

### 4. Why Deduplicate Before MERGE?

If the same order appears multiple times in one batch, the pipeline must choose the latest event.

Deduplication ensures that only one update per `order_id` is sent to the MERGE operation.

---

### 5. Why Build Gold from Silver?

Silver contains the clean current state of each order.

Gold uses that reliable state to create business metrics for reporting and analysis.

---

## Concepts Demonstrated

This project demonstrates the following data engineering concepts:

* Incremental ingestion
* Delta Lake `MERGE INTO`
* Upserts
* Deduplication
* Current-state modeling
* Bronze, Silver, and Gold layers
* Delta Lake table history
* Spark DataFrames
* Spark SQL
* Batch processing
* Idempotency checks
* Data lineage
* Business KPI modeling
* Databricks Git Folder workflow

---

## Interview Explanation

A concise explanation of this project:

> I built an incremental order processing pipeline in Databricks using Delta Lake. The Bronze layer stores all raw order events using append logic. The Silver layer maintains the latest state of each order using Delta Lake MERGE INTO, which performs upserts by updating existing orders when a newer event arrives and inserting new orders when they do not exist. The Gold layer creates business metrics such as orders by status, daily sales, recognized revenue, cancellation rate, and executive order summaries. This project demonstrates incremental processing, deduplication, current-state modeling, and Delta Lake table history.

---

## Future Improvements

Possible improvements for a production-ready version:

* Add Databricks Workflows to orchestrate notebook execution.
* Add automated data quality checks.
* Add retry logic and error handling.
* Add support for late-arriving events.
* Add a control table to track processed batches.
* Add historical trend analysis from Bronze.
* Add Slowly Changing Dimension Type 2 logic.
* Add dashboard visualizations using Databricks SQL.
* Add unit tests for transformation logic.
* Add schema evolution handling.
* Add alerts for high cancellation rates or revenue drops.

---

## Author

**Brayan Pérez Balladares**

Data Engineering Student | Databricks Learner | Building practical data engineering projects with Spark, Delta Lake, and cloud-based data platforms.

GitHub: [BrayanperezBalladares](https://github.com/BrayanperezBalladares)

---

## Project Status

Completed initial version.

```text
Sample batches: Completed
Bronze ingestion: Completed
Silver MERGE upsert: Completed
Gold business metrics: Completed
GitHub versioning: Completed
Documentation: In progress
```


