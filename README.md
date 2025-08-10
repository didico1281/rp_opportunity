# Account Daily Interest Calculation Pipeline

This project processes account transaction data using **PySpark** and **Delta Lake** to calculate **daily balances**, **interest rates**, and produce partitioned datasets for different processing layers (**Bronze, Silver, Gold**).

## 📌 Overview

The pipeline:
1. **Reads** raw transactions (Bronze layer) from Parquet files.
2. **Aggregates** transactions per account per day (Silver layer).
3. **Calculates**:
   - Daily balances
   - Interest on balances above 100
   - Inactive account interest
4. **Supports dynamic interest rates** via a control table.
5. **Writes** the final dataset to Delta Lake (Gold layer) partitioned by `year_month`.

## 📂 Project Structure

```
rp_project.py       # Main PySpark pipeline script
```

## 🔄 Processing Steps

### 1. Read Transactions (Bronze Layer)
```python
df_transactions = spark.read.format('parquet')     .option('header', 'true')     .option('inferSchema', 'true')     .load("/Volumes/recargapay/recargapay/vol_rp/bronze/*.parquet")
```

### 2. Aggregate Daily Amounts
- Groups by `account_id`, `user_id`, and `date`.
- Calculates total `daily_amount`.

### 3. Fill Missing Dates
- Generates full date ranges for each account.
- Joins with actual transactions, filling missing days with `0`.

### 4. Calculate Balances & Interests
- `daily_balance`: cumulative sum of `daily_amount`.
- `interest_above_100`: applies when `daily_balance > 100`.
- `inactive_interest`: applies when no transactions for the day.

### 5. Dynamic Interest Rates
- Uses a **control table** with `start_validity` and `end_validity` to apply different rates over time.

### 6. Save Output
- **Silver Layer**: `account_daily_amount`, `account_daily_interest_amount` (partitioned by `date`).
- **Gold Layer**: `account_daily_summary` (partitioned by `year_month`).

## 📊 Example Output
| account_id | user_id | date       | daily_amount | daily_balance | prev_balance | interest_above_100 | inactive_interest | total_with_interest |
|------------|---------|------------|--------------|---------------|--------------|--------------------|-------------------|---------------------|
| acc1       | usr1    | 2024-01-01 | 150.0        | 150.0         | null         | 1.50               | 0.00              | 151.50              |
| acc1       | usr1    | 2024-01-02 | 0.0          | 150.0         | 150.0        | 1.50               | 0.75              | 152.25              |

## ⚙️ Requirements
- Python 3.8+
- PySpark
- Delta Lake
- Databricks (optional, but configured for Databricks paths)

Install dependencies:
```bash
pip install pyspark delta-spark
```

## ▶️ Running
```bash
spark-submit rp_project.py
```

Or inside Databricks:
- Upload the script as a notebook.
- Configure cluster with Delta Lake.
- Execute cells sequentially.

## 📌 Notes
- Paths are set for Databricks Volumes: `/Volumes/recargapay/recargapay/...`
- Modify them to match your environment if running locally.
- Interest rates can be easily adjusted in the control table section.

---
**Author:** Diogenes Lopes  
