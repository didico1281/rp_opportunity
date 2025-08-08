Data Product Implementation for Daily Balance and Interest Calculation

1. Overview

This data product calculates daily balances for user accounts, applies interest rules based on account activity and balance thresholds, and tracks interest policies with valid date ranges. The implementation is performed using Apache Spark with PySpark and is designed for scalability, clarity, and adaptability to changing business rules.

2. Installation & Execution Instructions

Dependencies

Apache Spark (>= 3.0)

Python (>= 3.8)

PySpark

Pandas (for local testing or CSV preview)

Installation

# Install PySpark
pip install pyspark

# (Optional) Install Pandas for testing
pip install pandas

Running the Script

Place your transaction CSV file in an accessible directory.

Modify the path in the script where the CSV is loaded:

df = spark.read.option("header", "true").option("inferSchema", "true").csv("/path/to/transactions.csv")

Execute the script using your preferred IDE or CLI:

spark-submit process_transactions.py

Testing

Run unit tests on interest calculation using mock data.

Validate edge cases like missing days, zero balances, and changing rates.

Compare output with expected daily balances and interest.

3. Recommendations for Ingesting into a Production Database

Target System

A PostgreSQL or MySQL transactional database.

Alternatively, ingestion into a Delta Lake table for lakehouse architecture.

Suggested Schema for DB Table: account_daily_summary

CREATE TABLE account_daily_summary (
    account_id UUID,
    user_id UUID,
    date DATE,
    daily_amount NUMERIC,
    daily_balance NUMERIC,
    prev_balance NUMERIC,
    interest_above_100 NUMERIC,
    inactive_interest NUMERIC,
    total_with_interest NUMERIC,
    PRIMARY KEY (account_id, date)
);

Ingestion Strategy

Write PySpark output as a CSV or Parquet file.

Use batch job (e.g., Apache Airflow or DBT) to upsert data daily.

Use COPY or INSERT ... ON CONFLICT for loading into PostgreSQL.

4. Design Decisions

Functional Requirements

Aggregate transactions daily per user and account.

Apply different interest rates depending on daily balance and activity.

Track interest policy changes with validity dates.

Non-Functional Requirements

Scalability: Spark handles large volumes of financial transactions.

Maintainability: Parameters (e.g., rates and date ranges) are separated into a control table.

Flexibility: Easy to update interest policies without code changes.

Traceability: Daily balance, interest, and totals are fully traceable.

Design Justification

Using a parameter table for interest rates ensures that business rule changes do not require code deployment.

Creating date ranges per account ensures that missing days are filled and balances are continuous.

Separation of logic (aggregation, interest rules, enrichment) increases clarity.

5. Trade-offs & Constraints

Trade-offs Made:

Real-time vs. Batch: The solution uses batch processing for simplicity and reliability. Real-time streaming was not implemented due to time constraints.

Minimal Exception Handling: Initial version focuses on correct logic, leaving advanced error handling for future versions.

Persistence Layer: The solution writes to files; integration with transactional DBs is left to pipeline developers.

Constraints

Development assumed static CSV input, not a live transactional stream.

Schema inference and validation are basic; a production-grade schema registry is recommended for robustness.

6. Next Steps

Add integration tests with database output.

Implement pipeline orchestration (Airflow, DBT, or similar).

Add support for streaming ingestion (Kafka or AWS Kinesis).

Implement versioning and rollback for interest policy rules.

Author: Data Engineering TeamDate: August 2025