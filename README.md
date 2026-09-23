# ✈️ Travel Booking SCD2 Merge Project

An end-to-end **Databricks data engineering project** implementing incremental data ingestion, native PySpark data quality validation, **SCD Type 2 customer dimension**, booking fact processing, Delta Lake optimization, and analytics using SQL.

## 🛠️ Tech Stack

**Databricks | PySpark | Delta Lake | Unity Catalog | Databricks Volumes | Databricks Workflows | SQL**

---

## 🏗️ Architecture

```text
Databricks Volumes
       │
       ▼
Bronze Ingestion
       │
       ▼
Data Quality Checks
       │
       ├──────────────┐
       ▼              ▼
Customer SCD2     Booking Fact
       │              │
       └──────┬───────┘
              ▼
       Analytics Layer
              │
              ▼
      Databricks Workflows
```

### Medallion Architecture

* **Bronze:** Raw booking and customer data with audit metadata
* **Silver:** SCD2 customer dimension and aggregated booking fact
* **Analytics:** Revenue analytics, Customer 360, and DQ reporting

---

## 📂 Project Structure

```text
Travel_Booking_SCD2_Merge_Project/
│
├── notebooks/
│   ├── validate_inputs.ipynb
│   ├── 10_ingest_bookings_bronze.ipynb
│   ├── 11_ingest_customers_bronze.ipynb
│   ├── 20_dq_bookings.ipynb
│   ├── 21_dq_customers.ipynb
│   ├── 30_customer_dim_scd2.ipynb
│   ├── 31_booking_fact_build.ipynb
│   ├── 40_optimize_zorder.ipynb
│   └── 41_analyze_stats.ipynb
│
├── queries/
│   ├── travel_booking_init.sql
│   ├── customer360.sql
│   ├── daily_revenue.sql
│   ├── data_quality_summary.sql
│   └── log_completion_flow.sql
│
├── booking_data/
├── customer_data/
└── README.md
```

---

## 📥 Data Sources

### Booking Data

Daily booking files:

```text
bookings_YYYY-MM-DD.csv
```

Schema:

```text
booking_id
customer_id
booking_type
amount
discount
quantity
booking_date
```

### Customer Data

Daily customer master files:

```text
customers_YYYY-MM-DD.csv
```

Schema:

```text
customer_id
customer_name
customer_address
email
```

---

# 🔄 Pipeline Components

## 1. Input Validation

`validate_inputs.ipynb`

* Validates `arrival_date`
* Validates input file existence
* Initializes audit/run logging
* Prevents invalid pipeline execution

---

## 2. Bronze Ingestion

### Booking Ingestion

`10_ingest_bookings_bronze.ipynb`

* Reads daily booking CSV files from Databricks Volumes
* Adds ingestion metadata
* Adds business/arrival date
* Stores data as Delta
* Supports parameterized processing

### Customer Ingestion

`11_ingest_customers_bronze.ipynb`

* Reads daily customer files
* Adds audit metadata
* Stores customer data in Delta
* Prepares data for SCD2 processing

---

## 3. Data Quality

Native **PySpark data quality checks** are applied before downstream processing.

### Booking Checks

* Dataset is not empty
* `customer_id` is not null
* `amount` is not null
* `amount >= 0`
* `quantity >= 0`
* `discount >= 0`

### Customer Checks

* Dataset is not empty
* `customer_name` is not null
* `customer_address` is not null
* `email` is not null

DQ results are stored in:

```text
travel_bookings.ops.dq_results
```

Daily summaries are maintained in:

```text
travel_bookings.ops.dq_daily_summary
```

Example:

```text
business_date | dataset  | checks_passed | checks_failed
---------------------------------------------------------
2026-09-23    | customer | 4             | 0
2026-09-23    | booking  | 6             | 0
```

---

# 👤 SCD Type 2 Customer Dimension

`30_customer_dim_scd2.ipynb`

The customer dimension maintains historical versions of customer attributes.

### Key Columns

```text
customer_sk
customer_id
customer_name
customer_address
email
valid_from
valid_to
is_current
```

### SCD2 Flow

```text
Incoming Customer
       │
       ▼
Compare with Current Record
       │
   ┌───┴────┐
   │        │
 No Change  Changed
   │        │
   ▼        ▼
No Action  Close Old Version
              │
              ▼
         Insert New Version
```

Changes to tracked attributes such as name, address, or email create a new dimension version.

Delta Lake `MERGE` is used for the SCD2 processing.

---

# 📊 Booking Fact

`31_booking_fact_build.ipynb`

The booking fact table is maintained at a **daily grain**.

### Key Columns

```text
booking_type
customer_sk
business_date
total_amount_sum
total_quantity_sum
```

Processing includes:

* Customer surrogate-key enrichment
* Daily aggregation
* Amount aggregation
* Quantity aggregation
* Idempotent Delta MERGE

---

# ⚡ Performance Optimization

The project uses Delta Lake optimization techniques:

### OPTIMIZE + Z-ORDER

```sql
OPTIMIZE travel_bookings.default.booking_fact
ZORDER BY (business_date, customer_sk);
```

### VACUUM

Used to remove obsolete Delta files according to the configured retention policy.

### ANALYZE

```sql
ANALYZE TABLE travel_bookings.default.booking_fact
COMPUTE STATISTICS;
```

These techniques support efficient querying and table maintenance.

---

# 📈 Analytics

## Daily Revenue

Calculates revenue by:

* Business date
* Booking type

```text
business_date | booking_type | revenue
---------------------------------------
2026-09-23    | Flight       | ...
2026-09-23    | Hotel        | ...
```

## Customer 360

View:

```text
travel_bookings.analytics.customer_360
```

Provides:

* Current customer information
* Booking type
* Lifetime booking amount
* Lifetime booking quantity

## DQ Summary

View/table:

```text
travel_bookings.ops.dq_daily_summary
```

Provides daily passed/failed DQ metrics by dataset.

---

# 🔄 Databricks Workflow

The pipeline is orchestrated using **Databricks Workflows**.

```text
Validate Inputs
      │
      ▼
Booking Bronze
      │
      ▼
Customer Bronze
      │
      ▼
Booking DQ
      │
      ▼
Customer DQ
      │
      ▼
Customer SCD2
      │
      ▼
Booking Fact
      │
      ▼
Optimization
      │
      ▼
Analytics SQL
```

The workflow uses the parameter:

```text
arrival_date
```

This allows the same pipeline to process different business dates and supports controlled reruns.

---

# 🗄️ Data Model

## Customer Dimension

```sql
customer_sk       BIGINT
customer_id       INT
customer_name     STRING
customer_address  STRING
email             STRING
valid_from        DATE
valid_to          DATE
is_current        BOOLEAN
```

## Booking Fact

```sql
booking_type        STRING
customer_sk         BIGINT
business_date       DATE
total_amount_sum    DOUBLE
total_quantity_sum  BIGINT
```

---

# 🧪 Operational Tables

### DQ Results

```text
travel_bookings.ops.dq_results
```

Stores individual validation results:

```text
business_date
dataset
check_name
status
constraint
message
recorded_at
```

### DQ Daily Summary

```text
travel_bookings.ops.dq_daily_summary
```

Stores aggregated DQ metrics:

```text
business_date
dataset
checks_passed
checks_failed
recorded_at
```

---

# 🚀 Key Features

* Parameterized daily ingestion
* Databricks Volumes
* Native PySpark data quality framework
* SCD Type 2 implementation
* Surrogate key management
* Delta Lake MERGE
* Idempotent processing
* Daily fact aggregation
* OPTIMIZE and Z-ORDER
* VACUUM and table statistics
* Customer 360 analytics
* Revenue analytics
* DQ monitoring
* Databricks Workflows
* Unity Catalog

---

# ▶️ Pipeline Execution

Configure the pipeline parameter:

```python
dbutils.widgets.text("arrival_date", "2026-09-23")
```

Then execute the notebooks in sequence:

```text
1. validate_inputs
2. 10_ingest_bookings_bronze
3. 11_ingest_customers_bronze
4. 20_dq_bookings
5. 21_dq_customers
6. 30_customer_dim_scd2
7. 31_booking_fact_build
8. 40_optimize_zorder
9. 41_analyze_stats
```

---

# 🎯 Project Outcome

This project demonstrates an end-to-end **Databricks data warehouse pipeline** covering:

**Ingestion → Data Quality → SCD2 → Fact Processing → Optimization → Analytics → Workflow Orchestration**

It simulates a production-style travel booking data platform using **PySpark, Delta Lake, Unity Catalog, and Databricks Workflows**.
