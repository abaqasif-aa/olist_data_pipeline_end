# Olist Data Pipeline

A production-style end-to-end data engineering pipeline built with **Apache Spark**, **dbt**, and **Apache Airflow** on the Brazilian Olist e-commerce dataset (100k real orders across 9 tables).

---

## Architecture

```
Raw Data (Kaggle CSVs)
        │
        ▼
┌───────────────────┐
│   Bronze Layer    │  PySpark — read CSVs, cast types, write Parquet
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   Silver Layer    │  PySpark — join 7 tables, enrich, load to PostgreSQL
└───────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│              Gold Layer (dbt)             │
│  staging → intermediate → mart models    │
└───────────────────────────────────────────┘
        │
        ▼
┌───────────────────┐
│  Apache Airflow   │  Orchestrate daily pipeline end-to-end
└───────────────────┘
```

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Apache Spark (PySpark) | 3.5.0 | Data ingestion and transformation |
| dbt-core | 1.11 | SQL modelling, testing, documentation |
| dbt-postgres | 1.10 | dbt adapter for PostgreSQL |
| Apache Airflow | 2.10 | Pipeline orchestration and scheduling |
| PostgreSQL | 15 | Data warehouse |
| Python | 3.12 | Scripting and job management |
| WSL2 (Ubuntu) | 24.04 | Local development environment on Windows |

---

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 9 CSV files covering 100k orders from 2017–2018.

| File | Rows | Description |
|---|---|---|
| olist_orders_dataset.csv | 99,441 | Order status, timestamps |
| olist_order_items_dataset.csv | 112,650 | Line items, prices, freight |
| olist_customers_dataset.csv | 99,441 | Customer location |
| olist_products_dataset.csv | 32,951 | Product categories, dimensions |
| olist_sellers_dataset.csv | 3,095 | Seller location |
| olist_order_payments_dataset.csv | 103,886 | Payment type, value |
| olist_order_reviews_dataset.csv | 99,224 | Review scores, comments |
| olist_geolocation_dataset.csv | 1,000,163 | Zip code coordinates |
| product_category_name_translation.csv | 71 | Portuguese → English categories |

---

## Project Structure

```
olist-data-pipeline/
├── jobs/
│   ├── bronze_ingest.py          # Spark: CSVs → Parquet (bronze layer)
│   ├── silver_transform.py       # Spark: join + enrich → PostgreSQL (silver layer)
│   └── load_to_postgres.py       # Spark: load existing Parquet → PostgreSQL
├── olist_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders_enriched.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   └── int_orders_enriched.sql
│   │   └── marts/
│   │       ├── agg_daily_revenue.sql
│   │       ├── agg_seller_performance.sql
│   │       └── dim_customers.sql
│   ├── macros/
│   │   └── date_utils.sql        # format_date, extract_year, extract_month
│   ├── tests/
│   │   ├── assert_delivered_orders_have_payment.sql
│   │   ├── assert_delivery_delay_in_range.sql
│   │   └── assert_customers_have_spend.sql
│   └── dbt_project.yml
├── airflow/
│   └── dags/
│       └── olist_pipeline.py     # Daily DAG: bronze → silver → dbt run → dbt test
├── activate_project.ps1          # Windows environment setup script
├── .gitignore
└── README.md
```

---

## Pipeline Stages

### Bronze layer — `jobs/bronze_ingest.py`
- Reads all 9 Olist CSVs using PySpark with explicit schemas
- Deduplicates records
- Writes Parquet files to `data_lake/bronze/` partitioned by date
- No business logic — raw copy only

### Silver layer — `jobs/silver_transform.py`
- Reads bronze Parquet files
- Joins orders, customers, products, payments, sellers, and category translation
- Casts timestamp columns
- Adds derived columns:
  - `delivery_delay_days` — actual vs estimated delivery
  - `is_late` — boolean flag for late deliveries
  - `total_item_value` — price + freight
- Writes enriched table to `data_lake/silver/orders_enriched/` (Parquet)
- Loads silver table to PostgreSQL via JDBC

### Gold layer — dbt models

**Staging** (`view`)
- `stg_orders_enriched` — renames timestamp columns, filters cancelled orders

**Intermediate** (`incremental`)
- `int_orders_enriched` — adds `order_year`, `order_month` via macros, `revenue_per_item`
- Uses **45-day maturity window** with `delete+insert` strategy
- Indexes on `order_id` and `purchased_at` via post-hooks

**Marts** (`table`)
- `agg_daily_revenue` — daily revenue by product category
- `agg_seller_performance` — revenue, delay, late order % per seller
- `dim_customers` — total spend, order count, first/last order date per customer

---

## dbt Features

### Macros (`macros/date_utils.sql`)
Reusable date functions that work with both timestamp and date inputs:
```sql
{{ format_date('purchased_at') }}      -- DD/MM/YYYY
{{ extract_year('purchased_at') }}     -- 2017
{{ extract_month('purchased_at') }}    -- 10
```

### Incremental strategy
The intermediate model uses a 45-day lookback window to handle late-arriving and updated orders:
```sql
{% if is_incremental() %}
    where purchased_at > (
        select max(purchased_at) - interval '45 days' from {{ this }}
    )
{% endif %}
```

### Data quality tests
Generic tests via `schema.yml`:
- `not_null` on `order_id`, `customer_id`
- `unique` on `order_id`
- `accepted_values` on `order_status`

Singular tests catching real data issues:
- `assert_delivered_orders_have_payment` — no delivered order should have zero payment
- `assert_delivery_delay_in_range` — delay must be between -30 and 365 days
- `assert_customers_have_spend` — every customer must have a positive total spend

### Dev/prod environments
Separate schemas per environment in `~/.dbt/profiles.yml`:
```yaml
dev:
  schema: dev_abaq    # personal dev schema
prod:
  schema: public      # production schema
```

---

## Airflow DAG

Daily pipeline orchestrated in `airflow/dags/olist_pipeline.py`:

```
spark_bronze → spark_silver → dbt_run → dbt_test
```

Features:
- `@daily` schedule
- `retries=2` with 5-minute retry delay
- Email alerts on failure
- `catchup=False` to avoid backfill on first run

---

## Setup

### Prerequisites
- WSL2 with Ubuntu (recommended for Windows)
- Java 11 (Temurin)
- Python 3.12
- PostgreSQL 15
- Docker (optional, for PostgreSQL)

### 1 — Clone the repo
```bash
git clone https://github.com/yourusername/olist-data-pipeline.git
cd olist-data-pipeline
```

### 2 — Create virtual environment
```bash
python3 -m venv venv-linux
source venv-linux/bin/activate
pip install pyspark==3.5.0 dbt-core dbt-postgres
```

### 3 — Download the dataset
```bash
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d raw_data/
```

### 4 — Set up PostgreSQL
```bash
sudo apt install postgresql -y
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER olist WITH PASSWORD 'olist';"
sudo -u postgres psql -c "CREATE DATABASE olist_dw OWNER olist;"
```

### 5 — Configure dbt
```bash
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
olist_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: olist
      password: olist
      dbname: olist_dw
      schema: dev
      threads: 4
    prod:
      type: postgres
      host: localhost
      port: 5432
      user: olist
      password: olist
      dbname: olist_dw
      schema: public
      threads: 4
EOF
```

### 6 — Run the pipeline manually
```bash
# Bronze
python jobs/bronze_ingest.py

# Silver
python jobs/silver_transform.py

# dbt
cd olist_dbt
dbt run
dbt test
dbt docs generate && dbt docs serve --port 8082
```

### 7 — Set up Airflow (separate venv)
```bash
python3 -m venv venv-airflow
source venv-airflow/bin/activate
pip install "apache-airflow==2.10.4" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"

export AIRFLOW_HOME="$(pwd)/airflow"
airflow db migrate
airflow users create --username admin --password admin \
  --firstname A --lastname B --role Admin --email admin@example.com

# Terminal 1
airflow webserver --port 8080

# Terminal 2
airflow scheduler
```

---

## Key concepts demonstrated

- **Medallion architecture** — clear Bronze/Silver/Gold separation with specific responsibilities per layer
- **Incremental loads** — 45-day maturity window handles late-arriving and updated orders without full reprocessing
- **dbt macros** — reusable, database-agnostic date functions used across models
- **Dev/prod environment separation** — isolated schemas prevent dev work from touching production data
- **Data quality** — generic schema tests + custom singular tests catching real data issues in the Olist dataset
- **Pipeline orchestration** — Airflow DAG with task dependencies, retries, and alerting
- **Spark on Windows** — WSL2 setup resolving common winutils and Java version conflicts

---

## Author

Built as a hands-on portfolio project to prepare for data engineering interviews.

---

## License

MIT
