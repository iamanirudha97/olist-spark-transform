
# Olist E-Commerce Batch ETL Pipeline

**MongoDB Atlas → Apache Spark on Amazon EMR → Amazon S3**

An end-to-end batch data engineering pipeline built using PySpark, MongoDB Atlas, Amazon EMR, and Amazon S3.

The pipeline processes the Brazilian E-Commerce Public Dataset by Olist, transforms nested MongoDB documents using distributed Spark operations, and generates three curated, partitioned Parquet data marts for analytical workloads.

## Architecture

```text
                 Olist E-Commerce Dataset
                            |
                            v
                    Python Data Loader
                            |
                            v
                      MongoDB Atlas
             +--------------+--------------+
             |              |              |
        Nested Orders    Customers     Products / Sellers
             |              |              |
             +--------------+--------------+
                            |
                 MongoDB Spark Connector
                            |
                            v
                     Amazon EMR
                    Apache Spark
                            |
                +-----------+-----------+
                |           |           |
            Flattening   Cleaning   Enrichment
                |           |           |
                +-----------+-----------+
                            |
                    Derived Metrics
                            |
              Aggregations / Windows / Pivot
                            |
                    Data Mart Builder
                            |
                            v
                       Amazon S3
                   Partitioned Parquet
                            |
               +------------+------------+
               |            |            |
          Order Revenue   Seller      Customer State
              Mart     Performance       Mart
                            Mart
```

The MongoDB connection URI is retrieved through AWS Secrets Manager. The EMR instance role provides the permissions required to access the secret and write output to Amazon S3.

## Technology Stack

| Component | Technology |
|---|---|
| Language | Python |
| Processing | Apache Spark / PySpark |
| Compute | Amazon EMR |
| Source database | MongoDB Atlas |
| Database connector | MongoDB Spark Connector 11.1.0 |
| Cloud storage | Amazon S3 |
| File format | Apache Parquet (Snappy) |
| Credentials | AWS Secrets Manager |
| Region | us-east-2 |

## Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The dataset contains approximately 100,000 e-commerce orders from Brazil, including customers, products, sellers, payments, reviews, and delivery information.

### MongoDB Collections

| Collection | Description |
|---|---|
| `orders` | Order records with nested items, payments, and review data |
| `customers` | Customer identifiers and geographic information |
| `products` | Product categories and attributes |
| `sellers` | Seller identifiers and geographic information |

### Nested Order Structure

The ingestion process combines order-related data into nested MongoDB documents.

An illustrative order document:

```json
{
  "order_id": "ORDER_ID",
  "customer_id": "CUSTOMER_ID",
  "order_status": "delivered",
  "items": [
    {
      "order_item_id": 1,
      "product_id": "PRODUCT_ID",
      "seller_id": "SELLER_ID",
      "price": 100.0,
      "freight_value": 15.0
    }
  ],
  "payments": [
    {
      "payment_sequential": 1,
      "payment_type": "credit_card",
      "payment_installments": 2,
      "payment_value": 115.0
    }
  ],
  "review": {
    "review_score": 5
  }
}
```

The nested order structure allows Spark to process related order items, payments, and review information from a single source collection.

## ETL Pipeline

### 1. Extract

The Spark application reads four MongoDB Atlas collections:

- `orders`
- `customers`
- `products`
- `sellers`

The MongoDB Spark Connector loads these collections into Spark DataFrames.

The Atlas connection URI is retrieved securely from AWS Secrets Manager.

### 2. Transform

The transformation logic is organized into separate PySpark modules.

#### Data Cleaning and Standardization

**Files:** `src/transforms/orders.py`, `src/transforms/dimensions.py`

- Flatten nested order items using `explode()`.
- Flatten nested payment arrays.
- Extract review scores from nested review documents.
- Deduplicate customer, product, and seller dimensions by identifier.
- Normalize city names using lowercase and trimming.
- Normalize state abbreviations using uppercase and trimming.
- Replace missing product categories with `unknown`.
- Aggregate payment records at the order level.

#### Derived Features

**File:** `src/transforms/orders.py`

| Feature | Description |
|---|---|
| `delivery_days` | Days between purchase and delivery |
| `approval_lag_hours` | Hours between purchase and approval |
| `estimate_error_days` | Difference between actual and estimated delivery |
| `is_late` | Indicator for late delivery |
| `freight_ratio` | Freight cost divided by item price |
| `price_band` | LOW, MEDIUM, or HIGH |
| `review_sentiment` | POSITIVE, NEUTRAL, NEGATIVE, or UNKNOWN |
| `order_year` | Purchase year |
| `order_month` | Purchase month |

#### Joins and Enrichment

**File:** `src/transforms/dimensions.py`

Order items are enriched with customer, product, and seller information using DataFrame joins.

Payment information is aggregated to the order level before joining into the order summary.

#### Aggregations and Analytics

**File:** `src/transforms/analytics.py`

The pipeline performs:

- Revenue and units sold by product category.
- Seller revenue and distinct order counts.
- Seller ranking within each state.
- Customer purchase sequence analysis.
- Previous customer purchase identification.
- Cumulative customer spending.
- Payment-type pivot analysis.

**Window functions used:**

| Function | Purpose |
|---|---|
| `rank()` | Rank sellers by revenue within each state |
| `row_number()` | Number customer purchases |
| `lag()` | Retrieve previous purchase timestamp |
| `sum().over()` | Calculate cumulative customer spending |

### 3. Load

The pipeline builds three curated Parquet data marts and writes them to Amazon S3 using partitioned directory layouts.

## Curated Data Marts

### Order Revenue Mart

**S3 location:**

`s3://olist-spark-de-5713/marts/order_revenue/`

**Granularity:** One row per order.

Contains order identifiers, customer information, order status, timestamps, payment totals, item revenue, freight totals, review information, and delivery metrics.

**Partition columns:** `order_year`, `order_month`

```text
order_revenue/
├── order_year=2016/
├── order_year=2017/
└── order_year=2018/
    └── order_month=.../
```

Year/month partitioning organizes the mart for time-based analytical queries.

### Seller Performance Mart

**S3 location:**

`s3://olist-spark-de-5713/marts/seller_performance/`

**Granularity:** One row per seller.

| Column | Description |
|---|---|
| `seller_id` | Seller identifier |
| `seller_state` | Seller state |
| `revenue` | Total item revenue |
| `total_orders` | Distinct order count |
| `avg_review_score` | Average review score |
| `late_delivery_rate` | Average late-delivery indicator |

**Partition column:** `seller_state`

```text
seller_performance/
├── seller_state=MG/
├── seller_state=RJ/
└── seller_state=SP/
```

### Customer State Mart

**S3 location:**

`s3://olist-spark-de-5713/marts/customer_state/`

**Granularity:** One row per customer state.

| Column | Description |
|---|---|
| `customer_state` | State abbreviation |
| `total_orders` | Distinct order count |
| `total_revenue` | Total payment revenue |
| `avg_delivery_days` | Average delivery duration |

**Partition column:** `customer_state`

```text
customer_state/
├── customer_state=MG/
├── customer_state=RJ/
└── customer_state=SP/
```

State-based partitioning organizes geographic performance metrics.

## Project Structure

```text
olist-spark-transform/
├── src/
│   ├── job.py
│   └── transforms/
│       ├── __init__.py
│       ├── orders.py
│       ├── dimensions.py
│       ├── analytics.py
│       └── marts.py
└── README.md
```

### Module Responsibilities

| Module | Responsibility |
|---|---|
| `job.py` | Spark session, MongoDB reads, pipeline execution, S3 writes |
| `orders.py` | Nested order processing and derived metrics |
| `dimensions.py` | Dimension cleaning and joins |
| `analytics.py` | Aggregations, windows, and pivot |
| `marts.py` | Curated data mart construction |

## AWS Environment

The pipeline was executed on Amazon EMR using the following configuration:

| Setting | Value |
|---|---|
| AWS region | us-east-2 |
| Spark version | 4.0.2 |
| Scala version | 2.13 |
| MongoDB Spark Connector | 11.1.0 |
| Deployment mode | Cluster |
| S3 bucket | olist-spark-de-5713 |

### MongoDB Atlas Connectivity

The EMR cluster requires network connectivity to MongoDB Atlas.

The EMR outbound IP address or applicable CIDR range must be added to the MongoDB Atlas IP access list under **Security → Network Access**.

The MongoDB connection URI is stored in AWS Secrets Manager under:

```text
olist/mongodb/atlas-uri
```

The EMR instance role requires permission to retrieve this secret.

## Running the Pipeline

### 1. Upload the Spark Application

From the project root:

```bash
aws s3 cp src/job.py \
  s3://olist-spark-de-5713/scripts/job.py \
  --region us-east-2
```

### 2. Package Transformation Modules

```bash
cd src
zip -r transforms.zip transforms -i '*.py'
cd ..
```

Upload the package:

```bash
aws s3 cp src/transforms.zip \
  s3://olist-spark-de-5713/scripts/transforms.zip \
  --region us-east-2
```

### 3. Submit the EMR Spark Step

In the AWS EMR console, open the cluster and select **Steps → Add step**.

Use the following configuration:

| Field | Value |
|---|---|
| Type | Spark application |
| Deploy mode | Cluster |
| Application location | `s3://olist-spark-de-5713/scripts/job.py` |
| Arguments | Empty |
| Action on failure | Continue |

**Spark-submit options:**

```text
--packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0 --py-files s3://olist-spark-de-5713/scripts/transforms.zip
```

The application reads the configured MongoDB database and writes the three Parquet marts to S3.

## Validation and Results

The Spark application completed successfully on Amazon EMR.

### S3 Output Verification

All three partitioned marts were verified using AWS CLI.

| Mart | Verified partitions |
|---|---|
| Order revenue | 2016, 2017, 2018 |
| Seller performance | 23 seller states |
| Customer state | 27 states |

Each mart contained a Spark `_SUCCESS` marker.

### Data Quality Checks

Initial validation of the curated outputs produced:

| Metric | Result |
|---|---:|
| Order records | 99,441 |
| Unique order IDs | 99,441 |
| Duplicate order IDs | 0 |
| Seller records | 3,095 |
| Unique seller IDs | 3,095 |
| Customer state records | 27 |

These counts were validated before the final partitioned write.

### Revenue Reconciliation

Payment totals were compared with item revenue plus freight.

| Metric | Amount (BRL) |
|---|---:|
| Total payments | 16,008,872.12 |
| Item revenue + freight | 15,843,553.24 |
| Difference | 165,318.88 |

The discrepancy was concentrated primarily in canceled and unavailable orders.

For delivered orders, the difference was approximately BRL 2,688.02 (0.0174%).

## Engineering Decisions

### Nested Document Processing

The pipeline uses MongoDB's nested document structure to group order-related information.

Spark `explode()` operations flatten nested arrays into tabular records for downstream processing.

### Preventing Double Counting

Payments are aggregated by `order_id` before joining into the order summary.

Item revenue and freight are also aggregated at the order level before joining into the order revenue mart.

This prevents financial totals from being multiplied by joins involving multiple items or payments.

### Distributed Analytics

PySpark DataFrame operations perform cleaning, enrichment, grouped aggregations, window functions, and pivot transformations on Amazon EMR.

### Parquet Storage

Parquet provides columnar storage and compression for analytical workloads.

Partitioning organizes the output according to the primary analytical dimensions of each mart.

### Secure Credential Management

The MongoDB connection URI is retrieved at runtime from AWS Secrets Manager, keeping credentials out of the application source code.

## Conclusion

This project implements a cloud-based batch ETL pipeline using MongoDB Atlas, Apache Spark on Amazon EMR, and Amazon S3.

The implementation demonstrates nested-document processing, distributed DataFrame transformations, joins, aggregations, window functions, pivot operations, and partitioned Parquet data marts.

The pipeline executed successfully on EMR, and all three partitioned S3 outputs were verified.
