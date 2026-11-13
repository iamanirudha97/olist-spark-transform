import argparse
import boto3

from pyspark.sql import SparkSession

from transforms.orders import (
    flatten_order_items,
    flatten_payments,
    aggregate_payments,
    calculate_order_metrics,
    build_order_summary,
)
from transforms.dimensions import (
    clean_customers,
    clean_products,
    clean_sellers,
    enrich_order_items,
)
from transforms.analytics import (
    revenue_by_category,
    seller_performance,
    customer_purchase_analysis,
    payment_type_pivot,
)
from transforms.marts import (
    order_revenue_mart,
    seller_performance_mart,
    customer_state_mart,
)


def get_atlas_uri():
    client = boto3.client("secretsmanager", region_name="us-east-2")
    response = client.get_secret_value(
        SecretId="olist/mongodb/atlas-uri"
    )
    return response["SecretString"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default="olist")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Olist-Spark-ETL")
        .config(
            "spark.mongodb.read.connection.uri",
            get_atlas_uri()
        )
        .getOrCreate()
    )

    try:
        # 1. Read MongoDB collections
        def read_collection(name):
            return (
                spark.read.format("mongodb")
                .option("database", args.database)
                .option("collection", name)
                .load()
            )

        orders = read_collection("orders")
        customers = read_collection("customers")
        products = read_collection("products")
        sellers = read_collection("sellers")

        # 2. Transform orders
        items = flatten_order_items(orders)
        payments = flatten_payments(orders)
        payment_totals = aggregate_payments(payments)
        order_metrics = calculate_order_metrics(orders)
        order_summary = build_order_summary(
            order_metrics, payment_totals
        )

        # 3. Clean dimensions
        customers = clean_customers(customers)
        products = clean_products(products)
        sellers = clean_sellers(sellers)

        # 4. Enrich order items
        enriched_items = enrich_order_items(
            items, customers, products, sellers
        )

        # 5. Analytics
        category_revenue = revenue_by_category(enriched_items)
        seller_stats = seller_performance(enriched_items)
        customer_purchases = customer_purchase_analysis(
            order_summary, customers
        )
        payment_pivot = payment_type_pivot(payments)

        # 6. Build marts
        revenue_mart = order_revenue_mart(
            order_summary, enriched_items
        )
        seller_mart = seller_performance_mart(
            enriched_items, order_summary
        )
        state_mart = customer_state_mart(
            order_summary, customers
        )

        # 7. Write partitioned Parquet marts to S3
        output = "s3://olist-spark-de-5713/marts"

        revenue_mart.write.mode("overwrite") \
            .partitionBy("order_year", "order_month") \
            .parquet(f"{output}/order_revenue")

        seller_mart.write.mode("overwrite") \
            .partitionBy("seller_state") \
            .parquet(f"{output}/seller_performance")

        state_mart.write.mode("overwrite") \
            .partitionBy("customer_state") \
            .parquet(f"{output}/customer_state")

        # 8. Validate results
        print("Orders:", orders.count())
        print("Items:", items.count())
        print("Enriched items:", enriched_items.count())

        print("Revenue mart:", revenue_mart.count())
        print("Seller mart:", seller_mart.count())
        print("Customer state mart:", state_mart.count())

        category_revenue.show(5)
        seller_stats.show(5)
        customer_purchases.show(5)
        payment_pivot.show(5)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
