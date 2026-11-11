
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

KEYS = {
    "olist_customers_dataset": ["customer_id"],
    "olist_orders_dataset": ["order_id"],
    "olist_order_items_dataset": ["order_id", "order_item_id"],
    "olist_order_payments_dataset": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset": ["review_id", "order_id"],
    "olist_products_dataset": ["product_id"],
    "olist_sellers_dataset": ["seller_id"],
    "product_category_name_translation": ["product_category_name"],
}


def main():
    spark = (
        SparkSession.builder
        .appName("OlistDataExploration")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    datasets = {}

    # 1. Read all CSV files
    for path in sorted(DATA_DIR.glob("*.csv")):
        name = path.stem

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(path))
        )

        datasets[name] = df

        print(f"\n{'=' * 60}")
        print(f"DATASET: {name}")
        print(f"{'=' * 60}")

        print(f"Rows: {df.count():,}")
        print(f"Columns: {len(df.columns)}")
        print(f"Schema: {df.columns}")

        # 2. Check null and empty-string values

        # 2. Check null and empty-string values
                # 2. Check null and empty-string values
        missing_expressions = []

        for c in df.columns:
            condition = F.col(c).isNull() | (F.trim(F.col(c)) == "")

            expression = F.sum(
                F.when(condition, 1).otherwise(0)
            ).alias(c)

            missing_expressions.append(expression)

        row = df.select(*missing_expressions).first()

        missing = row.asDict() if row is not None else {}


        print("Missing values:")
        for column, count in missing.items():
            if count:
                print(f"  {column}: {count:,}")

        # 3. Check duplicate business keys
        keys = KEYS.get(name)

        if keys:
            dup_row = (
                df.groupBy(*keys)
                .count()
                .filter(F.col("count") > 1)
                .agg(F.sum(F.col("count") - 1))
                .first()
            )
            duplicate_count = dup_row[0] if dup_row and dup_row[0] is not None else 0

            print(
                f"Duplicate keys {keys}: "
                f"{duplicate_count:,}"
            )

    # 4. Check one-to-many relationships
    print("\n===== ORDER RELATIONSHIPS =====")

    for name in [
        "olist_order_items_dataset",
        "olist_order_payments_dataset",
        "olist_order_reviews_dataset",
    ]:
        df = datasets[name]

        grouped = df.groupBy("order_id").count()

        result = grouped.agg(
            F.count("*").alias("distinct_orders"),
            F.max("count").alias("max_records_per_order"),
            F.sum(
                F.when(F.col("count") > 1, 1).otherwise(0)
            ).alias("orders_with_multiple_records"),
        )

        print(f"\n{name}")
        result.show(truncate=False)

    reviews = datasets["olist_order_reviews_dataset"]

    valid_reviews = reviews.filter(
        F.col("order_id").isNotNull()
        & (F.trim(F.col("order_id")) != "")
    )

    print("Reviews with valid order IDs:", valid_reviews.count())

    print("Maximum reviews per valid order:")
    valid_reviews.groupBy("order_id").count().agg(
        F.max("count").alias("max_reviews_per_order")
    ).show()

    print("Review records with missing order IDs:")
    reviews.filter(
        F.col("order_id").isNull()
        | (F.trim(F.col("order_id")) == "")
    ).count()


    orders = datasets["olist_orders_dataset"]
    reviews = datasets["olist_order_reviews_dataset"]

    valid_reviews = reviews.filter(
        F.col("order_id").isNotNull()
        & (F.trim(F.col("order_id")) != "")
    )

    orphan_reviews = valid_reviews.join(
        orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )

    print("Reviews referencing nonexistent orders:", orphan_reviews.count())

    spark.stop()


if __name__ == "__main__":
    main()
