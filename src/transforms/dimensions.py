from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_customers(customers: DataFrame) -> DataFrame:
    return (
        customers
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
        )
        .dropDuplicates(["customer_id"])
        .withColumn(
            "customer_city",
            F.lower(F.trim("customer_city")),
        )
        .withColumn(
            "customer_state",
            F.upper(F.trim("customer_state")),
        )
    )


def clean_products(products: DataFrame) -> DataFrame:
    return (
        products
        .select(
            "product_id",
            "product_category_name",
            "product_weight_g",
        )
        .dropDuplicates(["product_id"])
        .withColumn(
            "product_category_name",
            F.coalesce(
                F.col("product_category_name"),
                F.lit("unknown"),
            ),
        )
    )


def clean_sellers(sellers: DataFrame) -> DataFrame:
    return (
        sellers
        .select(
            "seller_id",
            "seller_city",
            "seller_state",
        )
        .dropDuplicates(["seller_id"])
        .withColumn(
            "seller_city",
            F.lower(F.trim("seller_city")),
        )
        .withColumn(
            "seller_state",
            F.upper(F.trim("seller_state")),
        )
    )


def enrich_order_items(
    order_items: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    sellers: DataFrame,
) -> DataFrame:
    return (
        order_items
        .join(customers, on="customer_id", how="left")
        .join(products, on="product_id", how="left")
        .join(sellers, on="seller_id", how="left")
    )
