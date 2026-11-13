
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def flatten_order_items(orders: DataFrame) -> DataFrame:
    """One row per order item."""
    return (
        orders
        .withColumn("item", F.explode("items"))
        .select(
            "order_id",
            "customer_id",
            "order_status",
            F.col("item.order_item_id").alias("order_item_id"),
            F.col("item.product_id").alias("product_id"),
            F.col("item.seller_id").alias("seller_id"),
            F.col("item.shipping_limit_date").alias("shipping_limit_date"),
            F.col("item.price").alias("price"),
            F.col("item.freight_value").alias("freight_value"),
        )
        .withColumn(
            "freight_ratio",
            F.when(
                F.col("price") > 0,
                F.round(F.col("freight_value") / F.col("price"), 4),
            ),
        )
        .withColumn(
            "price_band",
            F.when(F.col("price") < 50, "LOW")
            .when(F.col("price") < 200, "MEDIUM")
            .otherwise("HIGH"),
        )
    )


def flatten_payments(orders: DataFrame) -> DataFrame:
    """One row per payment."""
    return (
        orders
        .withColumn("payment", F.explode("payments"))
        .select(
            "order_id",
            F.col("payment.payment_sequential").alias("payment_sequential"),
            F.col("payment.payment_type").alias("payment_type"),
            F.col("payment.payment_installments").alias(
                "payment_installments"
            ),
            F.col("payment.payment_value").alias("payment_value"),
        )
    )


def aggregate_payments(payments: DataFrame) -> DataFrame:
    """One row per order with total payment."""
    return (
        payments
        .groupBy("order_id")
        .agg(
            F.round(
                F.sum("payment_value"), 2
            ).alias("total_payment"),
            F.count("*").alias("payment_count"),
            F.max("payment_installments").alias("max_installments"),
        )
    )


def calculate_order_metrics(orders: DataFrame) -> DataFrame:
    """Order-level delivery and review metrics."""
    return (
        orders
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            F.col("review.review_score").alias("review_score"),
        )
        .withColumn(
            "order_year",
            F.year("order_purchase_timestamp"),
        )
        .withColumn(
            "order_month",
            F.month("order_purchase_timestamp"),
        )
        .withColumn(
            "delivery_days",
            F.datediff(
                "order_delivered_customer_date",
                "order_purchase_timestamp",
            ),
        )
        .withColumn(
            "approval_lag_hours",
            F.round(
                (
                    F.unix_timestamp("order_approved_at")
                    - F.unix_timestamp("order_purchase_timestamp")
                ) / 3600,
                2,
            ),
        )
        .withColumn(
            "estimate_error_days",
            F.datediff(
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ),
        )
        .withColumn(
            "is_late",
            F.when(
                F.col("order_delivered_customer_date").isNull()
                | F.col("order_estimated_delivery_date").isNull(),
                None,
            )
            .when(
                F.col("order_delivered_customer_date")
                > F.col("order_estimated_delivery_date"),
                1,
            )
            .otherwise(0),
        )
        .withColumn(
            "review_sentiment",
            F.when(F.col("review_score") >= 4, "POSITIVE")
            .when(F.col("review_score") == 3, "NEUTRAL")
            .when(F.col("review_score").isin(1, 2), "NEGATIVE")
            .otherwise("UNKNOWN"),
        )
    )


def build_order_summary(
    order_metrics: DataFrame,
    payment_totals: DataFrame,
) -> DataFrame:
    """Join order-level metrics with payment totals."""
    return order_metrics.join(
        payment_totals,
        on="order_id",
        how="left",
    )
