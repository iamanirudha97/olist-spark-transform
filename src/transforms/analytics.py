from pyspark.sql import Window
from pyspark.sql import functions as F


def revenue_by_category(items):
    return (
        items.groupBy("product_category_name")
        .agg(
            F.sum("price").alias("revenue"),
            F.count("*").alias("units_sold")
        )
    )


def seller_performance(items):
    stats = (
        items.groupBy("seller_id", "seller_state")
        .agg(
            F.sum("price").alias("revenue"),
            F.countDistinct("order_id").alias("orders")
        )
    )

    window = Window.partitionBy("seller_state").orderBy(
        F.desc("revenue")
    )

    return stats.withColumn(
        "seller_rank", F.rank().over(window)
    )


def customer_purchase_analysis(orders, customers):
    df = orders.join(
        customers.select("customer_id", "customer_unique_id"),
        "customer_id",
        "left"
    )

    window = Window.partitionBy("customer_unique_id").orderBy(
        "order_purchase_timestamp", "order_id"
    )

    return (
        df.withColumn("purchase_number", F.row_number().over(window))
        .withColumn(
            "previous_purchase",
            F.lag("order_purchase_timestamp").over(window)
        )
        .withColumn(
            "cumulative_spending",
            F.sum("total_payment").over(
                window.rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow
                )
            )
        )
    )


def payment_type_pivot(payments):
    return (
        payments.groupBy("order_id")
        .pivot("payment_type")
        .sum("payment_value")
        .fillna(0)
    )
