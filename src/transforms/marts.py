from pyspark.sql import functions as F


def order_revenue_mart(orders, items):
    item_totals = (
        items.groupBy("order_id")
        .agg(
            F.sum("price").alias("item_revenue"),
            F.sum("freight_value").alias("freight_total")
        )
    )

    return orders.join(item_totals, "order_id", "left")


def seller_performance_mart(items, orders):
    order_info = orders.select(
        "order_id", "is_late", "review_score"
    )

    df = items.join(order_info, "order_id", "left")

    return (
        df.groupBy("seller_id", "seller_state")
        .agg(
            F.sum("price").alias("revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("is_late").alias("late_delivery_rate")
        )
    )


def customer_state_mart(orders, customers):
    df = orders.join(
        customers.select("customer_id", "customer_state"),
        "customer_id",
        "left"
    )

    return (
        df.groupBy("customer_state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_payment").alias("total_revenue"),
            F.avg("delivery_days").alias("avg_delivery_days")
        )
    )
