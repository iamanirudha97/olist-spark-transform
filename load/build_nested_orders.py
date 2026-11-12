import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def read_csv(filename):
    with open(DATA_DIR / filename, newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))


def group_by_order(rows):
    grouped = defaultdict(list)

    for row in rows:
        order_id = row["order_id"]

        if order_id:
            grouped[order_id].append(row)

    return grouped


def select_reviews(rows):
    reviews_by_order = {}

    for row in rows:
        order_id = row["order_id"]

        if not order_id:
            continue

        current = reviews_by_order.get(order_id)

        if current is None:
            reviews_by_order[order_id] = row
            continue

        new_key = (
            row["review_answer_timestamp"],
            row["review_id"],
        )

        current_key = (
            current["review_answer_timestamp"],
            current["review_id"],
        )

        if new_key > current_key:
            reviews_by_order[order_id] = row

    return reviews_by_order



def to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def to_int(value):
    if value is None or value == "":
        return None
    return int(value)


def to_datetime(value):
    if value is None or value == "":
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def convert_order(order):
    date_fields = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for field in date_fields:
        order[field] = to_datetime(order[field])

    return order


def convert_item(item):
    item["order_item_id"] = to_int(item["order_item_id"])
    item["price"] = to_float(item["price"])
    item["freight_value"] = to_float(item["freight_value"])
    item["shipping_limit_date"] = to_datetime(
        item["shipping_limit_date"]
    )
    return item


def convert_payment(payment):
    payment["payment_sequential"] = to_int(
        payment["payment_sequential"]
    )
    payment["payment_installments"] = to_int(
        payment["payment_installments"]
    )
    payment["payment_value"] = to_float(
        payment["payment_value"]
    )
    return payment


def convert_review(review):
    if review is None:
        return None

    review["review_score"] = to_int(review["review_score"])
    review["review_creation_date"] = to_datetime(
        review["review_creation_date"]
    )
    review["review_answer_timestamp"] = to_datetime(
        review["review_answer_timestamp"]
    )
    return review


def build_documents():
    orders = read_csv("olist_orders_dataset.csv")
    items = read_csv("olist_order_items_dataset.csv")
    payments = read_csv("olist_order_payments_dataset.csv")
    reviews = read_csv("olist_order_reviews_dataset.csv")

    items_by_order = group_by_order(items)
    payments_by_order = group_by_order(payments)
    reviews_by_order = select_reviews(reviews)

    documents = []

    for order in orders:
        order_id = order["order_id"]

        document = {
            **convert_order(order),
            "items": [
                convert_item(item)
                for item in items_by_order.get(order_id, [])
            ],
            "payments": [
                convert_payment(payment)
                for payment in payments_by_order.get(order_id, [])
            ],
            "review": convert_review(reviews_by_order.get(order_id)),
        }

        documents.append(document)
    return documents


def main():
    documents = build_documents()
    print(f"MongoDB documents prepared: {len(documents):,}")
    
    if documents:
        sample = documents[0]
        sample_items = sample["items"] or []
        sample_payments = sample["payments"] or []

        print(f"Sample order ID: {sample['order_id']}")
        print(f"Items in sample: {len(sample_items)}")
        print(f"Payments in sample: {len(sample_payments)}")
        print(f"Sample has review: {sample['review'] is not None}")
        print(f"Price type: {type(sample_items[0]['price']).__name__}")
        print(
            "Purchase timestamp type: "
            f"{type(sample['order_purchase_timestamp']).__name__}"
        )

    # Count orders with matching reviews
    orders_with_reviews = sum(
        doc["review"] is not None
        for doc in documents
    )

    print(f"Orders with reviews: {orders_with_reviews:,}")


if __name__ == "__main__":
    main()
