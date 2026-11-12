import os
import time

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

from build_nested_orders import read_csv


BATCH_SIZE = 1000

DATASETS = {
    "customers": (
        "olist_customers_dataset.csv",
        "customer_id",
    ),
    "products": (
        "olist_products_dataset.csv",
        "product_id",
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        "seller_id",
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        "product_category_name",
    ),
}

PRODUCT_INT_FIELDS = [
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]


def convert_document(document, collection_name):
    # Convert empty strings to null.
    document = {
        key: value if value != "" else None
        for key, value in document.items()
    }

    if collection_name == "products":
        for field in PRODUCT_INT_FIELDS:
            value = document[field]
            document[field] = int(value) if value is not None else None

    return document


def insert_collection(db, collection_name, filename, key_field):
    rows = read_csv(filename)
    if not rows:
        raise ValueError(f"No records found in {filename}")

    if key_field not in rows[0]:
        raise ValueError(
            f"Missing key '{key_field}' in {filename}. "
            f"Available columns: {list(rows[0].keys())}"
        )

    documents = [
        convert_document(row, collection_name)
        for row in rows
    ]

    collection = db[collection_name]
    collection.create_index(key_field, unique=True)

    start_time = time.perf_counter()

    for start in range(0, len(documents), BATCH_SIZE):
        batch = documents[start:start + BATCH_SIZE]

        operations = [
            UpdateOne(
                {key_field: doc[key_field]},
                {"$set": doc},
                upsert=True,
            )
            for doc in batch
        ]

        collection.bulk_write(operations, ordered=False)
        print(
            f"{collection_name}: "
            f"Processed {min(start + BATCH_SIZE, len(documents)):,}"
            f"/{len(documents):,}",
            flush=True,
        )

    elapsed = time.perf_counter() - start_time
    count = collection.count_documents({})

    print(f"\nCollection: {collection_name}")
    print(f"Source records: {len(documents):,}")
    print(f"MongoDB documents: {count:,}")
    print(f"Insertion time: {elapsed:.2f} seconds")
    print(f"Throughput: {len(documents) / elapsed:.2f} docs/sec")

    if count != len(documents):
        raise ValueError(
            f"Count mismatch in {collection_name}: "
            f"source={len(documents)}, MongoDB={count}"
        )


def main():
    load_dotenv()

    uri = os.getenv("MONGODB_URI")
    database_name = os.getenv("MONGODB_DATABASE", "olist")

    if not uri:
        raise ValueError("MONGODB_URI is missing from .env")

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=10000,
    )

    try:
        client.admin.command("ping")
        db = client[database_name]

        for collection_name, (filename, key_field) in DATASETS.items():
            insert_collection(
                db,
                collection_name,
                filename,
                key_field,
            )

        print("\nReference data ingestion completed.")

    finally:
        client.close()


if __name__ == "__main__":
    main()
