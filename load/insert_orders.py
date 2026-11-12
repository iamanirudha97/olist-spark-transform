import os
import time

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

from build_nested_orders import build_documents


BATCH_SIZE = 1000


def insert_orders(documents):
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

        collection = client[database_name]["orders"]

        # Enforce uniqueness at the database level.
        collection.create_index("order_id", unique=True)

        total = len(documents)
        start_time = time.perf_counter()

        for start in range(0, total, BATCH_SIZE):
            batch = documents[start:start + BATCH_SIZE]

            operations = [
                UpdateOne(
                    {"order_id": doc["order_id"]},
                    {"$set": doc},
                    upsert=True,
                )
                for doc in batch
            ]

            result = collection.bulk_write(
                operations,
                ordered=False,
            )

            processed = start + len(batch)

            print(
                f"Processed {processed:,}/{total:,} | "
                f"Inserted: {result.upserted_count:,} | "
                f"Updated: {result.modified_count:,}"
            )

        elapsed = time.perf_counter() - start_time

        print("\n--- Ingestion Summary ---")
        print(f"Source documents: {total:,}")
        print(
            f"MongoDB documents: "
            f"{collection.count_documents({}):,}"
        )
        print(f"Insertion time: {elapsed:.2f} seconds")
        print(f"Throughput: {total / elapsed:.2f} docs/sec")

    finally:
        client.close()


def main():
    documents = build_documents()

    print(f"Prepared {len(documents):,} nested orders")

    insert_orders(documents)


if __name__ == "__main__":
    main()
