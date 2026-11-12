import os

from dotenv import load_dotenv
from pymongo import MongoClient


def main():
    load_dotenv()

    uri = os.getenv("MONGODB_URI")
    database_name = os.getenv("MONGODB_DATABASE", "olist")

    if not uri:
        raise ValueError("MONGODB_URI is missing from .env")

    client = MongoClient(uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        print("Successfully connected to MongoDB Atlas!")

        db = client[database_name]
        print(f"Target database: {db.name}")

        print("Existing collections:")
        print(db.list_collection_names())

    finally:
        client.close()


if __name__ == "__main__":
    main()
