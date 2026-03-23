"""
Q5: MongoDB Writer
"""

import json
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# MongoDB Configuration
MONGO_URI = "mongodb://admin:admin123@localhost:27018/"
DB_NAME = "fraud_detection"
COLLECTION_NAME = "flagged_transactions"
# Kafka Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "flagged_transactions"
CONSUMER_GROUP = "mongodb-fraud-writer"

def connect_mongo(uri: str, retries: int = 5) -> MongoClient:
    #Connect to mongodb
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log.info("Connected to MongoDB.")
            return client
        except ConnectionFailure as exc:
            log.warning("MongoDB attempt %d/%d failed: %s", attempt, retries, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to MongoDB after %d attempts." % retries)


def setup_collection(db):
    #create indexes for queryinh
    col = db[COLLECTION_NAME]
    col.create_index([("transaction_id", DESCENDING)], unique=True, sparse=True)
    col.create_index([("card_number", DESCENDING)])
    col.create_index([("event_time", DESCENDING)])
    col.create_index([("fraud_reason", DESCENDING)])
    log.info("MongoDB indexes created/verified on '%s'.", COLLECTION_NAME)
    return col


def parse_message(raw_value: bytes):
    # Deserialize kafka into dictionary
    try:
        return json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.error("Failed to parse message: %s", exc)
        return None


def enrich_document(doc: dict) -> dict:
    doc["_ingested_at"] = datetime.utcnow().isoformat()
    return doc


def main():
    # Connect to MongoDB
    client = connect_mongo(MONGO_URI)
    db = client[DB_NAME]
    collection = setup_collection(db)

    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,   # raw bytes; we parse manually
    )
    log.info("Listening on Kafka topic '%s'...", KAFKA_TOPIC)

    inserted = 0
    skipped = 0

    try:
        for msg in consumer:
            doc = parse_message(msg.value)
            if doc is None:
                skipped += 1
                continue

            doc = enrich_document(doc)

            try:
                collection.insert_one(doc)
                inserted += 1
                log.info(
                    "[+] Stored fraud alert #%d | card=...%s | $%.2f | reason=%s",
                    inserted,
                    str(doc.get("CARD_NUMBER", doc.get("card_number", "????")))[-4:],
                    float(doc.get("AMOUNT", doc.get("amount", 0))),
                    doc.get("FRAUD_REASON", doc.get("fraud_reason", "UNKNOWN")),
                )
            except DuplicateKeyError:
                log.debug("Duplicate transaction_id skipped.")
                skipped += 1

    except KeyboardInterrupt:
        log.info("Shutdown requested. Inserted=%d  Skipped=%d", inserted, skipped)
    finally:
        consumer.close()
        client.close()
        log.info("Connections closed.")


# Checks to make sure pipeline is running smoothly
def query_fraud_stats():
    #print summary of fraud alerts in mongodb
    client = connect_mongo(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    total = col.count_documents({})
    print(f"\nTotal flagged transactions stored: {total}")

    print("\nFraud reasons breakdown:")
    pipeline = [
        {"$group": {"_id": "$fraud_reason", "count": {"$sum": 1}, "total_amount": {"$sum": "$amount"}}},
        {"$sort": {"count": -1}}
    ]
    for doc in col.aggregate(pipeline):
        print(f"  {doc['_id']}: {doc['count']} transactions, total=${doc['total_amount']:.2f}")

    print("\nTop 5 flagged cards:")
    pipeline2 = [
        {"$group": {"_id": "$card_number", "count": {"$sum": 1}, "total": {"$sum": "$amount"}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    for doc in col.aggregate(pipeline2):
        print(f"  Card ...{doc['_id'][-4:]}: {doc['count']} alerts, total=${doc['total']:.2f}")

    client.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        query_fraud_stats()
    else:
        main()
