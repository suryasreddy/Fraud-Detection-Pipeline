"""
Q2 & Q3: Simulated Credit Card Transaction Generator
"""

import json
import time
import random
import logging
from datetime import datetime
from decimal import Decimal
from faker import Faker
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
#Initialize variables
log = logging.getLogger(__name__)
fake = Faker()

# These categories for merchants should be flagged as high risk
HIGH_RISK_MERCHANTS = [
    "Crypto Exchange", "Wire Transfer Service", "Online Casino",
    "Gift Card Store", "Pawn Shop", "Money Transfer"
]
# These categories for merchants should be considered normal 
NORMAL_MERCHANTS = [
    "Grocery Store", "Gas Station", "Restaurant", "Pharmacy",
    "Clothing Store", "Electronics Shop", "Coffee Shop", "Bookstore"
]

CARD_POOL = [fake.credit_card_number() for _ in range(20)]  # Reuse cards for pattern detection


def generate_transaction(card_number=None, force_fraud=False):
    # Generate a single simulated credit card transaction.
    card = card_number or random.choice(CARD_POOL)

    if force_fraud:
        # Inject fraud patterns: very high amount + high-risk merchant
        amount = round(random.uniform(800, 1000), 2)
        merchant_category = random.choice(HIGH_RISK_MERCHANTS)
        city = fake.city()
        state = fake.state_abbr()
    else:
        amount = round(random.uniform(1, 700), 2)
        merchant_category = random.choice(NORMAL_MERCHANTS + HIGH_RISK_MERCHANTS)
        city = fake.city()
        state = fake.state_abbr()

    transaction = {
        "card_number": card,
        "amount": amount,
        "timestamp": datetime.utcnow().isoformat(),
        "location": {
            "city": city,
            "state": state
        },
        "merchant": fake.company(),
        "merchant_category": merchant_category,
        "currency": "USD",
        "device_type": random.choice(["mobile", "web", "pos", "atm"]),
        "transaction_id": fake.uuid4(),
        "card_holder": fake.name(),
    }
    return transaction


def create_producer(bootstrap_servers="localhost:9092", retries=5):
    #Create and return a KafkaProducer with retries
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            log.info("Kafka producer connected to %s", bootstrap_servers)
            return producer
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after %d attempts" % retries)


def main():
    topic = "credit_card_transactions"
    bootstrap_servers = "localhost:9092"
    interval_seconds = 1 # Time between normal transactions
    fraud_every_n = 10 # Inject a fraud transaction every N messages

    producer = create_producer(bootstrap_servers)

    log.info("Starting transaction stream → topic '%s'", topic)
    count = 0

    try:
        while True:
            count += 1
            force_fraud = (count % fraud_every_n == 0)
            tx = generate_transaction(force_fraud=force_fraud)

            producer.send(
                topic,
                key=tx["card_number"],
                value=tx,
            )
            producer.flush()

            label = "FRAUD" if force_fraud else "normal"
            log.info(
                "[%s] card=%s amount=$%.2f city=%s merchant=%s",
                label,
                tx["card_number"][-4:],
                tx["amount"],
                tx["location"]["city"],
                tx["merchant_category"],
            )
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        log.info("Shutting down producer after %d transactions.", count)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
