# Fraud Detection Pipeline

Real-time credit card fraud detection system built with Apache Kafka, ksqlDB, MongoDB, and Apache Flink. Simulated transaction data is streamed through Kafka, analyzed for fraud patterns in real time using ksqlDB, stored in MongoDB, and analyzed with windowed Flink SQL queries.

## Architecture

```
Faker (Python) → Kafka Topic → ksqlDB Fraud Detection → Flagged Topic → MongoDB
                                                                      ↓
                                                              Flink SQL Analytics
```

## Tech Stack

- **Apache Kafka** — message streaming and ingestion
- **ksqlDB** — real-time fraud detection via SQL streams
- **MongoDB** — persistent storage for flagged transactions
- **Apache Flink** — windowed analytics on transaction streams
- **Python** — transaction generation and MongoDB writing
- **Docker** — all services run locally via Docker Compose

## Prerequisites

- Docker Desktop
- Python 3.9+
- Git

## Setup & Run

### 1. Clone the repo

```bash
git clone https://github.com/suryasreddy/Fraud-Detection-Pipeline.git
cd Fraud-Detection-Pipeline
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start all services

```bash
docker-compose up -d
```

Wait about 30 seconds for all containers to become healthy. Verify with:

```bash
docker-compose ps
```

### 4. Create the Kafka topic

```bash
docker exec -it kafka kafka-topics \
  --create \
  --topic credit_card_transactions \
  --bootstrap-server kafka:29092 \
  --partitions 1 \
  --replication-factor 1
```

### 5. Start the transaction producer

In a new terminal:

```bash
python3 transaction_producer.py
```

This generates a simulated credit card transaction every second and sends it to Kafka. A fraudulent transaction is injected every 10 messages.

### 6. Set up ksqlDB fraud detection

Open the ksqlDB CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Inside the CLI, set the offset and run the SQL file:

```sql
SET 'auto.offset.reset' = 'earliest';
```

Then paste the contents of `ksqldb_fraud_detection.sql`. This creates:
- `TRANSACTIONS` stream from the raw Kafka topic
- `FLAGGED_TRANSACTIONS` stream with fraud rules applied
- `CARD_TRANSACTION_COUNTS` table (per-card stats, 1-min windows)
- `RAPID_FIRE_CARDS` table (cards with >5 txns/min)
- `MERCHANT_AGGREGATES` table (merchant category totals, 5-min windows)

Verify with:
```sql
SHOW STREAMS;
SHOW TABLES;
```

Monitor live fraud alerts:
```sql
SELECT card_number, amount, fraud_reason, city, state
FROM flagged_transactions
EMIT CHANGES;
```

### 7. Start the MongoDB writer

In a new terminal:

```bash
python3 mongodb_writer.py
```

This consumes flagged transactions from Kafka and writes them to MongoDB. Verify stored data:

```bash
docker exec -it mongodb mongosh \
  -u admin -p admin123 \
  --authenticationDatabase admin \
  fraud_detection
```

```javascript
db.flagged_transactions.countDocuments()
db.flagged_transactions.aggregate([
  { $group: { _id: "$FRAUD_REASON", count: { $sum: 1 }, total: { $sum: "$AMOUNT" } } },
  { $sort: { count: -1 } }
])
```

### 8. Run Flink SQL analytics

Start the Flink SQL CLI:

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

Run the queries from `flink_analytics.sql` inside the CLI. Key analytics include:
- Transaction volume and amount stats in 10-second windows
- Fraud reason breakdown with counts and totals
- Merchant category risk profile
- Geographic fraud hotspots by state

## Fraud Detection Rules

| Rule | Condition |
|---|---|
| HIGH_AMOUNT | Transaction amount > $500 |
| HIGH_RISK_MERCHANT | Merchant category in Crypto Exchange, Wire Transfer, Online Casino, Gift Card Store, Pawn Shop, Money Transfer |
| LARGE_ATM_WITHDRAWAL | ATM transaction > $300 |
| FOREIGN_CURRENCY | Currency is not USD |

## Shutting Down

```bash
docker-compose down
```

To also remove stored MongoDB data:

```bash
docker-compose down -v
```
