-- ============================================================
-- Q4: Real-time Fraud Detection with ksqlDB
-- Run these statements in order via the ksqlDB CLI:
--   docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- STEP 1: Set offset to read all messages from the beginning
-- ─────────────────────────────────────────────────────────────
SET 'auto.offset.reset' = 'earliest';


-- ─────────────────────────────────────────────────────────────
-- STEP 2: Create the source stream from the Kafka topic
-- ─────────────────────────────────────────────────────────────
CREATE STREAM IF NOT EXISTS transactions (
    transaction_id  VARCHAR,
    card_number     VARCHAR,
    amount          DOUBLE,
    timestamp       VARCHAR,          -- stored as ISO-8601 string from producer
    location        STRUCT<city VARCHAR, state VARCHAR>,
    merchant        VARCHAR,
    merchant_category VARCHAR,
    currency        VARCHAR,
    device_type     VARCHAR,
    card_holder     VARCHAR
) WITH (
    KAFKA_TOPIC    = 'credit_card_transactions',
    VALUE_FORMAT   = 'JSON',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'
);


-- ─────────────────────────────────────────────────────────────
-- STEP 3: Verify the stream exists and shows data
-- ─────────────────────────────────────────────────────────────
-- DESCRIBE transactions;
-- SELECT * FROM transactions EMIT CHANGES LIMIT 5;


-- ─────────────────────────────────────────────────────────────
-- STEP 4: Fraud detection stream using filter rules
--
-- Fraud criteria applied:
--   Rule 1 – Transaction amount > $500 (high-value threshold)
--   Rule 2 – High-risk merchant category
--   Rule 3 – ATM transactions over $300 (unusual ATM withdrawal)
--   Rule 4 – Non-USD currency (foreign card misuse)
-- ─────────────────────────────────────────────────────────────
CREATE STREAM IF NOT EXISTS flagged_transactions
    WITH (KAFKA_TOPIC = 'flagged_transactions', VALUE_FORMAT = 'JSON') AS
SELECT
    transaction_id,
    card_number,
    amount,
    timestamp,
    location->city        AS city,
    location->state       AS state,
    merchant,
    merchant_category,
    currency,
    device_type,
    card_holder,
    CASE
        WHEN amount > 500
             THEN 'HIGH_AMOUNT'
        WHEN merchant_category IN (
                 'Crypto Exchange', 'Wire Transfer Service',
                 'Online Casino', 'Gift Card Store',
                 'Pawn Shop', 'Money Transfer')
             THEN 'HIGH_RISK_MERCHANT'
        WHEN device_type = 'atm' AND amount > 300
             THEN 'LARGE_ATM_WITHDRAWAL'
        WHEN currency != 'USD'
             THEN 'FOREIGN_CURRENCY'
        ELSE 'MULTIPLE_RULES'
    END AS fraud_reason,
    ROWTIME AS event_time
FROM transactions
WHERE
    amount > 500
    OR merchant_category IN (
        'Crypto Exchange', 'Wire Transfer Service',
        'Online Casino', 'Gift Card Store',
        'Pawn Shop', 'Money Transfer'
    )
    OR (device_type = 'atm' AND amount > 300)
    OR currency != 'USD'
EMIT CHANGES;


-- ─────────────────────────────────────────────────────────────
-- STEP 5: Aggregation – transaction count per card (1-min window)
-- Identifies cards with unusually high transaction frequency
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS card_transaction_counts
    WITH (KAFKA_TOPIC = 'card_transaction_counts', VALUE_FORMAT = 'JSON') AS
SELECT
    card_number,
    COUNT(*)            AS tx_count,
    SUM(amount)         AS total_amount,
    AVG(amount)         AS avg_amount,
    MAX(amount)         AS max_amount,
    WINDOWSTART         AS window_start,
    WINDOWEND           AS window_end
FROM transactions
    WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY card_number
EMIT CHANGES;


-- ─────────────────────────────────────────────────────────────
-- STEP 6: Rapid-fire detection – cards with > 5 txns in 1 minute
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rapid_fire_cards
    WITH (KAFKA_TOPIC = 'rapid_fire_cards', VALUE_FORMAT = 'JSON') AS
SELECT
    card_number,
    COUNT(*)    AS tx_count,
    SUM(amount) AS total_amount,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end
FROM transactions
    WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY card_number
HAVING COUNT(*) > 5
EMIT CHANGES;


-- ─────────────────────────────────────────────────────────────
-- STEP 7: Merchant-level aggregation (5-min window)
-- Flags merchants with unusually high total sales volumes
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS merchant_aggregates
    WITH (KAFKA_TOPIC = 'merchant_aggregates', VALUE_FORMAT = 'JSON') AS
SELECT
    merchant_category,
    COUNT(*)    AS tx_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end
FROM transactions
    WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY merchant_category
EMIT CHANGES;


-- ─────────────────────────────────────────────────────────────
-- STEP 8: Query to monitor flagged transactions live
-- ─────────────────────────────────────────────────────────────
-- SELECT card_number, amount, fraud_reason, city, state
-- FROM flagged_transactions
-- EMIT CHANGES;
