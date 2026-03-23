-- ============================================================
-- Q6: Real-time Analytics with Apache Flink SQL
-- Run inside the Flink SQL CLI:
--   docker compose run sql-client
-- Or via the Confluent learn-flink Docker environment:
--   docker compose run sql-client
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- SOURCE TABLE: Raw Transactions from Kafka
-- ─────────────────────────────────────────────────────────────
CREATE TABLE raw_transactions (
    transaction_id    STRING,
    card_number       STRING,
    amount            DECIMAL(10, 2),
    `timestamp`       STRING,
    city              STRING,
    state             STRING,
    merchant          STRING,
    merchant_category STRING,
    currency          STRING,
    device_type       STRING,
    card_holder       STRING,
    proc_time         AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'credit_card_transactions',
    'scan.startup.mode'            = 'earliest-offset',
    'properties.bootstrap.servers' = 'host.docker.internal:9092',
    'properties.group.id'          = 'flink-sql-raw',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);


-- ─────────────────────────────────────────────────────────────
-- SOURCE TABLE: Flagged Transactions from ksqlDB output topic
-- ─────────────────────────────────────────────────────────────
CREATE TABLE flagged_transactions (
    transaction_id    STRING,
    card_number       STRING,
    amount            DECIMAL(10, 2),
    `timestamp`       STRING,
    city              STRING,
    state             STRING,
    merchant          STRING,
    merchant_category STRING,
    currency          STRING,
    device_type       STRING,
    fraud_reason      STRING,
    card_holder       STRING,
    proc_time         AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'flagged_transactions',
    'scan.startup.mode'            = 'earliest-offset',
    'properties.bootstrap.servers' = 'host.docker.internal:9092',
    'properties.group.id'          = 'flink-sql-flagged',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);


-- ─────────────────────────────────────────────────────────────
-- QUERY 1: Transaction volume & amount stats (10-second windows)
-- Shows real-time throughput and value flow across all transactions
-- ─────────────────────────────────────────────────────────────
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '10' SECOND) AS STRING) AS window_start,
    CAST(TUMBLE_END  (proc_time, INTERVAL '10' SECOND) AS STRING) AS window_end,
    COUNT(*)           AS transaction_count,
    SUM(amount)        AS total_amount,
    MIN(amount)        AS min_amount,
    MAX(amount)        AS max_amount,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount
FROM raw_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '10' SECOND);


-- ─────────────────────────────────────────────────────────────
-- QUERY 2: Fraud alert breakdown by reason (1-min windows)
-- Identifies which fraud rules are triggering most frequently
-- ─────────────────────────────────────────────────────────────
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS STRING) AS window_start,
    fraud_reason,
    COUNT(*)    AS fraud_count,
    SUM(amount) AS total_flagged_amount,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_flagged_amount
FROM flagged_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), fraud_reason;


-- ─────────────────────────────────────────────────────────────
-- QUERY 3: High-frequency cards (cards appearing > 3× in 1 min)
-- Potential card cloning / enumeration attack detection
-- ─────────────────────────────────────────────────────────────
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS STRING) AS window_start,
    card_number,
    COUNT(*)    AS alert_count,
    SUM(amount) AS total_amount,
    MAX(amount) AS max_single_txn
FROM flagged_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), card_number
HAVING COUNT(*) > 3;


-- ─────────────────────────────────────────────────────────────
-- QUERY 4: Merchant category risk profile (5-min windows)
-- Groups suspicious activity by merchant category
-- ─────────────────────────────────────────────────────────────
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '5' MINUTE) AS STRING) AS window_start,
    merchant_category,
    COUNT(*)    AS tx_count,
    SUM(amount) AS total_amount,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount
FROM raw_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '5' MINUTE), merchant_category
ORDER BY total_amount DESC;


-- ─────────────────────────────────────────────────────────────
-- QUERY 5: Device-type fraud correlation (5-min windows)
-- Surfaces which devices (atm/mobile/web/pos) are most linked to fraud
-- ─────────────────────────────────────────────────────────────
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '5' MINUTE) AS STRING) AS window_start,
    r.device_type,
    COUNT(r.transaction_id)  AS total_transactions,
    COUNT(f.transaction_id)  AS flagged_transactions,
    CAST(
        COUNT(f.transaction_id) * 100.0 / NULLIF(COUNT(r.transaction_id), 0)
    AS DECIMAL(5,2))         AS fraud_rate_pct
FROM raw_transactions r
LEFT JOIN flagged_transactions f
    ON r.transaction_id = f.transaction_id
    AND r.proc_time BETWEEN f.proc_time - INTERVAL '1' SECOND
                        AND f.proc_time + INTERVAL '1' SECOND
GROUP BY TUMBLE(r.proc_time, INTERVAL '5' MINUTE), r.device_type;


-- ─────────────────────────────────────────────────────────────
-- QUERY 6: Geographic hotspots – states with most fraud (5-min)
-- ─────────────────────────────────────────────────────────────
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '5' MINUTE) AS STRING) AS window_start,
    state,
    COUNT(*)    AS fraud_count,
    SUM(amount) AS total_flagged_amount
FROM flagged_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '5' MINUTE), state
ORDER BY fraud_count DESC;
