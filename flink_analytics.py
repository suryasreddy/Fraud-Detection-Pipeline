"""
Q6: Real-time Analytics with Flink
"""

import logging
import os

from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
    DataTypes,
)
from pyflink.table.window import Tumble, Slide

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Kafka connection paramaters
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = "credit_card_transactions"
FLAGGED_TOPIC = "flagged_transactions"


def build_table_env() -> StreamTableEnvironment:
    #Return Table streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=settings,
    )
    return table_env


def create_raw_transactions_table(table_env: StreamTableEnvironment):
    # Kafka sources
    table_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS raw_transactions (
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
            'connector'              = 'kafka',
            'topic'                  = '{RAW_TOPIC}',
            'scan.startup.mode'      = 'earliest-offset',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'    = 'flink-raw-consumer',
            'format'                 = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    log.info("Table 'raw_transactions' created.")


def create_flagged_transactions_table(table_env: StreamTableEnvironment):
    table_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS flagged_transactions (
            transaction_id STRING,
            card_number    STRING,
            amount         DECIMAL(10, 2),
            `timestamp`    STRING,
            city           STRING,
            state          STRING,
            merchant       STRING,
            merchant_category STRING,
            currency       STRING,
            device_type    STRING,
            fraud_reason   STRING,
            event_time     BIGINT,
            proc_time      AS PROCTIME()
        ) WITH (
            'connector'              = 'kafka',
            'topic'                  = '{FLAGGED_TOPIC}',
            'scan.startup.mode'      = 'earliest-offset',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'    = 'flink-flagged-consumer',
            'format'                 = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    log.info("Table 'flagged_transactions' created.")


def create_print_sink(table_env: StreamTableEnvironment, name: str, schema_sql: str):
    table_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            {schema_sql}
        ) WITH (
            'connector' = 'print'
        )
    """)



# Query 1: Windowed analysis
QUERY_WINDOW_STATS = """
INSERT INTO sink_window_stats
SELECT
    CAST(HOP_START(proc_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND) AS STRING) AS window_start,
    CAST(HOP_END  (proc_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND) AS STRING) AS window_end,
    COUNT(*)           AS transaction_count,
    SUM(amount)        AS total_amount,
    MIN(amount)        AS min_amount,
    MAX(amount)        AS max_amount,
    AVG(amount)        AS avg_amount
FROM raw_transactions
GROUP BY HOP(proc_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND)
"""


# Query 2: Fraud reason breakdown 
QUERY_FRAUD_REASON = """
INSERT INTO sink_fraud_reason
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS STRING) AS window_start,
    fraud_reason,
    COUNT(*)    AS fraud_count,
    SUM(amount) AS total_flagged_amount,
    AVG(amount) AS avg_flagged_amount
FROM flagged_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), fraud_reason
"""


# Query 3: Top cards by fraud alert count 
QUERY_TOP_CARDS = """
INSERT INTO sink_top_cards
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS STRING) AS window_start,
    card_number,
    COUNT(*)    AS alert_count,
    SUM(amount) AS total_amount,
    MAX(amount) AS max_single_amount
FROM flagged_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), card_number
"""

# QUERY 4: Device type analysis 
QUERY_DEVICE_ANALYSIS = """
INSERT INTO sink_device_analysis
SELECT
    CAST(TUMBLE_START(proc_time, INTERVAL '5' MINUTE) AS STRING) AS window_start,
    device_type,
    COUNT(*)    AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM raw_transactions
GROUP BY TUMBLE(proc_time, INTERVAL '5' MINUTE), device_type
"""


def run_analytics(table_env: StreamTableEnvironment):
    # Run analytics queries

    # Windowed stats
    create_print_sink(table_env, "sink_window_stats", """
        window_start STRING,
        window_end   STRING,
        transaction_count BIGINT,
        total_amount DECIMAL(10,2),
        min_amount   DECIMAL(10,2),
        max_amount   DECIMAL(10,2),
        avg_amount   DECIMAL(10,2)
    """)

    # Fraud reason breakdown
    create_print_sink(table_env, "sink_fraud_reason", """
        window_start        STRING,
        fraud_reason        STRING,
        fraud_count         BIGINT,
        total_flagged_amount DECIMAL(10,2),
        avg_flagged_amount  DECIMAL(10,2)
    """)

    # Top cards
    create_print_sink(table_env, "sink_top_cards", """
        window_start      STRING,
        card_number       STRING,
        alert_count       BIGINT,
        total_amount      DECIMAL(10,2),
        max_single_amount DECIMAL(10,2)
    """)

    # Device analysis
    create_print_sink(table_env, "sink_device_analysis", """
        window_start      STRING,
        device_type       STRING,
        transaction_count BIGINT,
        total_amount      DECIMAL(10,2),
        avg_amount        DECIMAL(10,2)
    """)

    stmt_set = table_env.create_statement_set()
    stmt_set.add_insert_sql(QUERY_WINDOW_STATS)
    stmt_set.add_insert_sql(QUERY_FRAUD_REASON)
    stmt_set.add_insert_sql(QUERY_TOP_CARDS)
    stmt_set.add_insert_sql(QUERY_DEVICE_ANALYSIS)

    log.info("Submitting Flink analytics jobs...")
    result = stmt_set.execute()
    log.info("Jobs submitted. Job ID: %s", result.get_job_client().get_job_id())

    # Block until interrupted
    result.get_job_client().get_job_execution_result().result()


def main():
    table_env = build_table_env()

    log.info("Creating Kafka source tables...")
    create_raw_transactions_table(table_env)
    create_flagged_transactions_table(table_env)

    log.info("Running real-time analytics queries...")
    run_analytics(table_env)


if __name__ == "__main__":
    main()
