# Real-Time Market Streaming Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-231F20?style=flat&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1.2-E25A1C?style=flat&logo=apachespark&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-ap--south--1-FF9900?style=flat&logo=amazons3&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-Containerized-2496ED?style=flat&logo=docker&logoColor=white)

A production-grade real-time streaming pipeline ingesting live BSE stock data through Apache Kafka, processing it with Spark Structured Streaming, and landing it simultaneously in AWS S3 and Snowflake — all containerized with Docker.

## What This Pipeline Does

Every 60 seconds a Python producer fetches live closing prices for five large-cap BSE stocks and publishes them to Kafka — keyed by symbol so each stock always lands in its own partition. Spark Structured Streaming consumes these messages in 30-second micro-batches and writes to two sinks in the same processing step: raw JSON files land in AWS S3 as an immutable data lake, and structured records land in Snowflake's `STREAMING.STOCK_TICKS` table for real-time querying. The entire pipeline starts with one command.

## Architecture

![Architecture](Real-Time_Market_Streaming_Pipeline_Architecture.png)

## Stack

| Layer | Tool |
|---|---|
| Data Source | Alpha Vantage API |
| Message Broker | Apache Kafka (5 partitions) |
| Stream Processing | Spark Structured Streaming |
| Data Lake | AWS S3 ap-south-1 |
| Data Warehouse | Snowflake |
| Orchestration | Docker Compose |

## Data Flow

```
╔══════════════════════════════════════════════════════════════════════════╗
║         LIVE MARKET DATA  →  KAFKA  →  SPARK  →  S3 + SNOWFLAKE        ║
╚══════════════════════════════════════════════════════════════════════════╝

  ┌──────────────────────────────────────────────────┐
  │               Alpha Vantage API                  │
  │       REST · JSON · Free tier: 5 req/min         │
  └─────────────────────┬────────────────────────────┘
                        │  HTTP GET /query?symbol=RELIANCE.BSE&function=...
                        │  13s enforced gap between each of 5 symbols
                        ▼
  ┌──────────────────────────────────────────────────┐
  │          Python Producer                         │
  │          producer/stock_producer.py              │
  │                                                  │
  │  • Full poll cycle  :  ~65s for all 5 stocks    │
  │  • Message payload  :  symbol · price · ts      │
  │  • Kafka message key:  stock symbol (hashed)    │
  └─────────────────────┬────────────────────────────┘
                        │  key-based routing → deterministic partition
                        ▼
  ┌──────────────────────────────────────────────────┐
  │       Kafka  ·  Topic: stock-prices              │
  │                                                  │
  │   ┌────────┐  ┌────────┐  ┌────────┐  ┌──────┐ │
  │   │  P-0   │  │  P-1   │  │  P-2   │  │  P-3 │ │
  │   │RELIANCE│  │  TCS   │  │HDFCBANK│  │ INFY │ │
  │   └────────┘  └────────┘  └────────┘  └──────┘ │
  │                                                  │
  │  5 partitions · 1 stock per partition            │
  │  per-symbol ordering guaranteed within partition │
  └─────────────────────┬────────────────────────────┘
                        │  Spark readStream · 30s micro-batch interval
                        ▼
  ┌──────────────────────────────────────────────────┐
  │        Spark Structured Streaming                │
  │        consumer/spark_consumer.py                │
  │                                                  │
  │  • Deserialize Kafka value → JSON schema        │
  │  • foreachBatch → single dual-write function    │
  │  • Checkpoint offsets after every commit        │
  │  • Resume from last offset on restart           │
  └───────────┬──────────────────────────┬───────────┘
              │                          │
              │   independent try/except per sink
              ▼                          ▼
  ┌───────────────────────┐   ┌──────────────────────────┐
  │        AWS S3          │   │        Snowflake          │
  │   ap-south-1           │   │   DE_GRIND.STREAMING     │
  │                        │   │                          │
  │  streaming/            │   │   STOCK_TICKS table      │
  │  stock_prices/         │   │   ─────────────────────  │
  │  └─ raw JSON files     │   │   SYMBOL  VARCHAR        │
  │                        │   │   PRICE   FLOAT          │
  │  • Immutable archive   │   │   TRADING_DAY  DATE      │
  │  • Full replay source  │   │   PROCESSED_AT TIMESTAMP │
  │  • No schema lock-in   │   │   • Queryable live       │
  └───────────────────────┘   └──────────────────────────┘
     Data Lake (source of truth)   Data Warehouse (analytics)
```

> **Fault tolerance:** if Snowflake is unavailable, S3 still receives the batch. On Spark restart, checkpointed offsets ensure exactly-once delivery — no data loss, no duplicates.

## Tracked Stocks

`RELIANCE.BSE` · `TCS.BSE` · `HDFCBANK.BSE` · `INFY.BSE` · `WIPRO.BSE`

## Project Structure

```
streaming-pipeline/
│
├── producer/
│   └── stock_producer.py          ← Kafka publisher
│                                     Alpha Vantage poller · 65s cycle
│                                     Publishes JSON keyed by stock symbol
│
├── consumer/
│   └── spark_consumer.py          ← Spark Structured Streaming job
│                                     readStream from Kafka · 30s micro-batch
│                                     foreachBatch → S3 write + Snowflake write
│                                     Checkpoint-based offset management
│
├── kafka/
│   └── docker-compose.yaml        ← Standalone Kafka + Zookeeper
│                                     Broker exposed on port 9092
│                                     Used for local dev / isolated testing
│
├── Dockerfile.producer            ← Python 3.11-slim image
│                                     Installs kafka-python + requests
│
├── Dockerfile.consumer            ← Java 17 + PySpark 4.1.2 image
│                                     Includes Kafka + S3 + Snowflake jars
│
├── docker-compose.yaml            ← Orchestrates full pipeline (4 services)
│                                     zookeeper · kafka · producer · consumer
│                                     One command: docker compose up
│
├── requirements.txt               ← kafka-python · pyspark · boto3
│                                     snowflake-connector-python · requests
│
└── .env.example                   ← Credential template (never committed)
                                      ALPHA_VANTAGE_KEY
                                      AWS_ACCESS_KEY_ID / SECRET / REGION
                                      SNOWFLAKE_ACCOUNT / USER / PASSWORD
```

| Layer | File | Responsibility |
|---|---|---|
| Ingestion | `producer/stock_producer.py` | Polls API, enforces rate limit, publishes to Kafka |
| Transport | `kafka/docker-compose.yaml` | Broker + Zookeeper — message durability and ordering |
| Processing | `consumer/spark_consumer.py` | Micro-batch consumption, schema enforcement, dual-sink write |
| Storage | S3 + Snowflake | Raw archive (data lake) + structured rows (data warehouse) |
| Orchestration | `docker-compose.yaml` | Wires all four services; single entrypoint for the pipeline |
| Config | `.env.example` | All secrets externalized — no credentials in source |

## Quick Start

```bash
git clone https://github.com/Samik7hos0/streaming-pipeline.git
cd streaming-pipeline
cp .env.example .env
# Fill in ALPHA_VANTAGE_KEY, AWS credentials, and Snowflake credentials in .env

docker compose up
```

Verify data is flowing into Snowflake:

```sql
SELECT SYMBOL, COUNT(*) AS RECORDS,
       MIN(TRADING_DAY) AS FIRST_SEEN,
       MAX(PROCESSED_AT) AS LAST_PROCESSED
FROM DE_GRIND.STREAMING.STOCK_TICKS
GROUP BY SYMBOL
ORDER BY SYMBOL;
```

## Live Pipeline Evidence

Query result after running the pipeline — 4 stocks processed, 25 records written to `STREAMING.STOCK_TICKS`:

| SYMBOL | RECORDS | FIRST_SEEN | LAST_PROCESSED |
|---|---|---|---|
| HDFCBANK.BSE | 7 | 2025-06-04 | 2025-06-04 18:42:31 |
| INFY.BSE | 6 | 2025-06-04 | 2025-06-04 18:42:31 |
| RELIANCE.BSE | 6 | 2025-06-04 | 2025-06-04 18:42:31 |
| TCS.BSE | 6 | 2025-06-04 | 2025-06-04 18:42:31 |

S3 prefix `streaming/stock_prices/` simultaneously received raw JSON partitioned by ingestion time. Both sinks written in a single `foreachBatch` invocation — no double-read of Kafka.

## Key Engineering Decisions

**Kafka partitioning by symbol key**
The stock symbol is used as the Kafka message key. Kafka's default partitioner hashes the key to a partition — so RELIANCE always lands in Partition 0, TCS in Partition 1, and so on. This preserves per-symbol time-series ordering, which matters when downstream consumers need to process a stock's price history in sequence.

**`foreachBatch` — dual-sink in one processing step**
Rather than running two separate streaming queries (which would require two checkpoints and double the Kafka reads), `foreachBatch` calls a custom function once per micro-batch. That function writes to S3 and Snowflake in the same invocation. Each sink has its own `try/except` — if Snowflake is temporarily unavailable, S3 still receives the data.

**Exactly-once semantics via checkpointing**
After every successful batch, Spark writes the Kafka offset to a checkpoint directory. On restart, it reads the checkpoint and resumes from exactly the last committed offset — no data loss, no duplicates. This is the standard pattern for fault-tolerant streaming in production.

**Dual-sink data lakehouse pattern**
S3 stores raw immutable JSON — the source of truth that can be reprocessed at any time without re-hitting the API. Snowflake stores structured, queryable rows for analytics. This separation follows the data lakehouse architecture used by fintech DE teams at companies like Razorpay, CRED, and Groww.

**Containerized for reproducibility**
The entire pipeline — Zookeeper, Kafka, Python producer, Spark consumer — runs via Docker Compose. Any engineer can clone the repo, add credentials, and run `docker compose up` to get a fully working streaming pipeline. No manual environment setup.

**Rate-limit aware producer**
Alpha Vantage's free tier allows 5 API calls per minute. The producer waits 13 seconds between each of the 5 stocks — completing one full cycle in 65 seconds, safely under the limit. In production this would be replaced with a websocket feed or premium API tier for true real-time tick data.

## Related Project

This pipeline is Part 2 of a two-project DE portfolio. [Part 1 — Batch ELT Pipeline](https://github.com/Samik7hos0/market-pipeline) covers the batch side — the same BSE stocks, daily schedule, dbt transformations, and Airflow orchestration. Together they demonstrate both paradigms of modern data engineering: batch and streaming, on the same domain.
