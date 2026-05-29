"""
producer.py — Apache Kafka Producer for Social Media Stream Ingestion

Project: Real-Time Opinion Mining at Scale Using Distributed Processing
Team: Taha Naeem, Suleman Ahmad, Adil Hayat
Term: 2025-26

This module reads a local CSV dataset containing social media comments,
serialises each row into a JSON packet, and publishes the packets to a
Kafka topic named 'social_sentiment' running on localhost:9092. An
explicit 200 ms delay is introduced between messages to simulate the
arrival velocity of a real-time social media firehose.

Dependencies:
    - kafka-python >= 2.0.2
    - Python >= 3.8
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Logging configuration — all output goes to both console and a rotating
# log file for auditability in a production pipeline.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("producer_audit.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("Producer")

# ---------------------------------------------------------------------------
# External imports — the Kafka producer client.
# ---------------------------------------------------------------------------
try:
    from kafka import KafkaProducer
except ImportError as exc:
    logger.error(
        "The 'kafka-python' library is not installed. "
        "Run: pip install kafka-python"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants & configuration — all tunable parameters are gathered here so
# that switching to a different broker or topic requires no code changes.
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
KAFKA_TOPIC: str = "social_sentiment"
CSV_INPUT_PATH: str = "social_data.csv"
SEND_DELAY_SECONDS: float = 0.2  # 200 ms between messages
MAX_RECORDS: int | None = None  # set to an integer for testing with a subset


def build_kafka_producer() -> KafkaProducer:
    """
    Create and return a KafkaProducer instance configured with JSON
    serialisation and acknowledges from all in-sync replicas for
    durability.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",  # wait for all ISR replicas to acknowledge
        retries=3,
        max_in_flight_requests_per_connection=1,  # ensures ordering
        linger_ms=10,
        batch_size=32768,
    )


def expected_columns(row: dict) -> bool:
    """
    Validate that the row dictionary contains the three mandatory keys:
    'id', 'timestamp', and 'tweet_text'. Returns True if all are present
    and non-empty (id should be castable to str, timestamp should exist).
    """
    required = {"id", "timestamp", "tweet_text"}
    if not required.issubset(row.keys()):
        missing = required - row.keys()
        logger.warning("Row skipped — missing columns: %s", missing)
        return False
    if not row.get("tweet_text", "").strip():
        logger.warning("Row skipped — empty tweet_text field.")
        return False
    return True


def normalise_timestamp(raw_ts: str) -> str:
    """
    Attempt to parse an arbitrary timestamp string and return an ISO-8601
    representation. If parsing fails, the current UTC timestamp is used as
    a fallback so the pipeline never stalls on dirty data.
    """
    if not raw_ts or raw_ts.strip() == "":
        return datetime.now(timezone.utc).isoformat()

    # Try several common social-media timestamp formats.
    fmt_candidates = [
        "%a %b %d %H:%M:%S %Z %Y",       # "Mon Apr 06 22:19:45 PDT 2009"
        "%a %b %d %H:%M:%S %z %Y",        # with numeric timezone
        "%Y-%m-%dT%H:%M:%S%z",            # ISO-8601
        "%Y-%m-%d %H:%M:%S",              # SQL-style
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
    ]
    for fmt in fmt_candidates:
        try:
            parsed = datetime.strptime(raw_ts.strip(), fmt)
            return parsed.isoformat()
        except ValueError:
            continue

    logger.info("Could not parse timestamp '%s'; falling back to UTC now.", raw_ts)
    return datetime.now(timezone.utc).isoformat()


def stream_csv_to_kafka(producer: KafkaProducer, csv_path: str) -> int:
    """
    Open *csv_path* with a DictReader, iterate over every row, validate,
    serialise to JSON, and publish to the configured Kafka topic. Returns
    the total number of successfully published messages.

    The function implements explicit rate-limiting (*SEND_DELAY_SECONDS*)
    and logs every 1 000th message to indicate liveliness.
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(
            f"Input CSV file not found at expected path: {csv_path}"
        )

    published_count: int = 0
    # Detect encoding — social datasets are frequently encoded as latin-1
    # or utf-8 with BOM.
    encodings_to_try = ["utf-8", "utf-8-sig", "latin-1", "iso-8859-1"]

    for enc in encodings_to_try:
        try:
            f = open(csv_path, mode="r", encoding=enc, newline="")
            reader = csv.DictReader(f)
            # Force-read the first row to validate encoding
            first_row = next(reader, None)
            if first_row is not None:
                break  # encoding worked
            f.close()
        except (UnicodeDecodeError, UnicodeError):
            if f is not None:
                f.close()
            continue
    else:
        raise RuntimeError(
            f"Unable to decode {csv_path} with any of {encodings_to_try}."
        )

    logger.info(
        "Opened %s with encoding=%s. Streaming to topic '%s' on %s ...",
        csv_path, enc, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS,
    )

    # Re-process the first row since next() consumed it above.
    rows = [first_row] + list(reader)

    for idx, row in enumerate(rows, start=1):
        if MAX_RECORDS is not None and idx > MAX_RECORDS:
            logger.info("Reached MAX_RECORDS limit (%d). Stopping.", MAX_RECORDS)
            break

        # ---------------------------------------------------------------
        # Validation step — skip malformed rows gracefully.
        # ---------------------------------------------------------------
        if not expected_columns(row):
            continue

        # ---------------------------------------------------------------
        # Build the message payload with normalised fields.
        # ---------------------------------------------------------------
        payload = {
            "id": str(row["id"]).strip(),
            "timestamp": normalise_timestamp(row["timestamp"]),
            "tweet_text": row["tweet_text"].strip(),
        }

        try:
            future = producer.send(KAFKA_TOPIC, value=payload)
            # Block briefly to catch serialisation / broker errors early.
            future.get(timeout=5)
            published_count += 1
        except Exception as exc:
            logger.error(
                "Failed to send message at CSV line %d: %s", idx, exc
            )
            continue

        # ---------------------------------------------------------------
        # Liveliness log every 1 000 records.
        # ---------------------------------------------------------------
        if published_count % 1000 == 0:
            logger.info(
                "Published %d messages so far (last ID: %s).",
                published_count, payload["id"],
            )

        # ---------------------------------------------------------------
        # Rate-limiting — sleep to simulate real-time ingestion velocity.
        # ---------------------------------------------------------------
        time.sleep(SEND_DELAY_SECONDS)

    return published_count


def main() -> None:
    """
    Entry point: initialises the Kafka producer, begins streaming the CSV,
    and reports final statistics.
    """
    logger.info("=" * 60)
    logger.info("Producer started at %s", datetime.now(timezone.utc).isoformat())
    logger.info("=" * 60)

    try:
        producer = build_kafka_producer()
        total = stream_csv_to_kafka(producer, CSV_INPUT_PATH)
    except FileNotFoundError as fnf:
        logger.critical("Fatal: %s", fnf)
        sys.exit(1)
    except Exception as exc:
        logger.critical("Unhandled exception in main: %s", exc)
        sys.exit(2)
    finally:
        if "producer" in locals():
            producer.close(timeout=10)
            logger.info("Kafka producer connection closed.")

    logger.info(
        "Streaming complete. Published %d messages to '%s'.",
        total, KAFKA_TOPIC,
    )
    logger.info("Producer finished at %s", datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
