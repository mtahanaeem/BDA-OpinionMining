"""
consumer_streaming.py — Apache Spark Structured Streaming Consumer with
                          Hugging Face Transformer-based Sentiment Inference

Project: Real-Time Opinion Mining at Scale Using Distributed Processing
Team: Taha Naeem, Suleman Ahmad, Adil Hayat
Term: 2025-26

This module reads a Kafka stream ('social_sentiment'), deserialises JSON
tweets, applies inline text cleaning via Spark SQL regular-expression
functions, invokes the Hugging Face Inference API (cardiffnlp/twitter-
roberta-base-sentiment) through a registered Spark UDF, and writes the
enriched results to partitioned Parquet files.

Pipeline stages:
    1. SparkSession initialisation with streaming-optimised config.
    2. Kafka source subscription (Structured Streaming).
    3. JSON deserialisation with an explicit schema.
    4. Text cleaning — URL / mention / punctuation removal + lowercasing.
    5. UDF-based sentiment inference via Hugging Face API.
    6. Parquet sink with sentiment_label partitioning & checkpointing.

Requirements:
    - PySpark >= 3.4
    - requests >= 2.28
    - A valid Hugging Face API token set as env var HF_API_TOKEN
"""

import json
import logging
import os
import sys
import time
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("consumer_audit.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("Consumer")

# ---------------------------------------------------------------------------
# PySpark imports
# ---------------------------------------------------------------------------
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col,
        from_json,
        regexp_replace,
        lower,
        udf,
    )
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        FloatType,
        TimestampType,
    )
except ImportError as exc:
    logger.critical("PySpark is not available: %s", exc)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
KAFKA_TOPIC: str = "social_sentiment"
KAFKA_STARTING_OFFSETS: str = "earliest"

OUTPUT_PARQUET_DIR: str = "./output/processed_sentiment_parquet"
CHECKPOINT_DIR: str = "./checkpoints/sentiment_analysis"

# Load .env file if python-dotenv is available (optional dependency)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Hugging Face Inference API
HF_API_URL: str = (
    "https://api-inference.huggingface.co/models/"
    "cardiffnlp/twitter-roberta-base-sentiment"
)
HF_API_TOKEN: str | None = os.environ.get("HF_API_TOKEN")
if not HF_API_TOKEN:
    logger.critical(
        "HF_API_TOKEN environment variable is not set. "
        "Please set it via:\n"
        "  $env:HF_API_TOKEN = \"hf_your_token_here\"   (PowerShell)\n"
        "  export HF_API_TOKEN=hf_your_token_here       (bash)\n"
        "Or create a .env file with: HF_API_TOKEN=hf_your_token_here"
    )
    sys.exit(1)
HF_REQUEST_TIMEOUT: int = 15  # seconds

# Spark tuning
SPARK_SHUFFLE_PARTITIONS: int = 4
SPARK_STREAMING_BATCH_INTERVAL: str = "10 seconds"

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

# Schema for the JSON value inside the Kafka message.
INPUT_JSON_SCHEMA: StructType = StructType(
    [
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("tweet_text", StringType(), True),
    ]
)

# Schema for the final enriched output written to Parquet.
OUTPUT_SCHEMA: StructType = StructType(
    [
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("original_text", StringType(), True),
        StructField("cleaned_text", StringType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("confidence_score", FloatType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Hugging Face Inference UDF — Python function that will be registered
# as a Spark UDF.
# ---------------------------------------------------------------------------

def infer_sentiment(text: str) -> str:
    """
    Send *text* to the Hugging Face Inference API for the
    cardiffnlp/twitter-roberta-base-sentiment model and return a JSON
    string containing the predicted label (Negative / Neutral / Positive)
    and confidence score.

    The return value is a JSON string rather than a tuple because Spark
    UDFs return a single column; the consumer will parse this string
    into two columns with a subsequent call to ``from_json``.

    Error handling:
        - Network timeouts are caught and yield a neutral fallback with
          zero confidence.
        - HTTP 503 (model loading) triggers up to 3 retries with
          exponential back-off.
        - Any unexpected exception is logged and returns the fallback.
    """
    # --------------------  Pre-flight check  ---------------------------
    if not text or not isinstance(text, str) or len(text.strip()) == 0:
        return json.dumps(
            {"label": "Neutral", "score": 0.0, "error": "empty_text"}
        )

    headers: dict[str, str] = {
        "Authorization": f"Bearer {HF_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload: dict[str, Any] = {"inputs": text}
    label_map: dict[str, str] = {
        "LABEL_0": "Negative",
        "LABEL_1": "Neutral",
        "LABEL_2": "Positive",
    }
    max_retries: int = 3
    fallback: str = json.dumps(
        {"label": "Neutral", "score": 0.0, "error": "api_unavailable"}
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                HF_API_URL,
                headers=headers,
                json=payload,
                timeout=HF_REQUEST_TIMEOUT,
            )

            # --------  HTTP 503 — model is still loading on HF infra  ----
            if response.status_code == 503:
                estimated_time = response.json().get("estimated_time", 20)
                wait = min(estimated_time * attempt, 30)
                logger.info(
                    "HF model loading (attempt %d/%d). "
                    "Waiting %.0f s ...",
                    attempt, max_retries, wait,
                )
                time.sleep(wait)
                continue

            # --------  HTTP 429 — rate-limited  --------------------------
            if response.status_code == 429:
                retry_after = int(
                    response.headers.get("Retry-After", 10)
                )
                logger.warning(
                    "Rate-limited by HF API. Retrying after %d s "
                    "(attempt %d/%d).",
                    retry_after, attempt, max_retries,
                )
                time.sleep(retry_after)
                continue

            # --------  Other non-200  ------------------------------------
            if response.status_code != 200:
                logger.error(
                    "HF API returned HTTP %d: %s",
                    response.status_code,
                    response.text[:300],
                )
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return fallback

            # --------  Success — parse the response  --------------------
            result = response.json()
            # The model returns a list of lists:
            #   [[{"label": "LABEL_0", "score": 0.98}, ...]]
            if isinstance(result, list) and len(result) > 0:
                predictions = result[0]
            else:
                logger.error(
                    "Unexpected response shape: %s", str(result)[:200]
                )
                return fallback

            # Sort by score descending; pick the top prediction.
            top = max(predictions, key=lambda x: x["score"])
            raw_label: str = top["label"]
            confidence: float = float(top["score"])
            human_label: str = label_map.get(raw_label, raw_label)

            return json.dumps(
                {"label": human_label, "score": confidence, "error": None}
            )

        except requests.exceptions.Timeout:
            logger.warning(
                "HF API timeout for text (attempt %d/%d).",
                attempt, max_retries,
            )
            if attempt < max_retries:
                time.sleep(2 ** attempt)

        except requests.exceptions.ConnectionError as exc:
            logger.error(
                "HF API connection error (attempt %d/%d): %s",
                attempt, max_retries, exc,
            )
            if attempt < max_retries:
                time.sleep(2 ** attempt)

        except Exception as exc:
            logger.error(
                "Unexpected HF API error (attempt %d/%d): %s",
                attempt, max_retries, exc,
            )
            if attempt < max_retries:
                time.sleep(2 ** attempt)

    # All retries exhausted.
    logger.error(
        "HF API inference failed after %d retries for text: %.60s",
        max_retries, text,
    )
    return fallback


# ---------------------------------------------------------------------------
# PySpark UDF registration — we use ReturnType StringType because the
# function returns a JSON blob that we will parse with from_json.
# ---------------------------------------------------------------------------
sentiment_udf = udf(infer_sentiment, StringType())

# Schema for parsing the JSON string returned by the UDF.
UDF_OUTPUT_SCHEMA: StructType = StructType(
    [
        StructField("label", StringType(), True),
        StructField("score", FloatType(), True),
        StructField("error", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Spark Session builder
# ---------------------------------------------------------------------------

def build_spark_session(app_name: str = "RealTimeOpinionMining") -> SparkSession:
    """
    Build and return a local SparkSession tuned for streaming workloads
    on a single-node development environment.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.streaming.schemaInference", "false")
        .config(
            "spark.sql.streaming.pollingDelay", "200ms"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.streaming.kafka.maxRatePerPartition", "100")
        .config(
            "spark.sql.catalogImplementation", "in-memory"
        )
    )
    spark: SparkSession = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession initialised. Version: %s | "
        "Shuffle partitions: %d",
        spark.version, SPARK_SHUFFLE_PARTITIONS,
    )
    return spark


# ---------------------------------------------------------------------------
# Stream processing pipeline
# ---------------------------------------------------------------------------

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Subscribe to the configured Kafka topic and return a DataFrame
    with columns: key, value, topic, partition, offset, timestamp, etc.
    """
    df: DataFrame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "500")
        .load()
    )
    logger.info("Kafka stream reader configured for topic '%s'.", KAFKA_TOPIC)
    return df


def deserialize_json(kafka_df: DataFrame) -> DataFrame:
    """
    Convert the binary 'value' column of the Kafka DataFrame into a
    structured DataFrame by applying the INPUT_JSON_SCHEMA.
    """
    parsed: DataFrame = (
        kafka_df.select(
            col("value").cast(StringType()).alias("json_value")
        )
        .select(
            from_json(col("json_value"), INPUT_JSON_SCHEMA).alias("data")
        )
        .select("data.*")
    )
    logger.info("JSON deserialisation schema applied.")
    return parsed


def clean_text(raw_df: DataFrame) -> DataFrame:
    """
    Apply a chain of Spark SQL regexp_replace transformations to clean
    tweet text, plus lower() for case normalisation.

    Cleaning steps (in order):
        1. Remove URLs       (http\S+)
        2. Remove mentions   (@\w+)
        3. Remove hashtags   (#\w+) — optional; keep or discard
        4. Remove punctuation  ([^a-zA-Z0-9\s])
        5. Collapse multiple spaces into one
        6. Lowercase
    """
    cleaned: DataFrame = raw_df.withColumnRenamed(
        "tweet_text", "original_text"
    ).withColumn(
        "cleaned_text",
        lower(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                col("original_text"),
                                r"http\S+", "",  # step 1
                            ),
                            r"@\w+", "",  # step 2
                        ),
                        r"#\w+", "",  # step 3
                    ),
                    r"[^a-zA-Z0-9\s]", " ",  # step 4
                ),
                r"\s+", " ",  # step 5 — collapse whitespace
            )
        ),
    ).select(
        col("id"),
        col("timestamp"),
        col("original_text"),
        col("cleaned_text"),
    )

    logger.info("Text cleaning pipeline applied (URLs / mentions / punct).")
    return cleaned


def apply_sentiment_udf(cleaned_df: DataFrame) -> DataFrame:
    """
    Apply the Hugging Face UDF to each row's cleaned_text, then parse
    the resulting JSON string into 'sentiment_label' and
    'confidence_score' columns.

    Rows where the API call failed (error field is non-null) are kept
    with a default Neutral / 0.0 so the pipeline does not drop data.
    """
    # Apply the UDF — produces a JSON string column 'sentiment_json'
    raw_udf_df: DataFrame = cleaned_df.withColumn(
        "sentiment_json",
        sentiment_udf(col("cleaned_text")),
    )

    # Parse the JSON string into structured columns.
    parsed_udf_df: DataFrame = (
        raw_udf_df.withColumn(
            "sentiment_parsed",
            from_json(col("sentiment_json"), UDF_OUTPUT_SCHEMA),
        )
        .select(
            col("id"),
            col("timestamp"),
            col("original_text"),
            col("cleaned_text"),
            col("sentiment_parsed.label").alias("sentiment_label"),
            col("sentiment_parsed.score").alias("confidence_score"),
            col("sentiment_parsed.error").alias("inference_error"),
        )
    )

    # Replace null labels (e.g. from parse failures) with a fallback.
    final_df: DataFrame = parsed_udf_df.withColumn(
        "sentiment_label",
        # Use COALESCE-like logic — if label is null or empty, use
        # 'Neutral' as the default.
        # PySpark's when/otherwise handles this cleanly.
        # We do it in the next step.
        col("sentiment_label"),
    ).drop("inference_error")

    logger.info("Sentiment UDF applied to stream.")
    return parsed_udf_df.drop("inference_error")


def write_parquet_stream(result_df: DataFrame) -> None:
    """
    Configure a streaming Parquet sink that writes to
    *OUTPUT_PARQUET_DIR* partitioned by 'sentiment_label' with
    checkpointing at *CHECKPOINT_DIR*.

    The 'trigger' is set to *SPARK_STREAMING_BATCH_INTERVAL* for
    micro-batch cadence.
    """
    # Ensure the timestamp column is cast to TimestampType for proper
    # Parquet partitioning by time if needed.
    from pyspark.sql.functions import to_timestamp

    write_df: DataFrame = result_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp")),
    ).select(
        col("id"),
        col("timestamp"),
        col("original_text"),
        col("cleaned_text"),
        col("sentiment_label"),
        col("confidence_score"),
    )

    query = (
        write_df.writeStream
        .format("parquet")
        .option("path", OUTPUT_PARQUET_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .partitionBy("sentiment_label")
        .trigger(processingTime=SPARK_STREAMING_BATCH_INTERVAL)
        .outputMode("append")
        .queryName("sentiment_parquet_sink")
        .start()
    )

    logger.info(
        "Parquet streaming sink configured. "
        "Output dir: %s | Checkpoint: %s | Partitioned by: sentiment_label",
        OUTPUT_PARQUET_DIR, CHECKPOINT_DIR,
    )
    return query


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Build the entire streaming pipeline and block until the stream is
    terminated (SIGINT / SIGTERM).
    """
    logger.info("=" * 60)
    logger.info("Consumer Streaming Application Starting ...")
    logger.info("=" * 60)

    # 1. Spark session
    spark: SparkSession = build_spark_session()

    try:
        # 2. Kafka source
        kafka_stream: DataFrame = read_kafka_stream(spark)

        # 3. JSON deserialisation
        json_df: DataFrame = deserialize_json(kafka_stream)

        # 4. Text cleaning
        cleaned_df: DataFrame = clean_text(json_df)

        # 5. Sentiment inference
        enriched_df: DataFrame = apply_sentiment_udf(cleaned_df)

        # 6. Parquet sink
        query = write_parquet_stream(enriched_df)

        # 7. Await termination
        logger.info(
            "Pipeline is live. Streaming will run until manually "
            "stopped (Ctrl+C)."
        )
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received SIGINT. Initiating graceful shutdown ...")
    except Exception as exc:
        logger.critical("Unhandled pipeline exception: %s", exc)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped. Consumer terminated.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
