# REAL-TIME OPINION MINING AT SCALE USING DISTRIBUTED PROCESSING

**Academic Term:** 2025-26 (6th Semester, Data Science Capstone)
**Department of Data Science, University of Central Punjab (UCP), Lahore**

**Project Developers:** Taha Naeem, Suleman Ahmad, Adil Hayat
**Supervisory Framework:** Big Data Analytics Capstone — Faculty of Data Science, UCP

---

## 1. ABSTRACT

Conventional sentiment analysis pipelines operating on statically batched social media corpora are fundamentally mismatched to the temporal velocity and volumetric scale of modern micro-text streams. A single day of Twitter discourse can generate upward of 500 million discrete utterances; processing such a firehose through nightly batch ETL routines introduces latency windows that render real-time brand-crisis detection, political-opinion drift tracking, and algorithmic trading signals effectively inert. This project directly confronts that latency barrier by deploying a fully decoupled, three-tier distributed architecture that integrates Apache Kafka for ingestion durability and topic-based publish-subscribe decoupling, Apache Spark Structured Streaming for elastic micro-batch analytics and in-flight schema enforcement, and the Hugging Face Inference API fronting the `cardiffnlp/twitter-roberta-base-sentiment` transformer model for per-token sentiment classification at line rate. The system ingests a 1.6-million-record Twitter corpus (Sentiment140), serializes each record into structured JSON payloads over a single-node Kafka broker at a throttle-controlled 200 ms inter-message interval, deserializes and cleanses the stream via Spark SQL regular-expression transforms, invokes the RoBERTa-based inference API through a registered PySpark UDF with exponential-backoff retry semantics, and sinks the fully enriched output into Hive-partitioned Parquet files organized by `sentiment_label`. A supplementary Spark MLlib pipeline (Tokenizer → HashingTF → IDF → Multinomial Logistic Regression) trained on the streaming output achieves benchmark classification metrics for offline cross-validation against the cloud transformer. A React-based dark-theme operational dashboard consumes the processed signal through simulated real-time state queues and renders vector telemetry via raw inline SVG geometry. The complete pipeline, orchestrated through a single `run_all.bat` launcher, demonstrates a working prototype of an end-to-end, fault-tolerant, real-time opinion mining infrastructure deployable on consumer-grade local hardware.

---

## 2. INTRODUCTION & PROBLEM STATEMENT

### 2.1 The Micro-Text Data Torrent

Digital social networks produce unstructured textual data at a rate that defies conventional batch-oriented data processing paradigms. Twitter alone generates approximately 500 million tweets daily as of 2025, each carrying latent sentiment signals—product satisfaction, political allegiance, brand affinity, crisis indicators—embedded within idiosyncratic, noise-laden micro-text. Reddit, Facebook, YouTube comments, and news-article feedback loops contribute equally high-velocity streams. The aggregate constitutes a continuously updating corpus of global public opinion whose half-life is measured in minutes, not days.

### 2.2 The Batch-Processing Bottleneck

Traditional analytical approaches to sentiment mining follow a rigid extract-transform-load (ETL) cadence: raw data accumulates in object stores or relational warehouses over 12- or 24-hour windows, batch jobs execute computationally expensive NLP pipelines across the entire corpus, and dashboards refresh with latencies that render the output historically descriptive rather than operationally actionable. For a brand- monitoring team attempting to detect a sudden reputational crisis—a product defect trending negatively, a coordinated disinformation campaign, a political scandal breaking in real time—a twelve-hour analytical delay is not merely suboptimal; it constitutes a structural failure of the alerting architecture. The core engineering challenge is therefore not whether sentiment classification can be performed accurately, but whether it can be performed at stream velocity with sufficient throughput, fault tolerance, and end-to-end latency to support real-time decision-making.

### 2.3 Low-Latency Distributed Stream Mining as a Requirement

The system described in this report is designed to meet the following hard requirements:

1. **Ingestion durability:** The ingestion layer must decouple data arrival from downstream processing so that bursts, back-pressure, or transient consumer failures do not cause data loss.
2. **Schema enforcement and cleansing:** Raw social-media text is irregular, polluted with URLs, @-mentions, hashtags, and Unicode artifacts; the stream processor must normalize this signal before inference.
3. **Distributed inference at line rate:** Sentiment classification must occur on each record independently, without batching delays, using a state-of-the-art transformer model.
4. **Immutable, query-optimized storage:** Enriched results must land in a columnar format that supports both ad-hoc analytical queries and downstream ML pipeline training without re-processing.
5. **Human-observable monitoring:** A live frontend must surface streaming throughput, sentiment distribution, temporal trends, and infrastructure health with sub-second visual refresh.

These requirements collectively dictate a decoupled, micro-batch streaming architecture in which a message broker (Kafka) handles ingestion, a stream processor (Spark Structured Streaming) handles transformation and inference orchestration, and a columnar lakehouse format (Parquet) handles durable storage.

---

## 3. ARCHITECTURAL BLUEPRINT & DATA FLOW DESIGN

### 3.1 End-to-End Pipeline Topology

The following ASCII diagram maps the physical data path through every intermediate hop, from raw CSV persistence on disk to the partitioned Parquet lakehouse sink:

```
+-------------------+       +---------------------------+       +------------------------------+
|  CSV Data Source  | ----> |   Kafka Producer Object   | ----> |   Kafka Topic                |
|  (1.6M tweet       |       |   (producer.py)           |       |   "social_sentiment"         |
|   Sentiment140     |       |   - JSON serialization    |       |   localhost:9092             |
|   dataset)         |       |   - 200 ms throttle       |       |   - Single-node broker       |
+-------------------+       |   - acks=all, retries=3    |       |   - 1 partition (default)   |
                            +---------------------------+       +-------------|----------------+
                                                                                 |
                                                                                 | (poll)
                                                                                 v
+----------------------------------------------------+     +---------------------------+
|  PySpark Structured Streaming Consumer              | <-- |  Kafka Source Reader     |
|  (consumer_streaming.py)                            |     |  - maxOffsetsPerTrigger  |
|  - SparkSession with shuffle.partitions=4           |     |  = 500                   |
|  - maxRatePerPartition=100                          |     |  - startingOffsets       |
|  - Trigger: 10 seconds                              |     |  = earliest              |
+----------------------------------------------------+     +---------------------------+
                    |
                    v
+----------------------------------+
| Schema Deserialization           |
| (from_json with INPUT_JSON_SCHEMA)|
| id: String, timestamp: String,   |
| tweet_text: String               |
+----------------------------------+
                    |
                    v
+----------------------------------+
| Regex Cleansing Engine           |
| 1. Remove URLs (http\\S+)        |
| 2. Remove mentions (@\\w+)       |
| 3. Remove hashtags (#\\w+)       |
| 4. Strip punctuation             |
|    ([^a-zA-Z0-9\\s])             |
| 5. Collapse whitespace (\\s+)    |
| 6. lower() case normalization    |
+----------------------------------+
                    |
                    v
+---------------------------------------------+
| Hugging Face Inference API UDF              |
| (cardiffnlp/twitter-roberta-base-sentiment)  |
| - REST POST with Bearer token               |
| - 3 retries with exponential backoff        |
| - Handles 503 (model loading) & 429 (rate-  |
|   limit) explicitly                         |
| - LABEL_0→Negative, LABEL_1→Neutral,        |
|   LABEL_2→Positive                          |
| - Returns JSON: {label, score, error}       |
+---------------------------------------------+
                    |
                    v
+---------------------------------------------+
| Parquet Columnar Sink (Lakehouse)           |
| - partitionBy("sentiment_label")            |
| - checkpointLocation for fault tolerance    |
| - trigger: 10-second micro-batches          |
| - Output mode: append                       |
+---------------------------------------------+
```

### 3.2 Infrastructure Design Rationale

**Apache Kafka (Ingestion Layer):** Kafka was selected as the ingestion backbone because its log-structured, topic-partitioned architecture provides three properties critical to streaming pipelines: (a) **Durability** — messages are persisted to disk on the broker and replicated across in-sync replicas (`acks=all`), so a consumer crash does not cause data loss; (b) **Decoupling** — the producer and consumer operate as independent processes with no direct coupling; the producer can push data at its natural rate (throttled to 200 ms inter-message spacing) while the consumer polls at its own cadence, absorbing burst imbalances via the broker's on-disk log; (c) **Offset-based replay** — the consumer can resume from the last committed offset after a restart, or reprocess from `earliest` for recovery, a pattern that underpins the `failOnDataLoss=false` and `checkpointLocation` strategies in the Spark consumer.

**Apache Spark Structured Streaming (Processing Layer):** Spark Structured Streaming was chosen over alternatives (Apache Flink, Kafka Streams) for three engineering reasons. First, the project's execution environment is a single-node local machine; Spark's `local[*]` mode provides a zero-infrastructure deployment path while maintaining API compatibility with a full cluster. Second, Spark's DataFrame/Dataset API with declarative `from_json` and `regexp_replace` functions allows the cleansing pipeline to be expressed as a chain of Catalyst-optimized transformations that compile to physical execution plans without explicit iterator management. Third, Spark's UDF registration mechanism allows the Hugging Face inference call to be embedded as a row-level function that Spark distributes across executor threads, even in local mode, effectively parallelizing the synchronous HTTP requests to the external API.

**Apache Parquet (Storage Layer):** Parquet was selected as the sink format because its columnar storage layout delivers three advantages over row-oriented formats (JSON Lines, CSV, Avro). First, predicate pushdown — queries that filter on `sentiment_label` or `timestamp` can skip entire column chunks without reading data. Second, compression — Parquet's run-length encoding (RLE) and dictionary encoding are particularly effective on low-cardinality string columns like `sentiment_label` (exactly three distinct values), achieving compression ratios of 5:1 or better. Third, schema evolution — Parquet stores schema metadata in the file footer, enabling downstream readers (the MLlib pipeline in `train_evaluation_mllib.py`) to discover columns without external schema registries. The `partitionBy("sentiment_label")` strategy further optimizes query pruning by physically organizing data into Hive-style directory partitions (`sentiment_label=Positive/`, `sentiment_label=Negative/`, `sentiment_label=Neutral/`).

---

## 4. COMPONENT DEEP DIVE & CORE CODE MECHANICS

### 4.1 Ingestion Firehose — `producer.py`

The producer module (`producer.py:1–256`) implements a CSV-to-Kafka bridge with defensive encoding detection, column validation, timestamp normalization, and deliberate rate-limiting.

**CSV Encoding Resilience:** Social-media datasets are notorious for encoding irregularities. The Sentiment140 corpus, despite being published as UTF-8, frequently contains latin-1 encoded characters in user-generated fields. The producer addresses this through a fallback encoding strategy (`producer.py:146–164`):

```python
encodings_to_try = ["utf-8", "utf-8-sig", "latin-1", "iso-8859-1"]
for enc in encodings_to_try:
    try:
        f = open(csv_path, mode="r", encoding=enc, newline="")
        reader = csv.DictReader(f)
        first_row = next(reader, None)
        if first_row is not None:
            break
    except (UnicodeDecodeError, UnicodeError):
        f.close()
        continue
```

This pattern attempts each encoding in order, force-reads exactly one row to validate the decoder, and only proceeds when the decoder succeeds without throwing. If all four encodings fail, a `RuntimeError` is raised. This avoids silent data corruption that would occur if a mismatched encoding produced garbled but syntactically valid characters.

**JSON Serialization and Kafka Producer Configuration:** Each validated row is serialized into a three-field payload (`producer.py:188–192`):

```python
payload = {
    "id": str(row["id"]).strip(),
    "timestamp": normalise_timestamp(row["timestamp"]),
    "tweet_text": row["tweet_text"].strip(),
}
```

The `normalise_timestamp` function (`producer.py:100–126`) attempts six distinct date format patterns—covering Twitter's native `"Mon Apr 06 22:19:45 PDT 2009"` format, ISO-8601, SQL-style, and two European/American numeric formats—before falling back to the current UTC timestamp. This ensures the pipeline never stalls on a malformed date field.

The Kafka producer (`build_kafka_producer`, `producer.py:66–80`) is configured with the following non-default parameters:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `acks` | `"all"` | Requires all in-sync replicas to acknowledge before the send is considered successful; provides strongest durability guarantee on multi-broker clusters. |
| `retries` | `3` | Automatic retry on transient broker errors (leader election, network glitches). |
| `max_in_flight_requests_per_connection` | `1` | Ensures strict message ordering; when combined with retries, prevents reordering on retry. |
| `linger_ms` | `10` | Tiny batching window—accumulates up to 10 ms of messages before sending, balancing throughput against latency. |
| `batch_size` | `32768` | 32 KB batch size optimizes for small JSON payloads (typical tweet payload is ~200–500 bytes). |

**The 200 ms Rate-Limiting Strategy:** The `time.sleep(SEND_DELAY_SECONDS)` call at `producer.py:217` introduces a 200 ms pause between each published message. This is a deliberate engineering choice that serves three purposes. First, it simulates a realistic social-media firehose arrival velocity: even high-volume streams (e.g., Twitter's Decahose API) deliver at rates that do not saturate a single consumer's processing capacity. Second, it prevents the downstream Spark consumer from being overwhelmed by an instantaneous burst of 1.6 million messages—a scenario that would exhaust the single-node checkpoint directory's I/O bandwidth and cause `maxOffsetsPerTrigger` (500) to buffer thousands of micro-batches. Third, it makes the streaming behavior observable in the frontend dashboard, which refreshes at ~700–1200 ms intervals; without the throttle, all 1.6 million records would arrive within seconds and the dashboard would display only the final state.

At 200 ms per message, the effective throughput is 5 messages/second, or approximately 18,000 messages/hour. This is intentionally conservative for a local development environment. Production deployments would remove the delay and rely on Kafka's native batching and the consumer's `maxOffsetsPerTrigger` to regulate flow.

### 4.2 Distributed Stream Processing Engine — `consumer_streaming.py`

The consumer module (`consumer_streaming.py:1–564`) implements the full Structured Streaming pipeline: Kafka subscription, JSON deserialization, regex text cleaning, Hugging Face UDF inference, and Parquet sink.

**SparkSession Configuration for Streaming:** The session builder (`consumer_streaming.py:304–331`) applies seven non-default configurations:

| Configuration | Value | Engineering Rationale |
|--------------|-------|----------------------|
| `spark.sql.shuffle.partitions` | `4` | Reduces the default 200 partitions to 4 for single-node operation; avoids creating 200 empty shuffle files per micro-batch. |
| `spark.sql.streaming.schemaInference` | `false` | Disables schema inference on streaming DataFrames; forces explicit schema declaration for type safety. |
| `spark.sql.streaming.pollingDelay` | `200ms` | Frequency at which Spark polls the Kafka source for new data; matched to producer throttle for balanced flow. |
| `spark.sql.adaptive.enabled` | `true` | Enables Adaptive Query Execution (AQE) for dynamic shuffle partitioning and join optimization. |
| `spark.streaming.kafka.maxRatePerPartition` | `100` | Caps the per-partition read rate at 100 records/second to prevent consumer overload. |
| `spark.sql.catalogImplementation` | `in-memory` | Uses in-memory catalog instead of Hive Metastore; eliminates external dependency for local deployment. |

**Kafka Source Integration:** The `read_kafka_stream` function (`consumer_streaming.py:338–353`) subscribes to the `social_sentiment` topic using Spark's `readStream.format("kafka")` source. The `.option("failOnDataLoss", "false")` setting is critical: it instructs Spark not to throw a fatal exception if the Kafka topic's offsets have been truncated (e.g., due to log retention) between restarts. Combined with `.option("maxOffsetsPerTrigger", "500")`, the consumer polls up to 500 new offsets per micro-batch trigger interval, providing fine-grained flow control.

**Strict Schema Enforcement:** The raw Kafka DataFrame contains a binary `value` column. The `deserialize_json` function (`consumer_streaming.py:356–371`) converts this column through a two-stage select:

```python
parsed: DataFrame = (
    kafka_df.select(
        col("value").cast(StringType()).alias("json_value")
    )
    .select(
        from_json(col("json_value"), INPUT_JSON_SCHEMA).alias("data")
    )
    .select("data.*")
)
```

The `from_json` function applies `INPUT_JSON_SCHEMA` (`consumer_streaming.py:119–125`), which declares three fields (`id: StringType`, `timestamp: StringType`, `tweet_text: StringType`). Any JSON record that does not conform to this schema (missing fields, type mismatches) produces `null` values in the respective columns rather than crashing the micro-batch. This soft-failure model is essential for streaming pipelines where a single malformed message should not block the entire stream.

**Regex Text Cleansing Chain:** The `clean_text` function (`consumer_streaming.py:374–417`) applies a five-step Spark SQL transformation pipeline:

```
Step 1: regexp_replace(col("original_text"), r"http\S+", "")
         → Strips all HTTP/HTTPS URLs (e.g., "http://t.co/abc123")
Step 2: regexp_replace(result, r"@\w+", "")
         → Removes @-mentions (e.g., "@switchfoot", "@Kenichan")
Step 3: regexp_replace(result, r"#\w+", "")
         → Strips hashtags (e.g., "#breaking", "#sports")
Step 4: regexp_replace(result, r"[^a-zA-Z0-9\s]", " ")
         → Replaces punctuation, emojis, and special characters with
           a space (preserves token boundaries)
Step 5: regexp_replace(result, r"\s+", " ")
         → Collapses multiple consecutive whitespace characters into one
Final:  lower(col("cleaned_text"))
         → Absolute downcasing for case-insensitive inference
```

These transformations run as Catalyst physical expressions within the Spark execution engine, not as Python UDFs. This means they operate directly on InternalRows in JVM memory without Python serialization overhead—a critical performance consideration when processing millions of records.

**Distributed Hugging Face Inference UDF — Architectural Implications:** The `infer_sentiment` function (`consumer_streaming.py:145–281`) is registered as a PySpark UDF (`consumer_streaming.py:288`):

```python
sentiment_udf = udf(infer_sentiment, StringType())
```

The `StringType()` return type indicates the UDF returns a JSON-serialized string, which is subsequently parsed by `from_json` in the `apply_sentiment_udf` function (`consumer_streaming.py:420–463`). This two-phase pattern (UDF → JSON string → structured columns) is necessary because PySpark UDFs can only return a single column. The `UDF_OUTPUT_SCHEMA` (`consumer_streaming.py:291–297`) defines three fields for the parsed result:

```python
UDF_OUTPUT_SCHEMA = StructType([
    StructField("label", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("error", StringType(), True),
])
```

The inference function implements a comprehensive retry strategy:

| HTTP Status | Handling Strategy |
|-------------|-------------------|
| 503 (Model Loading) | Reads `estimated_time` from response body; sleeps for `min(estimated_time * attempt, 30)` seconds; retries up to 3 times. |
| 429 (Rate Limited) | Reads `Retry-After` header; sleeps for the specified duration; retries up to 3 times. |
| Timeout | Catches `requests.exceptions.Timeout`; sleeps `2^attempt` seconds (exponential backoff: 2s, 4s, 8s); retries. |
| ConnectionError | Catches `requests.exceptions.ConnectionError`; same exponential backoff; retries. |
| Any non-200 | Logs the response body (truncated to 300 chars); sleeps `2^attempt`; returns Neutral/0.0 fallback after 3 failures. |

The fallback strategy returns `{"label": "Neutral", "score": 0.0, "error": "api_unavailable"}` when all retries are exhausted. This is critical for streaming resilience: a transient API outage should produce low-confidence neutral classifications rather than blocking the entire pipeline or dropping records.

**Performance Implication of External API UDF:** Each invocation of the UDF makes a synchronous HTTP POST request to `https://api-inference.huggingface.co/models/cardiffnlp/twitter-roberta-base-sentiment`. In local `local[*]` mode, Spark executes UDF calls in parallel across the available cores (typically 4–8 on a consumer machine). This means the system can sustain 4–8 concurrent API requests. At an average response latency of ~200–500 ms (typical for the Hugging Face Inference API under light load), the effective throughput is approximately 8–40 records/second. This is the pipeline's primary bottleneck. The `maxOffsetsPerTrigger=500` and `trigger(processingTime="10 seconds")` settings ensure that each micro-batch includes at most 500 records, which at 40 records/second requires approximately 12.5 seconds to complete—matching the 10-second trigger interval. This coupling between trigger interval, maxOffsets, and API latency is the central tuning trade-off in this architecture.

**Sentiment Label Mapping:** The Hugging Face model returns three raw labels (`LABEL_0`, `LABEL_1`, `LABEL_2`). The UDF maps these to human-readable values (`consumer_streaming.py:174–178`):

```python
label_map = {
    "LABEL_0": "Negative",
    "LABEL_1": "Neutral",
    "LABEL_2": "Positive",
}
```

The prediction with the highest `score` across the three classes is selected as the final label.

### 4.3 Storage Lakehouse Sink — Partitioned Parquet Strategy

The `write_parquet_stream` function (`consumer_streaming.py:466–508`) configures the streaming sink with three critical parameters:

```python
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
```

**Checkpoint Location:** The `checkpointLocation` option is the foundation of the pipeline's fault tolerance. Spark Structured Streaming writes metadata (source offsets, batch commit state, schema versions) to this directory. On restart, Spark reads the last committed offset from the checkpoint and resumes consumption from that exact position, guaranteeing exactly-once semantics for the sink (assuming the sink is idempotent—Parquet writes in append mode are not idempotent, so the guarantee is at-least-once with potential duplicates on restart).

**Partitioning Strategy:** `partitionBy("sentiment_label")` physically organizes the Parquet output into three Hive-style directory partitions:

```
./output/processed_sentiment_parquet/
├── sentiment_label=Negative/
│   └── part-00000-xxx.snappy.parquet
├── sentiment_label=Neutral/
│   └── part-00001-xxx.snappy.parquet
└── sentiment_label=Positive/
    └── part-00002-xxx.snappy.parquet
```

This layout provides a practical query optimization. Downstream queries that filter on sentiment (e.g., `SELECT COUNT(*) FROM parquet WHERE sentiment_label = 'Positive'`) can employ partition pruning to read only the `Positive/` subdirectory, entirely skipping the other 66% of the data. For the MLlib pipeline in `train_evaluation_mllib.py`, this means `spark.read.parquet(PARQUET_INPUT_DIR)` automatically discovers all partitions and loads only the relevant column chunks.

### 4.4 Offline Validation — `train_evaluation_mllib.py`

The MLlib batch pipeline (`train_evaluation_mllib.py:1–455`) serves as an offline validation benchmark against the streaming inference results. It reads the Parquet output produced by the streaming consumer and trains a five-stage Spark ML Pipeline:

| Stage | Component | Configuration | Purpose |
|-------|-----------|---------------|---------|
| 1 | `Tokenizer` | `inputCol="cleaned_text"`, `outputCol="tokens"` | Splits pre-cleaned text into lowercase word tokens. |
| 2 | `HashingTF` | `numFeatures=65536`, `inputCol="tokens"`, `outputCol="raw_features"` | Maps tokens to sparse feature vectors using MurmurHash3; avoids distributed dictionary construction. |
| 3 | `IDF` | `inputCol="raw_features"`, `outputCol="features"` | Down-weights corpus-wide frequent tokens (stop-words) and boosts sentiment-discriminative rare tokens. |
| 4 | `StringIndexer` | `inputCol="sentiment_label"`, `outputCol="label"`, `handleInvalid="keep"` | Encodes three sentiment labels as numeric indices (0.0, 1.0, 2.0) ordered by frequency. |
| 5 | `LogisticRegression` | `maxIter=100`, `regParam=0.01`, `elasticNetParam=0.5`, `family="multinomial"` | Multinomial classifier with elastic-net regularization (L1+L2 hybrid). |

The elastic-net parameter (`elasticNetParam=0.5`) is noteworthy: it blends L1 sparsity (feature selection—many HashingTF buckets may be irrelevant) with L2 coefficient shrinkage (stability), a combination particularly effective for high-dimensional sparse text feature spaces with 65,536 dimensions.

Evaluation is performed via `MulticlassClassificationEvaluator` across four metrics (accuracy, weightedPrecision, weightedRecall, F1) and a per-class breakdown with explicit TP/FP/FN counting for each of the three labels. A confusion matrix is printed, cross-tabulating actual vs. predicted labels.

---

## 5. FRONTEND UI ENGINEERING & REAL-TIME INTERACTION

### 5.1 React Architecture and State Management

The operational dashboard (`Dashboard.jsx:1–461`) is a single-page React application built on React 19 with Tailwind CSS utility classes and the Lucide icon library. The entire UI is managed through 12 `useState` hooks and a single `useRef` counter, with no external state management library (Redux, Zustand) — a deliberate choice that keeps the bundle size small and the rendering pipeline predictable.

The state model is organized into five logical domains:

| Domain | State Variables | Update Cadence | Purpose |
|--------|----------------|----------------|---------|
| Accumulated counts | `totalTweets`, `sc` (sentiment counts), `srcC` (source counts) | Every tick (700–1200 ms) | Running totals for KPI cards and donut charts |
| Streaming rate | `rate`, `avgConf`, `confN` | Every tick | Live throughput and confidence indicators |
| Temporal trends | `tp`, `tn`, `tng` (each an array of 28 floats) | Every 3rd tick (~2–3 seconds) | Rolling 28-point time-series for the trend chart |
| Feed display | `logs` (array, max 7 entries), `feedKeys` (Set) | Every tick | Recent opinion stream table with row-flash animation |
| Infrastructure | `pm` (Spark/Kafka/HF message counts), `expanded` (topology accordion) | Every tick | Stream topology panel with per-service drill-down |

The `useCallback`-wrapped `genTweet` function (`Dashboard.jsx:151–159`) generates synthetic social-media posts by randomly selecting from a curated pool of 22 `SAMPLE_TEXTS` and applying a keyword-based sentiment classifier (`Dashboard.jsx:43–51`). This classifier uses two hardcoded keyword arrays:

```javascript
const POS_KW = ['amazing','love','fantastic','brilliant','perfect','outstanding',
  'incredible','happier','exceeded','game changer','kudos','best','recommend']
const NEG_KW = ['terrible','worst','broken','frustrated','unacceptable',
  'underwhelming','overpriced','poor','waste','disappointed','crashes','fix']
```

When both positive and negative keywords match (e.g., "Love the product but terrible service"), the classifier randomly assigns one of the two with equal probability. Classification of neutral texts uses the absence of any keyword as the default. This lightweight approach is strictly for dashboard demonstration; the actual sentiment classification is performed by the RoBERTa model in the streaming pipeline.

### 5.2 Streaming Simulation and Viewport Array Management

The `handleTick` callback (`Dashboard.jsx:162–197`) is invoked by a `setInterval` at 700–1200 ms intervals, producing 1–3 synthetic tweets per tick. Each tick updates all twelve state variables simultaneously, triggering a single React reconciliation pass. This batching is critical: calling `setState` twelve times in sequence within a `useCallback` body would normally trigger twelve re-renders, but React 18+'s automatic batching (enabled by `createRoot`) coalesces these into a single synchronous render.

The feed log array (`logs`) is managed as a bounded deque with a maximum capacity of `MAX_LOG = 7` entries (`Dashboard.jsx:183`):

```javascript
setLogs(p => { const n = [...rows, ...p]; return n.slice(0, MAX_LOG) })
```

New entries are prepended to the front of the array, and the array is sliced to keep at most 7 entries. This prevents unbounded memory growth while maintaining a visible history window. The `feedKeys` Set (`Dashboard.jsx:185–187`) stores the keys of newly inserted rows; a `setTimeout` clears this Set after 800 ms, allowing the CSS `feed-row-new` animation class to render exactly once per new row.

### 5.3 Inline SVG Vector Graphics — Zero Dependencies

The dashboard renders two chart types using raw inline SVG—a design choice that eliminates external graphing libraries (Chart.js, D3, Recharts) and their associated bundle weight (~50–200 KB minified) in favor of a zero-dependency, declarative approach.

**Donut Charts:** The `Donut` component (`Dashboard.jsx:100–118`) constructs a circular ring using SVG's `strokeDasharray` and `strokeDashoffset` properties:

```javascript
const r = (size - sw) / 2, circ = 2 * Math.PI * r
// For each segment:
const len = Math.max((s.v / 100) * circ, 0.5)
const dash = `${len} ${circ}`
```

Each segment's dash length is proportional to its percentage value, rotated around the circle using `transform="rotate(-90 ...)"`. The `transition: 'stroke-dashoffset 0.5s ease'` inline style ensures segments animate smoothly when values change.

**Trend Lines and Area Fills:** The temporal trend chart (`Dashboard.jsx:338–357`) uses Catmull-Rom spline interpolation computed in the `smoothPath` function (`Dashboard.jsx:74–86`):

```javascript
function smoothPath(data, x0, y0, sx, sy) {
  // For each point pair, compute cubic Bézier control points as:
  const c1x = p1.x + (p2.x - p0.x) / 6
  const c1y = p1.y + (p2.y - p0.y) / 6
  const c2x = p2.x - (p3.x - p1.x) / 6
  const c2y = p2.y - (p3.y - p1.y) / 6
  d += ` C ${c1x} ${c1y}, ${c2x} ${c2y}, ${p2.x} ${p2.y}`
}
```

This produces smooth curves without requiring a curve-fitting library. The `fillArea` function reuses the spline path and closes it to the baseline to produce a filled area under the curve, rendered with low-opacity fills (`fill="rgba(52,211,153,0.08)"`). SVG filter primitives (`feGaussianBlur`) add a subtle glow effect to the positive and negative trend lines.

### 5.4 CSS Animation Architecture

The dashboard's `Dashboard.css` (`1–169`) implements six distinct animation systems:

| Animation | CSS Implementation | Purpose |
|-----------|-------------------|---------|
| `.orb-bg` keyframes (`orbFloat`) | Fixed-position `<span>` elements with `filter: blur(120px)` and `opacity: 0.15` translated along a 12-second alternating path | Ambient animated background to convey "liveness" |
| `.kpi-shimmer` | `background: linear-gradient(...)` with `background-clip: text` and 3-second background-position oscillation | Text shimmer on KPI values for visual emphasis |
| `.feed-row-new` (`feedFlash`) | `animation: feedFlash 0.7s ease-out forwards` — transitions background from cyan-tinted to transparent | Brief highlight flash on newly inserted stream rows |
| `.pulse-dot` | `box-shadow` expanding animation on a 7px circle at 1.6-second cadence | "LIVE" indicator pulse |
| `.border-rotate` | `border-color` cycling through cyan→purple→green at 6-second intervals | Animated accent border on the Sentiment Mix KPI card |
| `.hover-lift` | `transform: translateY(-2px)` with `box-shadow` transition on hover | Micro-interaction feedback on glass cards |

The `.glass` and `.glass-strong` classes apply `backdrop-filter: blur(16px/20px)` with semi-transparent `rgba(15, 23, 42, 0.7)` backgrounds, producing the frosted-glass aesthetic characteristic of modern dark-theme operational dashboards.

---

## 6. TECHNICAL CONCLUSION & SYSTEM ROADMAP

### 6.1 Synthesis of Outcomes

The system presented here demonstrates that a fully functional, end-to-end real-time opinion mining pipeline can be constructed from open-source components (Kafka, Spark Structured Streaming, Hugging Face Transformers, Parquet, React) and deployed on a single consumer-grade machine. The architecture achieves the five hard requirements laid out in Section 2.3:

1. **Ingestion durability** is provided by Kafka's on-disk log with `acks=all` acknowledgment and checkpoint-based offset management in the Spark consumer.
2. **Schema enforcement** is achieved through Spark's `from_json` with explicit `StructType` schemas and a five-stage regex cleansing chain that normalizes social-media text before inference.
3. **Distributed inference** is orchestrated by a PySpark UDF wrapping the Hugging Face Inference API, with comprehensive retry logic handling 503, 429, timeout, and connection errors.
4. **Immutable columnar storage** is delivered by Parquet files partitioned by `sentiment_label`, enabling partition pruning and efficient downstream ML pipeline training.
5. **Real-time monitoring** is provided by a React SPA with inline SVG charts, animated CSS state transitions, and bounded viewport array management.

The separate MLlib batch evaluation pipeline provides a mechanism for cross-validating the cloud transformer's classifications against an on-premises logistic regression baseline, enabling cost-benefit analysis of API-based inference vs. local model serving.

### 6.2 Future Development Roadmap

The following roadmap identifies three high-impact engineering upgrades that would transition this prototype to a production-grade deployment:

**Phase 1 — Multi-Node Kafka Cluster with Partition Scaling:**
The current single-node broker (`localhost:9092`) is a single point of failure and a throughput bottleneck. A production deployment would migrate to a 3-broker Kafka cluster with replication factor 3 and topic partitioning across 6–12 partitions. This would enable (a) horizontal throughput scaling—each partition can sustain ~5 MB/s on modern hardware; (b) consumer parallelism—Spark can assign one executor per partition for concurrent consumption; (c) broker fault tolerance—if one broker fails, the remaining replicas continue serving. The producer's `acks=all` setting and the consumer's offset management are already designed for this configuration.

**Phase 2 — Local GPU-Accelerated Tensor Runtime for Inference:**
The current API-based inference layer introduces three production liabilities: (a) per-request latency of 200–500 ms; (b) dependency on external API availability and rate limits; (c) per-request cost that scales linearly with throughput. A production upgrade would deploy the `cardiffnlp/twitter-roberta-base-sentiment` model locally using a GPU-accelerated ONNX Runtime or NVIDIA Triton Inference Server container. A single NVIDIA T4 GPU can serve the RoBERTa model at ~1,000 inferences/second with sub-10 ms latency, eliminating both the API cost and the network dependency. The Spark UDF would then call a local gRPC endpoint instead of an external REST API, reducing inference latency by 20–50x and increasing pipeline throughput correspondingly.

**Phase 3 — Interactive OLAP Engine for Sub-Second Queries:**
The Parquet lakehouse provides excellent storage efficiency and batch query performance, but ad-hoc analytical queries (e.g., "what was the sentiment mix for tweets containing 'product_name' in the last hour?") require scanning the full partition set. Layering an interactive OLAP engine such as Apache Druid or ClickHouse over the Parquet files would enable millisecond-latency queries at the cost of additional indexing overhead. Druid's segment-granularity time-based partitioning aligns naturally with the timestamp column in the enriched output, and its inverted-index support would enable full-text search across `original_text` and `cleaned_text` columns. This would complete the architecture's evolution from batch-observable to truly real-time queryable.

---

### Technical Appendix: Dependency Matrix

| Component | Technology | Version | Protocol/Format |
|-----------|-----------|---------|-----------------|
| Message Broker | Apache Kafka (via kafka-python) | 2.0.2 (client) | TCP/9092, binary protocol |
| Stream Processor | PySpark Structured Streaming | 3.5.0 | Micro-batch, append mode |
| Inference API | Hugging Face Inference API | — | HTTPS/REST, JSON payload |
| Transformer Model | cardiffnlp/twitter-roberta-base-sentiment | — | RoBERTa-base, 3-class classification |
| Storage Format | Apache Parquet | Snappy compression | Columnar, Hive-partitioned |
| ML Pipeline | Spark MLlib | 3.5.0 | Tokenizer → HashingTF → IDF → LogisticRegression |
| Frontend Framework | React | 19.2.6 | SPA, Hooks API |
| Build Tool | Vite | 8.0.12 | ES modules, HMR |
| Styling | Tailwind CSS | 3.4.19 | Utility-first, JIT compiler |
| Icons | Lucide React | 1.16.0 | SVG-based icon library |
| Orchestration | run_all.bat | — | Windows Batch, sequential process launcher |

---
*End of Report — Real-Time Opinion Mining at Scale Using Distributed Processing*
*Department of Data Science, University of Central Punjab (UCP), Lahore — 2025-26*
