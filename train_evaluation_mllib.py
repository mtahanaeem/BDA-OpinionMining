"""
train_evaluation_mllib.py — Spark MLlib Batch Training & Evaluation

Project: Real-Time Opinion Mining at Scale Using Distributed Processing
Team: Taha Naeem, Suleman Ahmad, Adil Hayat
Term: 2025-26

This module reads the partitioned Parquet output produced by the
streaming consumer, builds a native Spark MLlib classification pipeline,
trains a Logistic Regression model, and prints comprehensive evaluation
metrics (Accuracy, Precision, Recall, F1-Score) to stdout for academic
validation and cross-referencing against the cloud Transformer model.

Pipeline stages:
    1. Load Parquet from ./output/processed_sentiment_parquet
    2. Text vectorisation: Tokenizer + HashingTF + IDF (optional)
    3. Label indexing: StringIndexer on sentiment_label
    4. Train/Test split (80/20)
    5. LogisticRegression classifier
    6. MulticlassClassificationEvaluator for Accuracy, Precision,
       Recall, F1-Score
    7. Detailed per-class metrics output

Dependencies:
    PySpark >= 3.4
"""

import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("MLlibEvaluator")

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, length, desc, rand
    from pyspark.ml.feature import (
        Tokenizer,
        HashingTF,
        IDF,
        StringIndexer,
        IndexToString,
    )
    from pyspark.ml.classification import (
        LogisticRegression,
        LogisticRegressionModel,
    )
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.evaluation import (
        MulticlassClassificationEvaluator,
    )
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
except ImportError as exc:
    logger.critical("PySpark ML is not available: %s", exc)
    sys.exit(1)


# -------------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------------
PARQUET_INPUT_DIR: str = "./output/processed_sentiment_parquet"
MODEL_SAVE_PATH: str = "./models/spark_mllib_lr_model"
PIPELINE_SAVE_PATH: str = "./models/spark_mllib_pipeline"

# ML hyper-parameters
TRAIN_TEST_SPLIT: list[float] = [0.8, 0.2]
RANDOM_SEED: int = 42
MAX_FEATURES: int = 2 ** 16  # 65,536 — HashingTF bucket count
MAX_ITER: int = 100
REG_PARAM: float = 0.01


# -------------------------------------------------------------------------
# Spark Session Builder
# -------------------------------------------------------------------------
def build_spark_session(app_name: str = "MLlibSentimentEvaluation") -> SparkSession:
    """
    Create a SparkSession tuned for batch ML workloads. We use a
    moderate shuffle partition count to avoid excessive task overhead
    on a local node.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    spark: SparkSession = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession initialised. Version: %s | Shuffle partitions: 8",
        spark.version,
    )
    return spark


# -------------------------------------------------------------------------
# Data Loading
# -------------------------------------------------------------------------
def load_parquet_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Recursively read all Parquet files under *path* (which may include
    Hive-style partition directories like sentiment_label=Positive/).
    The DataFrame is cached to speed up repeated access during
    training and evaluation.
    """
    if not os.path.isdir(path):
        raise FileNotFoundError(
            f"Parquet directory does not exist: {path}\n"
            f"Please run consumer_streaming.py first to generate output."
        )

    logger.info("Reading Parquet from: %s", path)
    df: DataFrame = spark.read.parquet(path)

    record_count: int = df.count()
    if record_count == 0:
        raise RuntimeError(
            f"Parquet directory at {path} contains zero records. "
            f"Ensure the streaming consumer has processed data."
        )

    logger.info(
        "Loaded %d records from Parquet. Schema:",
        record_count,
    )
    df.printSchema()

    # Show class distribution
    logger.info("Class distribution:")
    df.groupBy("sentiment_label").count().orderBy(
        desc("count")
    ).show(truncate=False)

    return df.cache()


# -------------------------------------------------------------------------
# ML Pipeline Construction
# -------------------------------------------------------------------------
def build_ml_pipeline() -> Pipeline:
    """
    Construct a Spark ML Pipeline consisting of:

        1. Tokenizer        — splits cleaned_text into words
        2. HashingTF        — maps tokenised words to feature vectors
        3. IDF              — down-weights corpus-wide frequent terms
        4. StringIndexer    — converts sentiment_label to label indices
        5. LogisticRegression — trains a multinomial classifier

    The Pipeline object ensures that all stages are fitted and
    transformed in a single consistent workflow.
    """
    # Stage 1: Tokenizer — breaks cleaned text into individual lowercase
    # tokens. We use cleaned_text from the streaming pipeline (already
    # stripped of URLs, mentions, punctuation).
    tokenizer: Tokenizer = Tokenizer(
        inputCol="cleaned_text",
        outputCol="tokens",
    )

    # Stage 2: HashingTF — maps token sequences to sparse feature
    # vectors of configurable dimensionality. HashingTF uses the
    # MurmurHash3 algorithm to assign a term to a bucket, avoiding
    # the need for a distributed vocabulary dictionary.
    hashing_tf: HashingTF = HashingTF(
        inputCol="tokens",
        outputCol="raw_features",
        numFeatures=MAX_FEATURES,
    )

    # Stage 3: IDF — computes the Inverse Document Frequency for
    # each term in the corpus. This down-weights tokens that appear
    # in every document (e.g., common stop-words) and boosts tokens
    # that are distinctive to specific sentiment classes.
    idf: IDF = IDF(
        inputCol="raw_features",
        outputCol="features",
    )

    # Stage 4: StringIndexer — maps the three sentiment labels
    # ("Negative", "Neutral", "Positive") to numeric indices
    # (0.0, 1.0, 2.0) ordered by label frequency. The metadata
    # attached to the output column preserves the label-to-index
    # mapping so it can be inverted later.
    label_indexer: StringIndexer = StringIndexer(
        inputCol="sentiment_label",
        outputCol="label",
        handleInvalid="keep",
    )

    # Stage 5: LogisticRegression — multinomial classifier (because
    # we have 3 classes). We use elastic-net regularisation (L1 + L2
    # hybrid) for robust feature selection.
    lr: LogisticRegression = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=MAX_ITER,
        regParam=REG_PARAM,
        elasticNetParam=0.5,
        family="multinomial",
        probabilityCol="probability",
        predictionCol="prediction",
    )

    # Assemble stages into a Pipeline.
    pipeline: Pipeline = Pipeline(
        stages=[
            tokenizer,
            hashing_tf,
            idf,
            label_indexer,
            lr,
        ]
    )

    logger.info(
        "ML Pipeline constructed with 5 stages: Tokenizer → "
        "HashingTF (%d features) → IDF → StringIndexer → "
        "LogisticRegression (multinomial, maxIter=%d, regParam=%.3f)",
        MAX_FEATURES, MAX_ITER, REG_PARAM,
    )
    return pipeline


# -------------------------------------------------------------------------
# Evaluation
# -------------------------------------------------------------------------
def evaluate_model(
    predictions: DataFrame,
    label_col: str = "label",
    prediction_col: str = "prediction",
) -> dict[str, float]:
    """
    Compute Accuracy, Weighted Precision, Weighted Recall, and
    Weighted F1-Score using Spark's MulticlassClassificationEvaluator.

    Returns a dictionary of metric-name → value.
    """
    metrics: dict[str, float] = {}

    for metric_name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
        evaluator: MulticlassClassificationEvaluator = (
            MulticlassClassificationEvaluator(
                labelCol=label_col,
                predictionCol=prediction_col,
                metricName=metric_name,
            )
        )
        try:
            value: float = evaluator.evaluate(predictions)
            metrics[metric_name] = round(value, 6)
        except Exception as exc:
            logger.warning("Could not compute metric '%s': %s", metric_name, exc)
            metrics[metric_name] = -1.0

    return metrics


def print_per_class_metrics(predictions: DataFrame) -> None:
    """
    Compute and print per-class Precision, Recall, and F1-Score by
    iterating over the distinct label values. This provides finer
    granularity than the weighted averages returned by the evaluator.
    """
    from pyspark.sql.functions import col, sum as _sum, count as _count

    logger.info("Per-class classification report:")
    logger.info(
        "%-12s %10s %10s %10s %10s",
        "Class", "Support", "Precision", "Recall", "F1-Score",
    )
    logger.info("-" * 56)

    # For each true label, compute TP, FP, FN.
    labels_df = predictions.select("label").distinct().collect()
    total_support = predictions.count()

    for row in labels_df:
        true_label = row["label"]
        # True Positives: predicted == true == this label
        tp = predictions.filter(
            (col("label") == true_label) & (col("prediction") == true_label)
        ).count()
        # False Positives: predicted == this label but true != this label
        fp = predictions.filter(
            (col("prediction") == true_label) & (col("label") != true_label)
        ).count()
        # False Negatives: true == this label but predicted != this label
        fn = predictions.filter(
            (col("label") == true_label) & (col("prediction") != true_label)
        ).count()
        support = tp + fn

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        logger.info(
            "%-12s %10d %10.4f %10.4f %10.4f",
            f"Label {int(true_label)}",
            support,
            precision,
            recall,
            f1,
        )

    logger.info("-" * 56)
    logger.info("%-12s %10d", "Total", total_support)


def print_confusion_matrix(predictions: DataFrame) -> None:
    """
    Print a human-readable confusion matrix showing the counts of
    actual (rows) vs. predicted (columns) labels.
    """
    logger.info("Confusion Matrix (rows=actual, columns=predicted):")
    cm = predictions.crosstab("label", "prediction").orderBy("label_prediction")
    cm.show(truncate=False)


# -------------------------------------------------------------------------
# Main Training & Evaluation Routine
# -------------------------------------------------------------------------
def main() -> None:
    """
    Entry point: load Parquet data, train the MLlib pipeline, evaluate,
    and persist the trained model.
    """
    logger.info("=" * 60)
    logger.info("Spark MLlib Batch Training & Evaluation")
    logger.info("=" * 60)

    spark: SparkSession = build_spark_session()

    try:
        # -------------------------------------------------------------
        # 1. Load data
        # -------------------------------------------------------------
        logger.info("[1/5] Loading Parquet data ...")
        df: DataFrame = load_parquet_data(spark, PARQUET_INPUT_DIR)

        # -------------------------------------------------------------
        # 2. Train/Test split
        # -------------------------------------------------------------
        logger.info("[2/5] Splitting into train/test (%.0f/%.0f) ...",
                     TRAIN_TEST_SPLIT[0] * 100, TRAIN_TEST_SPLIT[1] * 100)
        train_df, test_df = df.randomSplit(
            TRAIN_TEST_SPLIT, seed=RANDOM_SEED
        )
        train_count: int = train_df.count()
        test_count: int = test_df.count()
        logger.info(
            "Train: %d rows | Test: %d rows",
            train_count, test_count,
        )

        # -------------------------------------------------------------
        # 3. Build and train pipeline
        # -------------------------------------------------------------
        logger.info("[3/5] Constructing and training ML pipeline ...")
        pipeline: Pipeline = build_ml_pipeline()

        start_time: float = time.time()
        pipeline_model: PipelineModel = pipeline.fit(train_df)
        train_time: float = time.time() - start_time
        logger.info(
            "Pipeline training completed in %.2f seconds.",
            train_time,
        )

        # -------------------------------------------------------------
        # 4. Evaluate on test set
        # -------------------------------------------------------------
        logger.info("[4/5] Evaluating on test set ...")
        predictions: DataFrame = pipeline_model.transform(test_df)
        predictions.cache()
        pred_count: int = predictions.count()
        logger.info("Predictions generated for %d test rows.", pred_count)

        # Filter out rows where the label was unseen during training
        # (StringIndexer with handleInvalid="keep" assigns a special
        #  index, which we exclude from evaluation).
        predictions_valid: DataFrame = predictions.filter(
            col("label") >= 0
        )

        metrics: dict[str, float] = evaluate_model(predictions_valid)
        logger.info("=" * 60)
        logger.info("EVALUATION RESULTS")
        logger.info("=" * 60)
        logger.info("  Accuracy          : %.4f", metrics.get("accuracy", -1))
        logger.info("  Weighted Precision : %.4f", metrics.get("weightedPrecision", -1))
        logger.info("  Weighted Recall    : %.4f", metrics.get("weightedRecall", -1))
        logger.info("  Weighted F1-Score  : %.4f", metrics.get("f1", -1))
        logger.info("-" * 60)

        print_per_class_metrics(predictions_valid)
        print_confusion_matrix(predictions_valid)

        # -------------------------------------------------------------
        # 5. Save model to disk for future inference
        # -------------------------------------------------------------
        logger.info("[5/5] Persisting trained model ...")
        os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)

        pipeline_model.write().overwrite().save(PIPELINE_SAVE_PATH)
        logger.info("Pipeline model saved to: %s", PIPELINE_SAVE_PATH)

        # Also save just the LR model for lightweight loading.
        lr_stage_index: int = 4  # LogisticRegression is stage index 4
        lr_model: LogisticRegressionModel = pipeline_model.stages[lr_stage_index]
        lr_model.write().overwrite().save(MODEL_SAVE_PATH)
        logger.info("LogisticRegression model saved to: %s", MODEL_SAVE_PATH)

        logger.info("=" * 60)
        logger.info("MLlib evaluation complete.")
        logger.info("=" * 60)

    except FileNotFoundError as fnf:
        logger.critical("Data not found: %s", fnf)
        sys.exit(1)
    except RuntimeError as rte:
        logger.critical("Pipeline error: %s", rte)
        sys.exit(2)
    except Exception as exc:
        logger.critical("Unhandled exception: %s", exc)
        import traceback
        traceback.print_exc()
        sys.exit(3)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


# -------------------------------------------------------------------------
# CLI entry point
# -------------------------------------------------------------------------
if __name__ == "__main__":
    main()
