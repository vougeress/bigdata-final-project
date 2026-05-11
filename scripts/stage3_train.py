#!/usr/bin/env python3
# pylint: disable=C0301,C0103
"""Stage III: train fraud-risk models with Spark ML on Hive data."""

from __future__ import print_function

import csv
import json
import math
import os
import shutil
import subprocess
import time
from datetime import datetime

from pyspark import StorageLevel
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import (
    GBTClassificationModel,
    GBTClassifier,
    RandomForestClassificationModel,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VarianceThresholdSelector, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, NumericType, StringType


TEAM = os.environ.get("TEAM_NAME", "team20")
HIVE_DB = "{}_projectdb".format(TEAM)
LOCAL_DATA_DIR = "data"
LOCAL_MODEL_DIR = "models"
LOCAL_OUTPUT_ROOT = "output"
LOCAL_OUTPUT_DIR = os.path.join("output", "stage3")
STATUS_LOG_PATH = os.path.join(LOCAL_OUTPUT_DIR, "run_status.log")
VALIDATION_LOG_PATH = os.path.join(LOCAL_OUTPUT_DIR, "validation_progress.csv")
HDFS_DATA_DIR = "/user/{}/project/data".format(TEAM)
HDFS_OUTPUT_ROOT = "/user/{}/project/output".format(TEAM)
HDFS_OUTPUT_DIR = "/user/{}/project/output/stage3".format(TEAM)
HDFS_MODEL_DIR = "/user/{}/project/models".format(TEAM)

STAGE3_MODE = os.environ.get("STAGE3_MODE", "full").strip().lower()
SEED = int(os.environ.get("STAGE3_SEED", "20"))
SMOKE_FRACTION = float(os.environ.get("STAGE3_SMOKE_FRACTION", "0.02"))
CV_PARALLELISM = int(os.environ.get("STAGE3_CV_PARALLELISM", "2"))
CV_NUM_FOLDS = int(os.environ.get("STAGE3_CV_NUM_FOLDS", "3"))
PRUNE_BY_IMPORTANCE = os.environ.get("STAGE3_PRUNE_BY_IMPORTANCE", "0").strip().lower() in ("1", "true", "yes")
MAX_NUMERIC_FEATURES = int(os.environ.get("STAGE3_MAX_NUMERIC_FEATURES", "100"))
MAX_STRING_FEATURES = int(os.environ.get("STAGE3_MAX_STRING_FEATURES", "16"))

TOP_CARDINALITY_LIMIT = int(os.environ.get("STAGE3_TOP_CARDINALITY_LIMIT", "30"))
TOP_CATEGORY_KEEP = int(os.environ.get("STAGE3_TOP_CATEGORY_KEEP", "10"))
FEATURE_NULL_RATIO_DROP = float(os.environ.get("STAGE3_FEATURE_NULL_RATIO_DROP", "0.95"))
FEATURE_DOMINANT_RATIO_DROP = float(os.environ.get("STAGE3_FEATURE_DOMINANT_RATIO_DROP", "0.995"))
STRING_DISTINCT_DROP = int(os.environ.get("STAGE3_STRING_DISTINCT_DROP", "100"))
STRING_TOP_COVERAGE_MIN = float(os.environ.get("STAGE3_STRING_TOP_COVERAGE_MIN", "0.80"))
NUMERIC_IMPUTE_STRATEGY = os.environ.get("STAGE3_NUMERIC_IMPUTE_STRATEGY", "median").strip().lower()
IMPUTER_BATCH_SIZE = int(os.environ.get("STAGE3_IMPUTER_BATCH_SIZE", "32"))

GBT_MAX_ITER = int(os.environ.get("STAGE3_GBT_MAX_ITER", "50"))
SMOKE_GBT_MAX_ITER = int(os.environ.get("STAGE3_SMOKE_GBT_MAX_ITER", "10"))
MODEL_ORDER = [name.strip() for name in os.environ.get("STAGE3_MODEL_ORDER", "gbt,rf").split(",") if name.strip()]
REUSE_EXISTING_MODELS = os.environ.get("STAGE3_REUSE_EXISTING_MODELS", "0").strip().lower() in ("1", "true", "yes")
SEPARATE_EVALUATION = os.environ.get("STAGE3_SEPARATE_EVALUATION", "0").strip().lower() in ("1", "true", "yes")
TREE_TRAIN_FRACTION = float(os.environ.get("STAGE3_TREE_TRAIN_FRACTION", "0.35"))
TREE_MIN_POSITIVE_ROWS = int(os.environ.get("STAGE3_TREE_MIN_POSITIVE_ROWS", "5000"))
BALANCE_TREE_TRAIN = os.environ.get("STAGE3_BALANCE_TREE_TRAIN", "1").strip().lower() not in ("0", "false", "no")
TREE_NEG_TO_POS_RATIO = float(os.environ.get("STAGE3_TREE_NEG_TO_POS_RATIO", "1.0"))

EXCLUDED_FEATURES = set(["transactionid", "is_fraud", "label"])
RAW_HIVE_TABLES = {
    "train_identity": {
        "location": "/user/{}/project/warehouse/train_identity/".format(TEAM),
        "schema_url": "hdfs:///user/{}/project/warehouse/avsc/train_identity.avsc".format(TEAM),
    },
    "test_identity": {
        "location": "/user/{}/project/warehouse/test_identity/".format(TEAM),
        "schema_url": "hdfs:///user/{}/project/warehouse/avsc/test_identity.avsc".format(TEAM),
    },
}
MODEL_LABELS = {
    "gbt": "model1",
    "rf": "model2",
}


def require_mode():
    """Validate Stage III execution mode."""
    if STAGE3_MODE not in ("full", "smoke"):
        raise ValueError("Unsupported STAGE3_MODE: {}".format(STAGE3_MODE))
    if NUMERIC_IMPUTE_STRATEGY not in ("median", "mean"):
        raise ValueError("Unsupported STAGE3_NUMERIC_IMPUTE_STRATEGY: {}".format(NUMERIC_IMPUTE_STRATEGY))
    if IMPUTER_BATCH_SIZE <= 0:
        raise ValueError("STAGE3_IMPUTER_BATCH_SIZE must be positive")
    if not 0.0 <= FEATURE_NULL_RATIO_DROP <= 1.0:
        raise ValueError("STAGE3_FEATURE_NULL_RATIO_DROP must be between 0 and 1")
    if not 0.0 <= FEATURE_DOMINANT_RATIO_DROP <= 1.0:
        raise ValueError("STAGE3_FEATURE_DOMINANT_RATIO_DROP must be between 0 and 1")
    if STRING_DISTINCT_DROP <= 0:
        raise ValueError("STAGE3_STRING_DISTINCT_DROP must be positive")
    if not 0.0 <= STRING_TOP_COVERAGE_MIN <= 1.0:
        raise ValueError("STAGE3_STRING_TOP_COVERAGE_MIN must be between 0 and 1")
    if not MODEL_ORDER:
        raise ValueError("STAGE3_MODEL_ORDER must contain at least one model")
    for model_name in MODEL_ORDER:
        if model_name not in ("gbt", "rf"):
            raise ValueError("Unsupported model in STAGE3_MODEL_ORDER: {}".format(model_name))
    if MAX_NUMERIC_FEATURES <= 0:
        raise ValueError("STAGE3_MAX_NUMERIC_FEATURES must be positive")
    if MAX_STRING_FEATURES <= 0:
        raise ValueError("STAGE3_MAX_STRING_FEATURES must be positive")


def hdfs(args, check=True):
    """Run an hdfs dfs command."""
    cmd = ["hdfs", "dfs"] + args
    if check:
        subprocess.check_call(cmd)
        return 0
    return subprocess.call(cmd)


def hdfs_path_exists(path):
    """Check whether a path exists in HDFS."""
    return hdfs(["-test", "-e", path], check=False) == 0


class CyclicTimeEncoder(Transformer):
    """Encode cyclical numeric time parts into sine/cosine components."""

    def __init__(self, input_col, period, output_sin_col, output_cos_col):
        super(CyclicTimeEncoder, self).__init__()
        self.input_col = input_col
        self.period = float(period)
        self.output_sin_col = output_sin_col
        self.output_cos_col = output_cos_col

    def _transform(self, dataset):
        angle = F.col(self.input_col).cast("double") * F.lit((2.0 * math.pi) / self.period)
        return (
            dataset
            .withColumn(self.output_sin_col, F.sin(angle))
            .withColumn(self.output_cos_col, F.cos(angle))
        )


def ensure_dirs():
    """Create local and HDFS output directories."""
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    os.makedirs(LOCAL_MODEL_DIR, exist_ok=True)
    os.makedirs(LOCAL_OUTPUT_ROOT, exist_ok=True)
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
    hdfs(["-mkdir", "-p", HDFS_DATA_DIR], check=False)
    hdfs(["-mkdir", "-p", HDFS_OUTPUT_ROOT], check=False)
    hdfs(["-mkdir", "-p", HDFS_OUTPUT_DIR], check=False)
    hdfs(["-mkdir", "-p", HDFS_MODEL_DIR], check=False)


def status(message):
    """Print and persist a progress checkpoint."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = "[{} UTC] {}".format(timestamp, message)
    print(line)
    with open(STATUS_LOG_PATH, "a") as handle:
        handle.write(line + "\n")


def shell(command):
    """Run a shell command."""
    subprocess.check_call(command, shell=True)


def write_csv(path, fieldnames, rows):
    """Write a list of dict rows to CSV."""
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print("Written {}".format(path))


def write_json(path, payload):
    """Write JSON payload to disk."""
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    print("Written {}".format(path))


def init_validation_log():
    """Create the validation progress CSV header."""
    write_csv(
        VALIDATION_LOG_PATH,
        [
            "timestamp_utc",
            "model_name",
            "combo_index",
            "combo_total",
            "fold_index",
            "fold_total",
            "fold_metric_pr",
            "combo_mean_metric_pr",
            "elapsed_seconds",
            "eta_seconds",
            "params_json",
        ],
        [],
    )


def append_validation_row(row):
    """Append one validation progress row."""
    file_exists = os.path.exists(VALIDATION_LOG_PATH)
    with open(VALIDATION_LOG_PATH, "a", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp_utc",
                "model_name",
                "combo_index",
                "combo_total",
                "fold_index",
                "fold_total",
                "fold_metric_pr",
                "combo_mean_metric_pr",
                "elapsed_seconds",
                "eta_seconds",
                "params_json",
            ],
        )
        if not file_exists or os.path.getsize(VALIDATION_LOG_PATH) == 0:
            writer.writeheader()
        writer.writerow(row)


def load_existing_metric_rows(path):
    """Load previously saved metric rows if the file exists."""
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return rows
        for raw_row in reader:
            if not raw_row:
                continue
            row = list(raw_row)
            if len(row) < 7:
                continue
            if len(row) > 7:
                row = row[:6] + [",".join(row[6:])]
            parsed = {
                "model_name": row[0],
                "cv_metric_pr": row[1],
                "test_auc_roc": row[2],
                "test_auc_pr": row[3],
                "test_f1": row[4],
                "test_accuracy": row[5],
                "best_params_json": row[6],
            }
            for key in ("cv_metric_pr", "test_auc_roc", "test_auc_pr", "test_f1", "test_accuracy"):
                if parsed.get(key) not in (None, ""):
                    parsed[key] = float(parsed[key])
            rows.append(parsed)
    return rows


def load_existing_simple_csv_rows(path, fieldnames):
    """Load existing simple CSV rows while keeping only selected columns."""
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row:
                continue
            cleaned = {}
            has_value = False
            for name in fieldnames:
                value = row.get(name, "")
                cleaned[name] = value
                if value not in ("", None):
                    has_value = True
            if has_value:
                rows.append(cleaned)
    return rows


def merge_metric_rows(existing_rows, new_rows):
    """Merge metric rows by model_name, replacing older runs."""
    merged = {}
    for row in existing_rows + new_rows:
        merged[row["model_name"]] = row
    return [merged[name] for name in sorted(merged)]


def merge_rows_by_key(existing_rows, new_rows, key_name):
    """Merge simple row dictionaries by a stable key."""
    merged = {}
    for row in existing_rows + new_rows:
        key = row.get(key_name)
        if key in (None, ""):
            continue
        merged[key] = row
    rows = list(merged.values())
    rows.sort(key=lambda item: item[key_name])
    return rows


def load_merged_existing_metrics(paths):
    """Load and merge prior metric rows from multiple local CSV artifacts."""
    merged = []
    for path in paths:
        merged = merge_metric_rows(merged, load_existing_metric_rows(path))
    return merged


def load_feature_importance_rows(path):
    """Load feature importance rows from a CSV if present."""
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row:
                continue
            name = row.get("feature_name", "")
            importance_raw = row.get("importance", "")
            if not name or importance_raw in ("", None):
                continue
            rows.append({
                "feature_name": name,
                "importance": float(importance_raw),
            })
    return rows


def preferred_feature_importance_paths():
    """Return feature-importance files in fallback order."""
    gbt_path = os.path.join(LOCAL_OUTPUT_DIR, "feature_importance_gbt.csv")
    gbt_backup_path = os.path.join(LOCAL_OUTPUT_DIR, "feature_importance_gbt_backup.csv")
    rf_path = os.path.join(LOCAL_OUTPUT_DIR, "feature_importance_rf.csv")

    gbt_rows = load_feature_importance_rows(gbt_path)
    if gbt_rows:
        gbt_source = gbt_path
    else:
        gbt_source = gbt_backup_path

    return [gbt_source, rf_path]


def source_feature_name(transformed_name):
    """Map a transformed feature back to its source feature name."""
    if "_ohe_" in transformed_name:
        return transformed_name.split("_ohe_", 1)[0]
    if transformed_name.endswith("_imputed"):
        return transformed_name[:-8]
    return transformed_name


def load_source_importance_scores():
    """Aggregate transformed-feature importance into source-feature scores."""
    scores = {}
    used_paths = []
    for path in preferred_feature_importance_paths():
        for row in load_feature_importance_rows(path):
            source_name = source_feature_name(row["feature_name"])
            scores[source_name] = scores.get(source_name, 0.0) + row["importance"]
        used_paths.append(os.path.basename(path))
    if scores:
        status("Loaded previous-run feature importance for pruning from: {}".format(",".join(used_paths)))
    return scores


def merged_feature_importance(existing_rows, new_rows):
    """Prefer new feature-importance rows when present, else keep prior rows."""
    if new_rows:
        return new_rows
    return existing_rows


def load_best_validation_summary(model_name):
    """Recover the best validation summary for a model from validation_progress.csv."""
    if not os.path.exists(VALIDATION_LOG_PATH):
        return None
    best_row = None
    with open(VALIDATION_LOG_PATH, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("model_name") != model_name:
                continue
            if row.get("fold_index") != "summary":
                continue
            metric_raw = row.get("combo_mean_metric_pr")
            if metric_raw in (None, ""):
                continue
            metric = float(metric_raw)
            if best_row is None or metric > best_row["best_metric_pr"]:
                best_row = {
                    "best_metric_pr": metric,
                    "best_params_json": row.get("params_json", ""),
                }
    return best_row


def mirror_to_hdfs(local_paths):
    """Copy local artifacts to HDFS output directory."""
    for local_path in local_paths:
        hdfs(["-put", "-f", local_path, HDFS_OUTPUT_DIR])
        status("Mirrored {} -> {}".format(local_path, HDFS_OUTPUT_DIR))


def hdfs_to_local_text(hdfs_glob, local_path):
    """Merge text files from HDFS into one local file."""
    shell("hdfs dfs -cat {} > {}".format(hdfs_glob, local_path))
    status("Merged {} -> {}".format(hdfs_glob, local_path))


def save_dataframe_json(df, hdfs_dir, local_path):
    """Save a dataframe as one-part JSON to HDFS and local storage."""
    df.write.mode("overwrite").json(hdfs_dir)
    hdfs_to_local_text("{}/*.json".format(hdfs_dir), local_path)


def save_dataframe_csv(df, hdfs_dir, local_path):
    """Save a dataframe as one-part CSV to HDFS and local storage."""
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(hdfs_dir)
    )
    hdfs_to_local_text("{}/*.csv".format(hdfs_dir), local_path)


def model_label(model_name):
    """Map internal model id to repository artifact label."""
    return MODEL_LABELS[model_name]


def model_loader(model_name):
    """Return the Spark ML model loader for a given model family."""
    if model_name == "gbt":
        return GBTClassificationModel
    if model_name == "rf":
        return RandomForestClassificationModel
    raise ValueError("Unsupported model {}".format(model_name))


def build_spark_session():
    """Create a SparkSession with Hive support."""
    spark = (
        SparkSession.builder
        .appName("Stage3_{}".format(HIVE_DB))
        .config("spark.sql.warehouse.dir", "/user/{}/project/hive/warehouse".format(TEAM))
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
        .config("spark.hadoop.hive.exec.scratchdir", "/user/{}/tmp/hive/scratch".format(TEAM))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def lower_case_columns(df):
    """Normalize all column names to lowercase."""
    return df.toDF(*[name.lower() for name in df.columns])


def ensure_raw_hive_tables(spark):
    """Recreate Stage I external identity tables in Hive if Stage II dropped them."""
    spark.sql("USE {}".format(HIVE_DB))
    for table_name, spec in RAW_HIVE_TABLES.items():
        try:
            spark.table("{}.{}".format(HIVE_DB, table_name))
        except Exception:  # pylint: disable=broad-except
            status("Recreating Hive table {}.{} from Stage I HDFS data.".format(HIVE_DB, table_name))
            spark.sql(
                """
                CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
                LOCATION '{location}'
                TBLPROPERTIES ('avro.schema.url'='{schema_url}')
                """.format(
                    table_name=table_name,
                    location=spec["location"],
                    schema_url=spec["schema_url"],
                )
            )


def add_temporal_features(df):
    """Create deterministic time features and cyclical encodings."""
    frame = (
        df
        .withColumn("event_day", F.floor(F.col("transactiondt") / F.lit(86400)).cast("double"))
        .withColumn("event_hour_part", F.floor(F.expr("pmod(transactiondt, 86400)") / F.lit(3600)).cast("double"))
        .withColumn("event_minute_part", F.floor(F.expr("pmod(transactiondt, 3600)") / F.lit(60)).cast("double"))
        .withColumn("event_second_part", F.expr("pmod(transactiondt, 60)").cast("double"))
    )
    encoders = [
        CyclicTimeEncoder("event_hour_part", 24.0, "event_hour_sin", "event_hour_cos"),
        CyclicTimeEncoder("event_minute_part", 60.0, "event_minute_sin", "event_minute_cos"),
        CyclicTimeEncoder("event_second_part", 60.0, "event_second_sin", "event_second_cos"),
    ]
    for encoder in encoders:
        frame = encoder.transform(frame)
    return frame.drop("event_hour_part", "event_minute_part", "event_second_part")


def load_modeling_frame(spark):
    """Load train transaction and identity tables and derive helper features."""
    ensure_raw_hive_tables(spark)
    transactions = lower_case_columns(spark.table("{}.train_transaction_pb".format(HIVE_DB)))
    identity_table = "{}.train_identity".format(HIVE_DB)
    identity = lower_case_columns(spark.table(identity_table))
    identity_cols = [name for name in identity.columns if name != "transactionid"]
    joined = transactions.join(identity, on="transactionid", how="left")

    identity_present = None
    for name in identity_cols:
        expr = F.col(name).isNotNull()
        identity_present = expr if identity_present is None else (identity_present | expr)
    if identity_present is None:
        identity_present = F.lit(False)
    status("Loaded {}".format(identity_table))

    frame = (
        joined
        .withColumn("transactionid", F.col("transactionid").cast("long"))
        .withColumn("label", F.col("is_fraud").cast("double"))
        .withColumn("identity_present", F.when(identity_present, F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn(
            "log_transactionamt",
            F.when(F.col("transactionamt").isNull(), F.lit(None).cast("double"))
            .otherwise(F.log1p(F.col("transactionamt").cast("double")))
        )
        .withColumn(
            "email_domain_match",
            F.when(
                (F.col("p_emaildomain").isNotNull())
                & (F.col("r_emaildomain").isNotNull())
                & (F.col("p_emaildomain") == F.col("r_emaildomain")),
                F.lit(1.0),
            ).otherwise(F.lit(0.0))
        )
        .where(F.col("label").isNotNull())
    )
    return add_temporal_features(frame)


def persist_with_count(df, label, storage_level):
    """Persist and materialize a dataframe."""
    persisted = df.persist(storage_level)
    count = persisted.count()
    status("{} rows: {}".format(label, count))
    return persisted, count


def split_storage_level():
    """Keep wide raw splits off executor memory during full runs."""
    if STAGE3_MODE == "smoke":
        return StorageLevel.MEMORY_AND_DISK
    return StorageLevel.DISK_ONLY


def chunked(values, size):
    """Yield fixed-size batches from a sequence."""
    for start in range(0, len(values), size):
        yield values[start:start + size]


def reduce_for_smoke(df, name):
    """Reduce dataset size for smoke mode while preserving class balance."""
    sampled = df.sampleBy("label", {0.0: SMOKE_FRACTION, 1.0: SMOKE_FRACTION}, seed=SEED)
    sampled, count = persist_with_count(sampled, "{} (smoke sample)".format(name), StorageLevel.MEMORY_AND_DISK)
    if count == 0:
        raise ValueError("Smoke sample for {} is empty".format(name))
    label_counts = {int(row["label"]): row["count"] for row in sampled.groupBy("label").count().collect()}
    if 0 not in label_counts or 1 not in label_counts:
        raise ValueError("Smoke sample for {} lost one of the classes: {}".format(name, label_counts))
    return sampled


def reduce_for_tree_training(df, name):
    """Sample a dataframe for tree-based training to fit cluster limits."""
    if STAGE3_MODE == "smoke" or TREE_TRAIN_FRACTION >= 0.999:
        return df

    if BALANCE_TREE_TRAIN:
        counts = {int(row["label"]): int(row["count"]) for row in df.groupBy("label").count().collect()}
        positive_count = counts.get(1, 0)
        negative_count = counts.get(0, 0)
        if positive_count == 0 or negative_count == 0:
            return df
        negative_fraction = min(1.0, (positive_count * TREE_NEG_TO_POS_RATIO) / float(negative_count))
        fractions = {0.0: negative_fraction, 1.0: 1.0}
        status(
            "Balancing tree training set with negative fraction {:.4f} for target ratio {:.2f}:1.".format(
                negative_fraction, TREE_NEG_TO_POS_RATIO
            )
        )
    else:
        fractions = {0.0: TREE_TRAIN_FRACTION, 1.0: TREE_TRAIN_FRACTION}

    sampled = df.sampleBy("label", fractions, seed=SEED)
    if BALANCE_TREE_TRAIN:
        sampled = sampled.withColumn("class_weight", F.lit(1.0))
    sampled, count = persist_with_count(sampled, "{} (tree sample)".format(name), StorageLevel.MEMORY_AND_DISK)
    if count == 0:
        raise ValueError("Tree training sample for {} is empty".format(name))

    label_counts = {int(row["label"]): int(row["count"]) for row in sampled.groupBy("label").count().collect()}
    if label_counts.get(1, 0) < TREE_MIN_POSITIVE_ROWS:
        sampled.unpersist()
        status("Tree sample too small for positives ({}); falling back to full training data.".format(label_counts.get(1, 0)))
        return df

    status("Using sampled training split for tree model: {}".format(label_counts))
    return sampled


def summarize_profile_decisions(profile_rows):
    """Count feature-profile decisions for logging."""
    summary = {}
    for row in profile_rows:
        decision = row["inclusion_decision"]
        summary[decision] = summary.get(decision, 0) + 1
    return summary


def feature_profile_lookup(profile_rows):
    """Index profile rows by feature name."""
    return {row["feature_name"]: row for row in profile_rows}


def cardinality_bucket(distinct_count):
    """Describe the cardinality level of a feature."""
    if distinct_count <= 1:
        return "constant"
    if distinct_count <= 10:
        return "low"
    if distinct_count <= TOP_CARDINALITY_LIMIT:
        return "medium"
    return "high"


def analyze_features(train_df):
    """Profile features on the training split and keep only useful columns."""
    feature_cols = [name for name in train_df.columns if name not in EXCLUDED_FEATURES]
    row_count = train_df.count()

    agg_exprs = []
    for name in feature_cols:
        agg_exprs.append(F.sum(F.when(F.col(name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias("{}__nulls".format(name)))
        agg_exprs.append(F.approx_count_distinct(F.col(name)).alias("{}__distinct".format(name)))
    stats = train_df.agg(*agg_exprs).collect()[0].asDict()

    numeric_cols = []
    string_cols = []
    top_categories = {}
    profile_rows = []

    for name in sorted(feature_cols):
        field = train_df.schema[name]
        spark_type = field.dataType.simpleString()
        if isinstance(field.dataType, NumericType):
            source_type = "numeric"
        elif isinstance(field.dataType, StringType):
            source_type = "string"
        else:
            source_type = "other"

        nulls = float(stats["{}__nulls".format(name)])
        distinct_count = int(stats["{}__distinct".format(name)])
        null_ratio = nulls / float(row_count) if row_count else 1.0
        non_null_count = max(0.0, row_count - nulls)
        dominant_ratio = ""
        top_category_coverage = ""

        if source_type == "other":
            decision = "drop_unsupported"
            included = False
        elif null_ratio > FEATURE_NULL_RATIO_DROP:
            decision = "drop_high_null"
            included = False
        elif distinct_count <= 1:
            decision = "drop_constant_or_empty"
            included = False
        elif source_type == "numeric":
            decision = "include_numeric"
            included = True
            numeric_cols.append(name)
        else:
            top_rows = (
                train_df
                .where(F.col(name).isNotNull())
                .groupBy(name)
                .count()
                .orderBy(F.desc("count"), F.asc(name))
                .limit(TOP_CATEGORY_KEEP)
                .collect()
            )
            top_values = [row[name] for row in top_rows]
            top_count = int(top_rows[0]["count"]) if top_rows else 0
            dominant_ratio = (top_count / non_null_count) if non_null_count else 1.0
            top_category_coverage = (
                sum(int(row["count"]) for row in top_rows) / non_null_count
                if non_null_count else 1.0
            )

            if dominant_ratio > FEATURE_DOMINANT_RATIO_DROP:
                decision = "drop_dominant_value"
                included = False
            elif distinct_count > STRING_DISTINCT_DROP and top_category_coverage < STRING_TOP_COVERAGE_MIN:
                decision = "drop_high_cardinality_low_coverage"
                included = False
            elif distinct_count > TOP_CARDINALITY_LIMIT:
                decision = "include_string_topk"
                included = True
                string_cols.append(name)
                top_categories[name] = top_values
            else:
                decision = "include_string"
                included = True
                string_cols.append(name)

        profile_rows.append({
            "feature_name": name,
            "source_type": source_type,
            "spark_type": spark_type,
            "null_ratio": round(null_ratio, 6),
            "distinct_non_null": distinct_count,
            "dominant_ratio": round(dominant_ratio, 6) if dominant_ratio != "" else "",
            "top_category_coverage": round(top_category_coverage, 6) if top_category_coverage != "" else "",
            "cardinality_bucket": cardinality_bucket(distinct_count),
            "included": included,
            "inclusion_decision": decision,
        })

    return profile_rows, numeric_cols, string_cols, top_categories


def prune_features_by_importance(profile_rows, numeric_cols, string_cols):
    """Reduce retained features using previously exported source importance."""
    if not PRUNE_BY_IMPORTANCE:
        return numeric_cols, string_cols

    scores = load_source_importance_scores()
    if not scores:
        status("Importance-based pruning requested, but no prior feature importance files were found. Keeping full feature set.")
        return numeric_cols, string_cols

    profile_map = feature_profile_lookup(profile_rows)

    ranked_numeric = sorted(
        numeric_cols,
        key=lambda name: (-scores.get(name, 0.0), name)
    )
    ranked_strings = sorted(
        string_cols,
        key=lambda name: (-scores.get(name, 0.0), name)
    )

    kept_numeric = ranked_numeric[:MAX_NUMERIC_FEATURES]
    kept_strings = ranked_strings[:MAX_STRING_FEATURES]

    estimated_string_dims = 0
    for name in kept_strings:
        row = profile_map.get(name)
        if row is None:
            continue
        if row["inclusion_decision"] == "include_string_topk":
            estimated_string_dims += TOP_CATEGORY_KEEP + 2
        else:
            estimated_string_dims += int(row["distinct_non_null"]) + 1

    status(
        "Importance-based pruning enabled. Numeric features kept: {} -> {}. String features kept: {} -> {}. Estimated transformed dimensions before variance filter: ~{}.".format(
            len(numeric_cols),
            len(kept_numeric),
            len(string_cols),
            len(kept_strings),
            len(kept_numeric) + estimated_string_dims,
        )
    )
    status("Top kept string features: {}".format(",".join(kept_strings)))
    return kept_numeric, kept_strings


def compute_class_weights(train_df):
    """Compute inverse-frequency class weights on the training split."""
    counts = {}
    for row in train_df.groupBy("label").count().collect():
        counts[int(row["label"])] = float(row["count"])
    if 0 not in counts or 1 not in counts:
        raise ValueError("Expected both binary classes in training split, found {}".format(counts))
    total = counts[0] + counts[1]
    return {
        0: total / (2.0 * counts[0]),
        1: total / (2.0 * counts[1]),
    }


def prepare_frame(df, numeric_cols, string_cols, top_categories, class_weights):
    """Apply deterministic, training-derived feature cleanup."""
    prepared = df

    for name in numeric_cols:
        prepared = prepared.withColumn(name, F.col(name).cast("double"))

    for name in string_cols:
        if name in top_categories:
            prepared = prepared.withColumn(
                name,
                F.when(F.col(name).isNull(), F.lit("__MISSING__"))
                .when(F.col(name).cast("string").isin(top_categories[name]), F.col(name).cast("string"))
                .otherwise(F.lit("__OTHER__"))
            )
        else:
            prepared = prepared.withColumn(
                name,
                F.when(F.col(name).isNull(), F.lit("__MISSING__")).otherwise(F.col(name).cast("string"))
            )

    prepared = prepared.withColumn(
        "class_weight",
        F.when(F.col("label") == F.lit(1.0), F.lit(class_weights[1])).otherwise(F.lit(class_weights[0]))
    )
    return prepared


def build_preprocess_model(train_df, numeric_cols, string_cols):
    """Fit the preprocessing pipeline on the training split."""
    stages = []
    assembler_inputs = []

    imputed_numeric_cols = []
    if numeric_cols:
        imputed_numeric_cols = ["{}_imputed".format(name) for name in numeric_cols]
        for input_batch, output_batch in zip(
            chunked(numeric_cols, IMPUTER_BATCH_SIZE),
            chunked(imputed_numeric_cols, IMPUTER_BATCH_SIZE)
        ):
            stages.append(
                Imputer(
                    strategy=NUMERIC_IMPUTE_STRATEGY,
                    inputCols=input_batch,
                    outputCols=output_batch,
                )
            )
        assembler_inputs.extend(imputed_numeric_cols)

    indexed_cols = []
    encoded_cols = []
    if string_cols:
        indexed_cols = ["{}_idx".format(name) for name in string_cols]
        encoded_cols = ["{}_ohe".format(name) for name in string_cols]
        stages.append(StringIndexer(inputCols=string_cols, outputCols=indexed_cols, handleInvalid="keep"))
        stages.append(OneHotEncoder(inputCols=indexed_cols, outputCols=encoded_cols, dropLast=False))
        assembler_inputs.extend(encoded_cols)

    stages.append(VectorAssembler(inputCols=assembler_inputs, outputCol="features_raw", handleInvalid="keep"))
    stages.append(VarianceThresholdSelector(featuresCol="features_raw", outputCol="features", varianceThreshold=0.0))
    pipeline = Pipeline(stages=stages)
    return pipeline.fit(train_df), assembler_inputs


def align_feature_names(feature_names, vector_size):
    """Pad missing feature names if vector metadata is incomplete."""
    names = list(feature_names)
    if len(names) >= vector_size:
        return names[:vector_size]
    for idx in range(len(names), vector_size):
        names.append("feature_{}".format(idx))
    return names


def extract_feature_names(metadata, fallback_inputs):
    """Extract VectorAssembler feature names from Spark metadata."""
    attrs = metadata.get("ml_attr", {}).get("attrs", {})
    indexed = []
    for items in attrs.values():
        for item in items:
            indexed.append((item["idx"], item["name"]))
    if indexed:
        indexed.sort(key=lambda pair: pair[0])
        return [name for _, name in indexed]
    return list(fallback_inputs)


def build_estimator(model_name):
    """Create the estimator for a given model family."""
    common = {
        "labelCol": "label",
        "featuresCol": "features",
        "weightCol": "class_weight",
    }
    if model_name == "gbt":
        return GBTClassifier(
            seed=SEED,
            maxIter=SMOKE_GBT_MAX_ITER if STAGE3_MODE == "smoke" else GBT_MAX_ITER,
            **common
        )
    if model_name == "rf":
        return RandomForestClassifier(
            seed=SEED,
            featureSubsetStrategy="sqrt",
            **common
        )
    raise ValueError("Unsupported model {}".format(model_name))


def build_param_grid(model_name, estimator):
    """Create the hyperparameter grid for a model family."""
    smoke = STAGE3_MODE == "smoke"
    if model_name == "gbt":
        depth_values = [3] if smoke else [3, 5, 7]
        step_values = [0.05] if smoke else [0.05, 0.1, 0.2]
        return (
            ParamGridBuilder()
            .addGrid(estimator.maxDepth, depth_values)
            .addGrid(estimator.stepSize, step_values)
            .build()
        )
    if model_name == "rf":
        tree_values = [20] if smoke else [20, 40, 60]
        depth_values = [4] if smoke else [4, 6, 8]
        return (
            ParamGridBuilder()
            .addGrid(estimator.numTrees, tree_values)
            .addGrid(estimator.maxDepth, depth_values)
            .build()
        )
    raise ValueError("Unsupported model {}".format(model_name))


def evaluator():
    """Selection metric used in cross-validation."""
    return BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )


def evaluate_predictions(predictions):
    """Compute the required binary-classification metrics."""
    auc_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC").evaluate(predictions)
    auc_pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR").evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(predictions)
    accuracy = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
    return {
        "test_auc_roc": auc_roc,
        "test_auc_pr": auc_pr,
        "test_f1": f1,
        "test_accuracy": accuracy,
    }


def with_cv_folds(df):
    """Attach a deterministic fold id for manual cross-validation."""
    return df.withColumn("cv_fold", F.expr("pmod(abs(xxhash64(transactionid)), {})".format(CV_NUM_FOLDS)))


def extract_positive_probability(probability):
    """Get the positive-class probability from a Spark vector."""
    if probability is None:
        return None
    return float(probability[1])


PROBABILITY_AT_ONE = F.udf(extract_positive_probability, DoubleType())


def extract_best_params(model_name, best_model):
    """Return the tuned parameters of the best fitted model."""
    wanted = {
        "gbt": set(["maxDepth", "stepSize"]),
        "rf": set(["numTrees", "maxDepth"]),
    }[model_name]

    params = {}
    for param, value in best_model.extractParamMap().items():
        if param.name in wanted:
            params[param.name] = value
    return params


def metric_row(model_name, cv_model, best_model, predictions, best_params_json=None):
    """Build the CSV row for one model family."""
    metrics = evaluate_predictions(predictions)
    return {
        "model_name": model_name,
        "cv_metric_pr": cv_model["best_metric_pr"],
        "test_auc_roc": metrics["test_auc_roc"],
        "test_auc_pr": metrics["test_auc_pr"],
        "test_f1": metrics["test_f1"],
        "test_accuracy": metrics["test_accuracy"],
        "best_params_json": best_params_json or json.dumps(extract_best_params(model_name, best_model), sort_keys=True),
    }


def param_map_to_json(param_map):
    """Serialize Spark ParamMap for logging."""
    payload = {}
    for param, value in param_map.items():
        payload[param.name] = value
    return json.dumps(payload, sort_keys=True)


def run_manual_cv(model_name, estimator, grid, train_df):
    """Run manual k-fold CV so we can log intermediate validation metrics."""
    evaluator_pr = evaluator()
    train_with_folds = with_cv_folds(train_df).persist(StorageLevel.MEMORY_AND_DISK)
    train_with_folds.count()

    start_time = time.time()
    combo_summaries = []
    best_metric = None
    best_param_map = None

    for combo_index, param_map in enumerate(grid, start=1):
        combo_metrics = []
        combo_started = time.time()
        for fold_index in range(CV_NUM_FOLDS):
            fit_df = train_with_folds.where(F.col("cv_fold") != F.lit(fold_index)).drop("cv_fold")
            val_df = train_with_folds.where(F.col("cv_fold") == F.lit(fold_index)).drop("cv_fold")
            fitted = estimator.fit(fit_df, param_map)
            predictions = fitted.transform(val_df).select("label", "rawPrediction")
            fold_metric = evaluator_pr.evaluate(predictions)
            combo_metrics.append(fold_metric)
            append_validation_row({
                "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "model_name": model_name,
                "combo_index": combo_index,
                "combo_total": len(grid),
                "fold_index": fold_index + 1,
                "fold_total": CV_NUM_FOLDS,
                "fold_metric_pr": fold_metric,
                "combo_mean_metric_pr": sum(combo_metrics) / float(len(combo_metrics)),
                "elapsed_seconds": round(time.time() - start_time, 2),
                "eta_seconds": "",
                "params_json": param_map_to_json(param_map),
            })

        combo_mean = sum(combo_metrics) / float(len(combo_metrics))
        combo_summaries.append({
            "param_map": param_map,
            "metric_pr": combo_mean,
            "elapsed_seconds": time.time() - combo_started,
        })
        completed = len(combo_summaries)
        avg_combo_seconds = sum(item["elapsed_seconds"] for item in combo_summaries) / float(completed)
        remaining = len(grid) - completed
        eta_seconds = round(avg_combo_seconds * remaining, 2)
        append_validation_row({
            "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "model_name": model_name,
            "combo_index": combo_index,
            "combo_total": len(grid),
            "fold_index": "summary",
            "fold_total": CV_NUM_FOLDS,
            "fold_metric_pr": "",
            "combo_mean_metric_pr": combo_mean,
            "elapsed_seconds": round(time.time() - start_time, 2),
            "eta_seconds": eta_seconds,
            "params_json": param_map_to_json(param_map),
        })
        status(
            "Validation summary for {} combo {}/{}: areaUnderPR={:.6f}, ETA ~{:.0f}s.".format(
                model_name,
                combo_index,
                len(grid),
                combo_mean,
                eta_seconds,
            )
        )
        if best_metric is None or combo_mean > best_metric:
            best_metric = combo_mean
            best_param_map = param_map

    best_model = estimator.fit(train_df, best_param_map)
    train_with_folds.unpersist()
    return {
        "best_metric_pr": best_metric,
        "best_param_map": best_param_map,
        "best_model": best_model,
    }


def importance_rows(feature_names, importances):
    """Format tree-model feature importances for export."""
    rows = []
    for idx, value in enumerate(importances):
        rows.append({
            "feature_name": feature_names[idx],
            "importance": float(value),
        })
    rows.sort(key=lambda row: (-row["importance"], row["feature_name"]))
    return rows


def save_model(best_model, model_name):
    """Persist a Spark ML model to HDFS."""
    artifact_name = model_label(model_name)
    hdfs_path = os.path.join(HDFS_MODEL_DIR, artifact_name)
    best_model.write().overwrite().save("hdfs://{}".format(hdfs_path))
    local_path = os.path.join(LOCAL_MODEL_DIR, artifact_name)
    shutil.rmtree(local_path, ignore_errors=True)
    subprocess.call(["mkdir", "-p", LOCAL_MODEL_DIR])
    hdfs(["-get", hdfs_path, local_path])
    status("Saved {} as {}".format(model_name, hdfs_path))


def save_predictions(predictions, model_name):
    """Save label/prediction outputs for one best model."""
    artifact_name = model_label(model_name)
    hdfs_dir = os.path.join(HDFS_OUTPUT_ROOT, "{}_predictions".format(artifact_name))
    local_path = os.path.join(LOCAL_OUTPUT_ROOT, "{}_predictions.csv".format(artifact_name))
    save_dataframe_csv(
        predictions.select(F.col("label").cast("int").alias("label"), F.col("prediction").cast("int").alias("prediction")),
        hdfs_dir,
        local_path,
    )


def maybe_recover_existing_model(model_name):
    """Load an already-saved model and validation summary if recovery mode is enabled."""
    if not REUSE_EXISTING_MODELS:
        return None
    artifact_name = model_label(model_name)
    hdfs_path = os.path.join(HDFS_MODEL_DIR, artifact_name)
    if not hdfs_path_exists(hdfs_path):
        status("Recovery requested but no existing HDFS model found for {}.".format(model_name))
        return None
    validation_summary = load_best_validation_summary(model_name)
    if validation_summary is None:
        status("Recovery requested but no validation summary found for {}.".format(model_name))
        return None
    status("Reusing existing saved model for {} from {}.".format(model_name, hdfs_path))
    model = model_loader(model_name).load("hdfs://{}".format(hdfs_path))
    return {
        "best_metric_pr": validation_summary["best_metric_pr"],
        "best_params_json": validation_summary["best_params_json"],
        "best_model": model,
    }


def prediction_sample_row(test_df, prediction_frames):
    """Collect one deterministic prediction sample across the executed models."""
    sample = (
        test_df
        .orderBy(F.asc("transactionid"))
        .select("transactionid", "label")
        .limit(1)
        .collect()
    )
    if not sample:
        raise ValueError("Test split is empty; cannot build prediction_sample.csv")

    base = sample[0]
    row = {
        "transactionid": int(base["transactionid"]),
        "actual_label": int(base["label"]),
    }

    for model_name, predictions in sorted(prediction_frames.items()):
        values = (
            predictions
            .where(F.col("transactionid") == F.lit(base["transactionid"]))
            .select(PROBABILITY_AT_ONE("probability").alias("positive_probability"), "prediction")
            .limit(1)
            .collect()
        )
        if not values:
            raise ValueError("Missing prediction for {} on transaction {}".format(model_name, base["transactionid"]))
        row["{}_probability".format(model_name)] = values[0]["positive_probability"]
        row["{}_prediction".format(model_name)] = int(values[0]["prediction"])
    ordered_row = {
        "transactionid": row["transactionid"],
        "actual_label": row["actual_label"],
    }
    for model_name in sorted(prediction_frames):
        ordered_row["{}_probability".format(model_name)] = row["{}_probability".format(model_name)]
        ordered_row["{}_prediction".format(model_name)] = row["{}_prediction".format(model_name)]
    return ordered_row


def model_comparison_payload(metric_rows):
    """Build a compact JSON summary for dashboards and reports."""
    ordered = sorted(metric_rows, key=lambda row: (-row["test_auc_pr"], row["model_name"]))
    return {
        "label_proxy": "isFraud",
        "selection_metric": "areaUnderPR",
        "execution_mode": STAGE3_MODE,
        "seed": SEED,
        "best_model_by_test_auc_pr": ordered[0]["model_name"] if ordered else None,
        "models": ordered,
    }


def main():
    """Run the full Stage III pipeline."""
    require_mode()
    ensure_dirs()
    with open(STATUS_LOG_PATH, "w") as handle:
        handle.write("")
    if not REUSE_EXISTING_MODELS:
        init_validation_log()
    status("Stage III started (mode={}, seed={})".format(STAGE3_MODE, SEED))
    status("Requested model order: {}".format(",".join(MODEL_ORDER)))
    if PRUNE_BY_IMPORTANCE:
        status(
            "Previous-run importance pruning is enabled (max numeric features={}, max string features={}).".format(
                MAX_NUMERIC_FEATURES,
                MAX_STRING_FEATURES,
            )
        )
    else:
        status("Previous-run importance pruning is disabled; using the full retained feature set.")

    spark = build_spark_session()

    status("Loading Stage II Hive tables from {}.".format(HIVE_DB))
    base_df = load_modeling_frame(spark)

    if STAGE3_MODE == "smoke":
        status("Applying smoke-mode downsampling before balancing/split.")
        base_df = reduce_for_smoke(base_df, "Labeled dataset")

    status("Balancing labeled dataset before train/test split.")
    balanced_df = reduce_for_tree_training(base_df, "Balanced labeled dataset before split")
    balanced_df, _ = persist_with_count(balanced_df, "Balanced labeled dataset", split_storage_level())

    train_df, test_df = balanced_df.randomSplit([0.7, 0.3], seed=SEED)
    train_df, _ = persist_with_count(train_df, "Training split", split_storage_level())
    test_df, _ = persist_with_count(test_df, "Test split", split_storage_level())

    if STAGE3_MODE == "full":
        status("Saving train/test JSON artifacts for the repository and HDFS.")
        save_dataframe_json(train_df, os.path.join(HDFS_DATA_DIR, "train"), os.path.join(LOCAL_DATA_DIR, "train.json"))
        save_dataframe_json(test_df, os.path.join(HDFS_DATA_DIR, "test"), os.path.join(LOCAL_DATA_DIR, "test.json"))

    preprocessing_train_df = train_df
    status("Fitting feature pipeline on balanced training split; test split comes from the same balanced corpus.")
    feature_profile_rows, numeric_cols, string_cols, top_categories = analyze_features(preprocessing_train_df)
    decision_summary = summarize_profile_decisions(feature_profile_rows)
    status("Feature selection summary: {}".format(decision_summary))
    status("Included numeric features: {}".format(len(numeric_cols)))
    status("Included string features: {}".format(len(string_cols)))
    for name in sorted(top_categories):
        status("Top categories kept for {}: {}".format(name, len(top_categories[name])))

    class_weights = compute_class_weights(preprocessing_train_df)
    status("Class weights on balanced training split: {}".format(class_weights))

    numeric_cols, string_cols = prune_features_by_importance(feature_profile_rows, numeric_cols, string_cols)
    status("Final numeric features after pruning: {}".format(len(numeric_cols)))
    status("Final string features after pruning: {}".format(len(string_cols)))

    train_prepared = prepare_frame(preprocessing_train_df, numeric_cols, string_cols, top_categories, class_weights)
    test_prepared = prepare_frame(test_df, numeric_cols, string_cols, top_categories, class_weights)

    status(
        "Fitting preprocessing pipeline on sampled train (numeric strategy={}, imputer batches={}).".format(
            NUMERIC_IMPUTE_STRATEGY,
            max(1, (len(numeric_cols) + IMPUTER_BATCH_SIZE - 1) // IMPUTER_BATCH_SIZE),
        )
    )
    preprocess_model, assembler_inputs = build_preprocess_model(train_prepared, numeric_cols, string_cols)
    train_vector = preprocess_model.transform(train_prepared).select("transactionid", "label", "class_weight", "features")
    test_vector = preprocess_model.transform(test_prepared).select("transactionid", "label", "class_weight", "features")
    train_vector, _ = persist_with_count(train_vector, "Vectorized training split", StorageLevel.MEMORY_AND_DISK)
    test_vector, _ = persist_with_count(test_vector, "Vectorized test split", StorageLevel.MEMORY_AND_DISK)

    feature_names = extract_feature_names(train_vector.schema["features"].metadata, assembler_inputs)
    feature_names = align_feature_names(feature_names, train_vector.select("features").first()["features"].size)

    model_metric_rows = []
    prediction_frames = {}
    gbt_rows = []
    rf_rows = []
    for model_name in MODEL_ORDER:
        recovered = maybe_recover_existing_model(model_name)
        best_params_json = None
        if recovered is None:
            estimator = build_estimator(model_name)
            grid = build_param_grid(model_name, estimator)
            model_train_vector = train_vector

            status("Training model family {} with {} parameter combinations.".format(model_name, len(grid)))
            cv_model = run_manual_cv(model_name, estimator, grid, model_train_vector)
            best_model = cv_model["best_model"]
        else:
            cv_model = {"best_metric_pr": recovered["best_metric_pr"]}
            best_model = recovered["best_model"]
            best_params_json = recovered["best_params_json"]
        predictions = best_model.transform(test_vector).select("transactionid", "label", "probability", "prediction", "rawPrediction")
        predictions = predictions.cache()
        predictions.count()

        model_metric_rows.append(metric_row(model_name, cv_model, best_model, predictions, best_params_json=best_params_json))
        prediction_frames[model_name] = predictions
        if recovered is None:
            save_model(best_model, model_name)
        save_predictions(predictions, model_name)

        if model_name == "gbt":
            gbt_names = align_feature_names(feature_names, len(best_model.featureImportances))
            gbt_rows = importance_rows(gbt_names, best_model.featureImportances.toArray())
        elif model_name == "rf":
            rf_names = align_feature_names(feature_names, len(best_model.featureImportances))
            rf_rows = importance_rows(rf_names, best_model.featureImportances.toArray())

    prediction_row = prediction_sample_row(test_vector, prediction_frames)

    model_suffix = "_{}".format("_".join(sorted(MODEL_ORDER))) if SEPARATE_EVALUATION else ""
    local_metric_path = os.path.join(LOCAL_OUTPUT_DIR, "model_metrics{}.csv".format(model_suffix))
    aggregate_metric_path = os.path.join(LOCAL_OUTPUT_DIR, "model_metrics.csv")
    local_comparison_path = os.path.join(LOCAL_OUTPUT_DIR, "model_comparison.json")
    local_profile_path = os.path.join(LOCAL_OUTPUT_DIR, "feature_profile.csv")
    local_gbt_path = os.path.join(LOCAL_OUTPUT_DIR, "feature_importance_gbt.csv")
    local_rf_path = os.path.join(LOCAL_OUTPUT_DIR, "feature_importance_rf.csv")
    local_prediction_path = os.path.join(LOCAL_OUTPUT_DIR, "prediction_sample.csv")
    local_evaluation_path = os.path.join(LOCAL_OUTPUT_ROOT, "evaluation{}.csv".format(model_suffix))
    aggregate_evaluation_path = os.path.join(LOCAL_OUTPUT_ROOT, "evaluation.csv")
    local_metric_export_path = os.path.join(LOCAL_OUTPUT_DIR, "evaluation{}.csv".format(model_suffix))
    aggregate_metric_export_path = os.path.join(LOCAL_OUTPUT_DIR, "evaluation.csv")
    previous_metric_rows = load_merged_existing_metrics([
        aggregate_metric_path,
        aggregate_evaluation_path,
        aggregate_metric_export_path,
        local_metric_path,
        local_evaluation_path,
        local_metric_export_path,
    ])
    aggregate_metric_rows = merge_metric_rows(previous_metric_rows, model_metric_rows)
    if SEPARATE_EVALUATION:
        export_metric_rows = list(model_metric_rows)
    else:
        export_metric_rows = list(aggregate_metric_rows)
    comparison_payload = model_comparison_payload(aggregate_metric_rows)
    previous_gbt_rows = load_existing_simple_csv_rows(local_gbt_path, ["feature_name", "importance"])
    previous_rf_rows = load_existing_simple_csv_rows(local_rf_path, ["feature_name", "importance"])
    gbt_rows = merged_feature_importance(previous_gbt_rows, gbt_rows)
    rf_rows = merged_feature_importance(previous_rf_rows, rf_rows)

    write_csv(
        local_metric_path,
        ["model_name", "cv_metric_pr", "test_auc_roc", "test_auc_pr", "test_f1", "test_accuracy", "best_params_json"],
        export_metric_rows
    )
    write_csv(
        aggregate_metric_path,
        ["model_name", "cv_metric_pr", "test_auc_roc", "test_auc_pr", "test_f1", "test_accuracy", "best_params_json"],
        aggregate_metric_rows
    )
    write_json(local_comparison_path, comparison_payload)
    write_csv(
        local_profile_path,
        [
            "feature_name",
            "source_type",
            "spark_type",
            "null_ratio",
            "distinct_non_null",
            "dominant_ratio",
            "top_category_coverage",
            "cardinality_bucket",
            "included",
            "inclusion_decision",
        ],
        feature_profile_rows
    )
    write_csv(local_gbt_path, ["feature_name", "importance"], gbt_rows)
    write_csv(local_rf_path, ["feature_name", "importance"], rf_rows)
    prediction_fieldnames = ["transactionid", "actual_label"]
    for model_name in sorted(prediction_frames):
        prediction_fieldnames.append("{}_probability".format(model_name))
        prediction_fieldnames.append("{}_prediction".format(model_name))
    write_csv(
        local_prediction_path,
        prediction_fieldnames,
        [prediction_row]
    )
    write_csv(
        local_evaluation_path,
        ["model_name", "cv_metric_pr", "test_auc_roc", "test_auc_pr", "test_f1", "test_accuracy", "best_params_json"],
        export_metric_rows
    )
    write_csv(
        aggregate_evaluation_path,
        ["model_name", "cv_metric_pr", "test_auc_roc", "test_auc_pr", "test_f1", "test_accuracy", "best_params_json"],
        aggregate_metric_rows
    )
    write_csv(
        local_metric_export_path,
        ["model_name", "cv_metric_pr", "test_auc_roc", "test_auc_pr", "test_f1", "test_accuracy", "best_params_json"],
        export_metric_rows
    )
    write_csv(
        aggregate_metric_export_path,
        ["model_name", "cv_metric_pr", "test_auc_roc", "test_auc_pr", "test_f1", "test_accuracy", "best_params_json"],
        aggregate_metric_rows
    )
    evaluation_hdfs_dir = os.path.join(HDFS_OUTPUT_ROOT, "evaluation{}".format(model_suffix))
    save_dataframe_csv(
        spark.createDataFrame(export_metric_rows),
        evaluation_hdfs_dir,
        local_evaluation_path,
    )
    save_dataframe_csv(
        spark.createDataFrame(aggregate_metric_rows),
        os.path.join(HDFS_OUTPUT_ROOT, "evaluation"),
        aggregate_evaluation_path,
    )

    mirror_to_hdfs([
        aggregate_metric_path,
        local_metric_path,
        local_comparison_path,
        local_profile_path,
        local_gbt_path,
        local_rf_path,
        local_prediction_path,
        aggregate_evaluation_path,
        local_metric_export_path,
        aggregate_metric_export_path,
        VALIDATION_LOG_PATH,
        STATUS_LOG_PATH,
    ])

    for frame in prediction_frames.values():
        frame.unpersist()
    train_vector.unpersist()
    test_vector.unpersist()
    balanced_df.unpersist()
    train_df.unpersist()
    test_df.unpersist()

    status("Stage III modeling complete.")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pylint: disable=broad-except
        print("ERROR: {}".format(exc))
        raise
