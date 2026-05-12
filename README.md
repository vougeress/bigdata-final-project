# Big Data Final Project

This repository contains the Stage I-III pipeline for the credit risk and customer retention analytics project.

The current dataset files in `data/` follow the IEEE-CIS Fraud Detection naming convention:

- `train_transaction.csv`
- `train_identity.csv`
- `test_transaction.csv`
- `test_identity.csv`
- `sample_submission.csv` is optional and is not loaded into PostgreSQL/HDFS because it is only a Kaggle submission template.

The business goal is to analyze customer transactional behavior and prepare data for later models that estimate default probability, segment customers, and predict churn or inactivity.

## Stage I

Stage I automates:

1. Dataset collection into `data/`.
2. PostgreSQL table creation and CSV loading.
3. Sqoop ingestion from PostgreSQL to HDFS in Avro format with Snappy compression.

Run scripts from the repository root.

## Configuration

Set the cluster/database environment variables before running Stage I.
Use `hadoop-01.uni.innopolis.ru` for SSH/web access and `hadoop-04.uni.innopolis.ru` for PostgreSQL JDBC access from the IU Hadoop cluster.

```sh
export CLUSTER_HOST=hadoop-01.uni.innopolis.ru
export TEAM_NAME=team20
export PGHOST=hadoop-04.uni.innopolis.ru
export PGPORT=5432
export PGDATABASE=${TEAM_NAME}_projectdb
export PGUSER=${TEAM_NAME}
export PGPASSWORD='your-password'
```

If the PostgreSQL database has Citus enabled and tables should be distributed during creation, set:

```sh
export ENABLE_CITUS=1
```

For dataset download through the Kaggle CLI, also configure Kaggle credentials:

```sh
export KAGGLE_USERNAME='your-kaggle-username'
export KAGGLE_KEY='your-kaggle-api-key'
```

If Kaggle CLI is not available, place the four required CSV files manually in `data/`: train/test transaction and train/test identity.

## Commands

Collect or validate data:

```sh
bash scripts/data_collection.sh
```

Create PostgreSQL tables and load CSV files:

```sh
bash scripts/data_storage.sh
```

Import PostgreSQL tables to HDFS with Sqoop:

```sh
bash scripts/hdfs_ingest.sh
```

Generated SQL and Sqoop schema artifacts are written to `output/`.

To generate and inspect PostgreSQL DDL without connecting to the cluster:

```sh
python3 scripts/build_projectdb.py --generate-only
```

## Stage II

Stage II prepares the HDFS/Hive layer and exports EDA artifacts.

Run the Stage II Spark/Hive pipeline from the repository root on the cluster:

```sh
bash scripts/stage2.sh
```

This stage:

1. Loads the Stage I Avro data into Hive external tables.
2. Builds the partitioned and bucketed `train_transaction_pb` table.
3. Runs the Stage II EDA queries and exports `output/q1.csv` ... `output/q5.csv`.
4. Generates local chart images `output/q1.jpg` ... `output/q5.jpg`.

## Stage III

Stage III performs predictive data analysis with Spark ML on Hadoop YARN.

The supervised target is `isFraud`, which is used here as a risk/default proxy for the project because the IEEE-CIS dataset does not provide a native credit-default label.

Run the full modeling pipeline:

```sh
bash scripts/stage3.sh
```

Run models as separate jobs and keep a shared evaluation artifact:

```sh
STAGE3_MODEL_ORDER=gbt bash scripts/stage3.sh
STAGE3_MODEL_ORDER=rf bash scripts/stage3.sh
```

Run the smoke test path:

```sh
STAGE3_MODE=smoke bash scripts/stage3.sh
```

Stage III defaults:

```sh
export TEAM_NAME=team20
export SPARK_MASTER=yarn
export STAGE3_SEED=20
```

Stage III trains two Spark ML classifiers on Hive data:

1. Gradient-Boosted Trees
2. Random Forest

The pipeline reads `train_transaction_pb` and `train_identity`, recreates the raw Hive `train_identity` table from Stage I HDFS data if Stage II dropped it, derives helper features, balances the labeled dataset to a `1:1` fraud/non-fraud ratio, builds a reproducible 70/30 training/test split from that balanced corpus, fits preprocessing on the balanced training split only, transforms the balanced test split, performs 3-fold cross-validation, and exports artifacts for the dashboard/report.

To fit the cluster constraints while preserving the Stage III checklist:

- `model1` (`GBTClassifier`) is trained on a balanced `1:1` fraud/non-fraud corpus.
- `model2` (`RandomForest`) is trained on a balanced `1:1` fraud/non-fraud corpus.
- Each model uses `2` hyperparameters with `3` values each, resulting in `9` grid-search combinations per model.
- Feature filtering is intentionally aggressive for cluster efficiency:
  - unsupported columns are dropped;
  - columns with `null_ratio > 0.95` are dropped;
  - constant/empty columns are dropped;
  - dominant-value string columns are dropped;
  - very high-cardinality string columns with weak top-category coverage are dropped;
  - remaining high-cardinality strings keep only top-`10` categories and map the rest to `__OTHER__`.

Required Stage III repository/HDFS artifacts:

- `data/train.json`
- `data/test.json`
- `models/model1`
- `models/model2`
- `output/model1_predictions.csv`
- `output/model2_predictions.csv`
- `output/evaluation.csv`

Generated Stage III artifacts are written to `output/stage3/` and mirrored to HDFS under `/user/$TEAM_NAME/project/output/stage3/`.

Expected Stage III outputs:

- `model_metrics.csv`
- `model_comparison.json`
- `feature_profile.csv`
- `feature_importance_gbt.csv`
- `feature_importance_rf.csv`
- `prediction_sample.csv`
- `validation_progress.csv`

For long runs, `validation_progress.csv` records fold-level and per-combination validation metrics together with elapsed time and ETA so that partial progress is visible before the full grid search finishes.

Latest synchronized full-run Stage III metrics:

| Model | CV PR | Test AUC-PR | Test AUC-ROC | Test F1 | Test Accuracy | Best Params |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `gbt` | 0.925607 | 0.931504 | 0.922429 | 0.844644 | 0.844770 | `{"maxDepth": 7, "stepSize": 0.2}` |
| `rf` | 0.890258 | 0.885948 | 0.870194 | 0.794616 | 0.794962 | `{"maxDepth": 8, "numTrees": 60}` |

The latest full Stage III run also recorded:

- balanced labeled dataset rows: `40987`
- training split rows: `28760`
- test split rows: `12227`
- end-to-end run time: about `33m 16s`
- `gbt` CV time: about `20m 35s`
- `rf` CV time: about `6m 34s`

Important:

1. The Spark ML job should be run on YARN for grading purposes.
2. `test_transaction` and `test_identity` are not used for supervised evaluation because they do not contain labels.
3. Feature reduction is done with `VarianceThresholdSelector` instead of manual feature dropping to comply with the Stage III checklist.
4. Customer segmentation and churn/activity modeling are intentionally left out of Stage III implementation because IEEE-CIS does not include a stable customer identifier or a native churn label.

## Stage IV

Stage IV prepares the local assets required to build the final Apache Superset dashboard.

Run the local prerequisite check:

```sh
bash scripts/stage4.sh
```

This script verifies:

- Stage III repository artifacts through `scripts/check_stage3_artifacts.py`
- Superset-ready SQLite database generation through `scripts/create_stage4_superset_db.py`
- dashboard source assets through `scripts/check_stage4_assets.py`

The dashboard source checklist currently includes:

- Stage II datasets and charts:
  - `output/q1.csv` ... `output/q5.csv`
  - `output/q1.jpg` ... `output/q5.jpg`
- Stage III ML outputs:
  - `output/evaluation.csv`
  - `output/model1_predictions.csv`
  - `output/model2_predictions.csv`
  - `output/stage3/model_comparison.json`
  - `output/stage3/feature_profile.csv`
  - `output/stage3/feature_importance_gbt.csv`
  - `output/stage3/feature_importance_rf.csv`
- Superset-ready artifacts:
  - `output/stage4/superset_dashboard.sqlite`
  - `output/stage4/superset_dashboard_manifest.json`
  - `output/stage4/model_metrics_long.csv`
  - `output/stage4/best_model_summary.csv`
  - `output/stage4/feature_profile_summary.csv`
- report/dashboard documents:
  - `reports/stage3.md`
  - `reports/dashboard.md`

Superset dashboard build:

1. On the cluster, load the dashboard tables into PostgreSQL:
   `STAGE4_LOAD_POSTGRES=1 bash scripts/stage4.sh`.
2. In Superset, use the existing PostgreSQL connection to `team20_projectdb`
   and create datasets from the generated `stage4_*` tables.
3. Create the charts listed in `reports/dashboard.md`.
4. Publish one dashboard named `IEEE-CIS Fraud Risk: EDA to Spark ML`.

## Validation

Useful local validation commands:

```sh
python3 scripts/check_stage3_artifacts.py
python3 scripts/check_stage4_assets.py
bash scripts/lint_stage3.sh
bash scripts/lint_stage4.sh
```

Current local status for the synchronized repository state:

- `check_stage3_artifacts.py`: passes
- `check_stage4_assets.py`: passes
- `lint_stage3.sh`: passes, `10.00/10`
- `lint_stage4.sh`: passes, `10.00/10`
