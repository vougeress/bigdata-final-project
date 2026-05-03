# Big Data Final Project

This repository contains the Stage I pipeline for the credit risk and customer retention analytics project.

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
