# Stage II Summary

Stage II prepares the Stage I HDFS imports for analytical querying in Hive and extracts EDA results aligned with the project objectives: credit risk reduction and customer behavioral analysis.

## Storage Preparation

- Stage I Avro schema files are copied to HDFS under `/user/team20/project/warehouse/avsc`.
- Hive database `team20_projectdb` is created under `/user/team20/project/hive/warehouse/team20_projectdb.db`, separate from the Sqoop import warehouse.
- External Avro Hive tables are created for all Stage I HDFS imports:
  - `train_transaction`
  - `train_identity`
  - `test_transaction`
  - `test_identity`
- Column datatypes are checked with `DESCRIBE`.
- Optimized Hive table `train_transaction_pb` is created as ORC/Snappy, partitioned by `is_fraud` and bucketed by `TransactionID`.
- Raw unpartitioned Hive tables are dropped after creating the optimized table, and EDA uses `train_transaction_pb`.

## EDA Queries

- `q1`: fraud/default-risk rate by product category.
- `q2`: fraud/default-risk rate by transaction amount band.
- `q3`: risk profile by card network and card type.
- `q4`: top payer email domains by fraud/default-risk rate.
- `q5`: daily transaction volume, fraud/default-risk rate, and total amount.

Each query stores results in a Hive table `qx_results` and exports `output/qx.csv`.

## Superset Charts

Create Apache Superset datasets for:

- `q1_results`
- `q2_results`
- `q3_results`
- `q4_results`
- `q5_results`

Export charts as:

- `output/q1.jpg`
- `output/q2.jpg`
- `output/q3.jpg`
- `output/q4.jpg`
- `output/q5.jpg`
