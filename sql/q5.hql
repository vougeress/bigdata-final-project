USE `team20_projectdb`;
SET hive.execution.engine=mr;
SET hive.resultset.use.unique.column.names=false;

DROP TABLE IF EXISTS q5_results;
CREATE TABLE q5_results (
    transaction_day BIGINT,
    total_transactions BIGINT,
    fraud_transactions BIGINT,
    fraud_rate DOUBLE,
    total_amount DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE q5_results

SELECT
    CAST(FLOOR(`transactiondt` / 86400) AS BIGINT) AS transaction_day,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    SUM(`transactionamt`) AS total_amount
FROM train_transaction_pb
GROUP BY CAST(FLOOR(`transactiondt` / 86400) AS BIGINT)
ORDER BY transaction_day;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q5'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q5_results;
