USE `team20_projectdb`;
SET hive.execution.engine=mr;
SET hive.resultset.use.unique.column.names=false;

DROP TABLE IF EXISTS q3_results;
CREATE TABLE q3_results (
    card4 STRING,
    card6 STRING,
    total_transactions BIGINT,
    fraud_transactions BIGINT,
    fraud_rate DOUBLE,
    avg_amount DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE q3_results

SELECT
    COALESCE(`card4`, 'unknown') AS card4,
    COALESCE(`card6`, 'unknown') AS card6,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY COALESCE(`card4`, 'unknown'), COALESCE(`card6`, 'unknown')
ORDER BY fraud_rate DESC;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q3_results;
