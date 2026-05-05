USE `team20_projectdb`;
SET hive.execution.engine=mr;
SET hive.resultset.use.unique.column.names=false;

DROP TABLE IF EXISTS q1_results;
CREATE TABLE q1_results (
    productcd STRING,
    total_transactions BIGINT,
    fraud_transactions BIGINT,
    fraud_rate DOUBLE,
    avg_amount DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE q1_results

SELECT
    `productcd` AS productcd,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY `productcd`
ORDER BY fraud_rate DESC;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q1_results;
