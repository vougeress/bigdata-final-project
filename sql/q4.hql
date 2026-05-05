USE `team20_projectdb`;
SET hive.execution.engine=mr;
SET hive.resultset.use.unique.column.names=false;

DROP TABLE IF EXISTS q4_results;
CREATE TABLE q4_results (
    email_domain STRING,
    total_transactions BIGINT,
    fraud_transactions BIGINT,
    fraud_rate DOUBLE,
    avg_amount DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE q4_results

SELECT
    COALESCE(`p_emaildomain`, 'unknown') AS email_domain,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY COALESCE(`p_emaildomain`, 'unknown')
HAVING COUNT(*) >= 100
ORDER BY fraud_rate DESC
LIMIT 25;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q4'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q4_results;
