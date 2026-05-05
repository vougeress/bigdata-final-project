USE `team20_projectdb`;
SET hive.execution.engine=mr;
SET hive.resultset.use.unique.column.names=false;

DROP TABLE IF EXISTS q2_results;
CREATE TABLE q2_results (
    amount_band STRING,
    total_transactions BIGINT,
    fraud_transactions BIGINT,
    fraud_rate DOUBLE,
    avg_amount DOUBLE
)
STORED AS ORC;

INSERT OVERWRITE TABLE q2_results

SELECT
    CASE
        WHEN `transactionamt` < 25 THEN '00_under_25'
        WHEN `transactionamt` < 100 THEN '01_25_99'
        WHEN `transactionamt` < 250 THEN '02_100_249'
        WHEN `transactionamt` < 500 THEN '03_250_499'
        ELSE '04_500_plus'
    END AS amount_band,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS fraud_rate,
    AVG(`transactionamt`) AS avg_amount
FROM train_transaction_pb
GROUP BY
    CASE
        WHEN `transactionamt` < 25 THEN '00_under_25'
        WHEN `transactionamt` < 100 THEN '01_25_99'
        WHEN `transactionamt` < 250 THEN '02_100_249'
        WHEN `transactionamt` < 500 THEN '03_250_499'
        ELSE '04_500_plus'
    END
ORDER BY amount_band;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM q2_results;
