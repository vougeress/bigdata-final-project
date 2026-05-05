-- Q2: Fraud rate and average amount by transaction amount band
USE team20_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE TABLE q2_results
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
AS
SELECT
    CASE
        WHEN transactionamt < 25 THEN '00_under_25'
        WHEN transactionamt < 100 THEN '01_25_99'
        WHEN transactionamt < 250 THEN '02_100_249'
        WHEN transactionamt < 500 THEN '03_250_499'
        ELSE '04_500_plus'
    END AS amount_band,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    SUM(is_fraud) / COUNT(*) AS fraud_rate,
    AVG(transactionamt) AS avg_amount
FROM team20_projectdb.train_transaction_pb
GROUP BY
    CASE
        WHEN transactionamt < 25 THEN '00_under_25'
        WHEN transactionamt < 100 THEN '01_25_99'
        WHEN transactionamt < 250 THEN '02_100_249'
        WHEN transactionamt < 500 THEN '03_250_499'
        ELSE '04_500_plus'
    END
ORDER BY amount_band;

SELECT * FROM q2_results;
