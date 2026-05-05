-- Q5: Daily transaction volume and fraud rate over time
-- transactiondt is seconds offset from a reference date
USE team20_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE TABLE q5_results
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
AS
SELECT
    FLOOR(transactiondt / 86400) AS transaction_day,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    SUM(is_fraud) / COUNT(*) AS fraud_rate,
    SUM(transactionamt) AS total_amount
FROM team20_projectdb.train_transaction_pb
GROUP BY FLOOR(transactiondt / 86400)
ORDER BY transaction_day;

SELECT * FROM q5_results;
