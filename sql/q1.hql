-- Q1: Fraud rate by product category
USE team20_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE TABLE q1_results
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
AS
SELECT
    productcd,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    SUM(is_fraud) / COUNT(*) AS fraud_rate,
    AVG(transactionamt) AS avg_amount
FROM team20_projectdb.train_transaction_pb
GROUP BY productcd
ORDER BY fraud_rate DESC;

SELECT * FROM q1_results;
