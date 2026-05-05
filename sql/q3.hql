-- Q3: Fraud rate by card network (card4) and card type (card6)
USE team20_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE TABLE q3_results
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
AS
SELECT
    card4,
    card6,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    SUM(is_fraud) / COUNT(*) AS fraud_rate,
    AVG(transactionamt) AS avg_amount
FROM team20_projectdb.train_transaction_pb
WHERE card4 IS NOT NULL AND card6 IS NOT NULL
GROUP BY card4, card6
ORDER BY fraud_rate DESC;

SELECT * FROM q3_results;
