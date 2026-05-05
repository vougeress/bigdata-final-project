-- Q4: Top email domains by fraud rate (min 100 transactions)
USE team20_projectdb;

DROP TABLE IF EXISTS q4_results;

CREATE TABLE q4_results
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
AS
SELECT
    p_emaildomain AS email_domain,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_transactions,
    SUM(is_fraud) / COUNT(*) AS fraud_rate,
    AVG(transactionamt) AS avg_amount
FROM team20_projectdb.train_transaction_pb
WHERE p_emaildomain IS NOT NULL
GROUP BY p_emaildomain
HAVING COUNT(*) >= 100
ORDER BY fraud_rate DESC
LIMIT 25;

SELECT * FROM q4_results;
