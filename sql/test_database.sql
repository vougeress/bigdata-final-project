SELECT 'train_transaction' AS table_name, COUNT(*) AS rows_count FROM train_transaction
UNION ALL
SELECT 'train_identity' AS table_name, COUNT(*) AS rows_count FROM train_identity
UNION ALL
SELECT 'test_transaction' AS table_name, COUNT(*) AS rows_count FROM test_transaction
UNION ALL
SELECT 'test_identity' AS table_name, COUNT(*) AS rows_count FROM test_identity;
