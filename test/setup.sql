CREATE TABLE test_table (id int, name varchar(100));
CREATE PROJECTION test_table_p (id, name) AS SELECT * FROM test_table SEGMENTED BY HASH(id) ALL NODES OFFSET 1;
INSERT INTO test_table VALUES (1, 'matt');
COMMIT;
