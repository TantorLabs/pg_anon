CREATE TABLE tbl1 (a integer, b integer, txt text);

INSERT INTO tbl1 (a, b, txt) VALUES (1, 10, 'First row with random values');
INSERT INTO tbl1 (a, b, txt) VALUES (5, 20, 'Second row with random values');
INSERT INTO tbl1 (a, b, txt) VALUES (8, 15, 'Third row with random values');

COMMENT ON COLUMN tbl1.a IS ':sens';
COMMENT ON COLUMN tbl1.b IS 'some descr :sens';
COMMENT ON COLUMN tbl1.txt IS ':nosens';