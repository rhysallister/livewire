\unset ECHO
\i tests/test_setup.sql
\i tests/navassa_data.sql
/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(1);

SELECT pass('This Passes.');
SELECT * FROM finish();

ROLLBACK;
