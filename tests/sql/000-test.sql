\unset ECHO
\i test_setup.sql

/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(1);

SELECT has_function('lw_initialise', ARRAY['text', 'integer', 'double precision'], 'Check for lw_initialise');
SELECT * FROM finish();

ROLLBACK;
