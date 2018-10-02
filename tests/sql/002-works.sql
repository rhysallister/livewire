\unset ECHO
\i tests/test_setup.sql
\i tests/navassa_data.sql
\i tests/navassa_queries.sql
/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(1);

--SELECT pass('This Passes.');
SELECT is_empty(
    'SELECT * FROM navassa_shadow.__livewire', 'the __livewire table must be empty.' );
SELECT * FROM finish();

ROLLBACK;
