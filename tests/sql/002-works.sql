\unset ECHO
\i tests/test_setup.sql
\i tests/navassa_data.sql
\i tests/navassa_queries.sql
/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(2);

--SELECT pass('This Passes.');
SELECT is_empty(
    'SELECT * FROM navassa_shadow.__livewire', 'the __livewire table must be empty.' );
SET client_min_messages TO 'ERROR';
SELECT lw_traceall('navassa_shadow');
RESET client_min_messages;

SELECT isnt_empty(
    'SELECT * FROM navassa_shadow.__livewire', 'the __livewire table must not be empty.' );



SELECT * FROM finish();

ROLLBACK;
