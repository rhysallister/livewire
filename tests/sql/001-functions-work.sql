\unset ECHO
\i tests/test_setup.sql

/*******************************************************

Test Suite for LiveWire


*******************************************************/
SELECT lw_initialise('burlap',3450);


SELECT plan(5);



SELECT has_schema(
  'burlap', 
  'Check for existence of burlap after running lw_initialise');

SELECT has_table(
  'burlap', 
  'burlap',
  'Check for the existence of burlap.burlap after running lw_initialise');

SELECT has_table(
  'burlap',
  '__livewire', 
  'Check for the existence of burlap.__livewire after running lw_initialise');

SELECT has_table(
  'burlap',
  '__lines',
  'Check for the existence of burlap.__lines after running lw_initialise');

SELECT has_table(
  'burlap',
  '__nodes',
  'Check for the existence of burlap.__nodes after running lw_initialise');

SELECT * FROM finish();

ROLLBACK;
