\unset ECHO
\i tests/test_setup.sql
\i tests/navassa_data.sql
/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(4);
SELECT lw_initialise('navassa_shadow',3450);

SELECT isnt_empty(
  'SELECT * FROM navassa_shadow.navassa_shadow',
  'the navassa_shadow table must not be empty.'
  );

SELECT is_empty(
  'SELECT * FROM navassa_shadow.__livewire',
  'the __livewire table must be empty.'
  );

SELECT is_empty(
  'SELECT * FROM navassa_shadow.__lines',
  'the __lines table must be empty.'
  );

SELECT is_empty(
  'SELECT * FROM navassa_shadow.__nodes',
  'the __nodes table must be empty.'
  );


-- Non Test functions follow:

select lw_addedgeparticipant('navassa_shadow','{
  "schemaname":"navassa",						 
  "tablename": "primary_ag",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addedgeparticipant('navassa_shadow','{
  "schemaname":"navassa",						 
  "tablename": "primary_ug",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addedgeparticipant('navassa_shadow','{
  "schemaname":"navassa",						 
  "tablename": "risers",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addedgeparticipant('navassa_shadow','{
  "schemaname":"navassa",						 
  "tablename": "jumpers",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addnodeparticipant('navassa_shadow', $${
  "schemaname":"navassa",						 
  "tablename": "substation_transformers",
  "primarykey":"device_id",
  "geomcolumn": "g",
  "feederid":"feedername",
  "sourcequery": "1=1",
  "blockquery": "status='CLOSED'",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
    }$$);
	
select lw_addnodeparticipant('navassa_shadow', $${
  "schemaname":"navassa",						 
  "tablename": "isolating_devices",
  "primarykey":"device_id",
  "geomcolumn": "g",
  "feederid":"feedername",
  "sourcequery": "1=2",
  "blockquery": "status='OPEN'",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
    }$$);

select lw_addnodeparticipant('navassa_shadow', $${
  "schemaname":"navassa",						 
  "tablename": "transformer_devices",
  "primarykey":"device_id",
  "geomcolumn": "g",
  "feederid":"feedername",
  "sourcequery": "1=2",
  "blockquery": "status='OPEN'",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
    }$$);
  



--SET client_min_messages TO 'ERROR';
SELECT lw_traceall('navassa_shadow');
--RESET client_min_messages;
-- Non test functions end.
/*SELECT isnt_empty(
  'SELECT * FROM navassa_shadow.__livewire',
  'the __livewire table must not be empty.'
  );
*/


SELECT * FROM finish();

ROLLBACK;
