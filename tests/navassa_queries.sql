BEGIN;
select lw_initialise('nn11',3450);

select lw_addedgeparticipant('nn11','{
  "schemaname":"navassa",						 
  "tablename": "primary_ag",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addedgeparticipant('nn11','{
  "schemaname":"navassa",						 
  "tablename": "primary_ug",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addedgeparticipant('nn11','{
  "schemaname":"navassa",						 
  "tablename": "risers",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addedgeparticipant('nn11','{
  "schemaname":"navassa",						 
  "tablename": "jumpers",
  "primarykey":"lineid",
  "geomcolumn": "g",
  "feederid": "feedername",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}');

select lw_addnodeparticipant('nn11', $${
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
	
select lw_addnodeparticipant('nn11', $${
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

select lw_addnodeparticipant('nn11', $${
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

select lw_generate('nn11');

commit;
