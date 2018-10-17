/*    Add the configuration of a line layer to the config table    */

CREATE OR REPLACE FUNCTION lw_addedgeparticipant(
    lw_schema text,
    edgeinfo json
  )
    RETURNS SETOF void AS 

$lw_addedgeparticipant$

BEGIN

  EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig) 
      VALUES ('%2$I.%3$I', 'EDGE', %4$L)$$,
    lw_schema,
    edgeinfo->>'schemaname',
    edgeinfo->>'tablename',
    edgeinfo
    );

END;
$lw_addedgeparticipant$ language plpgsql;

COMMENT ON FUNCTION lw_addedgeparticipant(text, json) IS
  'Adds the configuration data for a edge table to the livewire config table';
