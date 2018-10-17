/*	Adds configuration data for a node participant	*/

CREATE OR REPLACE FUNCTION lw_addnodeparticipant(
    lw_schema text,
    nodeinfo json
  )
    RETURNS SETOF void AS 
$lw_addnodeparticipant$

BEGIN

  EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig)
    VALUES ('%2$I.%3$I', 'NODE', %4$L)$$,
    lw_schema, 
    nodeinfo->>'schemaname',
    nodeinfo->>'tablename',
    nodeinfo
    );

END;
$lw_addnodeparticipant$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_addnodeparticipant IS
  'Adds the configuration for a node table to the livewire config table';
