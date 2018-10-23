/*		Gets row from origin table		*/

CREATE OR REPLACE FUNCTION lw_originrow(
  IN lw_schema text,
  IN lw_id bigint,
  OUT dump jsonb
  ) AS
  
$lw_originrow$

DECLARE
  thisrow jsonb; 
  tableconfig jsonb;

BEGIN
  EXECUTE format($$ SELECT row_to_json(n.*) FROM %1$I.__nodes n 
                 WHERE lw_id = %2$s $$,
    lw_schema,
    lw_id
    )
    INTO thisrow;

  EXECUTE format($$ SELECT tableconfig FROM %1$I.%1$I WHERE tablename = %2$L $$,
    lw_schema,
    thisrow->>'lw_table'
    )
    INTO tableconfig;

  EXECUTE format($$ SELECT  row_to_json(a.*) FROM 
                (SELECT * FROM %1$s where %2$I =  %3$L) as a $$,
    thisrow->>'lw_table',
    tableconfig->>'primarykey',
    thisrow->>'lw_table_pkid'
    )
    INTO dump;

END;

$lw_originrow$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_originrow(IN lw_schema text, in lw_id bigint) IS
  'Returns the row of the original table given the lw_id of the shadow network.';
