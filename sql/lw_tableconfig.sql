/*		Gets the tolerance of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_tableconfig(
  IN lw_schema text,
  IN tablename text,
  OUT lw_tableconfig json
  ) AS
  
$lw_tableconfig$

BEGIN

  EXECUTE format($$ SELECT tableconfig
    FROM %1$I.%1$I WHERE tablename = %2$L $$, lw_schema, tablename)
    into lw_tableconfig;

END;

$lw_tableconfig$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tableconfig(IN lw_schema text, in tablename text) IS
  'Returns the tableconfig for a given table.';
