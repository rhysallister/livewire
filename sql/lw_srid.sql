/*		Gets the SRID of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_srid(
  IN lw_schema text,
  OUT lw_srid bigint
  ) AS
  
$lw_srid$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_srid')::bigint
    FROM %1$I.%1$I WHERE tabletype = 'config' $$, lw_schema)
    into lw_srid;

END;

$lw_srid$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_srid(text) is 
  'Returns the SRID of a given lw_schema';
