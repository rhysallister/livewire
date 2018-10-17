/*		Gets the tolerance of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_tolerance(
  IN lw_schema text,
  OUT lw_tolerance float
  ) AS
  
$lw_tolerance$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_tolerance')::float
    FROM %1$I.%1$I WHERE tabletype = 'config' $$, lw_schema)
    into lw_tolerance;

END;

$lw_tolerance$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tolerance(IN lw_schema text) IS
  'Returns the tolerance set for a given livewire.';

