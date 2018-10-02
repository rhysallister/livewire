/*		Gets the tolerance of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_tolerance(
  in lw_schema text,
  out lw_tolerance float
  ) as 
  
$lw_tolerance$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_tolerance')::float
    from %1$I.%1$I where tabletype = 'config' $$, lw_schema)
    into lw_tolerance;

END;

$lw_tolerance$ LANGUAGE plpgsql;
