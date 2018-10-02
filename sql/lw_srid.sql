/*		Gets the SRID of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_srid(
  in lw_schema text,
  out lw_srid bigint
  ) as 
  
$lw_srid$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_srid')::bigint
    from %1$I.%1$I where tabletype = 'config' $$, lw_schema)
    into lw_srid;

END;

$lw_srid$ LANGUAGE plpgsql;
