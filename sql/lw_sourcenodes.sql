/*    Returns an array of all SOURCE nodes in a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_sourcenodes(
    in lw_schema text,
    out myarray bigint[]
  ) AS 
$lw_sourcenodes$

DECLARE
  qrytxt text;

BEGIN 
  
  qrytxt := $$select array_agg(lw_id) from %1$I.__nodes
		where status = 'SOURCE'$$;  
  execute format(qrytxt,lw_schema) into myarray;

END;

$lw_sourcenodes$  LANGUAGE 'plpgsql';
