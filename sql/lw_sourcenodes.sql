/*    Returns an array of all SOURCE nodes in a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_sourcenodes(
  IN lw_schema text,
    out myarray bigint[]
  ) AS 
$lw_sourcenodes$

DECLARE
  qrytxt text;

BEGIN 
  
  qrytxt := $$SELECT array_agg(lw_id) FROM %1$I.__nodes
		WHERE status = 'SOURCE'$$;  
  execute format(qrytxt,lw_schema) into myarray;

END;

$lw_sourcenodes$  LANGUAGE 'plpgsql';

COMMENT ON FUNCTION lw_sourcenodes(text) is 
  'Returns an array of sourc nodes';