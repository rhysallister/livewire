/*    Returns an array of lw_ids that correspond to endnodes    */

CREATE OR REPLACE FUNCTION lw_endnodes(
    in lw_schema text,
    out myarray bigint[]
  ) AS 

$lw_endnodes$
DECLARE
  qrytxt text; 
BEGIN
  -- Find all end nodes in a given livewire
  
  qrytxt := 'select array_agg(lw_id) from
		(select source lw_id from 
		(select lw_id, source from %1$I.__lines 
		union 
		select lw_id, target from %1$I.__lines ) as lines
		group by source  
		having count(lw_id) = 1) as lw_ids';
  
  execute format(qrytxt,lw_schema) into myarray;

END;
$lw_endnodes$ LANGUAGE 'plpgsql';
