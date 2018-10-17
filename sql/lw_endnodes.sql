/*    Returns an array of lw_ids that correspond to endnodes    */

CREATE OR REPLACE FUNCTION lw_endnodes(
  IN lw_schema text,
  OUT myarray bigint[]
  ) AS 

$lw_endnodes$

DECLARE
  qrytxt text; 

BEGIN
  -- Find all end nodes in a given livewire
  
  qrytxt := 'SELECT array_agg(lw_id) FROM
		(SELECT source lw_id FROM 
		(SELECT lw_id, source FROM %1$I.__lines 
		UNION 
		SELECT lw_id, target FROM %1$I.__lines ) as lines
		group by source  
		having count(lw_id) = 1) as lw_ids';
  
  execute format(qrytxt,lw_schema) into myarray;

END;
$lw_endnodes$ LANGUAGE 'plpgsql';


COMMENT ON FUNCTION lw_endnodes IS
  'Returns an array of all endnodes in a given livewire';
