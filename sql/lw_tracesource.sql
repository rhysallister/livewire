/*    Given a source lw_id, trace a feeder and populate __livewire    */

CREATE OR REPLACE FUNCTION lw_tracesource(
  IN lw_schema text,
  IN source bigint,
  IN checksource boolean default true
  )
    RETURNS SETOF void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_tracesource$

DECLARE
  closeblock bigint;
  closeblocks bigint[];
  singlesource boolean;
  qrytxt text;
  zerocount bigint;

BEGIN

EXECUTE format('delete FROM %I.__livewire WHERE nodes[1] = %s',lw_schema,source);
IF checksource = True THEN
 RAISE NOTICE 'ALLCHECK';

RAISE NOTICE 'Verify single source directive';
  EXECUTE 'SELECT lw_singlesource($1, $2)' INTO singlesource USING lw_schema, source;
  IF NOT singlesource THEN
   RAISE EXCEPTION 'One or more sources can reach one or more sources';
  END IF;


END IF;
 
 
  
  /*    Trace FROM source out to distance  */
  qrytxt := $_$
		INSERT into %1$I.__livewire
        SELECT  
          array_agg(node order by path_seq) nodes ,
          array_remove(array_agg(edge ORDER BY path_seq),-1::bigint) edges
        FROM pgr_dijkstra(
        	 $$SELECT lw_id  id, source, target, st_3dlength(g) AS cost  
        	 FROM %1$I.__lines l  $$,
        	 array[%2$s]::bigint[],
        	 (SELECT lw_endnodes('%1$s')),
        	 true
        	 )
        JOIN %1$I.__nodes on lw_id = node
        GROUP BY start_vid, end_vid
  $_$;  
  --raise notice '%', format(qrytxt,lw_schema, source, distance);
  EXECUTE format(qrytxt,lw_schema, source);





END;
$lw_tracesource$;

COMMENT ON FUNCTION lw_tracesource(in lw_schema text, in source bigint, in truth boolean) IS
  'Returns geometric trace give a livewire name and a set of lw_ids FROM __nodes.';