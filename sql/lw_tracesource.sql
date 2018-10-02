/*    Given a source lw_id, trace a feeder and populate __livewire    */

CREATE OR REPLACE FUNCTION lw_tracesource(
    in lw_schema text,
    in source bigint,
    in checksource boolean default true
  )
    RETURNS SETOF void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_tracesource$

DECLARE
  closeblock bigint;
  closeblocks bigint[];
  qrytxt text;
  zerocount bigint;

BEGIN

EXECUTE format('delete from %I.__livewire where nodes[1] = %s',lw_schema,source);

if checksource = True THEN

/*    Verify that this source cannot reach other sources....that would be bad   */
  qrytxt := $_$
    select count(*) from pgr_dijkstra(
      $$select lw_id  id, source, target, st_3dlength(g) * multiplier as cost  
      from %1$I.__lines  $$,
      %2$s, 
      lw_sourcenodes(%1$L),
      false
    )
  $_$;
  EXECUTE format(qrytxt,lw_schema, source) into zerocount; 
  IF zerocount > 0 THEN
    RAISE EXCEPTION 'Zerocount is not zero!!';
  END IF;


END IF;
 
 
  
  /*    Trace from source out to distance  */
  qrytxt := $_$
		INSERT into %1$I.__livewire
        select  
          array_agg(node order by path_seq) nodes ,
          array_remove(array_agg(edge order by path_seq),-1::bigint) edges
        from pgr_dijkstra(
        	 $$select lw_id  id, source, target, st_3dlength(g) as cost  
        	 from %1$I.__lines l  $$,
        	 array[%2$s]::bigint[],
        	 (select lw_endnodes('%1$s')),
        	 true
        	 )
        join %1$I.__nodes on lw_id = node
        group by start_vid, end_vid
  $_$;  
  --raise notice '%', format(qrytxt,lw_schema, source, distance);
  EXECUTE format(qrytxt,lw_schema, source);





END;
$lw_tracesource$;
