/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_tracesource(
    in lw_schema text,
    in source bigint,
    in checksource boolean default true
  )
    RETURNS void
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
      from %1$I.lines  $$,
      %2$s, 
      lw_sourcenodes(lw_schema),
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


  /*    Find blocks within 20km of current extent of feeder. Trace from found blocks to source.   */
 /*
  qrytxt := $_$
    select array_agg(lw_id) from %1$I.nodes 
    where status = 'BLOCK' and g && (
      select st_expand(st_collect(g),2000) 
      from %1$I.lines where lw_id in (
        select unnest(edges) from %1$I.livewire where nodes[1] =  %2$s
        )
    )
  $_$;

qrytxt:= $_$
  select array_agg(lw_id) from (
  select lw_id from %1$I.nodes
  where status = 'BLOCK'
  order by g <-> (
      select st_collect(g)
      from %1$I.lines where lw_id in (
        select unnest(edges) from %1$I.livewire where nodes[1] =  %2$s
        ))
    limit 10) as foo$_$;


  execute format(qrytxt,lw_schema,source) into closeblocks;

  foreach closeblock in array closeblocks loop
    qrytxt := $_$
      INSERT into %1$I.livewire
      select 
      array_agg(node order by path_seq) nodes ,
        array_remove(array_agg(edge order by path_seq),-1::bigint) edges
      from pgr_dijkstra(
      $$select lw_id  id, source, target, 
      st_length(g) * case when %3$s in (source,target) then 1 else multiplier end  as cost  
        from %1$I.lines
        $$,
        array[%2$s]::bigint[],
        array[%3$s]::bigint[],
        false
        )
        join %1$I.nodes on lw_id = node
      group by start_vid, end_vid $_$;
    execute format(qrytxt,lw_schema,source, closeblock);
  END LOOP;
*/




END;
$lw_tracesource$;
