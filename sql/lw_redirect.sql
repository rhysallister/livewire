/*    'redirect' lines based upon their source origin    */

CREATE OR REPLACE FUNCTION lw_redirect(
	lw_schema text,
	source bigint)
    RETURNS SETOF void AS 

$lw_redirect$

DECLARE
  qrytxt text;
  updtxt text;
  looprec record;
  timer timestamptz;
  tolerance float;

BEGIN
/*    Trace from all blocks to source   */
  tolerance = lw_tolerance(lw_schema);
  
  qrytxt := $$
  with recursive aaa(node_lw_id, line_lw_id, node_status,  status, line_g, path,  cycle ) as (
	select  n.lw_id, l.lw_id, n.status, 
	 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN 'GOOD' ELSE 'FLIPPED' END, 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.g ELSE st_reverse(l.g) END,
	array[n.lw_id], false
	from %1$I.__nodes n
	join %1$I.__lines l on st_3ddwithin(n.g, l.g, %2$s)
	where n.lw_id = %3$s
	UNION ALL
	SELECT n.lw_id, l.lw_id, n.status,
	
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN 'GOOD' ELSE 'FLIPPED' END, 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.g ELSE st_reverse(l.g) END, path || n.lw_id, n.lw_id =ANY(path)
	FROM aaa
	join %1$I.__lines l on st_3ddwithin(st_endpoint(aaa.line_g),l.g,%2$s)  
	join %1$I.__nodes n on st_3ddwithin(st_endpoint(aaa.line_g),n.g,%2$s)  
	where aaa.line_lw_id <> l.lw_id   and node_status <> 'BLOCK' and not cycle
	),
	bbb as (SELECT * FROM aaa)
	UPDATE %1$I.__lines l set 
	g = st_reverse(l.g), x1 = l.x2, x2 = l.x1, y1 = l.y2, y2 = l.y1, z1 = l.z2, z2 = l.z1, source = l.target, target = l.source
	from bbb where l.lw_id = bbb.line_lw_id
    and  node_status <> 'BLOCK' and status = 'FLIPPED'
   $$;
   execute format(qrytxt,lw_schema, tolerance, source); 
  end;
  

$lw_redirect$ language plpgsql;








/*    'redirect' lines based upon their source origin    */

/*create or replace function lw_redirect(
  lw_schema text,
  source bigint,
  visitedl bigint[] default array[-1]::bigint[],
  visitedn bigint[] default array[-1]::bigint[]
  )
  RETURNS SETOF void AS 
$lw_traceall$

DECLARE
  qrytxt text;
  updtxt text;
  looprec record;
  timer timestamptz;
  tolerance float;

BEGIN
/*    Trace from all blocks to source   */
  tolerance = lw_tolerance(lw_schema);
 
 IF tolerance = 0 THEN
    -- tolerance is 0
    qrytxt := $$SELECT n.lw_id node_id, l.lw_id line_id, source, target, 
		case when st_3dintersects(n.g,st_startpoint(l.g)) then 
                'GOOD' ELSE 'FLIP' END stat
		from %1$I.__nodes n,%1$I.__lines l
		where 
		st_3dintersects(n.g,l.g)
		and n.lw_id = %2$s 
		and not (l.lw_id =ANY (%3$L))
		and not (n.lw_id =ANY (%4$L)) 
		and status <> 'BLOCK' $$;
  ELSE
    -- tolerance is not 0
    qrytxt := format(
                $$SELECT n.lw_id node_id, l.lw_id line_id, source, target, 
		case when st_3ddwithin(n.g,st_startpoint(l.g),%1$s) then 'GOOD' ELSE 'FLIP' END stat
		from %%1$I.__nodes n,%%1$I.__lines l
		where 
		st_3ddwithin(n.g,l.g,%1$s)
		and n.lw_id = %%2$s 
		and not (l.lw_id =ANY (%%3$L))
		and not (n.lw_id =ANY (%%4$L)) 
		and status <> 'BLOCK' $$, 
              tolerance);

  
  END IF;
  
  
  for looprec in EXECUTE(format(qrytxt,lw_schema,source,visitedl,visitedn)) LOOP
--  		RAISE NOTICE '%', looprec; 
	if looprec.stat = 'FLIP' THEN
	  updtxt := $$UPDATE %1$I.__lines
                        set g = st_reverse(g),
                        source = %2$s,
                        target = %3$s
                        where lw_id = %4$s returning *$$;

	--raise notice '%', format(updtxt, lw_schema,looprec.target, looprec.source,looprec.line_id) ;
		
	execute  format(updtxt, lw_schema,looprec.target, looprec.source,looprec.line_id) ;
	visitedl := visitedl || looprec.line_id::bigint;		 
	visitedn := visitedn || looprec.target::bigint;
--	raise notice 'visitedl:  %', visitedl;
--	raise notice 'visitedn:  %', visitedn;
	source := looprec.source;
	else
        visitedl := visitedl || looprec.line_id::bigint;		 
        visitedn := visitedn || looprec.source::bigint;
	source := looprec.target;
	end if;

--	raise notice '%', format('SELECT lw_redirect_(%1$L,%2$s,%3$L,%4$L)',  
--		lw_schema,source,visitedl,visitedn);

	execute format('SELECT lw_redirect(%1$L,%2$s,%3$L,%4$L)', 
			lw_schema,source,visitedl,visitedn);

END LOOP;
  end;
  

$lw_traceall$ language plpgsql; */
