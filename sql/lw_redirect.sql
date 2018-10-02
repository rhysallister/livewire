/*    'redirect' lines based upon their source origin    */

create or replace function lw_redirect(
  lw_schema text,
  source bigint,
  visitedl bigint[] default array[-1]::bigint[],
  visitedn bigint[] default array[-1]::bigint[]
  )
  RETURNS void AS 
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
		case when st_equals(n.g,st_startpoint(l.g)) then 
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
  

$lw_traceall$ language plpgsql;
