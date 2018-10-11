/*    Initiate trace of all sources   */

CREATE OR REPLACE FUNCTION lw_traceall(
  lw_schema text
	)
    RETURNS SETOF void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_traceall$

  declare
   
   looprec record;
   qrytxt text;
   timer timestamptz;
   starttime timestamptz;
   zerocount bigint;
  BEGIN
  starttime := clock_timestamp();


  /*    Verify all sources cannot reach each other.... that would be bad   */
  qrytxt := $_$
    select count(*) from pgr_dijkstra(
           $$select lw_id  id, source, target, st_3dlength(g) * multiplier   as cost  
           from %1$I.__lines  $$,
           (select lw_sourcenodes('%1$s')), 
           (select lw_sourcenodes('%1$s')), 
           false
           )
  $_$;
  RAISE NOTICE 'Verify single source directive';
  timer := clock_timestamp();
  EXECUTE format(qrytxt,lw_schema) into zerocount; 
  if zerocount > 0 THEN
    raise exception 'One or more sources can reach or one or more sources.';
  END IF;
   RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;


  qrytxt := $$ SELECT row_number() over (), count(lw_id) over (), lw_id
		FROM %I.__nodes where status = 'SOURCE'$$;
  for looprec in EXECUTE(format(qrytxt, lw_schema)) LOOP
                RAISE NOTICE 'SOURCE: % | % of %', looprec.lw_id,looprec.row_number, looprec.count;
                timer := clock_timestamp();
                perform lw_redirect(lw_schema,looprec.lw_id::int);
                perform lw_tracesource(lw_schema, looprec.lw_id::int, False);
		RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;
  END LOOP;


END;
  

$lw_traceall$;
