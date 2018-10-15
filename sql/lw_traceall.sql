/*    Initiate trace of all sources   */

CREATE OR REPLACE FUNCTION lw_traceall(
  lw_schema text
	)
    RETURNS SETOF void AS 

$lw_traceall$

  declare
   
   looprec record;
   qrytxt text;
   timer timestamptz;
   starttime timestamptz;
   singlesource boolean;
   zerocount bigint;
  BEGIN
  starttime := clock_timestamp();


  

  /*    Verify all sources cannot reach each other.... that would be bad   */
  
  RAISE NOTICE 'Verify single source directive';
  EXECUTE 'SELECT lw_singlesource($1)' INTO singlesource USING lw_schema;
  IF NOT singlesource THEN
   RAISE EXCEPTION 'One or more sources can reach one or more sources';
  END IF;
    

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
  

$lw_traceall$ LANGUAGE plpgsql;
