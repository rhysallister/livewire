/*    Add the configuration of a line layer to the config table    */

CREATE OR REPLACE FUNCTION lw_addedgeparticipant(
    lw_schema text,
    edgeinfo json
  )
    RETURNS SETOF void AS 

$lw_addedgeparticipant$

 
BEGIN

  EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig) 
    VALUES ('%2$I.%3$I', 'EDGE', %4$L)$$,
    lw_schema, edgeinfo->>'schemaname',edgeinfo->>'tablename', edgeinfo);




END;
$lw_addedgeparticipant$ language plpgsql;
/*	Adds configuration data for a node participant	*/

CREATE OR REPLACE FUNCTION lw_addnodeparticipant(
    lw_schema text,
    nodeinfo json
  )
    RETURNS SETOF void AS 
$lw_addnodeparticipant$

BEGIN


  EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig)
    VALUES ('%2$I.%3$I', 'NODE', %4$L)$$,
    lw_schema, nodeinfo->>'schemaname',nodeinfo->>'tablename', nodeinfo);



END;
$lw_addnodeparticipant$ LANGUAGE plpgsql;
CREATE FUNCTION lw_edgedelete()  RETURNS trigger AS 

$lw_edgedelete$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_edgedelete$ LANGUAGE plpgsql;
CREATE FUNCTION lw_edgeinsert()  RETURNS trigger AS 

$lw_edgeinsert$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_edgeinsert$ LANGUAGE plpgsql;
CREATE FUNCTION lw_edgeupdate()  RETURNS trigger AS 

$lw_edgeupdate$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_edgeupdate$ LANGUAGE plpgsql;
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
/*	Populate the network tables. 		*/

CREATE OR REPLACE FUNCTION lw_generate(
	  lw_schema text
	)
    RETURNS SETOF void AS 
$lw_generate$

DECLARE
  looprec record;
  srid int;
  tolerance float;
  
BEGIN
	
  tolerance = lw_tolerance(lw_schema); 
  srid = lw_srid(lw_schema);  -- GET LW_SRID
  
  EXECUTE format($$ TRUNCATE %1$I.__lines $$, lw_schema);
  EXECUTE format($$ TRUNCATE %1$I.__nodes $$, lw_schema);

  FOR looprec IN EXECUTE format('SELECT * from   %1$I.%1$I' ,lw_schema) LOOP
    IF looprec.tabletype = 'EDGE' THEN
      PERFORM lw_generateedge(lw_schema,looprec.tablename);
    ELSIF looprec.tabletype = 'NODE' THEN
      PERFORM lw_generatenode(lw_schema,looprec.tablename);
    END IF;
  END LOOP ;
	
	
  IF tolerance = 0 THEN
    
    EXECUTE format($$ 
      with one as ( 
        select st_astext(st_startpoint(g)) aa from %1$I.__lines
        union
        select st_astext(st_endpoint(g)) from %1$I.__lines),
      two as (
        select distinct aa from %1$I.__nodes
        right join one on st_3dintersects(g, st_setsrid(aa::geometry,%2$L))
        where g is null)
      insert into %1$I.__nodes (lw_table_pkid,status, g) 
      SELECT -1, 'NODE', st_setsrid(aa::geometry,%2$L) from two $$,
    lw_schema, srid);
  
    EXECUTE format($$
      UPDATE %1$I.__lines l
      set source = n.lw_id 
      from %1$I.__nodes n 
      where st_3dintersects(n.g, st_startpoint(l.g)) $$,
    lw_schema);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      SET target = n.lw_id 
      from %1$I.__nodes n
      where st_3dintersects(n.g, st_endpoint(l.g)) $$,
    lw_schema);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      SET multiplier = -1
      FROM %1$I.__nodes n 
      where st_3dintersects(n.g, l.g) and n.status = 'BLOCK' $$,
    lw_schema);
  
  ELSE
  
    EXECUTE format($$ 
      with one as ( 
        select st_astext(st_startpoint(g)) aa from %1$I.__lines
        union
        select st_astext(st_endpoint(g)) from %1$I.__lines),
      two as (
        select distinct aa from %1$I.__nodes
        right join one on st_3ddwithin(g, st_setsrid(aa::geometry,%2$L),%3$L)
        where g is null)
      insert into %1$I.__nodes (lw_table_pkid,status, g) 
      SELECT -1, 'NODE', st_setsrid(aa::geometry,%2$L) from two $$,
    lw_schema, srid, tolerance);
  
    EXECUTE format($$
      UPDATE %1$I.__lines l
      set source = n.lw_id 
      from %1$I.__nodes n
      where st_3ddwithin(n.g, st_startpoint(l.g), %2$L) $$,
    lw_schema, tolerance);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      set target = n.lw_id 
      from %1$I.__nodes n
      where st_3ddwithin(n.g, st_endpoint(l.g), %2$L) $$,
    lw_schema, tolerance);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      set multiplier = -1
      from %1$I.__nodes n
      where st_3ddwithin(n.g, l.g, %2$L) and n.status = 'BLOCK' $$,
    lw_schema, tolerance);
 END IF; 
	

END;
$lw_generate$ LANGUAGE plpgsql;
/*    Populate edge tables    */

CREATE OR REPLACE FUNCTION lw_generateedge(
    lw_schema text,
    tablename text
  )
    RETURNS SETOF void AS 
$lw_addedgeparticipant$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  ei json;
  triginfo json;
  looprec record;

BEGIN

  srid = lw_srid(lw_schema);  -- GET LW_SRID
  
  /*    Get table config data   */
  EXECUTE format(
    'SELECT tableconfig FROM %1$I.%1$I WHERE tablename = %2$L',
    lw_schema,tablename
  ) into ei; 

  /*    check that table exists   */
  PERFORM * FROM pg_catalog.pg_class pc
    JOIN pg_catalog.pg_namespace pn on pc.relnamespace=pn.oid
    WHERE nspname = ei->>'schemaname'
    AND relname = ei->>'tablename';
  IF NOT FOUND THEN
    RAISE '% not found in system catalogs', ei->>'tablename';
  END IF;

  /*    check that table has a geometry column    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class on attrelid = oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ei->>'schemaname' AND relname = ei->>'tablename' 
    AND attname = ei->>'geomcolumn' AND typname = 'geometry';
  IF NOT FOUND THEN
    RAISE '% not found or is not of type geometry', ei->>'geomcolumn';
  END IF;

  /*    check that phase column exists    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc on attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ei->>'schemaname'
    AND relname = ei->>'tablename' AND attname = ei->>'phasecolumn';
  IF NOT FOUND THEN
    RAISE 'phase column does not exist';
  END IF;

  /*    check that feederid column exists    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc on attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ei->>'schemaname'
    AND relname = ei->>'tablename' AND attname = ei->>'feederid';
  IF NOT FOUND THEN
    RAISE 'feederid column does not exist';
  END IF;

  /*    Check that config info has the correct phase keys   */
  PERFORM count(*) FROM json_each_text(ei->'phasemap')
    WHERE key in ('ABC','AB','AC','BC','A','B','C') 
    AND value IS NOT NULL except SELECT 7;
  IF FOUND THEN
    RAISE 'phase column mapping not accurate';
  END IF;

  /*    Check that unique column is unique    */
  EXECUTE format(
    'SELECT %3$I FROM %1$I.%2$I group by %3$I 
    having count(%3$I) > 1',
    ei->>'schemaname', ei->>'tablename', ei->>'primarykey'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Primary key column is not unique';
  END IF;

  /*    Check that geometry column has no duplicates    */
  EXECUTE format(
    'SELECT st_astext(%3$I) FROM %1$I.%2$I group by %3$I
    having count(st_astext(%3$I)) > 1',
    ei->>'schemaname', ei->>'tablename', ei->>'geomcolumn'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Geometry column has duplicate points; Table %', tablename;
  END IF;
  
  qrytxt := format($qrytxt$ 
    with one AS (
      SELECT 
        %3$I pk, 
        CASE 
          WHEN %5$I = %6$L THEN 'ABC'
          WHEN %5$I = %7$L THEN 'AB'
          WHEN %5$I = %8$L THEN 'BC'
          WHEN %5$I = %9$L THEN 'AC'
          WHEN %5$I = %10$L THEN 'A'
          WHEN %5$I = %11$L THEN 'B'
          WHEN %5$I = %12$L THEN 'C'
        END phase,
        (st_dumppoints(%4$I)).* 
      FROM %1$I.%2$I
    ),
    two AS (
      SELECT 
        pk, phase, st_force3d(st_setsrid(st_makeline(geom,lead(geom) 
        over (partition by pk ORDER BY path)),%13$L))::geometry(LINESTRINGZ,%13$L) geom 
     FROM one)
    SELECT 
      '%1$I.%2$I' lw_table, pk lw_table_pkid, st_x(st_startpoint(geom)) x1, 
      st_y(st_startpoint(geom)) y1, st_z(st_startpoint(geom)) z1, 
      st_x(st_endpoint(geom)) x2, st_y(st_endpoint(geom)) y2, 
      st_z(st_endpoint(geom)) z2, 1 multiplier, phase, geom
    FROM two 
    WHERE 
     geom IS NOT NULL 
    $qrytxt$,
    ei->>'schemaname', ei->>'tablename', ei->>'primarykey', ei->>'geomcolumn',
    ei->>'phasecolumn', ei->'phasemap'->>'ABC', ei->'phasemap'->>'AB',
    ei->'phasemap'->>'BC', ei->'phasemap'->>'AC', ei->'phasemap'->>'A',
    ei->'phasemap'->>'B', ei->'phasemap'->>'C', srid);
  
EXECUTE format(
  'INSERT INTO %I.__lines (lw_table, lw_table_pkid,x1,y1,z1,x2,y2,z2,multiplier,phase,g) %s',
  lw_schema,qrytxt
  );


/*    Triggers to keep base tables in sync with origin tables         */

  triginfo := '{
    "edge_update": "lw_edgeupdate()",
    "edge_delete": "lw_edgedelete()",
    "edge_insert": "lw_edgeinsert()"}';


  FOR looprec in  select * from json_each_text(triginfo) LOOP
    qrytxt := $$ CREATE TRIGGER %3$I BEFORE UPDATE ON %1$I.%2$I
      FOR EACH ROW EXECUTE PROCEDURE %4$s $$;
    EXECUTE format(qrytxt, ei->>'schemaname',ei->>'tablename',looprec.key, looprec.value);
  END LOOP;



END;
$lw_addedgeparticipant$ LANGUAGE plpgsql;
/*    Populate node tables	 */

CREATE OR REPLACE FUNCTION lw_generatenode(
    lw_schema text,
    tablename text
  )
    RETURNS SETOF void AS 
$lw_generatenode$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  ni json;
  triginfo json;
  looprec record;

BEGIN
  srid = lw_srid(lw_schema);  -- GET LW_SRID
  
  /*    Get table config data   */
  EXECUTE format(
    'select tableconfig from %1$I.%1$I where tablename = %2$L',
    lw_schema,tablename
  ) into ni; 
  
  /*    check that table exists   */
  PERFORM * from pg_catalog.pg_class pc
    JOIN pg_catalog.pg_namespace pn on pc.relnamespace=pn.oid
    where nspname = ni->>'schemaname'
    and relname = ni->>'tablename';
  IF NOT FOUND THEN
    RAISE '% not found in system catalogs', ni->>'tablename';
  END IF;
  
  /*    check that table has a geometry column    */
  PERFORM * from pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class on attrelid = oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ni->>'schemaname' and relname = ni->>'tablename' 
    and attname = ni->>'geomcolumn' and typname = 'geometry';
  IF NOT FOUND THEN
    RAISE '% not found or is not of type geometry', ni->>'geomcolumn';
  END IF;

  /*    check that phase column exists    */
  PERFORM * from pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc on attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ni->>'schemaname'
    and relname = ni->>'tablename' and attname = ni->>'phasecolumn';
  IF NOT FOUND THEN
    RAISE 'phase column does not exist';
  END IF;

  /*    Check that config info has the correct phase keys   */
  PERFORM count(*) from json_each_text(ni->'phasemap')
    where key in ('ABC','AB','AC','BC','A','B','C') and value is not null
    except select 7;
  IF FOUND THEN
    RAISE 'phase column mapping not accurate';
  END IF;

  /*    Check that unique column is unique    */
  EXECUTE format(
    'SELECT %3$I from %1$I.%2$I group by %3$I having count(%3$I) > 1',
    ni->>'schemaname', ni->>'tablename', ni->>'primarykey'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Primary key column is not unique';
  END IF;

  /*    Check that geometry column has no duplicates    */
  EXECUTE format(
    'SELECT st_astext(%3$I) from %1$I.%2$I group by %3$I
    having count(st_astext(%3$I)) > 1',
    ni->>'schemaname', ni->>'tablename', ni->>'geomcolumn'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Geometry column has duplicate points';
  END IF;

  qrytxt := format($qrytxt$ 
    with one as (
      select 
        %3$I pk, 
        CASE 
          WHEN %5$I = %6$L THEN 'ABC'
          WHEN %5$I = %7$L THEN 'AB'
          WHEN %5$I = %8$L THEN 'BC'
          WHEN %5$I = %9$L THEN 'AC'
          WHEN %5$I = %10$L THEN 'A'
          WHEN %5$I = %11$L THEN 'B'
          WHEN %5$I = %12$L THEN 'C'
        END::text phase,
        CASE  
          WHEN %14$s THEN 'SOURCE'
          WHEN %15$s THEN 'BLOCK'
          ELSE 'DEVICE' 
        END status,
        %4$I geom 
      from %1$I.%2$I
   ),
   two as (
     select 
       pk, 
       phase, 
       status,
       st_force3d(st_setsrid(geom,%13$L))::geometry(POINTZ,%13$L) geom 
    from one)
  select 
    '%1$I.%2$I' lw_table, pk lw_table_pkid, status, phase, geom from two 
  where geom is not null
  $qrytxt$,
  ni->>'schemaname', ni->>'tablename', ni->>'primarykey', ni->>'geomcolumn',
  ni->>'phasecolumn', ni->'phasemap'->>'ABC', ni->'phasemap'->>'AB', 
  ni->'phasemap'->>'BC', ni->'phasemap'->>'AC', ni->'phasemap'->>'A', 
  ni->'phasemap'->>'B', ni->'phasemap'->>'C', srid, ni->>'sourcequery',
  ni->>'blockquery');
   
  EXECUTE format('INSERT INTO %I.__nodes (lw_table, lw_table_pkid, status, phase,g) %s',
  lw_schema, qrytxt
  );

 /*	Triggers to keep base tables in sync with origin tables		*/

  triginfo := '{
    "node_update": "lw_nodeupdate()",
    "node_delete": "lw_nodedelete()",
    "node_insert": "lw_nodeinsert()"}';


  FOR looprec in  select * from json_each_text(triginfo) LOOP
    qrytxt := $$ CREATE TRIGGER %3$I BEFORE UPDATE ON %1$I.%2$I
      FOR EACH ROW EXECUTE PROCEDURE %4$s $$;
    EXECUTE format(qrytxt, ni->>'schemaname',ni->>'tablename',looprec.key, looprec.value);
  END LOOP;




END;
$lw_generatenode$ LANGUAGE plpgsql;
/*      Creates schema and livewire base tables      */

CREATE OR REPLACE FUNCTION lw_initialise(
  lw_schema text,
  lw_srid integer,
  lw_tolerance float default 0)
RETURNS SETOF void AS 

$lw_initialise$

BEGIN

  IF lw_tolerance < 0 THEN
    RAISE EXCEPTION 'Tolerances cannot be less than 0.';
  ELSIF lw_tolerance > 0 THEN
    RAISE NOTICE 'Your tolerance is greater than 0.';
    RAISE NOTICE 'LiveWire works best when line ends are coincident.';
    RAISE NOTICE 'Your mileage may vary.';
  END IF;
  
  EXECUTE format($$ CREATE SCHEMA IF NOT EXISTS %1$I; $$,lw_schema);
  
  EXECUTE format($$ CREATE TABLE %1$I.__lines
    (
    lw_table text,
    lw_table_pkid text NOT NULL,
    lw_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    source integer,
    target integer,
    x1 double precision,
    y1 double precision,
    z1 double precision,
    x2 double precision,
    y2 double precision,
    z2 double precision,
    multiplier bigint,
    phase text,
    feederid text,
    g geometry(LineStringZ,%2$L),
    CONSTRAINT phase_check CHECK 
      (phase = ANY (ARRAY[
          'ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text,
          'A'::text, 'B'::text, 'C'::text]))); $$,
    lw_schema, lw_srid);
  
  EXECUTE format($$ CREATE TABLE %1$I.__nodes
    (
    lw_table text,
    lw_table_pkid text NOT NULL,
    lw_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    status text,
    phase text,
    feederid text,
    g geometry(PointZ,%2$L),
    CONSTRAINT phase_check CHECK 
      (phase = ANY (ARRAY[
        'ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text,
        'A'::text, 'B'::text, 'C'::text])))$$,
    lw_schema, lw_srid);
  
  EXECUTE format($$ CREATE TABLE %1$I.__livewire
    (
        nodes bigint[],
        edges bigint[]
    ) $$, lw_schema);
  
  EXECUTE format($$ CREATE TABLE %1$I.%1$I
    (
        tablename text PRIMARY KEY,
        tabletype text,
        tableconfig json
    ) $$, lw_schema);
  
  EXECUTE format($$ CREATE INDEX ON %1$I.__lines USING gist (g) $$,lw_schema);
  EXECUTE format($$ CREATE INDEX ON %1$I.__nodes USING gist (g) $$,lw_schema);
  EXECUTE format($$ CREATE UNIQUE INDEX ON %1$I.%1$I (tabletype) 
    where tabletype = 'config' $$,lw_schema); 
  
  EXECUTE format($$ INSERT INTO %1$I.%1$I VALUES 
    ('%1$I.%1$I','config', '{"lw_tolerance": "%2$s", "lw_srid" : "%3$s"}'::json) $$,
  lw_schema, lw_tolerance, lw_srid);

END;

$lw_initialise$ LANGUAGE plpgsql;
CREATE FUNCTION lw_nodedelete()  RETURNS trigger AS 

$lw_nodedelete$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_nodedelete$ LANGUAGE plpgsql;
CREATE FUNCTION lw_nodeinsert()  RETURNS trigger AS 

$lw_nodeinsert$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_nodeinsert$ LANGUAGE plpgsql;
CREATE FUNCTION lw_nodeupdate()  RETURNS trigger AS 

$lw_nodemodify$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_nodemodify$ LANGUAGE plpgsql;
/*    'redirect' lines based upon their source origin    */

create or replace function lw_redirect(
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
  

$lw_traceall$ language plpgsql;
/*    Returns an array of all SOURCE nodes in a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_sourcenodes(
    in lw_schema text,
    out myarray bigint[]
  ) AS 
$lw_sourcenodes$

DECLARE
  qrytxt text;

BEGIN 
  
  qrytxt := $$select array_agg(lw_id) from %1$I.__nodes
		where status = 'SOURCE'$$;  
  execute format(qrytxt,lw_schema) into myarray;

END;

$lw_sourcenodes$  LANGUAGE 'plpgsql';
/*		Gets the SRID of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_srid(
  in lw_schema text,
  out lw_srid bigint
  ) as 
  
$lw_srid$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_srid')::bigint
    from %1$I.%1$I where tabletype = 'config' $$, lw_schema)
    into lw_srid;

END;

$lw_srid$ LANGUAGE plpgsql;
/*		Gets the tolerance of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_tolerance(
  in lw_schema text,
  out lw_tolerance float
  ) as 
  
$lw_tolerance$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_tolerance')::float
    from %1$I.%1$I where tabletype = 'config' $$, lw_schema)
    into lw_tolerance;

END;

$lw_tolerance$ LANGUAGE plpgsql;
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
  EXECUTE format(qrytxt,lw_schema) into zerocount; 
  if zerocount > 0 THEN
    raise exception 'One or more sources can reach or one or more sources.';
  END IF;
 


  qrytxt := $$ SELECT row_number() over (), count(lw_id) over (), lw_id
		FROM %I.__nodes where status = 'SOURCE'$$;
  for looprec in EXECUTE(format(qrytxt, lw_schema)) LOOP
                RAISE NOTICE 'SOURCE: % | % of %', looprec.lw_id,looprec.row_number, looprec.count;
                timer := clock_timestamp();
                perform lw_redirect(lw_schema,looprec.lw_id::int);
                perform lw_tracesource(lw_schema, looprec.lw_id::int);
		RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;
  END LOOP;


END;
  

$lw_traceall$;
CREATE FUNCTION lw_tracednstream(
  in lw_schema text,
  in lw_id bigint,
  out g geometry) as

$lw_tracednstream$
  DECLARE
    qrytxt text;

  BEGIN
    qrytxt := 'SELECT st_union(g) g FROM %1$I.__lines WHERE lw_id IN
                (SELECT distinct(unnest(edges[(array_position(
                  nodes::int[], %2$s)):])) FROM %1$I.__livewire 
              WHERE %2$s =ANY (nodes))';
    EXECUTE format(qrytxt, lw_schema, lw_id) INTO g;

  END;

$lw_tracednstream$ LANGUAGE plpgsql;
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
CREATE FUNCTION lw_traceupstream(
  in lw_schema text,
  in lw_id bigint,
  out g geometry) as

$lw_traceupstream$
  DECLARE
    qrytxt text;

  BEGIN
    qrytxt := 'SELECT st_union(g) g FROM %1$I.__lines WHERE lw_id IN
                (SELECT distinct(unnest(edges[:(array_position(
                  nodes::int[], %2$s)-1)])) FROM %1$I.__livewire 
              WHERE %2$s =ANY (nodes))';
    EXECUTE format(qrytxt, lw_schema, lw_id) INTO g;

  END;

$lw_traceupstream$ LANGUAGE plpgsql;
