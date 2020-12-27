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
    lw_schema,
    edgeinfo->>'schemaname',
    edgeinfo->>'tablename',
    edgeinfo
    );

END;
$lw_addedgeparticipant$ language plpgsql;

COMMENT ON FUNCTION lw_addedgeparticipant(text, json) IS
  'Adds the configuration data for a edge table to the livewire config table';
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
    lw_schema, 
    nodeinfo->>'schemaname',
    nodeinfo->>'tablename',
    nodeinfo
    );

END;
$lw_addnodeparticipant$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_addnodeparticipant IS
  'Adds the configuration for a node table to the livewire config table';
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

COMMENT ON FUNCTION lw_edgedelete IS
  'Trigger function to fire for a delete on any edge particpant';
CREATE FUNCTION lw_edgeinsert()  RETURNS trigger AS 

$lw_edgeinsert$

  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  
  END;
$lw_edgeinsert$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_edgeINSERT IS
  'Trigger function to fire for an INSERT on any edge particpant';
CREATE FUNCTION lw_edgeupdate()  RETURNS trigger AS 

$lw_edgeupdate$

  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;

  END;
$lw_edgeupdate$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_edgeupdate IS
  'Trigger function to fire for an update on any edge particpant';
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
  
  qrytxt := $$SELECT array_agg(lw_id::bigint) ||
  (SELECT array_agg(distinct lw_id) from %1$I.__nodes where status = 'BLOCK') FROM
		(SELECT source lw_id FROM 
		(SELECT lw_id, source FROM %1$I.__lines 
		UNION 
		SELECT lw_id, target FROM %1$I.__lines ) as lines
		group by source  
		having count(lw_id) = 1) as lw_ids$$;
  
  execute format(qrytxt,lw_schema) into myarray;

END;
$lw_endnodes$ LANGUAGE 'plpgsql';


COMMENT ON FUNCTION lw_endnodes IS
  'Returns an array of all endnodes in a given livewire';
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
  qrytxt text;
  
BEGIN
	
  tolerance = lw_tolerance(lw_schema); 
  srid = lw_srid(lw_schema);  -- GET LW_SRID
  
  EXECUTE format($$ TRUNCATE %1$I.__lines $$, lw_schema);
  EXECUTE format($$ TRUNCATE %1$I.__nodes $$, lw_schema);
  EXECUTE format(
    $$ ALTER TABLE %1$I.__lines ALTER COLUMN lw_id RESTART $$, lw_schema);
  EXECUTE format(
    $$ ALTER TABLE %1$I.__nodes ALTER COLUMN lw_id RESTART $$, lw_schema);


  FOR looprec IN EXECUTE format('SELECT * FROM   %1$I.%1$I' ,lw_schema) LOOP
    IF looprec.tabletype = 'EDGE' THEN
      PERFORM lw_generateedge(lw_schema,looprec.tablename);
    ELSIF looprec.tabletype = 'NODE' THEN
      PERFORM lw_generatenode(lw_schema,looprec.tablename);
    END IF;
  END LOOP ;
	
	
  IF tolerance = 0 THEN
    
    EXECUTE format($$ 
      with one as ( 
        SELECT st_astext(st_startpoint(g)) aa FROM %1$I.__lines
        UNION         SELECT st_astext(st_endpoint(g)) FROM %1$I.__lines),
      two as (
        SELECT distinct aa FROM %1$I.__nodes
        right join one on st_3dintersects(g, st_setsrid(aa::geometry,%2$L))
        WHERE g is null)
      INSERT into %1$I.__nodes (lw_table_pkid,status, g) 
      SELECT -1, 'NODE', st_setsrid(aa::geometry,%2$L) FROM two $$,
    lw_schema, srid);
  
    EXECUTE format($$
      UPDATE %1$I.__lines l
      set source = n.lw_id 
      FROM %1$I.__nodes n 
      WHERE st_3dintersects(n.g, st_startpoint(l.g)) $$,
    lw_schema);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      SET target = n.lw_id 
      FROM %1$I.__nodes n
      WHERE st_3dintersects(n.g, st_endpoint(l.g)) $$,
    lw_schema);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      SET multiplier = -1
      FROM %1$I.__nodes n 
      WHERE st_3dintersects(n.g, l.g) and n.status = 'BLOCK' $$,
    lw_schema);
  
  ELSE
  
    EXECUTE format($$ 
      with one as ( 
        SELECT st_astext(st_startpoint(g)) aa FROM %1$I.__lines
        UNION         SELECT st_astext(st_endpoint(g)) FROM %1$I.__lines),
      two as (
        SELECT distinct aa FROM %1$I.__nodes
        right join one on st_3ddwithin(g, st_setsrid(aa::geometry,%2$L),%3$L)
        WHERE g is null)
      INSERT into %1$I.__nodes (lw_table_pkid,status, g) 
      SELECT -1, 'NODE', st_setsrid(aa::geometry,%2$L) FROM two $$,
    lw_schema, srid, tolerance);
  
    EXECUTE format($$
      UPDATE %1$I.__lines l
      set source = n.lw_id 
      FROM %1$I.__nodes n
      WHERE st_3ddwithin(n.g, st_startpoint(l.g), %2$L) $$,
    lw_schema, tolerance);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      set target = n.lw_id 
      FROM %1$I.__nodes n
      WHERE st_3ddwithin(n.g, st_endpoint(l.g), %2$L) $$,
    lw_schema, tolerance);

    EXECUTE format($$
      UPDATE %1$I.__lines l
      set multiplier = -1
      FROM %1$I.__nodes n
      WHERE st_3ddwithin(n.g, l.g, %2$L) and n.status = 'BLOCK' $$,
    lw_schema, tolerance);
 END IF; 
	
 
 EXECUTE format(
  'SELECT  count(*)  FROM %1$I.__lines  WHERE source = target',
  lw_schema
 ) INTO looprec;
 IF looprec.count > 1 THEN
   qrytxt := 'SELECT lw_table, lw_table_pkid, st_length(g) FROM %1$I.__lines WHERE source = target';
  RAISE NOTICE 'The follwing rows are probably below the tolerance threshold:'; 
  FOR looprec in EXECUTE format(qrytxt, lw_schema) LOOP
     RAISE NOTICE 'Primary Key % in table %.', looprec.lw_table_pkid, looprec.lw_table;
  END LOOP;
  RAISE EXCEPTION 'Fix the data and rerun lw_generate.';
END IF;


END;
$lw_generate$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_generate IS
  'Populate the shadow tables';
/*    Populate edge tables    */

CREATE OR REPLACE FUNCTION lw_generateedge(
    lw_schema text,
    tablename text
  )
    RETURNS SETOF void AS 
$lw_generateedge$

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
    JOIN pg_catalog.pg_namespace pn ON pc.relnamespace=pn.oid
    WHERE nspname = ei->>'schemaname'
    AND relname = ei->>'tablename';
  IF NOT FOUND THEN
    RAISE '% not found in system catalogs', ei->>'tablename';
  END IF;

  /*    check that table has a geometry column    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt ON pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc ON attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn ON relnamespace = pn.oid
    WHERE nspname = ei->>'schemaname' AND relname = ei->>'tablename' 
    AND attname = ei->>'geomcolumn' AND typname = 'geometry';
  IF NOT FOUND THEN
    RAISE '% not found or is not of type geometry', ei->>'geomcolumn';
  END IF;

  /*    check that phase column exists    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt ON pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc ON attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn ON relnamespace = pn.oid
    WHERE nspname = ei->>'schemaname'
    AND relname = ei->>'tablename' AND attname = ei->>'phasecolumn';
  IF NOT FOUND THEN
    RAISE 'phase column does not exist';
  END IF;

  /*    check that feederid column exists    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt ON pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc ON attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn ON relnamespace = pn.oid
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
/*
  triginfo := '{
    "edge_update": "lw_edgeupdate()",
    "edge_delete": "lw_edgedelete()",
    "edge_insert": "lw_edgeinsert()"}';


  FOR looprec in  SELECT * FROM json_each_text(triginfo) LOOP
    qrytxt := $$ CREATE TRIGGER %3$I BEFORE UPDATE ON %1$I.%2$I
      FOR EACH ROW EXECUTE PROCEDURE %4$s $$;
    EXECUTE format(qrytxt, ei->>'schemaname',ei->>'tablename',looprec.key, looprec.value);
  END LOOP;

*/

END;
$lw_generateedge$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_generateedge IS '';
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
    'SELECT tableconfig FROM %1$I.%1$I WHERE tablename = %2$L',
    lw_schema,tablename
  ) into ni; 
  
  /*    check that table exists   */
  PERFORM * FROM pg_catalog.pg_class pc
    JOIN pg_catalog.pg_namespace pn on pc.relnamespace=pn.oid
    WHERE nspname = ni->>'schemaname'
    and relname = ni->>'tablename';
  IF NOT FOUND THEN
    RAISE '% not found in system catalogs', ni->>'tablename';
  END IF;
  
  /*    check that table has a geometry column    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc on attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ni->>'schemaname' and relname = ni->>'tablename' 
    and attname = ni->>'geomcolumn' and typname = 'geometry';
  IF NOT FOUND THEN
    RAISE '% not found or is not of type geometry', ni->>'geomcolumn';
  END IF;

  /*    check that phase column exists    */
  PERFORM * FROM pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc on attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ni->>'schemaname'
    and relname = ni->>'tablename' and attname = ni->>'phasecolumn';
  IF NOT FOUND THEN
    RAISE 'phase column does not exist';
  END IF;

  /*    Check that config info has the correct phase keys   */
  PERFORM count(*) FROM json_each_text(ni->'phasemap')
    WHERE key in ('ABC','AB','AC','BC','A','B','C') and value is not null
    except SELECT 7;
  IF FOUND THEN
    RAISE 'phase column mapping not accurate';
  END IF;

  /*    Check that unique column is unique    */
  EXECUTE format(
    'SELECT %3$I FROM %1$I.%2$I group by %3$I having count(%3$I) > 1',
    ni->>'schemaname', ni->>'tablename', ni->>'primarykey'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Primary key column is not unique';
  END IF;

  /*    Check that geometry column has no duplicates    */
  EXECUTE format(
    'SELECT st_astext(%3$I) FROM %1$I.%2$I group by %3$I
    having count(st_astext(%3$I)) > 1',
    ni->>'schemaname', ni->>'tablename', ni->>'geomcolumn'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Geometry column has duplicate points';
  END IF;

  qrytxt := format($qrytxt$ 
    with one as (
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
        END::text phase,
        CASE  
          WHEN %14$s THEN 'SOURCE'
          WHEN %15$s THEN 'BLOCK'
          ELSE 'DEVICE' 
        END status,
        %4$I geom 
      FROM %1$I.%2$I
   ),
   two as (
     SELECT 
       pk, 
       phase, 
       status,
       st_force3d(st_setsrid(geom,%13$L))::geometry(POINTZ,%13$L) geom 
    FROM one)
  SELECT 
    '%1$I.%2$I' lw_table, pk lw_table_pkid, status, phase, geom FROM two 
  WHERE geom is not null
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
 /*
  triginfo := '{
    "node_update": "lw_nodeupdate()",
    "node_delete": "lw_nodedelete()",
    "node_insert": "lw_nodeinsert()"}';


  FOR looprec in  SELECT * FROM json_each_text(triginfo) LOOP
    qrytxt := $$ CREATE TRIGGER %3$I BEFORE UPDATE ON %1$I.%2$I
      FOR EACH ROW EXECUTE PROCEDURE %4$s $$;
    EXECUTE format(qrytxt, ni->>'schemaname',ni->>'tablename',looprec.key, looprec.value);
  END LOOP;

*/


END;
$lw_generatenode$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_generatenode is '';
/*      Creates schema and livewire base tables      */

CREATE OR REPLACE FUNCTION lw_initialise(
  lw_schema text,
  lw_srid integer,
  lw_tolerance float default 0,
  lw_trackorigin boolean default False,
  lw_description text default NULL)
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

  IF lw_description IS NULL THEN
    lw_description := format('%1$s is a livewire with srid of %2$s',lw_schema, lw_srid);
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
  EXECUTE format($$ CREATE INDEX ON %1$I.__livewire USING gin (nodes) $$,
    lw_schema);
  EXECUTE format($$ CREATE INDEX ON %1$I.__livewire USING gin (edges) $$,
    lw_schema);
  EXECUTE format($$ CREATE UNIQUE INDEX ON %1$I.%1$I (tabletype) 
    WHERE tabletype = 'config' $$,lw_schema); 
  
  EXECUTE format($$ INSERT INTO %1$I.%1$I VALUES 
    ('%1$I.%1$I','config', '{"lw_tolerance": "%2$s", "lw_srid" : "%3$s", "lw_trackorigin": "%4$s"}'::json) $$,
  lw_schema, lw_tolerance, lw_srid, lw_trackorigin);

END;

$lw_initialise$ LANGUAGE plpgsql;


COMMENT ON FUNCTION lw_initialise IS 'lw_initialise: Livewire function to instantiate a new livewire';
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

COMMENT ON FUNCTION lw_nodedelete is 
  'Trigger function to fire for a delete on any node particpant';CREATE FUNCTION lw_nodeinsert()  RETURNS trigger AS 

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

COMMENT ON FUNCTION lw_nodeinsert is 
'Trigger function to fire for an insert on any node particpant';CREATE FUNCTION lw_nodeupdate()  RETURNS trigger AS 

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

COMMENT ON FUNCTION lw_nodeupdate is 
  'Trigger function to fire for an update on any node particpant';
/*		Gets row from origin table		*/

CREATE OR REPLACE FUNCTION lw_originrow(
  IN lw_schema text,
  IN lw_id bigint,
  OUT dump jsonb
  ) AS
  
$lw_originrow$

DECLARE
  thisrow jsonb; 
  tableconfig jsonb;

BEGIN
  EXECUTE format($$ SELECT row_to_json(n.*) FROM %1$I.__nodes n 
                 WHERE lw_id = %2$s $$,
    lw_schema,
    lw_id
    )
    INTO thisrow;

  EXECUTE format($$ SELECT tableconfig FROM %1$I.%1$I WHERE tablename = %2$L $$,
    lw_schema,
    thisrow->>'lw_table'
    )
    INTO tableconfig;

  EXECUTE format($$ SELECT  row_to_json(a.*) FROM 
                (SELECT * FROM %1$s where %2$I =  %3$L) as a $$,
    thisrow->>'lw_table',
    tableconfig->>'primarykey',
    thisrow->>'lw_table_pkid'
    )
    INTO dump;

END;

$lw_originrow$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_originrow(IN lw_schema text, in lw_id bigint) IS
  'Returns the row of the original table given the lw_id of the shadow network.';
/*    'redirect' lines based upon their source origin    */

CREATE OR REPLACE FUNCTION lw_redirect(
	lw_schema text,
	source bigint)
    RETURNS SETOF void AS 

$lw_redirect$

DECLARE
  feedername text;
  qrytxt text;
  updtxt text;
  looprec record;
  tablename text;
  timer timestamptz;
  tolerance float;

BEGIN
/*    Trace FROM all blocks to source   */

  tolerance = lw_tolerance(lw_schema);
  RAISE NOTICE 'Redirecting Source: %', source;
  timer =  clock_timestamp();
  EXECUTE format('SELECT lw_table from %1$I.__nodes where lw_id = %2$L', lw_schema, source) INTO tablename;
  feedername = lw_tableconfig(lw_schema, tablename)->> 'feederid';
  feedername = lw_originrow(lw_schema, source)->>feedername;
  RAISE NOTICE '%', feedername;
  
 
   
  qrytxt := $$
  with recursive aaa(node_lw_id, line_lw_id, node_status,  status, source, target, line_g, path,  cycle ) as (
	SELECT  n.lw_id, l.lw_id, n.status, 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN 'GOOD' ELSE 'FLIPPED' END, 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN target ELSE source END,
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN source ELSE target END,
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.g ELSE st_reverse(l.g) END,
	array[n.lw_id], false
	FROM %1$I.__nodes n
	join %1$I.__lines l on st_3ddwithin(n.g, l.g, %2$s)
	WHERE n.lw_id = %3$s
	UNION ALL
	SELECT n.lw_id, l.lw_id, n.status,
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN 'GOOD' ELSE 'FLIPPED' END, 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.target ELSE l.source END,
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.source ELSE l.target END,
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.g ELSE st_reverse(l.g) END, path || n.lw_id, n.lw_id =ANY(path)
	FROM aaa
	join %1$I.__lines l on st_3ddwithin(st_endpoint(aaa.line_g),l.g,%2$s)  
	join %1$I.__nodes n on st_3ddwithin(st_endpoint(aaa.line_g),n.g,%2$s)  
	WHERE aaa.line_lw_id <> l.lw_id   and node_status <> 'BLOCK' and not cycle
	),
	bbb as (SELECT * FROM aaa)
	UPDATE %1$I.__lines l set 
	g = bbb.line_g, x1 = st_x(st_startpoint(line_g)), x2 =st_x(st_endpoint(line_g)), 
	y1 = st_y(st_startpoint(line_g)), y2 = st_y(st_endpoint(line_g)), 
	z1 = st_z(st_startpoint(line_g)), z2 = st_z(st_endpoint(line_g)), 
	source = bbb.target, target = bbb.source, feederid = %4$L
	FROM bbb WHERE l.lw_id = bbb.line_lw_id
    and  node_status <> 'BLOCK' 
   $$;
   execute format(qrytxt,lw_schema, tolerance, source, feedername); 
  --  Update the nodes with the appropriate feederid 
  qrytxt := $$
  update %1$I.__nodes n set feederid =  %4$L
    from %1$I.__lines l where l.feederid =  %4$L
  and n.status <> 'BLOCK' and st_3ddwithin(l.g,n.g,%2$s )
   $$;
  execute format(qrytxt,lw_schema, tolerance, source, feedername); 

  RAISE NOTICE 'Duration: %', clock_timestamp() - timer;
  end;
  
$lw_redirect$ language plpgsql;

COMMENT ON FUNCTION lw_redirect is 
  'Makes shadow network directed based upon a give source node.';
CREATE OR REPLACE FUNCTION lw_singlesource(
  IN lw_schema text,
  OUT truth boolean
        )
   
AS $lw_singlesource$

  DECLARE

   qrytxt text;
   zerocount bigint; 
  BEGIN

  /*    Verify all sources cannot reach each other.... that would be bad   */
  qrytxt := $_$
    SELECT count(*) FROM pgr_dijkstra(
           $$SELECT lw_id  id, source, target, st_3dlength(g) * multiplier   as cost
           FROM %1$I.__lines  $$,
           (SELECT lw_sourcenodes('%1$s')),
           (SELECT lw_sourcenodes('%1$s')),
           false
           )
  $_$;
  EXECUTE format(qrytxt,lw_schema) into zerocount;
  IF zerocount > 0 THEN
    truth = False;
  ELSE
    truth = True;
  END IF;

END;

$lw_singlesource$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_singlesource(text) is 
  'Given an lw_schema determine if any sourcenode can reach any other sourcenode';

CREATE OR REPLACE FUNCTION lw_singlesource(
  IN lw_schema text,
  IN lw_id bigint,
  OUT truth boolean
        )
   
AS $lw_singlesource$

  DECLARE

   qrytxt text;
   zerocount bigint; 
  BEGIN


  /*    Verify all sources cannot reach each other.... that would be bad   */
  qrytxt := $_$
    SELECT count(*) FROM pgr_dijkstra(
           $$SELECT lw_id  id, source, target, st_3dlength(g) * multiplier AS cost
           FROM %1$I.__lines  $$,
           '%2$s',
           (SELECT lw_sourcenodes('%1$s')),
           false
           )
  $_$;
  EXECUTE format(qrytxt,lw_schema, lw_id) into zerocount;
  IF zerocount > 0 THEN
    truth = False;
  ELSE
    truth = True;
  END IF;


END;

$lw_singlesource$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_singlesource(text, bigint) is 
  'Given an lw_schema and an lw_id determine if lw_id can reach more than one source';
/*    Returns an array of all SOURCE nodes in a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_sourcenodes(
  IN lw_schema text,
    out myarray bigint[]
  ) AS 
$lw_sourcenodes$

DECLARE
  qrytxt text;

BEGIN 
  
  qrytxt := $$SELECT array_agg(lw_id) FROM %1$I.__nodes
		WHERE status = 'SOURCE'$$;  
  execute format(qrytxt,lw_schema) into myarray;

END;

$lw_sourcenodes$  LANGUAGE 'plpgsql';

COMMENT ON FUNCTION lw_sourcenodes(text) is 
  'Returns an array of sourc nodes';/*		Gets the SRID of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_srid(
  IN lw_schema text,
  OUT lw_srid bigint
  ) AS
  
$lw_srid$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_srid')::bigint
    FROM %1$I.%1$I WHERE tabletype = 'config' $$, lw_schema)
    into lw_srid;

END;

$lw_srid$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_srid(text) is 
  'Returns the SRID of a given lw_schema';
/*		Gets the tolerance of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_tableconfig(
  IN lw_schema text,
  IN tablename text,
  OUT lw_tableconfig json
  ) AS
  
$lw_tableconfig$

BEGIN

  EXECUTE format($$ SELECT tableconfig
    FROM %1$I.%1$I WHERE tablename = %2$L $$, lw_schema, tablename)
    into lw_tableconfig;

END;

$lw_tableconfig$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tableconfig(IN lw_schema text, in tablename text) IS
  'Returns the tableconfig for a given table.';
/*		Gets the tolerance of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_tolerance(
  IN lw_schema text,
  OUT lw_tolerance float
  ) AS
  
$lw_tolerance$

BEGIN

  EXECUTE format($$ SELECT (tableconfig->>'lw_tolerance')::float
    FROM %1$I.%1$I WHERE tabletype = 'config' $$, lw_schema)
    into lw_tolerance;

END;

$lw_tolerance$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tolerance(IN lw_schema text) IS
  'Returns the tolerance set for a given livewire.';
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
		FROM %I.__nodes WHERE status = 'SOURCE'$$;
  for looprec in EXECUTE(format(qrytxt, lw_schema)) LOOP
                RAISE NOTICE 'SOURCE: % | % of %', looprec.lw_id,looprec.row_number, looprec.count;
                timer := clock_timestamp();
                perform lw_redirect(lw_schema,looprec.lw_id::int);
                perform lw_tracesource(lw_schema, looprec.lw_id::int, False);
		RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;
  END LOOP;


END;
  

$lw_traceall$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_traceall(in lw_schema text) IS 
  'Initiates a trace to populate the livewire shadow table';
CREATE FUNCTION lw_tracednstream(
  IN lw_schema text,
  IN lw_id bigint,
  OUT g geometry) AS

$lw_tracednstream$
  DECLARE
    qrytxt text;

  BEGIN
    qrytxt := 'SELECT st_union(g) g FROM %1$I.__lines WHERE lw_id IN
                (SELECT distinct(unnest(edges[(array_position(
                  nodes::int[], %2$s)):])) FROM %1$I.__livewire 
              WHERE ARRAY[%2$s]::bigint[] && (nodes))';
    EXECUTE format(qrytxt, lw_schema, lw_id) INTO g;

  END;

$lw_tracednstream$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tracednstream(in lw_schema text, in lw_id bigint) IS
  'Returns geometric trace give a livewire name and an lw_id form __nodes.';


CREATE FUNCTION lw_tracednstream(
  IN lw_schema text,
  IN lw_ids bigint[],
  out g geometry) AS
$lw_tracednstream$
  DECLARE
    rec record;
    qrytxt text;
    z bigint;
  BEGIN

    qrytxt := 'CREATE TEMPORARY TABLE __ ON COMMIT DROP AS 
               SELECT  *, null::bigint[] ne FROM %1$I.__livewire 
               WHERE %2$L && nodes';
    EXECUTE format(qrytxt, lw_schema, lw_ids); 


    FOREACH z in ARRAY lw_ids LOOP

      UPDATE __ set ne =  CASE 
        WHEN edges[(array_position(nodes::bigint[], z)):] is null 
        THEN ne 
        ELSE edges[(array_position(nodes::bigint[], z)):] end;
    END LOOP;
    
    qrytxt := 'SELECT st_union(g) FROM %1$I.__lines WHERE lw_id IN
               (SELECT distinct unnest(ne) FROM __)';
    EXECUTE format(qrytxt, lw_schema) into g;
  END;

$lw_tracednstream$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tracednstream(in lw_schema text, in lw_ids bigint[]) IS
  'Returns geometric trace give a livewire name and a set of lw_ids FROM __nodes.';


CREATE FUNCTION lw_tracednstream(
  IN lw_schema text,
  IN lw_ids bigint[],
  IN bl_ids bigint[],
  out g geometry) AS
$lw_tracednstream$
  DECLARE
    rec record;
    qrytxt text;
    z bigint;
  BEGIN

    qrytxt := 'CREATE TEMPORARY TABLE __ ON COMMIT DROP AS 
               SELECT  *, null::bigint[] n, null::bigint[] e 
               FROM %1$I.__livewire WHERE %2$L && nodes';
    EXECUTE format(qrytxt, lw_schema, lw_ids);


    FOREACH z in ARRAY lw_ids LOOP

      UPDATE __ set e =  CASE
        WHEN edges[(array_position(nodes::bigint[], z)):] is null
        THEN e
        ELSE edges[(array_position(nodes::bigint[], z)):] end,
      n = CASE WHEN nodes[array_position(nodes::bigint[], z):] is null
        THEN n
        ELSE nodes[array_position(nodes::bigint[], z):] END;
    END LOOP;

    FOREACH z in ARRAY bl_ids LOOP
      UPDATE __ set e =  CASE
        WHEN e[:(array_position(n::bigint[], z))] is null
        THEN e
        ELSE e[:(array_position(n::bigint[], z)) - 1] end;
    END LOOP;
    
    qrytxt := 'SELECT st_union(g) FROM %1$I.__lines WHERE lw_id IN
               (SELECT distinct unnest(e) FROM __)';
    EXECUTE format(qrytxt, lw_schema) into g;
  END;


$lw_tracednstream$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tracednstream(in lw_schema text, in lw_ids bigint[], in bw_ids bigint[]) IS
  'Returns geometric trace given a livewire name, a set of lw_ids as origins and a set of bl_ids as points to stop the trace
  FROM the __nodes table.';


CREATE FUNCTION lw_tracednstream(
  IN lw_schema text,
  IN in_g geometry,
  out g geometry) AS
$lw_tracednstream$
  DECLARE
    qrytxt text;

  BEGIN
    qrytxt := 'SELECT lw_tracednstream(%1$L,lw_id)  FROM %1$I.__nodes
              ORDER BY g <-> %2$L LIMIT 1';
   EXECUTE format(qrytxt, lw_schema, in_g) INTO g;

  END;

$lw_tracednstream$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tracednstream(in lw_schema text, in in_g geometry) IS
  'Returns geometric trace give a livewire name and a geometry.';
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
  'Returns geometric trace give a livewire name and a set of lw_ids FROM __nodes.';CREATE FUNCTION lw_traceupstream(
  IN lw_schema text,
  IN lw_id bigint,
  OUT g geometry) AS

$lw_traceupstream$
  DECLARE
    qrytxt text;

  BEGIN
    qrytxt := 'SELECT st_union(g) g FROM %1$I.__lines WHERE lw_id IN
                (SELECT distinct(unnest(edges[:(array_position(
                  nodes::int[], %2$s)-1)])) FROM %1$I.__livewire 
              WHERE ARRAY[%2$s]::bigint[] && (nodes))';
    EXECUTE format(qrytxt, lw_schema, lw_id) INTO g;

  END;

$lw_traceupstream$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_tracednstream(in lw_schema text, in lw_ids bigint[]) IS
  'Returns an upstream geometric trace given a livewire name and an lw_id FROM __nodes.';
/*		Returns the version of livewire 		*/

CREATE OR REPLACE FUNCTION lw_version(
  OUT lw_version text
  ) AS
  
$lw_version$

SELECT 'LiveWire 0.4 Build '

$lw_version$ LANGUAGE sql;

COMMENT ON FUNCTION lw_version() IS
  'Returns the version of livewire.';
