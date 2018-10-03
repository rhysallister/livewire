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
