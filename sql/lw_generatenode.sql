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
    JOIN pg_catalog.pg_class on attrelid = oid
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