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