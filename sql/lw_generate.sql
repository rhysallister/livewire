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
