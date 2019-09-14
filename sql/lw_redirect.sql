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
