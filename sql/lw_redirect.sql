/*    'redirect' lines based upon their source origin    */

CREATE OR REPLACE FUNCTION lw_redirect(
	lw_schema text,
	source bigint)
    RETURNS SETOF void AS 

$lw_redirect$

DECLARE
  qrytxt text;
  updtxt text;
  looprec record;
  timer timestamptz;
  tolerance float;

BEGIN
/*    Trace FROM all blocks to source   */
  tolerance = lw_tolerance(lw_schema);
  
  qrytxt := $$
  with recursive aaa(node_lw_id, line_lw_id, node_status,  status, line_g, path,  cycle ) as (
	SELECT  n.lw_id, l.lw_id, n.status, 
	 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN 'GOOD' ELSE 'FLIPPED' END, 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.g ELSE st_reverse(l.g) END,
	array[n.lw_id], false
	FROM %1$I.__nodes n
	join %1$I.__lines l on st_3ddwithin(n.g, l.g, %2$s)
	WHERE n.lw_id = %3$s
	UNION ALL
	SELECT n.lw_id, l.lw_id, n.status,
	
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN 'GOOD' ELSE 'FLIPPED' END, 
	CASE WHEN st_3ddwithin(n.g, st_startpoint(l.g),%2$s) THEN l.g ELSE st_reverse(l.g) END, path || n.lw_id, n.lw_id =ANY(path)
	FROM aaa
	join %1$I.__lines l on st_3ddwithin(st_endpoint(aaa.line_g),l.g,%2$s)  
	join %1$I.__nodes n on st_3ddwithin(st_endpoint(aaa.line_g),n.g,%2$s)  
	WHERE aaa.line_lw_id <> l.lw_id   and node_status <> 'BLOCK' and not cycle
	),
	bbb as (SELECT * FROM aaa)
	UPDATE %1$I.__lines l set 
	g = st_reverse(l.g), x1 = l.x2, x2 = l.x1, y1 = l.y2, y2 = l.y1, z1 = l.z2, z2 = l.z1, source = l.target, target = l.source
	FROM bbb WHERE l.lw_id = bbb.line_lw_id
    and  node_status <> 'BLOCK' and status = 'FLIPPED'
   $$;
   execute format(qrytxt,lw_schema, tolerance, source); 
  end;
  

$lw_redirect$ language plpgsql;

COMMENT ON FUNCTION lw_redirect is 
  'Makes shadow network directed based upon a give source node.';