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
