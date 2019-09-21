CREATE FUNCTION lw_traceupstream(
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
