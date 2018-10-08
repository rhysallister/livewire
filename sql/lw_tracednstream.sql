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


CREATE FUNCTION lw_tracednstream(
  in lw_schema text,
  in lw_ids bigint[],
  out g geometry) as

$lw_tracednstream$
  DECLARE
    rec record;
    qrytxt text;
    z bigint;
  BEGIN

    qrytxt := 'CREATE TEMPORARY TABLE __ ON COMMIT DROP AS 
               SELECT  *, null::bigint[] ne FROM %1$I.__livewire 
               where %2$L && nodes';
    EXECUTE format(qrytxt, lw_schema, lw_ids); 


    FOREACH z in ARRAY lw_ids LOOP

      UPDATE __ set ne =  CASE 
        WHEN edges[(array_position(nodes::bigint[], z)):] is null 
        THEN ne 
        ELSE edges[(array_position(nodes::bigint[], z)):] end;
    END LOOP;
    
    qrytxt := 'SELECT st_union(g) from %1$I.__lines WHERE lw_id IN
               (select distinct unnest(ne) FROM __)';
    EXECUTE format(qrytxt, lw_schema) into g;
  END;

$lw_tracednstream$ LANGUAGE plpgsql;


CREATE FUNCTION lw_tracednstream(
  in lw_schema text,
  in lw_ids bigint[],
  in bl_ids bigint[],
  out g geometry) as

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
    
    qrytxt := 'SELECT st_union(g) from %1$I.__lines WHERE lw_id IN
               (select distinct unnest(e) FROM __)';
    EXECUTE format(qrytxt, lw_schema) into g;
  END;


$lw_tracednstream$ LANGUAGE plpgsql;

CREATE FUNCTION lw_tracednstream(
  in lw_schema text,
  in in_g geometry,
  out g geometry) as

$lw_tracednstream$
  DECLARE
    qrytxt text;

  BEGIN
    qrytxt := 'SELECT lw_tracednstream(%1$L,lw_id)  FROM %1$I.__nodes
              ORDER BY g <-> %2$L LIMIT 1';
   EXECUTE format(qrytxt, lw_schema, in_g) INTO g;

  END;

$lw_tracednstream$ LANGUAGE plpgsql;

