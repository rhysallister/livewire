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
