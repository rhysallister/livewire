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
    select count(*) from pgr_dijkstra(
           $$select lw_id  id, source, target, st_3dlength(g) * multiplier   as cost
           from %1$I.__lines  $$,
           (select lw_sourcenodes('%1$s')),
           (select lw_sourcenodes('%1$s')),
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
    select count(*) from pgr_dijkstra(
           $$select lw_id  id, source, target, st_3dlength(g) * multiplier   as cost
           from %1$I.__lines  $$,
           '%2$s',
           (select lw_sourcenodes('%1$s')),
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


