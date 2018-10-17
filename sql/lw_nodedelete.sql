CREATE FUNCTION lw_nodedelete()  RETURNS trigger AS 

$lw_nodedelete$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_nodedelete$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_nodedelete is 
  'Trigger function to fire for a delete on any node particpant';