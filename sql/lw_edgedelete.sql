CREATE FUNCTION lw_edgedelete()  RETURNS trigger AS 

$lw_edgedelete$

  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_edgedelete$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_edgedelete IS
  'Trigger function to fire for a delete on any edge particpant';
