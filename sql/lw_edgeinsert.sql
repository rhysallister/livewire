CREATE FUNCTION lw_edgeinsert()  RETURNS trigger AS 

$lw_edgeinsert$

  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  
  END;
$lw_edgeinsert$ LANGUAGE plpgsql;

COMMENT ON FUNCTION lw_edgeINSERT IS
  'Trigger function to fire for an INSERT on any edge particpant';
