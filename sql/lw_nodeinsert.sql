CREATE FUNCTION lw_nodeinsert()  RETURNS trigger AS 

$lw_nodeinsert$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '-------------------------------------------------';
    RAISE NOTICE '%', row_to_json((NEW.*)); 
    RAISE NOTICE '%', row_to_json((OLD.*)); 
    RETURN NEW;
  END;
$lw_nodeinsert$ LANGUAGE plpgsql;
