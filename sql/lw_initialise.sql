/*      Creates schema and livewire base tables      */

CREATE OR REPLACE FUNCTION lw_initialise(
  lw_schema text,
  lw_srid integer,
  lw_tolerance float default 0)
RETURNS SETOF void AS 

$lw_initialise$

BEGIN

  IF lw_tolerance < 0 THEN
    RAISE EXCEPTION 'Tolerances cannot be less than 0.';
  ELSIF lw_tolerance > 0 THEN
    RAISE NOTICE 'Your tolerance is greater than 0.';
    RAISE NOTICE 'LiveWire works best when line ends are coincident.';
    RAISE NOTICE 'Your mileage may vary.';
  END IF;
  
  EXECUTE format($$ CREATE SCHEMA IF NOT EXISTS %1$I; $$,lw_schema);
  
  EXECUTE format($$ CREATE TABLE %1$I.__lines
    (
    lw_table text,
    lw_table_pkid text NOT NULL,
    lw_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    source integer,
    target integer,
    x1 double precision,
    y1 double precision,
    z1 double precision,
    x2 double precision,
    y2 double precision,
    z2 double precision,
    multiplier bigint,
    phase text,
    feederid text,
    g geometry(LineStringZ,%2$L),
    CONSTRAINT phase_check CHECK 
      (phase = ANY (ARRAY[
          'ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text,
          'A'::text, 'B'::text, 'C'::text]))); $$,
    lw_schema, lw_srid);
  
  EXECUTE format($$ CREATE TABLE %1$I.__nodes
    (
    lw_table text,
    lw_table_pkid text NOT NULL,
    lw_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    status text,
    phase text,
    feederid text,
    g geometry(PointZ,%2$L),
    CONSTRAINT phase_check CHECK 
      (phase = ANY (ARRAY[
        'ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text,
        'A'::text, 'B'::text, 'C'::text])))$$,
    lw_schema, lw_srid);
  
  EXECUTE format($$ CREATE TABLE %1$I.__livewire
    (
        nodes bigint[],
        edges bigint[]
    ) $$, lw_schema);
  
  EXECUTE format($$ CREATE TABLE %1$I.%1$I
    (
        tablename text PRIMARY KEY,
        tabletype text,
        tableconfig json
    ) $$, lw_schema);
  
  EXECUTE format($$ CREATE INDEX ON %1$I.__lines USING gist (g) $$,lw_schema);
  EXECUTE format($$ CREATE INDEX ON %1$I.__nodes USING gist (g) $$,lw_schema);
  EXECUTE format($$ CREATE UNIQUE INDEX ON %1$I.%1$I (tabletype) 
    where tabletype = 'config' $$,lw_schema); 
  
  EXECUTE format($$ INSERT INTO %1$I.%1$I VALUES 
    ('%1$I.%1$I','config', '{"lw_tolerance": "%2$s", "lw_srid" : "%3$s"}'::json) $$,
  lw_schema, lw_tolerance, lw_srid);

END;

$lw_initialise$ LANGUAGE plpgsql;


COMMENT ON FUNCTION lw_initialise IS 'lw_initialise: Livewire funcytion to instantiate a new livewire';
