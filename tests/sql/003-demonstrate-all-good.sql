\unset ECHO
\i tests/tests.sql

SELECT plan(3);

CREATE SCHEMA livewire_testing;

CREATE TABLE livewire_testing.electric_devices (
    id integer NOT NULL,
    device_type text NOT NULL,
    device_phase text NOT NULL,
    status boolean NOT NULL,
    feedername text,
    g public.geometry(Point,3450)
);

COPY livewire_testing.electric_devices (id, device_type, device_phase, status, feedername, g) FROM stdin;
0	FEEDER	ABC	t	Live-A	01010000207A0D000066666666D6571E4100000000AD0F3F41
1	FEEDER	ABC	t	Live-B	01010000207A0D000066666666EE581E41CDCCCC4CF40F3F41
2	FEEDER	ABC	t	Wire-X	01010000207A0D0000CDCCCCCC1C5F1E4100000080CF0D3F41
3	FEEDER	ABC	t	Wire-Y	01010000207A0D0000CDCCCCCC945E1E419A999919EB0D3F41
4	ISOLATOR	ABC	f	\N	01010000207A0D000033333333915A1E41CDCCCC4C2F0F3F41
5	ISOLATOR	ABC	f	\N	01010000207A0D000033333333875C1E4133333333020F3F41
6	ISOLATOR	ABC	f	\N	01010000207A0D00009A999999875B1E41CDCCCCCCA50E3F41
\.

CREATE TABLE livewire_testing.electric_lines (
    id integer NOT NULL,
    line_phase text NOT NULL,
    feedername text,
    g public.geometry(LineString,3450)
);

COPY livewire_testing.electric_lines (id, line_phase, feedername, g) FROM stdin;
0	ABC	\N	01020000207A0D00000200000066666666EE581E41CDCCCC4CF40F3F4166666666725A1E4100000000910F3F41
1	ABC	\N	01020000207A0D00000300000066666666D6571E4100000000AD0F3F419A999999FD591E4100000080130F3F4133333333915A1E41CDCCCC4C2F0F3F41
2	abc	\N	01020000207A0D00000300000033333333915A1E41CDCCCC4C2F0F3F41333333334D5B1E4133333333540F3F4166666666725A1E4100000000910F3F41
3	ABC	\N	01020000207A0D000002000000CDCCCCCC945E1E419A999919EB0D3F419A999999875B1E41CDCCCCCCA50E3F41
4	ABC	\N	01020000207A0D0000020000009A999999FD591E4100000080130F3F419A999999875B1E41CDCCCCCCA50E3F41
5	ABC	\N	01020000207A0D000003000000CDCCCCCC1C5F1E4100000080CF0D3F41CDCCCCCCFA5F1E41000000000F0E3F4133333333875C1E4133333333020F3F41
6	ABC	\N	01020000207A0D000002000000333333334D5B1E4133333333540F3F4133333333875C1E4133333333020F3F41
\.

SELECT lw_initialise('livewire_testing', 3450); -- Initialise this livewire


SELECT results_eq(
  'SELECT tablename, tabletype, tableconfig::text FROM livewire_testing.livewire_testing',
  $$VALUES ('livewire_testing.livewire_testing', 'config','{"lw_tolerance": "0", "lw_srid" : "3450", "lw_trackorigin": "f"}')$$,
  'Confirm that after lw_initialise the correct row is in the config table'
  );


SELECT lw_addnodeparticipant('livewire_testing', $${
  "schemaname":"livewire_testing",
  "tablename": "electric_devices",
  "primarykey":"id",
  "geomcolumn": "g",
  "feederid":"feedername",
  "sourcequery": "device_type = 'FEEDER' AND status = True ",
  "blockquery": "device_type = 'ISOLATOR' AND status = False",
  "phasecolumn": "device_phase",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}$$);

SELECT lw_addedgeparticipant('livewire_testing', $${
  "schemaname":"livewire_testing",
  "tablename": "electric_lines",
  "primarykey":"id",
  "geomcolumn": "g",
  "feederid":"feedername",
  "phasecolumn": "line_phase",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}$$);


SELECT results_eq(
  'SELECT count(*) FROM livewire_testing.livewire_testing',
  'VALUES (3::bigint)',
  'Confirm that the config table has three rows'
  );

SET client_min_messages TO 'ERROR';
SELECT lw_generate('livewire_testing');

SELECT lives_ok(
  $$SELECT lw_traceall('livewire_testing');$$,
  'All should be well here.'
  );

SELECT * FROM finish();

ROLLBACK;
