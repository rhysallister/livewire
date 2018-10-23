\unset ECHO
\i tests/tests.sql

/*******************************************************************************
				Test Suite for LiveWire
*******************************************************************************/

SELECT plan(29);

SELECT has_extension('public', 'livewire', 'Check for the existense of the livewire extension');
SELECT has_function('public', 'lw_addedgeparticipant', ARRAY['text', 'json'], 'Check for lw_addedgeparticipant');
SELECT has_function('public', 'lw_addnodeparticipant', ARRAY['text', 'json'], 'Check for lw_addnodeparticipant');
SELECT has_function('public', 'lw_edgedelete', 'Check for lw_edgedelete');
SELECT has_function('public', 'lw_edgeinsert', 'Check for lw_edgeinsert');
SELECT has_function('public', 'lw_edgeupdate', 'Check for lw_edgeupdate');
SELECT has_function('public', 'lw_endnodes', ARRAY['text'], 'Check for lw_endnodes');
SELECT has_function('public', 'lw_generateedge', ARRAY['text', 'text'], 'Check for lw_generatedege');
SELECT has_function('public', 'lw_generatenode', ARRAY['text', 'text'], 'Check for lw_generatenode');
SELECT has_function('public', 'lw_generate', ARRAY['text'], 'Check for lw_generate');
SELECT has_function('public', 'lw_initialise', ARRAY['text', 'integer', 'double precision'], 'Check for lw_initialise');
SELECT has_function('public', 'lw_nodedelete', 'Check for lw_nodedelete');
SELECT has_function('public', 'lw_nodeinsert', 'Check for lw_nodeinsert');
SELECT has_function('public', 'lw_nodeupdate', 'Check for lw_nodeupdate');
SELECT has_function('public', 'lw_originrow', ARRAY['text', 'bigint'], 'Check for lw_originrow');
SELECT has_function('public', 'lw_redirect', ARRAY['text', 'bigint'], 'Check for lw_redirect');
SELECT has_function('public', 'lw_singlesource', ARRAY['text'], 'Check for lw_singlesource');
SELECT has_function('public', 'lw_singlesource', ARRAY['text', 'bigint'], 'Check for lw_singlesource - Single source variant');
SELECT has_function('public', 'lw_sourcenodes', ARRAY['text'], 'Check for lw_sourcenodes');
SELECT has_function('public', 'lw_srid', ARRAY['text'], 'Check for lw_srid');
SELECT has_function('public', 'lw_tableconfig', ARRAY['text','text'], 'Check for lw_tableconfig');
SELECT has_function('public', 'lw_tolerance', ARRAY['text'], 'Check for lw_tolerance');
SELECT has_function('public', 'lw_traceall', ARRAY['text'], 'Check for lw_traceall');
SELECT has_function('public', 'lw_tracednstream', ARRAY['text','bigint'], 'Check for lw_tracednstream - Single source variant');
SELECT has_function('public', 'lw_tracednstream', ARRAY['text','bigint[]'], 'Check for lw_tracednstream - Multiple source variant');
SELECT has_function('public', 'lw_tracednstream', ARRAY['text','bigint[]','bigint[]'], 'Check for lw_tracednstream - Multiple source and blockers variant');
SELECT has_function('public', 'lw_tracednstream', ARRAY['text','geometry'], 'Check for lw_tracednstream - Multiple source and blockers variant');
SELECT has_function('public', 'lw_tracesource', ARRAY['text', 'bigint','boolean'], 'Check for lw_tracesource');
SELECT has_function('public', 'lw_traceupstream', ARRAY['text','bigint'], 'Check for lw_traceupstream');
SELECT * FROM finish();

ROLLBACK;
