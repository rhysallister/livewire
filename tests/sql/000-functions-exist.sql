\unset ECHO
\i tests/test_setup.sql

/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(23);

SELECT has_function('lw_addedgeparticipant', ARRAY['text', 'json'], 'Check for lw_addedgeparticipant');
SELECT has_function('lw_addnodeparticipant', ARRAY['text', 'json'], 'Check for lw_addnodeparticipant');
SELECT has_function('lw_edgedelete', 'Check for lw_edgedelete');
SELECT has_function('lw_edgeinsert', 'Check for lw_edgeinsert');
SELECT has_function('lw_edgeupdate', 'Check for lw_edgeupdate');
SELECT has_function('lw_endnodes', ARRAY['text'], 'Check for lw_endnodes');
SELECT has_function('lw_generateedge', ARRAY['text', 'text'], 'Check for lw_generatedege');
SELECT has_function('lw_generatenode', ARRAY['text', 'text'], 'Check for lw_generatenode');
SELECT has_function('lw_generate', ARRAY['text'], 'Check for lw_generate');
SELECT has_function('lw_initialise', ARRAY['text', 'integer', 'double precision'], 'Check for lw_initialise');
SELECT has_function('lw_nodedelete', 'Check for lw_nodedelete');
SELECT has_function('lw_nodeinsert', 'Check for lw_nodeinsert');
SELECT has_function('lw_nodeupdate', 'Check for lw_nodeupdate');
SELECT has_function('lw_redirect', ARRAY['text', 'bigint','bigint[]', 'bigint[]'], 'Check for lw_redirect');
SELECT has_function('lw_sourcenodes', ARRAY['text'], 'Check for lw_sourcenodes');
SELECT has_function('lw_srid', ARRAY['text'], 'Check for lw_srid');
SELECT has_function('lw_tolerance', ARRAY['text'], 'Check for lw_tolerance');
SELECT has_function('lw_traceall', ARRAY['text'], 'Check for lw_traceall');
SELECT has_function('lw_tracednstream', ARRAY['text','bigint'], 'Check for lw_tracednstream - Single source variant');
SELECT has_function('lw_tracednstream', ARRAY['text','bigint[]'], 'Check for lw_tracednstream - Multiple source variant');
SELECT has_function('lw_tracednstream', ARRAY['text','bigint[]','bigint[]'], 'Check for lw_tracednstream - Multiple source and blockers variant');
SELECT has_function('lw_tracesource', ARRAY['text', 'bigint','boolean'], 'Check for lw_tracesource');
SELECT has_function('lw_traceupstream', ARRAY['text','bigint'], 'Check for lw_traceupstream');
SELECT * FROM finish();

ROLLBACK;
