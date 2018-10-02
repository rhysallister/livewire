\unset ECHO
\i tests/test_setup.sql

/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(13);

SELECT has_function('lw_initialise', ARRAY['text', 'integer', 'double precision'], 'Check for lw_initialise');
SELECT has_function('lw_addedgeparticipant', ARRAY['text', 'json'], 'Check for lw_addedgeparticipant');
SELECT has_function('lw_addnodeparticipant', ARRAY['text', 'json'], 'Check for lw_addnodeparticipant');
SELECT has_function('lw_generateedge', ARRAY['text', 'text'], 'Check for lw_generatedege');
SELECT has_function('lw_generatenode', ARRAY['text', 'text'], 'Check for lw_generatenode');
SELECT has_function('lw_generate', ARRAY['text'], 'Check for lw_generate');
SELECT has_function('lw_srid', ARRAY['text'], 'Check for lw_srid');
SELECT has_function('lw_tolerance', ARRAY['text'], 'Check for lw_tolerance');
SELECT has_function('lw_redirect', ARRAY['text', 'bigint','bigint[]', 'bigint[]'], 'Check for lw_redirect');
SELECT has_function('lw_endnodes', ARRAY['text'], 'Check for lw_endnodes');
SELECT has_function('lw_sourcenodes', ARRAY['text'], 'Check for lw_sourcenodes');
SELECT has_function('lw_traceall', ARRAY['text'], 'Check for lw_traceall');
SELECT has_function('lw_tracesource', ARRAY['text', 'bigint','boolean'], 'Check for lw_tracesource');
SELECT * FROM finish();

ROLLBACK;
