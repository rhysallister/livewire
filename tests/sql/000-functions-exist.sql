\unset ECHO
\i tests/test_setup.sql

/*******************************************************

Test Suite for LiveWire


*******************************************************/


SELECT plan(8);

SELECT has_function('lw_initialise', ARRAY['text', 'integer', 'double precision'], 'Check for lw_initialise');
SELECT has_function('lw_addedgeparticipant', ARRAY['text', 'json'], 'Check for lw_addedgeparticipant');
SELECT has_function('lw_addnodeparticipant', ARRAY['text', 'json'], 'Check for lw_addnodeparticipant');
SELECT has_function('lw_generateedge', ARRAY['text', 'text'], 'Check for lw_generatedege');
SELECT has_function('lw_generatenode', ARRAY['text', 'text'], 'Check for lw_generatenode');
SELECT has_function('lw_generate', ARRAY['text'], 'Check for lw_generate');
SELECT has_function('lw_srid', ARRAY['text'], 'Check for lw_srid');
SELECT has_function('lw_tolerance', ARRAY['text'], 'Check for lw_tolerance');
SELECT * FROM finish();

ROLLBACK;
