\set QUIET 1

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager

\set SHOW_CONTEXT never
-- Revert all changes on failure.
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

BEGIN;

 SET client_min_messages TO 'ERROR';
/*
 CREATE EXTENSION IF NOT EXISTS pgtap;
 CREATE EXTENSION IF NOT EXISTS postgis;
 CREATE EXTENSION IF NOT EXISTS pgrouting;
 CREATE EXTENSION IF NOT EXISTS livewire;
--  RESET client_min_messages;

-- Load the TAP functions.
--\i tests/pgtap.sql

*/
