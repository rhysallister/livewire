\set QUIET 1

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager

-- Revert all changes on failure.
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- Load the TAP functions.
BEGIN;
 SET client_min_messages TO 'ERROR';
 CREATE EXTENSION IF NOT EXISTS postgis;
 CREATE EXTENSION IF NOT EXISTS pgrouting;
 CREATE EXTENSION IF NOT EXISTS livewire;
 RESET client_min_messages;
\i tests/pgtap.sql
