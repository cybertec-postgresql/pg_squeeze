/* pg_squeeze--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_squeeze" to load this file. \quit

CREATE FUNCTION squeeze_table(relation text, clustering_index text)
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_table'
LANGUAGE C;
