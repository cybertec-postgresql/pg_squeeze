/* pg_squeeze--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_squeeze UPDATE TO '1.6'" to load this file. \quit

DROP FUNCTION process_next_task();

DROP FUNCTION squeeze_table(name, name, name, name, name[][]);

DROP FUNCTION start_worker();
CREATE FUNCTION start_worker()
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_start_worker'
LANGUAGE C;

DROP FUNCTION stop_worker();
CREATE FUNCTION stop_worker()
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_stop_worker'
LANGUAGE C;

CREATE FUNCTION squeeze_table(
       tabschema	name,
       tabname		name,
       clustering_index name		DEFAULT NULL,
       rel_tablespace 	name		DEFAULT NULL,
       ind_tablespaces	name[][]	DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_table_new'
LANGUAGE C;

CREATE FUNCTION get_active_workers()
RETURNS TABLE (
	pid		int4,
	tabschema	name,
	tabname		name,
	ins_initial	bigint,
	ins		bigint,
	upd		bigint,
	del		bigint)
AS 'MODULE_PATHNAME', 'squeeze_get_active_workers'
LANGUAGE C;

ALTER TABLE log  ADD COLUMN ins_initial bigint;
ALTER TABLE log  ADD COLUMN ins bigint;
ALTER TABLE log  ADD COLUMN upd bigint;
ALTER TABLE log  ADD COLUMN del bigint;
