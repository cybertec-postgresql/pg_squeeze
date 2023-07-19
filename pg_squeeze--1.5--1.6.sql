/* pg_squeeze--1.5--1.6.sql */

DROP FUNCTION process_next_task();

DROP FUNCTION squeeze_table(name, name, name, name, name[][]);

DROP FUNCTION stop_worker();
CREATE FUNCTION stop_worker()
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_stop_worker'
LANGUAGE C;

CREATE FUNCTION squeeze_table(name, name, name)
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_table_new'
LANGUAGE C;
