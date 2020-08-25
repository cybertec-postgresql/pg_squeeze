/* pg_squeeze--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_squeeze UPDATE TO '1.3'" to load this file. \quit

ALTER TABLE tables_internal DROP COLUMN class_id;
ALTER TABLE tables_internal DROP COLUMN class_id_toast;
ALTER TABLE tables_internal DROP COLUMN free_space;
ALTER TABLE tables_internal DROP COLUMN last_task_created;

DELETE FROM tables;
ALTER TABLE tables DROP COLUMN schedule;
CREATE DOMAIN minute AS int CHECK (VALUE BETWEEN 0 AND 59);
CREATE DOMAIN hour AS int CHECK (VALUE BETWEEN 0 AND 23);
CREATE DOMAIN dom AS int CHECK (VALUE BETWEEN 1 AND 31);
CREATE DOMAIN month AS int CHECK (VALUE BETWEEN 1 AND 12);
CREATE DOMAIN dow AS int CHECK (VALUE BETWEEN 0 AND 7);
CREATE TYPE schedule AS (
	minutes	minute[],
	hours	hour[],
	days_of_month dom[],
	months month[],
	days_of_week dow[]);
ALTER TABLE tables ADD COLUMN schedule schedule NOT NULL;

DROP TABLE tasks;
CREATE DOMAIN task_state AS TEXT CHECK(VALUE IN ('new', 'ready', 'processed'));
CREATE TABLE tasks (
	id		serial	NOT NULL	PRIMARY KEY,

	table_id	int	NOT NULL	REFERENCES tables
	ON DELETE CASCADE,

	-- Task creation time.
	created		timestamptz NOT NULL	DEFAULT now(),

	-- The latest known free space in the underlying table. Note that it
	-- includes dead tuples, since these are eliminated by squeeze_table()
	-- function.
	free_space	double precision,

	-- How many times did we try to process the task? The common use case
	-- is that a concurrent DDL broke the processing.
	tried		int	NOT NULL	DEFAULT 0,

	-- Either squeezed or skipped by the "squeeze worker" (because there's
	-- not enough free space or the table is not big enough).
	state		task_state	NOT NULL	DEFAULT 'new'
);

DROP FUNCTION add_new_tasks();
DROP FUNCTION start_next_task();
DROP FUNCTION cleanup_task(a_task_id int);
DROP FUNCTION process_current_task();
DROP FUNCTION stop_worker();

CREATE VIEW scheduled_for_now AS
	SELECT	i.table_id, t.tabschema, t.tabname
	FROM	squeeze.tables_internal i,
		pg_catalog.pg_stat_user_tables s,
		squeeze.tables t,
		pg_class c, pg_namespace n
	WHERE
		(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
		i.table_id = t.id AND
		n.nspname = t.tabschema AND c.relnamespace = n.oid AND
		c.relname = t.tabname AND
		(
			((t.schedule).minutes ISNULL OR
			EXTRACT(minute FROM now())::int = ANY((t.schedule).minutes))
			AND
			((t.schedule).hours ISNULL OR
			EXTRACT(hour FROM now())::int = ANY((t.schedule).hours))
			AND
			((t.schedule).months ISNULL OR
			EXTRACT(month FROM now())::int = ANY((t.schedule).months))
			AND
			(
				-- At least one of the "days_of_month" and
				-- "days_of_week" components must
				-- match. However if one matches, NULL value
				-- of the other must not be considered "any
				-- day of month/week". Instead, NULL can only
				-- cause a match if both components have it.
				((t.schedule).days_of_month ISNULL AND
				(t.schedule).days_of_week ISNULL)
				OR
				EXTRACT(day FROM now())::int = ANY((t.schedule).days_of_month)
				OR
				EXTRACT(dow FROM now())::int = ANY((t.schedule).days_of_week)
				OR
				-- Sunday can be expressed as both 0 and 7.
				EXTRACT(isodow FROM now())::int = ANY((t.schedule).days_of_week)
			)
		);

CREATE FUNCTION check_schedule() RETURNS void
LANGUAGE sql
AS $$
	-- Delete the processed tasks, but ignore those scheduled and
	-- processed in the current minute - we don't want to schedule those
	-- again now.
	DELETE FROM squeeze.tasks t
	WHERE	state = 'processed' AND
		(EXTRACT(HOUR FROM now()) <> EXTRACT(HOUR FROM t.created) OR
		EXTRACT(MINUTE FROM now()) <> EXTRACT(MINUTE FROM t.created));

	-- Create task where schedule does match.
	INSERT INTO squeeze.tasks(table_id)
	SELECT	i.table_id
	FROM	squeeze.tables_internal i,
		pg_catalog.pg_stat_user_tables s,
		squeeze.tables t,
		pg_class c, pg_namespace n
	WHERE
		(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
		i.table_id = t.id AND
		n.nspname = t.tabschema AND c.relnamespace = n.oid AND
		c.relname = t.tabname
		-- Is there a matching schedule?
		AND EXISTS (
			SELECT *
			FROM squeeze.scheduled_for_now
			WHERE table_id = i.table_id
		)
		-- Ignore tables for which a task currently exists.
		AND NOT t.id IN (SELECT table_id FROM squeeze.tasks);
$$;

-- Update new tasks with the information on free space in the corresponding
-- tables.
CREATE OR REPLACE FUNCTION update_free_space_info() RETURNS void
LANGUAGE sql
AS $$
	-- If VACUUM completed recenly enough, we consider the percentage of
	-- dead tuples negligible and so retrieve the free space from FSM.
	UPDATE squeeze.tasks k
	SET	free_space = 100 * squeeze.get_heap_freespace(c.oid)
	FROM	squeeze.tables t,
		squeeze.tables_internal i,
		pg_catalog.pg_class c,
		pg_catalog.pg_namespace n,
		pg_catalog.pg_stat_user_tables s
	WHERE	k.state = 'new' AND k.table_id = t.id AND i.table_id = t.id
		AND t.tabname = c.relname AND c.relnamespace = n.oid AND
		t.tabschema = n.nspname AND
		(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
		(
			(s.last_vacuum >= now() - t.vacuum_max_age)
			OR
			(s.last_autovacuum >= now() - t.vacuum_max_age)
		)
		AND
		-- Each processing makes the previous VACUUM unimportant.
		(
			i.last_task_finished ISNULL
			OR
			i.last_task_finished < s.last_vacuum
			OR
			i.last_task_finished < s.last_autovacuum
		);

	-- If VACUUM didn't run recently or there's no FSM, take the more
	-- expensive approach.
	UPDATE	squeeze.tasks k
	SET	free_space = a.approx_free_percent + a.dead_tuple_percent
	FROM	squeeze.tables t,
		pg_catalog.pg_class c,
		pg_catalog.pg_namespace n,
		squeeze.pgstattuple_approx(c.oid) a
	WHERE	k.state = 'new' AND k.free_space ISNULL AND
		k.table_id = t.id AND t.tabname = c.relname AND
		c.relnamespace = n.oid AND t.tabschema = n.nspname;
$$;

CREATE FUNCTION dispatch_new_tasks() RETURNS void
LANGUAGE sql
AS $$
	-- First, get rid of tables not big enough for processing.
	UPDATE squeeze.tasks k
	SET	state = 'processed'
	FROM	squeeze.tables t,
		pg_catalog.pg_class c,
		pg_catalog.pg_namespace n
	WHERE	k.state = 'new' AND k.table_id = t.id AND t.tabname = c.relname
		AND c.relnamespace = n.oid AND t.tabschema = n.nspname AND
		pg_catalog.pg_relation_size(c.oid, 'main') < t.min_size * 1048576;

	SELECT squeeze.update_free_space_info();

	-- Make the actual decision.
	--
	-- Ignore tasks having NULL in free_space - those have been created
	-- after update_free_space_info() had finished, so the should waite
	-- for the next run of dispatch_new_tasks().
	UPDATE	squeeze.tasks k
	SET	state =
		CASE
			WHEN k.free_space >
			((100 - squeeze.get_heap_fillfactor(c.oid)) + t.free_space_extra)
			THEN 'ready'
			ELSE 'processed'
		END
	FROM	squeeze.tables t,
		pg_catalog.pg_class c,
		pg_catalog.pg_namespace n
	WHERE	k.state = 'new' AND k.free_space NOTNULL AND k.table_id = t.id
		AND t.tabname = c.relname AND c.relnamespace = n.oid AND
		t.tabschema = n.nspname;

$$;

CREATE FUNCTION finalize_task(a_task_id int)
RETURNS void
LANGUAGE sql
AS $$
	WITH updated(table_id) AS (
		UPDATE squeeze.tasks t
		SET state = 'processed'
		WHERE id = a_task_id
		RETURNING table_id
	)
	UPDATE squeeze.tables_internal t
	SET last_task_finished = now()
	FROM updated u
	WHERE u.table_id = t.table_id;
$$;

CREATE FUNCTION cancel_task(a_task_id int)
RETURNS void
LANGUAGE sql
AS $$
	UPDATE squeeze.tasks t
	SET state = 'processed'
	WHERE id = a_task_id;
$$;

CREATE FUNCTION process_next_task()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
	v_tabschema	name;
	v_tabname	name;
	v_cl_index	name;
	v_rel_tbsp	name;
	v_ind_tbsps	name[][];
	v_task_id	int;
	v_tried		int;
	v_last_try	bool;
	v_skip_analyze	bool;
	v_stmt		text;
	v_start		timestamptz;

	-- Error info to be logged.
	v_sql_state	text;
	v_err_msg	text;
	v_err_detail	text;
BEGIN
	-- Retrieve the table corresponding to the least recently created task
	-- in the 'ready' state.
	SELECT tb.tabschema, tb.tabname, tb.clustering_index,
tb.rel_tablespace, tb.ind_tablespaces, t.id, t.tried,
t.tried >= tb.max_retry, tb.skip_analyze
	INTO v_tabschema, v_tabname, v_cl_index, v_rel_tbsp, v_ind_tbsps,
 v_task_id, v_tried, v_last_try, v_skip_analyze
	FROM squeeze.tasks t, squeeze.tables tb
	WHERE t.table_id = tb.id AND t.state = 'ready'
	ORDER BY t.created
	LIMIT 1;

	IF NOT FOUND THEN
		-- No task currently available?
		RETURN;
	END IF;

	-- Do the actual work.
	BEGIN
		v_start := clock_timestamp();

		-- Do the actual processing.
		--
		-- If someone dropped the table in between, the exception
		-- handler below should log the error and cleanup the task.
		PERFORM squeeze.squeeze_table(v_tabschema, v_tabname,
 v_cl_index, v_rel_tbsp, v_ind_tbsps);

		INSERT INTO squeeze.log(tabschema, tabname, started, finished)
		VALUES (v_tabschema, v_tabname, v_start, clock_timestamp());

		PERFORM squeeze.finalize_task(v_task_id);

		IF NOT v_skip_analyze THEN
                        -- Analyze the new table, unless user rejects it
                        -- explicitly.
			--
			-- XXX Besides updating planner statistics in general,
			-- this sets pg_class(relallvisible) to 0, so that
			-- planner is not too optimistic about this
			-- figure. The preferrable solution would be to run
			-- (lazy) VACUUM (with the ANALYZE option) to
			-- initialize visibility map. However, to make the
			-- effort worthwile, we shouldn't do it until all
			-- transactions can see all the changes done by
			-- squeeze_table() function. What's the most suitable
			-- way to wait? Asynchronous execution of the VACUUM
			-- is probably needed in any case.
                        v_stmt := 'ANALYZE "' || v_tabschema || '"."' ||
                                v_tabname || '"';

			EXECUTE v_stmt;
		END IF;
	EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS v_sql_state := RETURNED_SQLSTATE;
			GET STACKED DIAGNOSTICS v_err_msg := MESSAGE_TEXT;
			GET STACKED DIAGNOSTICS v_err_detail := PG_EXCEPTION_DETAIL;

			INSERT INTO squeeze.errors(tabschema, tabname,
				sql_state, err_msg, err_detail)
			VALUES (v_tabschema, v_tabname, v_sql_state, v_err_msg,
				v_err_detail);

			-- If the active task failed too many times, cancel
			-- it.
			IF v_last_try THEN
				PERFORM squeeze.cancel_task(v_task_id);
				RETURN;
			ELSE
				-- Account for the current attempt.
				UPDATE squeeze.tasks
				SET tried = tried + 1
				WHERE id = v_task_id;
			END IF;
	END;
END;
$$;

CREATE FUNCTION stop_worker()
RETURNS SETOF record
LANGUAGE sql
AS $$
	-- When looking for the PID we rely on the fact that the worker holds
	-- lock on the extension. If the worker is not running, we could (in
	-- theory) kill a regular backend trying to ALTER or DROP the
	-- extension right now. It's not worth taking a different approach
	-- just to avoid this extremely unlikely case (which shouldn't cause
	-- data corruption).
	SELECT	pid, pg_terminate_backend(pid)
	FROM	pg_catalog.pg_locks l,
		pg_catalog.pg_extension e
	WHERE  e.extname = 'pg_squeeze' AND
		(l.classid, l.objid) = (3079, e.oid);
$$;
