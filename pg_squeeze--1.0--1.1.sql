/* pg_squeeze--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_squeeze UPDATE TO '1.1'" to load this file. \quit

-- Migrate the information contained in first_check and task_interval columns
-- into the new schedule column before dropping them.
ALTER TABLE tables ADD COLUMN schedule timetz[];
UPDATE tables t SET schedule = (
	SELECT array_agg(g.s::timetz)
	FROM generate_series(
		first_check,
		first_check + '1 day' - '1 second'::interval,
		task_interval) AS g(s)
);
ALTER TABLE tables ALTER COLUMN schedule SET NOT NULL;

ALTER TABLE tables DROP COLUMN first_check;
ALTER TABLE tables DROP COLUMN task_interval;

ALTER TABLE tasks DROP COLUMN autovac;
ALTER TABLE tasks DROP COLUMN autovac_toast;

-- The squeeze_table() function is no longer interested in the autovacuum
-- related arguments.
DROP FUNCTION squeeze_table(name, name, name, name, name[][], bool, bool);
CREATE FUNCTION squeeze_table(
       tabchema		name,
       tabname		name,
       clustering_index name,
       rel_tablespace 	name,
       ind_tablespaces	name[][])
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_table'
LANGUAGE C;

-- No longer needed.
DROP FUNCTION is_autovacuum_enabled(oid);

-- Reflect the changes of both "tables" and "tasks" tables.
CREATE OR REPLACE FUNCTION add_new_tasks() RETURNS void
LANGUAGE sql
AS $$
	-- The previous estimates are obsolete now.
	UPDATE squeeze.tables_internal
	SET free_space = NULL, class_id = NULL, class_id_toast = NULL;

	-- Mark tables that we're interested in.
	UPDATE	squeeze.tables_internal i
	SET class_id = c.oid, class_id_toast = c.reltoastrelid
	FROM	pg_catalog.pg_stat_user_tables s,
		squeeze.tables t,
		pg_class c, pg_namespace n
	WHERE
		(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
		i.table_id = t.id AND
		n.nspname = t.tabschema AND c.relnamespace = n.oid AND
		c.relname = t.tabname AND
		-- Is there a matching schedule?
		EXISTS (
		       SELECT	u.s
		       FROM	squeeze.tables t_sub,
		       		UNNEST(t_sub.schedule) u(s)
		       WHERE	t_sub.id = t.id AND
		       		-- The schedule must have passed ...
		       		u.s <= now()::timetz AND
				-- ... and it should be one for which no
				-- task was created yet.
				(u.s > i.last_task_created::timetz OR
				i.last_task_created ISNULL)
		)
		-- Ignore tables for which a task currently exists.
		AND NOT t.id IN (SELECT table_id FROM squeeze.tasks);

	-- If VACUUM completed recenly enough, we consider the percentage of
	-- dead tuples negligible and so retrieve the free space from FSM.
	UPDATE	squeeze.tables_internal i
	SET free_space = 100 * squeeze.get_heap_freespace(i.class_id)
	FROM	pg_catalog.pg_stat_user_tables s,
		squeeze.tables t
	WHERE
		i.class_id NOTNULL AND
		i.table_id = t.id AND
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
	-- expensive approach. (Use WITH as LATERAL doesn't work for UPDATE.)
	WITH t_approx(table_id, free_space) AS (
		SELECT	i.table_id, a.approx_free_percent + a.dead_tuple_percent
		FROM	squeeze.tables_internal i,
			squeeze.pgstattuple_approx(i.class_id) AS a
		WHERE i.class_id NOTNULL AND i.free_space ISNULL)
	UPDATE squeeze.tables_internal i
	SET	free_space = a.free_space
	FROM	t_approx a
	WHERE	i.table_id = a.table_id;

	-- Create a new task for each table having more free space than
	-- needed.
	UPDATE	squeeze.tables_internal i
	SET	last_task_created = now()
	FROM	squeeze.tables t
	WHERE	i.class_id NOTNULL AND t.id = i.table_id AND i.free_space >
		((100 - squeeze.get_heap_fillfactor(i.class_id)) + t.free_space_extra)
		AND
		pg_catalog.pg_relation_size(i.class_id, 'main') > t.min_size * 1048576;

	-- now() is supposed to return the same value as it did in the previous
	-- query.
	INSERT INTO squeeze.tasks(table_id)
	SELECT	table_id
	FROM	squeeze.tables_internal i
	WHERE	i.last_task_created = now();
$$;

-- No longer used.
DROP FUNCTION set_reloptions(name, name, bool, bool, bool);

-- start_next_task() used to call set_reloptions(), so modify it accordingly.
DROP FUNCTION start_next_task();

CREATE OR REPLACE FUNCTION start_next_task()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
	v_tabschema	name;
	v_tabname	name;
BEGIN
	PERFORM
	FROM squeeze.tasks WHERE active;
	IF FOUND THEN
		RETURN;
	END IF;

	UPDATE	squeeze.tasks t
	INTO	v_tabschema, v_tabname
	SET	active = true
	FROM	squeeze.tables tb
	WHERE
		tb.id = t.table_id AND
		t.id = (SELECT id FROM squeeze.tasks ORDER BY id LIMIT 1)
	RETURNING tb.tabschema, tb.tabname;

	IF NOT FOUND THEN
		RETURN;
	END IF;
END;
$$;


-- process_current_task() used to call set_reloptions(), so modify it
-- accordingly.
DROP FUNCTION process_current_task();

CREATE OR REPLACE FUNCTION process_current_task()
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
	SELECT tb.tabschema, tb.tabname, tb.clustering_index,
tb.rel_tablespace, tb.ind_tablespaces, t.id, t.tried,
t.tried >= tb.max_retry, tb.skip_analyze
	INTO v_tabschema, v_tabname, v_cl_index, v_rel_tbsp, v_ind_tbsps,
 v_task_id, v_tried, v_last_try, v_skip_analyze
	FROM squeeze.tasks t, squeeze.tables tb
	WHERE t.table_id = tb.id AND t.active;

	IF NOT FOUND THEN
		-- Unexpected deletion by someone else?
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

		PERFORM squeeze.cleanup_task(v_task_id);

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

			-- If the active task failed too many times, delete
			-- it. start_next_task() will prepare the next one.
			IF v_last_try THEN
				PERFORM squeeze.cleanup_task(v_task_id);
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
