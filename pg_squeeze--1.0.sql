/* pg_squeeze--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_squeeze" to load this file. \quit

CREATE TABLE tables (
	id		serial	NOT NULL	PRIMARY KEY,
	tabschema	name	NOT NULL,
	tabname		name	NOT NULL,
	UNIQUE(tabschema, tabname),

	-- Clustering index.
	clustering_index name,

	-- Tablespace the table should be put into.
	rel_tablespace 	name,

	-- Index-to-tablespace mappings. Each row of the array is expected to
	-- consist of 2 columns: index name and target tablespace.
	ind_tablespaces	name[][],

	-- The minimum time that needs to elapse after task creation before we
	-- check again if the table is eligible for squeeze. (User might
	-- prefer squeezing the table at defined time rather than taking an
	-- immediate action to address excessive bloat.)
	task_interval	interval	NOT NULL	DEFAULT '1 hour',
	CHECK (task_interval >= '1 minute'),

	-- The first check ever. Can be used when rescheduling processing of
	-- particular table.
	first_check	timestamptz	NOT NULL,

	-- The minimum number of tuples that triggers processing.
	--
	-- TODO Tune the default value.
	min_dead_tuples	int	NOT NULL	DEFAULT	50,

	-- Fraction of table size to add to min_dead_tuples when deciding
	-- whether to create a task to squeeze table.
	--
	-- TODO Tune the default value.
	squeeze_scale_factor	real	NOT NULL	DEFAULT 0.5,
	CHECK (squeeze_scale_factor > 0 AND squeeze_scale_factor <= 1),

	-- If statistics are older than this, no new task is created.
	--
	-- TODO Tune the default value.
	stats_max_age	interval	NOT NULL	DEFAULT '1 hour',

	max_retry	int		NOT NULL	DEFAULT 0
);

-- Fields that would normally fit into "tables" but require no attention of
-- the user are separate. Thus "tables" can be considered an user interface.
CREATE TABLE tables_internal (
       table_id	int	NOT NULL	PRIMARY KEY
       REFERENCES tables ON DELETE CASCADE,

	-- If at least task_interval elapsed since the last task creation and
        -- there's no task for the table in the queue, add a new one.
	--
	-- We could apply task_interval to last_task_finished instead, but
	-- that would add the task duration as an extra delay to the next
	-- schedule, making the schedule less predictable. (Of course the
	-- schedule is shifted anyway if the task processing takes more than
	-- task_interval.)
	last_task_created	timestamptz,

	last_task_finished	timestamptz
);

-- Trigger to keep "tables_internal" in-sync with "tables".
--
-- (Deletion is handled by foreign key.)
CREATE FUNCTION tables_internal_trig_func()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO squeeze.tables_internal(table_id)
	VALUES (NEW.id);

	RETURN NEW;
END;
$$;

CREATE TRIGGER tables_internal_trig AFTER INSERT
ON squeeze.tables
FOR EACH ROW
EXECUTE PROCEDURE squeeze.tables_internal_trig_func();

-- Task queue. If completed with success, the task is moved into "log" table.
--
-- If task fails and tables(max_retry) is greater than zero, processing will
-- be retried automatically as long as tasks(tried) < tables(max_retry) +
-- 1. Then the task will be removed from the queue.
CREATE TABLE tasks (
	id		serial	NOT NULL	PRIMARY KEY,

	table_id	int	NOT NULL	REFERENCES tables,

	-- Is this the task the next call of process() function will pick?
	active		bool	NOT NULL	DEFAULT false,

	-- How many times did we try to process the task? The common use case
	-- is that a concurrent DDL broke the processing.
	tried		int	NOT NULL	DEFAULT 0
);

-- Make sure there is at most one active task anytime.
CREATE UNIQUE INDEX ON tasks(active) WHERE active;

CREATE TABLE errors (
	id		bigserial	NOT NULL	PRIMARY KEY,
	occurred	timestamptz	NOT NULL	DEFAULT now(),
	tabschema	name	NOT NULL,
	tabname		name	NOT NULL,

	sql_state	text	NOT NULL,
	err_msg		text	NOT NULL,
	err_detail	text
);

-- Overview of all the registered tables for which the required freshness of
-- statistics is not met.
CREATE VIEW unusable_stats AS
SELECT	t.tabschema, t.tabname, s.last_analyze, s.last_autoanalyze
FROM	squeeze.tables t,
	pg_catalog.pg_stat_user_tables s
WHERE	(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
	(
		COALESCE(s.last_analyze, s.last_autoanalyze) ISNULL
		OR
		COALESCE(s.last_analyze, s.last_autoanalyze) < now() - t.stats_max_age
	);


-- Create tasks for newly qualifying tables.
CREATE FUNCTION add_new_tasks() RETURNS void
LANGUAGE sql
AS $$
    WITH tasks_new(id) AS (
	 UPDATE	squeeze.tables_internal i
	 SET	last_task_created = now()
	 FROM	pg_catalog.pg_stat_user_tables s,
		squeeze.tables t
	 WHERE
		(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
		i.table_id = t.id AND
		t.first_check <= now() AND
		-- Checked too far in the past or never at all?
		(
			i.last_task_created + t.task_interval < now()
			OR
			i.last_task_created IS NULL
		)
		AND
		-- Threshold exceeded?
		s.n_dead_tup >= t.min_dead_tuples +
			      t.squeeze_scale_factor * (s.n_live_tup + s.n_dead_tup)
		AND
		-- Can we still rely on the statistics?
		(
			(s.last_analyze >= now() - t.stats_max_age)
			OR
			(s.last_autoanalyze >= now() - t.stats_max_age)
		)
		-- Ignore tables for which a task currently exists.
		AND NOT t.id IN (SELECT table_id FROM squeeze.tasks)
		AND
		-- Each processing makes the current statistics obsolete.
		(
			i.last_task_finished ISNULL
			OR
			i.last_task_finished < s.last_analyze
			OR
			i.last_task_finished < s.last_autoanalyze
		)
	 RETURNING t.id
    )
    INSERT INTO squeeze.tasks(table_id)
    SELECT	id
    FROM	tasks_new;
$$;

-- Mark the next task as active.
CREATE FUNCTION start_next_task()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
	v_tabschema	name;
	v_tabname	name;
	v_stmt		text;
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

	-- squeeze_table() function requires the "user_catalog_option" to be
	-- set, but cannot do it in its own transaction. So do it now.
	v_stmt := 'ALTER TABLE ' || v_tabschema || '.' || v_tabname ||
		' SET (user_catalog_table=true)';
	EXECUTE v_stmt;
END;
$$;

-- Delete task and make the table available for task creation again.
--
-- By adjusting last_task_created make ANALYZE necessary before the next task
-- can be created for the table.
CREATE FUNCTION cleanup_task(a_task_id int)
RETURNS void
LANGUAGE sql
AS $$
	WITH deleted(table_id) AS (
		DELETE FROM squeeze.tasks t
		WHERE id = a_task_id
		RETURNING table_id
	)
	UPDATE squeeze.tables_internal t
	SET last_task_finished = now()
	FROM deleted d
	WHERE d.table_id = t.table_id;
$$;

-- Process the currently active task.
CREATE FUNCTION process_current_task()
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
	v_max_reached	bool;
	v_stmt		text;

	-- Error info to be logged.
	v_sql_state	text;
	v_err_msg	text;
	v_err_detail	text;
BEGIN
	SELECT tb.tabschema, tb.tabname, tb.clustering_index,
tb.rel_tablespace, tb.ind_tablespaces, t.id, t.tried,
t.tried >= tb.max_retry + 1
	INTO v_tabschema, v_tabname, v_cl_index, v_rel_tbsp, v_ind_tbsps,
 v_task_id, v_tried, v_max_reached
	FROM squeeze.tasks t, squeeze.tables tb
	WHERE t.table_id = tb.id AND t.active;

	IF NOT FOUND THEN
		-- Unexpected deletion by someone else?
		RETURN;
	END IF;

	-- If the active task failed too many times, delete it.
	-- start_next_task() will prepare the next one.
	IF v_max_reached THEN
		PERFORM squeeze.cleanup_task(v_task_id);

		-- squeeze_table() resets the storage option on successful
		-- completion, but here we must do it explicitly.
		v_stmt := 'ALTER TABLE ' || v_tabschema || '.' ||
		v_tabname || ' RESET (user_catalog_table)';

		RAISE NOTICE '%', v_stmt;
		EXECUTE v_stmt;

		RETURN;
	END IF;

	-- Do the actual work.
	BEGIN
		PERFORM squeeze.squeeze_table(v_tabschema, v_tabname,
 v_cl_index, v_rel_tbsp, v_ind_tbsps);

		PERFORM squeeze.cleanup_task(v_task_id);
	EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS v_sql_state := RETURNED_SQLSTATE;
			GET STACKED DIAGNOSTICS v_err_msg := MESSAGE_TEXT;
			GET STACKED DIAGNOSTICS v_err_detail := PG_EXCEPTION_DETAIL;

			INSERT INTO squeeze.errors (tabschema, tabname,
				sql_state, err_msg, err_detail)
			VALUES (v_tabschema, v_tabname, v_sql_state, v_err_msg,
				v_err_detail);

			-- Account for the current attempt.
			UPDATE squeeze.tasks
			SET tried = tried + 1
			WHERE id = v_task_id;
	END;
END;
$$;


CREATE FUNCTION squeeze_table(
       tabchema		name,
       tabname		name,
       clustering_index name,
       rel_tablespace 	name,
       ind_tablespaces	name[][])
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_table'
LANGUAGE C;

CREATE FUNCTION start_worker()
RETURNS int
AS 'MODULE_PATHNAME', 'start_worker'
LANGUAGE C;

-- Stop "squeeze worker" if it's currently running.
CREATE FUNCTION stop_worker()
RETURNS boolean
LANGUAGE sql
AS $$
	-- When looking for the PID we rely on the fact that the worker holds
	-- lock on the extension. If the worker is not running, we could (in
	-- theory) kill a regular backend trying to ALTER or DROP the
	-- extension right now. It's not worth taking a different approach
	-- just to avoid this extremely unlikely case (which shouldn't cause
	-- data corruption).
	SELECT	pg_terminate_backend(pid)
	FROM	pg_catalog.pg_locks l,
		pg_catalog.pg_extension e
	WHERE  e.extname = 'pg_squeeze' AND
		(l.classid, l.objid) = (3079, e.oid);
$$;
