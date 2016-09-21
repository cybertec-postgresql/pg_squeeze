/* pg_squeeze--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_squeeze" to load this file. \quit

CREATE TABLE tables (
	id		serial	NOT NULL	PRIMARY KEY,
	tabschema	name	NOT NULL,
	tabname		name	NOT NULL,
	UNIQUE(tabschema, tabname),

	-- The minimum time that needs to elapse before we check again if the
	-- table is eligible for squeeze. (User might prefer squeezing the
	-- table at defined time rather than taking an immediate action to
	-- address excessive bloat.)
	check_interval	interval	NOT NULL	DEFAULT '1 hour',
	CHECK (check_interval >= '1 minute'),

	-- The first check ever. Can be used when rescheduling processing of
	-- particular table.
	first_check	timestamptz	NOT NULL	DEFAULT now(),

	-- If at least check_interval elapsed since the last check and there's
        -- no task for the table in the queue, add a new one.
	last_check	timestamptz,

	-- The maximum tolerable fraction of dead tuples in the table. Once
	-- this gets exceeded, a task is created for the table.
	--
	-- TODO Tune the default value.
	max_dead_frac	real	NOT NULL	DEFAULT 0.5,
	CHECK (max_dead_frac > 0 AND max_dead_frac <= 1),

	-- If statistics are older than this, no new task is created.
	--
	-- TODO Tune the default value.
	stats_max_age	interval	NOT NULL	DEFAULT '1 hour',

	retry_max	int	NOT NULL	DEFAULT 0
);

-- Task queue. If completed with success, the task is moved into "log" table.
--
-- If task fails and tables(retry_max) is greater than zero, processing will
-- be retried automatically as long as tasks(tried) < tables(retry_max) +
-- 1. Then the task will be removed from the queue.
CREATE TABLE tasks (
	id		serial	NOT NULL	PRIMARY KEY,

	table_id	int	NOT NULL	REFERENCES tables,

	-- Is this the task the next call of process() function will pick?
	active		bool	NOT NULL	DEFAULT false,

	-- Have we eventually succeeded?
	success		bool	NOT NULL	DEFAULT false,

	-- How many times did we try to process the task? The common use case
	-- is that a concurrent DDL broke the processing.
	tried		int	NOT NULL	DEFAULT 0
);

-- Make sure there is at most one active task anytime.
CREATE UNIQUE INDEX ON tasks(active) WHERE active;

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
    WITH tasks_new(id, tabschema, tabname, max_dead_frac, stats_max_age) AS (
	 UPDATE	squeeze.tables t
	 SET	last_check = now()
	 WHERE
		first_check <= now() AND
		-- Checked too far in the past or never at all?
		(
			t.last_check + t.check_interval < now()
			OR
			t.last_check IS NULL
		) AND
		-- Ignore tables for which a task currently exists.
		NOT t.id IN (SELECT table_id FROM squeeze.tasks)
	 RETURNING t.id, t.tabschema, t.tabname, t.max_dead_frac,
		t.stats_max_age
    )
    INSERT INTO squeeze.tasks(table_id)
    SELECT	id
    FROM	tasks_new t,
	    pg_catalog.pg_stat_user_tables s
    WHERE
	(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
	-- Important threshold exceeded (avoid deletion by zero)?
	(s.n_live_tup + s.n_dead_tup) > 0 AND
	(s.n_dead_tup::real / (s.n_live_tup + s.n_dead_tup)) > t.max_dead_frac
	AND
	-- Can we still rely on the statistics?
	(
		(s.last_analyze >= now() - t.stats_max_age)
		OR
		(s.last_autoanalyze >= now() - t.stats_max_age)
	);
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


-- Process the currently active task.
CREATE FUNCTION process_current_task()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
	v_tabschema	name;
	v_tabname	name;
	v_task_id	int;
	v_tried		int;
	v_success	bool;
	v_max_reached	bool;
	v_stmt		text;
BEGIN
	SELECT tb.tabschema, tb.tabname, t.id, t.tried, t.success,
		t.tried >= tb.retry_max + 1
	INTO v_tabschema, v_tabname, v_task_id, v_tried, v_success,
		 v_max_reached
	FROM squeeze.tasks t, squeeze.tables tb
	WHERE t.table_id = tb.id AND t.active;

	IF NOT FOUND THEN
		-- Unexpected deletion by someone else?
		RETURN;
	END IF;

	-- If the current task succeeded or failures caused reaching of the
	-- maximum number of attempts, delete it. start_next_task() will
	-- prepare the next one.
	IF v_success OR v_max_reached THEN
		DELETE FROM squeeze.tasks t
		WHERE id = v_task_id;

		-- squeeze_table() resets the storage option on successful
		-- completion, but we must do manually otherwise.
		IF NOT v_success THEN
			v_stmt := 'ALTER TABLE ' || v_tabschema || '.' ||
			v_tabname || ' RESET (user_catalog_table)';

			RAISE NOTICE '%', v_stmt;
			EXECUTE v_stmt;
		END IF;

		RETURN;
	END IF;

	-- Do the actual work.
	BEGIN
		-- TODO Pass the missing arguments if appropriate.
		PERFORM squeeze.squeeze_table(v_tabschema, v_tabname, NULL, NULL, NULL);

		DELETE FROM squeeze.tasks
		WHERE id = v_task_id;
	EXCEPTION

		WHEN OTHERS THEN
			--TODO Insert record into log table.
			RAISE NOTICE 'ERROR';

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
