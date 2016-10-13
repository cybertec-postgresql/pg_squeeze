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

	-- The minimum percentage of free space that triggers processing, in
	-- addition to the percentage determined by fillfactor.
	--
	-- TODO Tune the default value.
	free_space_extra int NOT NULL DEFAULT 50,
	CHECK (free_space_extra >= 0 AND free_space_extra < 100),

	-- The minimum number of pages that triggers processing.
	--
	-- TODO Tune the default value.
	min_pages	int	NOT NULL	DEFAULT 64,
	CHECK (min_pages > 0),

	-- If statistics are older than this, no new task is created.
	--
	-- TODO Tune the default value.
	stats_max_age	interval	NOT NULL	DEFAULT '1 hour',

	max_retry	int		NOT NULL	DEFAULT 0,

	-- No ANALYZE after the processing has completed.
	skip_analyze	bool		NOT NULL	DEFAULT false
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
	tried		int	NOT NULL	DEFAULT 0,

	-- The initial value of "autovacuum_enabled" relation option is stored
	-- here, and will be restored when we're done.
	autovac		bool	NOT NULL,
	autovac_toast	bool	NOT NULL
);

-- Make sure there is at most one active task anytime.
CREATE UNIQUE INDEX ON tasks(active) WHERE active;

-- Each successfully completed processing of a table is recorded here.
CREATE TABLE log (
	tabschema	name	NOT NULL,
	tabname		name	NOT NULL,
	started		timestamptz	NOT NULL,
	finished	timestamptz	NOT NULL
);

-- XXX Some other indexes might be useful. Analyze the typical use later.
CREATE INDEX ON log(started);

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
SELECT	t.tabschema, t.tabname, s.last_vacuum, s.last_autovacuum
FROM	squeeze.tables t,
	pg_catalog.pg_stat_user_tables s
WHERE	(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
	(
		COALESCE(s.last_vacuum, s.last_autovacuum) ISNULL
		OR
		COALESCE(s.last_vacuum, s.last_autovacuum) < now() - t.stats_max_age
	);


CREATE FUNCTION is_autovacuum_enabled (
       relid	oid
)
RETURNS bool
AS 'MODULE_PATHNAME', 'is_autovacuum_enabled'
LANGUAGE C;

CREATE FUNCTION get_heap_fillfactor(a_relid oid)
RETURNS int
AS 'MODULE_PATHNAME', 'get_heap_fillfactor'
VOLATILE
LANGUAGE C;

CREATE FUNCTION get_heap_freespace(a_relid oid)
RETURNS double precision
AS 'MODULE_PATHNAME', 'get_heap_freespace'
VOLATILE
LANGUAGE C;

-- Create tasks for newly qualifying tables.
CREATE FUNCTION add_new_tasks() RETURNS void
LANGUAGE sql
AS $$
    WITH tasks_new(id, autovac, autovac_toast) AS (
	 UPDATE	squeeze.tables_internal i
	 SET	last_task_created = now()
	 FROM	pg_catalog.pg_stat_user_tables s,
		squeeze.tables t,
		pg_class c, pg_namespace n
	 WHERE
		(t.tabschema, t.tabname) = (s.schemaname, s.relname) AND
		i.table_id = t.id AND
		n.nspname = t.tabschema AND c.relnamespace = n.oid AND
		c.relname = t.tabname AND
		t.first_check <= now() AND
		-- Checked too far in the past or never at all?
		(
			i.last_task_created + t.task_interval < now()
			OR
			i.last_task_created IS NULL
		)
		AND
		-- Threshold(s) exceeded?
		100 * squeeze.get_heap_freespace(c.oid) >
			(100 - squeeze.get_heap_fillfactor(c.oid)) + t.free_space_extra
		AND
		c.relpages > t.min_pages
		AND
		-- Can we still rely on the statistics?
		(
			(s.last_vacuum >= now() - t.stats_max_age)
			OR
			(s.last_autovacuum >= now() - t.stats_max_age)
		)
		-- Ignore tables for which a task currently exists.
		AND NOT t.id IN (SELECT table_id FROM squeeze.tasks)
		AND
		-- Each processing makes the current statistics obsolete.
		(
			i.last_task_finished ISNULL
			OR
			i.last_task_finished < s.last_vacuum
			OR
			i.last_task_finished < s.last_autovacuum
		)
	 RETURNING t.id, squeeze.is_autovacuum_enabled(c.oid),
	 	   squeeze.is_autovacuum_enabled(c.reltoastrelid)
    )
    INSERT INTO squeeze.tasks(table_id, autovac, autovac_toast)
    SELECT	id, autovac, autovac_toast
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
	--
	-- Also disable autovacuum for the table and its TOAST relation. As
	-- we're gonna "squeeze" the table, VACUUM no longer makes sense.
	PERFORM squeeze.set_reloptions(v_tabschema, v_tabname, true, false,
		false);
END;
$$;

-- Delete task and make the table available for task creation again.
--
-- By adjusting last_task_created make VACUUM necessary before the next task
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
	v_autovac	bool;
	v_autovac_toast bool;
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
t.tried >= tb.max_retry, tb.skip_analyze, t.autovac, t.autovac_toast
	INTO v_tabschema, v_tabname, v_cl_index, v_rel_tbsp, v_ind_tbsps,
 v_task_id, v_tried, v_last_try, v_skip_analyze, v_autovac, v_autovac_toast
	FROM squeeze.tasks t, squeeze.tables tb
	WHERE t.table_id = tb.id AND t.active;

	IF NOT FOUND THEN
		-- Unexpected deletion by someone else?
		RETURN;
	END IF;

	-- Do the actual work.
	BEGIN
		v_start := clock_timestamp();

		PERFORM squeeze.squeeze_table(v_tabschema, v_tabname,
 v_cl_index, v_rel_tbsp, v_ind_tbsps, v_autovac, v_autovac_toast);

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

			INSERT INTO squeeze.errors (tabschema, tabname,
				sql_state, err_msg, err_detail)
			VALUES (v_tabschema, v_tabname, v_sql_state, v_err_msg,
				v_err_detail);

			-- If the active task failed too many times, delete
			-- it. start_next_task() will prepare the next one.
			IF v_last_try THEN
				PERFORM squeeze.cleanup_task(v_task_id);

				-- squeeze_table() resets the options on
				-- successful completion, but here we must do
				-- it explicitly on error.
				PERFORM squeeze.set_reloptions(v_tabschema,
					v_tabname, false, v_autovac,
					v_autovac_toast);

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

CREATE FUNCTION set_reloptions (
	tabchema	name,
	tabname		name,
	user_cat	bool,
	autovac		bool,
	autovac_toast	bool
)
RETURNS void
AS 'MODULE_PATHNAME', 'set_reloptions'
LANGUAGE C;

CREATE FUNCTION squeeze_table(
       tabchema		name,
       tabname		name,
       clustering_index name,
       rel_tablespace 	name,
       ind_tablespaces	name[][],
       autovac		bool,
       autovac_toast	bool)
RETURNS void
AS 'MODULE_PATHNAME', 'squeeze_table'
LANGUAGE C;

CREATE FUNCTION start_worker()
RETURNS int
AS 'MODULE_PATHNAME', 'squeeze_start_worker'
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
