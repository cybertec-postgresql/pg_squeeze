/* pg_squeeze--1.1.sql */

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

	-- Times to check whether a new task should be created for the table.
	schedule	timetz[]	NOT NULL,

	-- The minimum percentage of free space that triggers processing, in
	-- addition to the percentage determined by fillfactor.
	--
	-- TODO Tune the default value.
	free_space_extra int NOT NULL DEFAULT 50,
	CHECK (free_space_extra >= 0 AND free_space_extra < 100),

	-- The minimum size of the table (in megabytes) needed to trigger
	-- processing.
	--
	-- TODO Tune the default value.
	min_size	real	NOT NULL	DEFAULT 8,
	CHECK (min_size > 0.0),

	-- If at most this interval elapsed since the last VACUUM, try to use
	-- FSM to estimate free space. Otherwise (or if there's no FSM), use
	-- squeeze.pgstattuple_approx() function.
	--
	-- TODO Tune the default value.
	vacuum_max_age	interval	NOT NULL	DEFAULT '1 hour',

	max_retry	int		NOT NULL	DEFAULT 0,

	-- No ANALYZE after the processing has completed.
	skip_analyze	bool		NOT NULL	DEFAULT false
);

COMMENT ON TABLE tables IS
	'List of tables registered for regular squeeze.';
COMMENT ON COLUMN tables.id IS
	 'Identifier of the registered table (generated column).';
COMMENT ON COLUMN tables.tabschema IS
	'Database schema of the registered table.';
COMMENT ON COLUMN tables.tabname IS
	'Table registered for regular squeeze.';
COMMENT ON COLUMN tables.clustering_index IS
	'Index to control ordering of table rows.';
COMMENT ON COLUMN tables.rel_tablespace IS
	'Tablespace into which the registered table should be moved.';
COMMENT ON COLUMN tables.ind_tablespaces IS
	'Index-to-tablespace mappings to be applied.';
COMMENT ON COLUMN tables.schedule IS
	'Array of scheduled times to check and possibly process the table.';
COMMENT ON COLUMN tables.free_space_extra IS
	'In addition to free space derived from fillfactor, this extra '
	 'percentage of free space is needed to schedule processing of the '
	 'table.';
COMMENT ON COLUMN tables.min_size IS
	'Besides meeting the free_space_extra criterion, the table size must '
	'be at least this many MBs to be scheduled for squeezee.';
COMMENT ON COLUMN tables.vacuum_max_age IS
	'If less than this elapsed since the last VACUUM, try to use FSM to '
	'estimate the amount of free space.';
COMMENT ON COLUMN tables.max_retry IS
	'The maximum nmber of times failed processing is retried.';
COMMENT ON COLUMN tables.skip_analyze IS
	'Only squeeze the table, without running ANALYZE afterwards.';


-- Fields that would normally fit into "tables" but require no attention of
-- the user are separate. Thus "tables" can be considered an user interface.
CREATE TABLE tables_internal (
	table_id	int	NOT NULL	PRIMARY KEY
	REFERENCES tables ON DELETE CASCADE,

	-- pg_class(oid) of the table. It's an auxiliary field that lets us
	-- avoid repeated retrieval of the field in add_new_tasks().
	class_id	oid,

	-- Likewise.
	class_id_toast	oid,

	-- The latest known free space. Note that it includes dead tuples,
	-- since these are eliminated by squeeze_table() function.
	free_space	double precision,

	-- When was the most recent task created?
	last_task_created	timestamptz,

	-- When was the most recent task finished?
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

	table_id	int	NOT NULL	REFERENCES tables
	ON DELETE CASCADE,

	-- Is this the task the next call of process() function will pick?
	active		bool	NOT NULL	DEFAULT false,

	-- How many times did we try to process the task? The common use case
	-- is that a concurrent DDL broke the processing.
	tried		int	NOT NULL	DEFAULT 0
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

COMMENT ON TABLE log IS
	'Successfully completed squeeze operations.';
COMMENT ON COLUMN log.tabschema IS
	 'Database schema of the table processed.';
COMMENT ON COLUMN log.tabname IS
	 'Name of the table not squeezed.';
COMMENT ON COLUMN log.started IS
	 'When the processing started.';
COMMENT ON COLUMN log.finished IS
	 'When the processing finished.';

CREATE TABLE errors (
	id		bigserial	NOT NULL	PRIMARY KEY,
	occurred	timestamptz	NOT NULL	DEFAULT now(),
	tabschema	name	NOT NULL,
	tabname		name	NOT NULL,

	sql_state	text	NOT NULL,
	err_msg		text	NOT NULL,
	err_detail	text
);

COMMENT ON TABLE errors IS
	'Failed attempts to squeeze table.';
COMMENT ON COLUMN errors.id IS
	 'Identifier of the failure (generated column).';
COMMENT ON COLUMN errors.occurred IS
	'Time the errors has occurred.';
COMMENT ON COLUMN errors.tabschema IS
	 'Database schema of the table not squeezed.';
COMMENT ON COLUMN errors.tabname IS
	 'Name of the table not squeezed.';
COMMENT ON COLUMN errors.sql_state IS
	'"SQL state" encountered.';
COMMENT ON COLUMN errors.err_msg IS
	'Error message caught.';
COMMENT ON COLUMN errors.err_detail IS
	'Detailed error message, if available.';

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

CREATE FUNCTION pgstattuple_approx(IN reloid regclass,
    OUT table_len BIGINT,               -- physical table length in bytes
    OUT scanned_percent FLOAT8,         -- what percentage of the table's pages was scanned
    OUT approx_tuple_count BIGINT,      -- estimated number of live tuples
    OUT approx_tuple_len BIGINT,        -- estimated total length in bytes of live tuples
    OUT approx_tuple_percent FLOAT8,    -- live tuples in % (based on estimate)
    OUT dead_tuple_count BIGINT,        -- exact number of dead tuples
    OUT dead_tuple_len BIGINT,          -- exact total length in bytes of dead tuples
    OUT dead_tuple_percent FLOAT8,      -- dead tuples in % (based on estimate)
    OUT approx_free_space BIGINT,       -- estimated free space in bytes
    OUT approx_free_percent FLOAT8)     -- free space in % (based on estimate)
AS 'MODULE_PATHNAME', 'squeeze_pgstattuple_approx'
LANGUAGE C STRICT PARALLEL SAFE;

-- Unregister dropped tables. (CASCADE behaviour ensures deletion of the
-- related records in "tables_internal" and "tasks" tables.)
CREATE FUNCTION cleanup_tables() RETURNS void
LANGUAGE sql
AS $$
	DELETE
	FROM squeeze.tables t
	WHERE (t.tabschema, t.tabname) NOT IN (
		SELECT n.nspname, c.relname
		FROM
			pg_class c
			JOIN pg_namespace n
			ON c.relnamespace = n.oid);
$$;

-- Update the information on free space for table that has valid
-- tables_internal(class_id).
CREATE FUNCTION update_free_space_info() RETURNS void
LANGUAGE sql
AS $$
	UPDATE squeeze.tables_internal
	SET free_space = NULL
	WHERE class_id NOTNULL;

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
$$;

-- Create tasks for newly qualifying tables.
CREATE FUNCTION add_new_tasks() RETURNS void
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
				i.last_task_created ISNULL OR
				-- The next schedule can be in front of the
				-- last task if a new day started.
				i.last_task_created::date < current_date)
		)
		-- Ignore tables for which a task currently exists.
		AND NOT t.id IN (SELECT table_id FROM squeeze.tasks);

	SELECT squeeze.update_free_space_info();

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
