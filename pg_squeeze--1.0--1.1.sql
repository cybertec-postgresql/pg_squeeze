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
	INSERT INTO squeeze.tasks(table_id, autovac, autovac_toast)
	SELECT	table_id, squeeze.is_autovacuum_enabled(i.class_id),
		squeeze.is_autovacuum_enabled(i.class_id_toast)
	FROM	squeeze.tables_internal i
	WHERE	i.last_task_created = now();
$$;
