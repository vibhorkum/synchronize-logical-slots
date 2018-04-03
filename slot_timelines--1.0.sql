-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION slot_timelines" to load this file. \quit

CREATE OR REPLACE FUNCTION pg_create_logical_slot_timelines( slot_name TEXT, 
	plugin TEXT)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_create_logical_slot_timelines( TEXT, TEXT )
IS 'Create a logical slot at a particular lsn and xid.';

CREATE OR REPLACE FUNCTION pg_advance_logical_slot_timelines( slot_name TEXT, 
	new_xmin xid, 
	new_catalog_xmin xid, 
	new_restart_lsn pg_lsn, 
	new_confirmed_lsn pg_lsn)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_advance_logical_slot_timelines(TEXT, 
	xid, 
	xid, 
	pg_lsn, 
	pg_lsn)
IS 'Advance a logical slot directly.';

CREATE TABLE slot_timeline_table( info TEXT );

/* 
 * function to list old timelines using timeline file 
 */

CREATE OR REPLACE FUNCTION get_old_timeline(interval) 
RETURNS TEXT
LANGUAGE sql
AS
$function$
WITH time_line_file AS (
			SELECT pg_catalog.pg_stat_file( 'pg_xlog/' || filename ).*, 
				'pg_xlog/'||filename as timeline_file 
			FROM pg_catalog.pg_ls_dir( 'pg_xlog' ) foo( filename )
			WHERE filename ~* '.history'
			ORDER BY modification DESC LIMIT 1
			)
SELECT lpad(a[1],8,'0') 
FROM (
	SELECT pg_catalog.string_to_array(
				trim(
				     pg_catalog.regexp_replace(
				     pg_catalog.pg_read_file(timeline_file),'\t',',','g')),',') AS A 
	FROM time_line_file 
    	WHERE modification > (clock_timestamp() - $1) ) foo(a)
$function$;

COMMENT ON FUNCTION get_old_timeline(interval) IS 'Access timeline using timeline file';

CREATE OR REPLACE FUNCTION error_msg_detail(TEXT, TEXT, TEXT, TEXT)
RETURNS TEXT
LANGUAGE sql
AS
$function$
 SELECT 
         CASE 
                WHEN coalesce ($1, '') != '' 
                AND    $1 != e'\n' THEN 'MESSAGE: ' 
                              || $1
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce ($2, '') != '' 
                AND    $2 != e'\n' THEN e'\nDETAIL: ' 
                              || $2 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce ($3, '') != '' 
                AND    $3 != e'\n' THEN e'\nHINT: ' 
                              || $3 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce ($4, '') != '' 
                AND    $4 != e'\n' THEN e' CONTEXT: ' 
                              || $4
                ELSE '' 
         END; 
$function$;
COMMENT ON FUNCTION error_msg_detail(TEXT, TEXT, TEXT, TEXT) IS 'Function to print error messages as a text';


CREATE OR REPLACE FUNCTION list_timeline_wals( TEXT, TEXT)
RETURNS SETOF RECORD
LANGUAGE SQL
AS 
$function$
SELECT filename as old_timeline_wals, 
	OVERLAY(filename PLACING $2 FROM 1 FOR 8) 
FROM pg_catalog.pg_ls_dir('pg_xlog') foo(filename) 
WHERE filename !~* '.history' 
  AND filename !~* '.partial'
  AND filename ~* ('^'||$1)
$function$;

COMMENT ON FUNCTION list_timeline_wals( TEXT, TEXT) IS 'List WAL files based on timeline';

CREATE OR REPLACE FUNCTION wal_exists(wal_name TEXT)
RETURNS boolean
LANGUAGE plpgsql
AS
$function$
DECLARE
    message TEXT; 
    detail TEXT; 
    hint TEXT; 
    context TEXT; 
    error_msg TEXT;
    return_val BOOLEAN := false; 
BEGIN
   SELECT true INTO return_Val 
   FROM pg_stat_file('pg_xlog/'||wal_name) ;
   RETURN return_val;
EXCEPTION 
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_text, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;
   error_msg := error_msg_detail(message, detail, hint, context); 
  RAISE NOTICE '%',error_msg;
  RETURN return_val;
END;
$function$;

COMMENT ON FUNCTION wal_exists(wal_name TEXT) IS 'Function to verify if WAL file exists in pg_xlog';

CREATE OR REPLACE FUNCTION link_timeline_wal(TEXT, TEXT)
RETURNS boolean
LANGUAGE plpgsql
AS
$function$
DECLARE
    cmd TEXT;
    message TEXT; 
    detail TEXT; 
    hint TEXT; 
    context TEXT; 
    error_msg TEXT;
BEGIN
	RAISE NOTICE 'Link old_wal => %, new_wal => %',rec.old_wal, rec.new_wal;
	cmd := 'COPY slot_timeline_table 
			FROM PROGRAM '||quote_literal('ln -s pg_xlog/'||$1||' pg_xlog/'||$2);
	EXECUTE cmd;
	RETURN true;
EXCEPTION 
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_text, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;
   error_msg := error_msg_detail(message, detail, hint, context); 
  RAISE NOTICE '%',error_msg;
  RETURN false;
END;
$function$;

COMMENT ON FUNCTION link_timeline_wal(TEXT, TEXT) IS 'Function which links old timeline file with new timeline file';



CREATE OR REPLACE FUNCTION clean_old_linked_timeline_wals(TEXT)
RETURNS boolean
LANGUAGE plpgsql
AS
$function$
DECLARE
    cmd TEXT;
    message TEXT; 
    detail TEXT; 
    hint TEXT; 
    context TEXT; 
    error_msg TEXT;
BEGIN
	cmd := 'COPY slot_timeline_table 
			FROM PROGRAM '||quote_literal('find -L pg_xlog/'
			||$1
			||'*  -maxdepth 1 -type l ! -exec test -e {} \; -print|xargs rm -f');
	EXECUTE cmd;
	RETURN true;
	    
EXCEPTION 
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_text, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;
   error_msg := error_msg_detail(message, detail, hint, context); 
  RAISE NOTICE '%',error_msg;
  RETURN false;
END;
$function$;

COMMENT ON FUNCTION link_timeline_wal(TEXT, TEXT) IS 'Function which links old timeline file with new timeline file';



CREATE OR REPLACE FUNCTION standby_update_logical_slots (master_fdw TEXT) 
RETURNS TEXT 
LANGUAGE plpgsql
AS 
  $function$ 
  DECLARE 
    slot_sql TEXT := 'SELECT pg_replication_slots ' 
		     || 'FROM pg_replication_slots where slot_type = ' 
    		     || quote_literal('logical'); 

    slot_name_sql TEXT := 'SELECT slot_name '
			  ||'FROM pg_replication_slots WHERE slot_type = '
			  ||quote_literal('logical'); 
    slot_exists BOOLEAN; 
    master_slot_info pg_catalog.pg_replication_slots; 
    standby_slot_info pg_catalog.pg_replication_slots;
    remove_slot RECORD;
    rec RECORD;
    message TEXT; 
    detail TEXT; 
    hint TEXT; 
    conTEXT TEXT; 
    error_msg TEXT; 
    old_timeline TEXT;
    current_timeline TEXT;
    cmd TEXT;
    dblink_exists BOOLEAN;
    fdw_exists BOOLEAN;
  BEGIN 
    /* verify if database is in recovery mode or not 
       if in recovery mode then only run this function */
    SELECT CASE WHEN COUNT(1) >= 1 THEN true ELSE false END INTO fdw_exists
         FROM pg_catalog.pg_user_mappings WHERE srvname = 'master_fdw'; 
    IF NOT fdw_exists THEN
       RETURN 'ERROR: master_fdw doesnt exists, please use SELECT failover_logical_slot_init function to initialize';
    END IF;

    IF pg_is_in_recovery() THEN 
      RAISE NOTICE 'database is in recovery mode'; 
      PERFORM dblink_connect('master', master_fdw); 
      RAISE NOTICE 'created dblink'; 
      RAISE NOTICE 'SLOT: %', slot_sql; 

      FOR master_slot_info IN 
              SELECT (master_slot).* 
              FROM   dblink('master', slot_sql) foo ( master_slot pg_catalog.pg_replication_slots ) 
      LOOP 
          RAISE notice '%',master_slot_info; 
      
      SELECT 
             CASE 
                    WHEN count(1) > 0 THEN TRUE 
                    ELSE FALSE 
             END 
      INTO   slot_exists 
      FROM   pg_replication_slots 
      WHERE  slot_name = master_slot_info.slot_name; 
       
      RAISE notice 'SLOT EXISTS: %', slot_exists; 
      IF NOT slot_exists THEN 
        RAISE notice 'Master slot=> %, plugin => %', master_slot_info.slot_name::TEXT, 
							master_slot_info.plugin; 
        PERFORM pg_create_logical_slot_timelines ( master_slot_info.slot_name::TEXT, 
						 	master_slot_info.plugin); 
      END IF; 

      SELECT pg_replication_slots INTO standby_slot_info 
	FROM pg_replication_slots 
	WHERE slot_name = master_slot_info.slot_name 
	 AND slot_type = 'logical';
      RAISE NOTICE 'Standby slot => %, Primary slot => %',standby_slot_info, master_slot_info;

      IF standby_slot_info IS DISTINCT FROM master_slot_info AND 
	 master_slot_info.confirmed_flush_lsn <= pg_last_xlog_replay_location() 
      THEN 
      		PERFORM pg_advance_logical_slot_timelines (master_slot_info.slot_name::TEXT, 
						master_slot_info.xmin, 
						master_slot_info.catalog_xmin, 
						master_slot_info.restart_lsn, 
						master_slot_info.confirmed_flush_lsn);
      END IF; 

    END LOOP; 

    FOR remove_slot IN 
		WITH remote_slots AS (SELECT slot_name 
				      FROM dblink('master', slot_name_sql) foo (slot_name name) 
					)
    SELECT slot_name::TEXT 
    FROM pg_replication_slots 
    WHERE slot_name NOT IN (SELECT slot_name 
			    FROM remote_slots)
    LOOP 
        PERFORM pg_drop_replication_slot(remove_slot.slot_name);
        RAISE NOTICE 'removed slot: %',remove_slot;
    END LOOP;
    SELECT CASE WHEN 'master' = ANY(dblink_get_connections()) THEN 
			TRUE 
		ELSE FALSE END INTO dblink_exists;
    IF dblink_exists THEN
       PERFORM dblink_disconnect('master');
    END IF;
    RETURN NULL;
  ELSE

     old_timeline := get_old_timeline('1 hours'::interval);
     RAISE NOTICE 'OLD => %', old_timeline;

     current_timeline := substr(pg_xlogfile_name(pg_current_xlog_location()), 1, 8);
     RAISE NOTICE '%',current_timeline;

     IF old_timeline IS NOT NULL AND old_timeline <> current_timeline THEN
        RAISE NOTICE 'Renaming';
        FOR rec IN SELECT old_wal, new_wal 
		   FROM list_timeline_wals(old_timeline, current_timeline) 
		      foo(old_wal TEXT, new_wal TEXT)
        LOOP
           RAISE NOTICE 'old_wal => %, new_wal => %',rec.old_wal, rec.new_wal;
           IF NOT wal_exists(rec.new_wal) THEN
   	        PERFORM link_timeline_wal(old_wal, new_wal);
           END IF;
        END LOOP;  
        PERFORM clean_old_linked_timeline_wals(current_timeline);
     END IF;
     RETURN NULL;
  END IF;  
EXCEPTION 
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_TEXT, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;

    error_msg := error_msg_detail(message, detail, hint, context); 
    SELECT CASE WHEN 'master' = ANY(dblink_get_connections()) THEN 
	true ELSE FALSE END INTO dblink_exists;
    IF dblink_exists THEN
       perform dblink_disconnect('master');
    END IF;
  RETURN error_msg; 
END; 
$function$;

ALTER FUNCTION standby_update_logical_slots(TEXT) SET log_error_verbosity = 'terse';

CREATE OR REPLACE FUNCTION failover_logical_slot_init( host TEXT, 
			port TEXT, 
			dbname TEXT, 
			username TEXT, 
			password TEXT)
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS
$function$
DECLARE
    cmd TEXT;
    master_fdw TEXT;
    message TEXT; 
    detail TEXT; 
    hint TEXT; 
    conTEXT TEXT; 
    error_msg TEXT;
BEGIN
    cmd := format( 'CREATE SERVER master_fdw FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L)',
                   host, port, dbname);
    DROP SERVER IF EXISTS master_fdw CASCADE;
    EXECUTE cmd;
    cmd := format('CREATE USER MAPPING FOR %I SERVER master_fdw OPTIONS (user %L, password %L)',username, username, password);
    EXECUTE cmd;
    RETURN NEXT 'Update following parameters in postgresql.conf';
    RETURN NEXT 'sync_logical_slot.database = '||quote_literal(current_database);
    RETURN NEXT 'sync_logical_slot.master_fdw = '||quote_literal('master_fdw');
    RETURN NEXT 'shared_preload_libraries = '||quote_literal('$libdir/sync_logical_slot');
    RETURN;
EXCEPTION
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_TEXT, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;
  error_msg := error_msg_detail(message, detail, hint, context);   
  RETURN NEXT error_msg;
  RETURN;
END;
$function$;

