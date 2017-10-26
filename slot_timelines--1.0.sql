-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION slot_timelines" to load this file. \quit

CREATE OR REPLACE FUNCTION pg_create_logical_slot_timelines(slot_name text, plugin text)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_create_logical_slot_timelines(text, text)
IS 'Create a logical slot at a particular lsn and xid. Do not use in production servers, it is not safe. The slot is created with an invalid xmin and lsn.';

CREATE OR REPLACE FUNCTION pg_advance_logical_slot_timelines(slot_name text, new_xmin xid, new_catalog_xmin xid, new_restart_lsn pg_lsn, new_confirmed_lsn pg_lsn)
RETURNS void
LANGUAGE c AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_advance_logical_slot_timelines(text, xid, xid, pg_lsn, pg_lsn)
IS 'Advance a logical slot directly.';

CREATE TABLE slot_timeline_table(info TEXT);
CREATE OR REPLACE FUNCTION get_old_timeline(interval) 
RETURNS TEXT
LANGUAGE sql
AS
$function$
WITH time_line_file AS ( select pg_stat_file('pg_xlog/'||filename).*, 'pg_xlog/'||filename as timeline_file from pg_ls_dir('pg_xlog') foo(filename)  where filename ~* '.history' order by modification desc limit 1)
 SELECT lpad(a[1],8,'0') FROM (SELECT string_to_array(trim(regexp_replace(pg_read_file(timeline_file),'\t',',','g')),',') as a FROM time_line_file 
    WHERE modification > (clock_timestamp() - $1) ) foo(a)
$function$;

CREATE OR REPLACE FUNCTION list_timeline_wals(old_timeline text, new_timeline text)
RETURNS SETOF RECORD
LANGUAGE SQL
AS 
$function$
SELECT filename as old_timeline_wals, overlay(filename placing new_timeline from 1 for 8) from pg_ls_dir('pg_xlog') foo(filename) where filename !~* '.history' AND filename !~* '.partial'  AND filename ~* ('^'||$1)
$function$;

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
   SELECT true INTO return_Val FROM pg_stat_file(wal_name) ;
   RETURN return_val;
EXCEPTION 
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_text, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;
  SELECT 
         CASE 
                WHEN coalesce (message, '') != '' 
                AND    message != e'\n' THEN 'MESSAGE: ' 
                              || message 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (detail, '') != '' 
                AND    detail != e'\n' THEN e'\nDETAIL: ' 
                              || detail 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (hint, '') != '' 
                AND    hint != e'\n' THEN e'\nHINT: ' 
                              || hint 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (context, '') != '' 
                AND    context != e'\n' THEN e' CONTEXT: ' 
                              || context 
                ELSE '' 
         END 
  INTO   error_msg; 
  RAISE NOTICE '%',error_msg;
  RETURN return_val;
END;
$function$;

CREATE OR REPLACE FUNCTION standby_update_logical_slots (master_fdw TEXT) 
RETURNS text 
LANGUAGE plpgsql
AS 
  $function$ 
  DECLARE 
    slot_sql TEXT := 'SELECT pg_replication_slots FROM pg_replication_slots where slot_type = ' 
    || quote_literal('logical'); 
    slot_name_sql TEXT := 'SELECT slot_name FROM pg_replication_slots WHERE slot_type = '||quote_literal('logical'); 
    slot_exists BOOLEAN; 
    master_slot_info pg_replication_slots; 
    standby_slot_info pg_replication_slots;
    remove_slot RECORD;
    rec RECORD;
    message TEXT; 
    detail TEXT; 
    hint TEXT; 
    context TEXT; 
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
      RAISE NOTICE '%', slot_sql; 
      FOR master_slot_info IN 
              SELECT (master_slot).* 
              FROM   dblink('master', slot_sql) foo (master_slot pg_catalog.pg_replication_slots) 
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
       
      RAISE notice '%', slot_exists; 
      IF NOT slot_exists THEN 
        RAISE notice '%,%', master_slot_info.slot_name::text, master_slot_info.plugin; 
        perform pg_create_logical_slot_timelines (master_slot_info.slot_name::text, master_slot_info.plugin); 
      END IF; 

      SELECT pg_replication_slots INTO standby_slot_info FROM pg_replication_slots WHERE slot_name = master_slot_info.slot_name AND slot_type = 'logical';
      RAISE NOTICE '%,%',standby_slot_info, master_slot_info;
      IF standby_slot_info IS DISTINCT FROM master_slot_info THEN 
        perform pg_advance_logical_slot_timelines (master_slot_info.slot_name::TEXT, master_slot_info.xmin, master_slot_info.catalog_xmin, master_slot_info.restart_lsn, master_slot_info.confirmed_flush_lsn);
      END IF; 

    END LOOP; 

    FOR remove_slot IN WITH remote_slots AS (SELECT slot_name FROM dblink('master', slot_name_sql) foo (slot_name name) )
    SELECT slot_name::TEXT FROM pg_replication_slots WHERE slot_name NOT IN (SELECT slot_name FROM remote_slots)
    LOOP 
        PERFORM pg_drop_replication_slot(remove_slot.slot_name);
        RAISE NOTICE 'removed slot: %',remove_slot;
    END LOOP;
    SELECT CASE WHEN 'master' = ANY(dblink_get_connections()) THEN true ELSE FALSE END INTO dblink_exists;
    IF dblink_exists THEN
       perform dblink_disconnect('master');
    END IF;
    RETURN NULL;
  ELSE
     SELECT get_old_timeline('1 hours'::interval) INTO old_timeline;
     RAISE NOTIce '%',old_timeline;
     SELECT substr(pg_xlogfile_name(pg_current_xlog_location()), 1, 8) INTO current_timeline;
     RAISE NOTICE '%',current_timeline;
     IF old_timeline IS NOT NULL AND old_timeline <> current_timeline THEN
        RAISE NOTICE 'renaming';
        FOR rec IN SELECT old_wal,new_wal FROM list_timeline_wals(old_timeline,current_timeline) foo(old_wal TEXT, new_wal TEXT)
        LOOP
           RAISE NOTICE '%,%',rec.old_wal, rec.new_wal;
           IF NOT wal_exists('pg_xlog/'||rec.new_wal) THEN
             RAISE NOTICE 'RENAME %,%',rec.old_wal, rec.new_wal;
             cmd := 'COPY slot_timeline_table FROM PROGRAM '||quote_literal('ln -s '||rec.old_wal||' pg_xlog/'||rec.new_wal);
             EXECUTE cmd;
           END IF;
        END LOOP;    
     END IF;
     RETURN NULL;
  END IF;  

EXCEPTION 
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_text, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;
  SELECT 
         CASE 
                WHEN coalesce (message, '') != '' 
                AND    message != e'\n' THEN 'MESSAGE: ' 
                              || message 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (detail, '') != '' 
                AND    detail != e'\n' THEN e'\nDETAIL: ' 
                              || detail 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (hint, '') != '' 
                AND    hint != e'\n' THEN e'\nHINT: ' 
                              || hint 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (context, '') != '' 
                AND    context != e'\n' THEN e' CONTEXT: ' 
                              || context 
                ELSE '' 
         END 
  INTO   error_msg; 
   
    SELECT CASE WHEN 'master' = ANY(dblink_get_connections()) THEN true ELSE FALSE END INTO dblink_exists;
    IF dblink_exists THEN
       perform dblink_disconnect('master');
    END IF;
  RETURN error_msg; 
END; 
$function$;

CREATE OR REPLACE FUNCTION failover_logical_slot_init( host text, port text, dbname text, username text, password text)
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
    context TEXT; 
    error_msg TEXT;
BEGIN
    cmd := format( 'CREATE SERVER master_fdw FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L)',
                   host,port,dbname);
    DROP SERVER IF EXISTS master_fdw CASCADE;
    EXECUTE cmd;
    cmd := format('CREATE USER MAPPING FOR %I SERVER master_fdw OPTIONS (user %L, password %L)',username,username, password);
    EXECUTE cmd;
    RETURN NEXT 'update following parameters in postgresql.conf';
    RETURN NEXT 'sync_logical_slot.database = '||quote_literal(current_database);
    RETURN NEXT 'sync_logical_slot.master_fdw = '||quote_literal('master_fdw');
    RETURN NEXT 'shared_preload_libraries = '||quote_literal('$libdir/sync_logical_slot');
    RETURN;
EXCEPTION
WHEN OTHERS THEN 
  GET STACKED DIAGNOSTICS 
          message = message_text, 
          detail = pg_exception_detail, 
          hint = pg_exception_hint, 
          context = pg_exception_context;
  SELECT 
         CASE 
                WHEN coalesce (message, '') != '' 
                AND    message != e'\n' THEN 'MESSAGE: ' 
                              || message 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (detail, '') != '' 
                AND    detail != e'\n' THEN e'\nDETAIL: ' 
                              || detail 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (hint, '') != '' 
                AND    hint != e'\n' THEN e'\nHINT: ' 
                              || hint 
                ELSE '' 
         END 
                || 
         CASE 
                WHEN coalesce (context, '') != '' 
                AND    context != e'\n' THEN e' CONTEXT: ' 
                              || context 
                ELSE '' 
         END 
  INTO   error_msg; 
  RETURN NEXT error_msg;
  RETURN;
END;
$function$;

