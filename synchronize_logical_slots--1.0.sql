-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION synchronize_logical_slots" to load this file. \quit

CREATE FUNCTION sync_logical_launch(sql pg_catalog.text, dbname pg_catalog.text,
					   queue_size pg_catalog.int4 DEFAULT 65536)
    RETURNS pg_catalog.int4 STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION sync_logical_result(pid pg_catalog.int4)
    RETURNS SETOF pg_catalog.record STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION sync_logical_detach(pid pg_catalog.int4)
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

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

/*
 function for verifying if standby is synchronous standby or not
*/
CREATE OR REPLACE FUNCTION is_standby_synchronous()
RETURNS BOOLEAN
SECURITY DEFINER
LANGUAGE plpgsql
AS
$function$
    DECLARE
        master_conninfo TEXT;
        return_state BOOLEAN := false;
        connected_to_master BOOLEAN := false;
    BEGIN
       IF pg_catalog.pg_is_in_recovery() THEN
          /*
           Check are we still streaming from master
          */
          SELECT CASE WHEN COUNT(1) > 0 THEN true
                      ELSE false
                 END INTO connected_to_master
          FROM pg_catalog.pg_stat_wal_receiver;
          /*
           if yes then check with master for the sync state
          */
           IF connected_to_master THEN
              SELECT CASE WHEN sync_state = 'sync' THEN true
                          ELSE false
                     END INTO return_state
              FROM dblink( pg_catalog.current_setting('primary_conninfo')
                           || ' dbname='
                           || current_database,
                           $SQL$
                             SELECT sync_state
                             FROM pg_catalog.pg_stat_replication
                             WHERE client_addr=inet_client_addr()
                           $SQL$
                       ) foo (sync_state TEXT);
           END IF;
       END IF;
       RETURN return_state;
    END;
$function$;

COMMENT ON FUNCTION is_standby_synchronous() IS 'Function to check if current standby is synchronous or not';

/*
 function for synchronizing the logical slots
*/
CREATE OR REPLACE FUNCTION synchronize_logical_slots()
RETURNS TEXT
LANGUAGE plpgsql
AS
  $function$
  DECLARE
    slot_sql TEXT := 'SELECT pg_replication_slots '
                      || 'FROM pg_catalog.pg_replication_slots '
                      || 'WHERE slot_type = '
                      || quote_literal('logical')
                      || ' AND temporary = false';

    slot_name_sql TEXT := 'SELECT slot_name '
                          || 'FROM pg_catalog.pg_replication_slots '
                          || 'WHERE slot_type = '
                          || quote_literal('logical')
                          || ' AND temporary = false';

    create_slot_sql TEXT;
    advance_slot_sql TEXT;
    slot_exists BOOLEAN;
    master_slot_info pg_catalog.pg_replication_slots;
    standby_slot_info pg_catalog.pg_replication_slots;
    remove_slot RECORD;
    message TEXT;
    detail TEXT;
    hint TEXT;
    context TEXT;
    error_msg TEXT;
    master_conninfo TEXT;
  BEGIN

    master_conninfo := current_setting('primary_conninfo')
                          || ' dbname='
                          || current_database;
    /* verify if database is in recovery mode and is synchronous standby
       if in recovery mode then only run this function
    */
    IF is_standby_synchronous() THEN
       /*
         create master conninfo using primary_conninfo
       */
      master_conninfo := current_setting('primary_conninfo')
                          || ' dbname='
                          || current_database;
      RAISE NOTICE 'master conninfo: %',
                                       master_conninfo;
      RAISE NOTICE 'database is synchronous standby';
      RAISE NOTICE 'SLOT: %', slot_sql;
      /*
        Based on the logical slots information from master
        create or advance the logical slots on standby
      */
      FOR master_slot_info IN
              SELECT (master_slot).*
              FROM   dblink(master_conninfo, slot_sql)
                     foo ( master_slot pg_catalog.pg_replication_slots )
      LOOP
          RAISE NOTICE '%', master_slot_info;

      SELECT
             CASE
                    WHEN count(1) > 0 THEN TRUE
                    ELSE FALSE
             END
      INTO   slot_exists
      FROM   pg_catalog.pg_replication_slots
      WHERE  slot_name = master_slot_info.slot_name;

      RAISE NOTICE 'Slot % exists: %',
                    master_slot_info.slot_name,
                    slot_exists;

      IF NOT slot_exists THEN
          RAISE NOTICE 'Master slot=> %, plugin => % doesnt exists',
                            master_slot_info.slot_name::TEXT,
                            master_slot_info.plugin;
          create_slot_sql := 'SELECT * FROM '
                           || 'pg_catalog.pg_create_logical_replication_slot('
                           || quote_literal(master_slot_info.slot_name::TEXT)
                           || ','
                           || quote_literal(master_slot_info.plugin)
                           || ');';
          PERFORM * FROM sync_logical_result(
                           sync_logical_launch(
                                       create_slot_sql,
                                       master_slot_info.database
                                        )) as foo(slot_name name, end_lsn pg_lsn);
      END IF;

      SELECT pg_replication_slots INTO standby_slot_info
      FROM pg_catalog.pg_replication_slots
      WHERE slot_name = master_slot_info.slot_name
      AND slot_type = 'logical' and temporary=false;

      RAISE NOTICE 'standby slot: %, primary slot: %',
                    standby_slot_info,
                    master_slot_info;
      /*
        check if we need to advance the slots or not and accordingly
        advance the slots
      */
      IF standby_slot_info IS DISTINCT FROM master_slot_info
         AND
         master_slot_info.confirmed_flush_lsn <= pg_last_wal_replay_lsn()
      THEN
            RAISE NOTICE 'Advancing slot % by %',
                          master_slot_info.slot_name::TEXT,
                          master_slot_info.confirmed_flush_lsn;

            advance_slot_sql := 'SELECT * FROM '
                                || 'pg_catalog.pg_replication_slot_advance('
                                || quote_literal(master_slot_info.slot_name::TEXT)
                                || ','
                                || quote_literal(master_slot_info.confirmed_flush_lsn)
                                || ');';
            PERFORM * FROM sync_logical_result(
                             sync_logical_launch(advance_slot_sql, 
                             master_slot_info.database
                                        )) as foo(slot_name name, end_lsn pg_lsn);
      END IF;

    END LOOP;

    /*
      if any slot got deleted from the master then we should
      drop the slots from standby
    */
    FOR remove_slot IN
        WITH master_slots
        AS (SELECT slot_name
            FROM dblink(master_conninfo, slot_name_sql) foo (slot_name name)
            )
    SELECT slot_name::TEXT
    FROM pg_replication_slots
    WHERE slot_name NOT IN (SELECT slot_name
                FROM master_slots)
    LOOP
        PERFORM pg_catalog.pg_drop_replication_slot(remove_slot.slot_name);
        RAISE NOTICE 'removed slot: %',remove_slot;
    END LOOP;
  END IF;

RETURN NULL;

EXCEPTION
WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS
          message = message_text,
          detail = pg_exception_detail,
          hint = pg_exception_hint,
          context = pg_exception_context;
RETURN error_msg_detail(message, detail, hint, context);
END;
$function$;

COMMENT ON FUNCTION synchronize_logical_slots() IS 'Function for synchronizing the slots';

REVOKE ALL ON FUNCTION sync_logical_launch(pg_catalog.text, pg_catalog.text, pg_catalog.int4)
	FROM public;
REVOKE ALL ON FUNCTION sync_logical_result(pg_catalog.int4)
	FROM public;
REVOKE ALL ON FUNCTION sync_logical_detach(pg_catalog.int4)
	FROM public;
REVOKE ALL ON FUNCTION is_standby_synchronous() FROM PUBLIC;
REVOKE ALL ON FUNCTION synchronize_logical_slots() FROM PUBLIC;
REVOKE ALL ON FUNCTION error_msg_detail(TEXT, TEXT, TEXT, TEXT) FROM PUBLIC;
