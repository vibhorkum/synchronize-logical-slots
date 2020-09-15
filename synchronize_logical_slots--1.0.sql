-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION synchronize_logical_slots" to load this file. \quit

CREATE OR REPLACE FUNCTION sync_logical_launch(sql pg_catalog.text, dbname pg_catalog.text,
					   queue_size pg_catalog.int4 DEFAULT 65536)
    RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION sync_logical_launch(TEXT, TEXT, pg_catalog.int4) IS 'Function to perform asynchronous execution';

CREATE OR REPLACE FUNCTION sync_logical_result(pid pg_catalog.int4)
    RETURNS SETOF pg_catalog.record STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION sync_logical_result(pg_catalog.int4) IS 'Function to get the result of sync_logical_launch pid';

CREATE OR REPLACE FUNCTION sync_logical_detach(pid pg_catalog.int4)
    RETURNS pg_catalog.void STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION sync_logical_detach(pg_catalog.int4) IS 'Function to close the process with discarding the results';

CREATE OR REPLACE FUNCTION create_logical_slot(slot_name name, plugin name, dbname text)
RETURNS INTEGER
LANGUAGE plpgsql
AS
$function$
 DECLARE
   logical_sql TEXT := 'SELECT * FROM '
                       || 'pg_catalog.pg_create_logical_replication_slot('
                       || quote_literal(slot_name)
                       || ','
                       || quote_literal(plugin)
                       || ');';
   bg_pid INTEGER;
 BEGIN
   SELECT sync_logical_launch(
            logical_sql,
            dbname) INTO bg_pid;
   RETURN bg_pid;
  END;
$function$;

COMMENT ON FUNCTION create_logical_slot(name, name,text) IS 'Function to create logical slot asynchronousily';


CREATE OR REPLACE FUNCTION advance_logical_slot(slot_name name, upto_lsn pg_lsn, dbname text)
RETURNS INTEGER
LANGUAGE plpgsql
AS
$function$
 DECLARE
   logical_sql TEXT := 'SELECT * FROM '
                       || 'pg_catalog.pg_replication_slot_advance('
                       || quote_literal(slot_name)
                       || ','
                       || quote_literal(upto_lsn)
                       || ');';
   bg_pid INTEGER;
 BEGIN
   SELECT sync_logical_launch(
            logical_sql,
            dbname) INTO bg_pid;
   RETURN bg_pid;
 END;
$function$;

COMMENT ON FUNCTION advance_logical_slot(name, pg_lsn, text) IS 'FUNCTION to advance logical slot asynchronously';

CREATE OR REPLACE FUNCTION primary_checkpoint()
RETURNS TEXT
SECURITY DEFINER
LANGUAGE SQL
AS
$SQL$
  SELECT *
  FROM sync_logical_result(
             sync_logical_launch(
                 'CHECKPOINT;',
                 current_database))
  AS foo(msg TEXT);
$SQL$;

COMMENT ON FUNCTION primary_checkpoint() IS 'Function to perform checkpoint';

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
        primary_conninfo TEXT;
        return_state BOOLEAN := false;
        connected_to_primary BOOLEAN := false;
    BEGIN
       IF pg_catalog.pg_is_in_recovery() THEN
          /*
           Check are we still streaming from primary
          */
          SELECT CASE WHEN COUNT(1) > 0 THEN true
                      ELSE false
                 END INTO connected_to_primary
          FROM pg_catalog.pg_stat_wal_receiver;
          /*
           if yes then check with primary for the sync state
          */
           IF connected_to_primary THEN
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

    checkpoint_sql TEXT := 'SELECT primary_checkpoint();';
    bg_pid   INTEGER;
    slot_exists BOOLEAN;
    primary_slot_info pg_catalog.pg_replication_slots;
    standby_slot_info RECORD;
    remove_slot RECORD;
    message TEXT;
    detail TEXT;
    hint TEXT;
    context TEXT;
    error_msg TEXT;
    primary_conninfo TEXT;
  BEGIN

     primary_conninfo := current_setting('primary_conninfo')
                          || ' dbname='
                          || current_database;
    /* verify if database is in recovery mode and is synchronous standby
     if in recovery mode then only run this function
    */
    IF is_standby_synchronous() THEN
       /*
         create primary conninfo using primary_conninfo
       */
        primary_conninfo := current_setting('primary_conninfo')
                            || ' dbname='
                            || current_database;
        RAISE NOTICE 'primary conninfo: %',
                                       primary_conninfo;
        RAISE NOTICE 'database is synchronous standby';
        RAISE NOTICE 'SLOT: %', slot_sql;
        /*
          Based on the logical slots information from primary
          create or advance the logical slots on standby
        */
        FOR primary_slot_info IN
                SELECT (primary_slot).*
                FROM   dblink(primary_conninfo, slot_sql)
                       foo ( primary_slot pg_catalog.pg_replication_slots )
        LOOP
            RAISE NOTICE '%', primary_slot_info;

            SELECT
               CASE
                   WHEN count(1) > 0 THEN TRUE
                   ELSE FALSE
                END
            INTO   slot_exists
            FROM   pg_catalog.pg_replication_slots
            WHERE  slot_name = primary_slot_info.slot_name;

            RAISE NOTICE 'Slot % exists: %',
                      primary_slot_info.slot_name,
                      slot_exists;

            /* if slot exists we want to check if plugins are same
               in case plugins are not same we are going to recreate
               else advance the slot with proper checks
            */
            IF slot_exists THEN
              SELECT * INTO standby_slot_info
              FROM pg_catalog.pg_replication_slots
              WHERE slot_name = primary_slot_info.slot_name
              AND slot_type = 'logical' and temporary=false;

              RAISE NOTICE 'INFO: Standby Slot: %', standby_slot_info;
              RAISE NOTICE 'INFO: Primary Slot: %', primary_slot_info;

              IF standby_slot_info.plugin != primary_slot_info.plugin THEN
                  PERFORM pg_catalog.pg_drop_replication_slot( primary_slot_info.slot_name );
                  SELECT create_logical_slot(
                                   primary_slot_info.slot_name,
                                   primary_slot_info.plugin,
                                   primary_slot_info.database) INTO bg_pid;
                  PERFORM * FROM dblink( primary_conninfo,
                                         checkpoint_sql)
                            AS foo( msg TEXT );
                  PERFORM * FROM sync_logical_result( bg_pid )
                            AS foo( slot_name name, lsn pg_lsn );
              ELSIF primary_slot_info.confirmed_flush_lsn < pg_last_wal_replay_lsn()
                    AND
                    standby_slot_info.restart_lsn < primary_slot_info.confirmed_flush_lsn
                    AND
                    standby_slot_info.confirmed_flush_lsn < primary_slot_info.confirmed_flush_lsn
              THEN
                  RAISE NOTICE 'Advancing the slot';
                  SELECT advance_logical_slot(
                         primary_slot_info.slot_name,
                         primary_slot_info.confirmed_flush_lsn,
                         primary_slot_info.database) INTO bg_pid;
                  PERFORM * FROM sync_logical_result( bg_pid )
                            AS foo( slot_name name, lsn pg_lsn );
              END IF;
            END IF;

            /*
              if slot doesn't exists then re-create
            */
            IF NOT slot_exists THEN
                RAISE NOTICE 'Primary slot=> %, plugin => % doesnt exists',
                              primary_slot_info.slot_name::TEXT,
                              primary_slot_info.plugin;
                SELECT create_logical_slot(
                                 primary_slot_info.slot_name,
                                 primary_slot_info.plugin,
                                 primary_slot_info.database) INTO bg_pid;
                PERFORM * FROM dblink( primary_conninfo,
                                       checkpoint_sql)
                          AS foo(msg TEXT);
                PERFORM * FROM sync_logical_result( bg_pid )
                          AS foo( slot_name name, lsn pg_lsn );
            END IF;

        END LOOP;

      /*
        if any slot got deleted from the primary then we should
        drop the slots from standby
      */
      FOR remove_slot IN
          WITH primary_slots
          AS (SELECT slot_name
              FROM dblink(primary_conninfo, slot_name_sql) foo (slot_name name)
              )
        SELECT slot_name::TEXT
        FROM pg_replication_slots
        WHERE slot_name NOT IN (SELECT slot_name
                FROM primary_slots)
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
REVOKE ALL ON FUNCTION create_logical_slot(name, name, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION advance_logical_slot(name, pg_lsn, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION primary_checkpoint() FROM PUBLIC;
