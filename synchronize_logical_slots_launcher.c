/*-------------------------------------------------------------------------
 *
 * synchronize_logical_slots_launcher.c
 *      uses the synchrnoize_logical_slots function to synchronize slots 
 *      on master
 *
 * Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      synchronize_logical_slots/synchronize_logical_slots_launcher.c
 *
 *-------------------------------------------------------------------------
 */

/* Some general headers for custom bgworker facility */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

/* Entry point of library loading */
void _PG_init(void);

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* GUC variables */
static char *databasename = "postgres";
static int interval = 60;

/* Worker name */
static char *worker_name = "synchronize_logical_slots_launcher";

#if PG_VERSION_NUM >= 90500
/*
 * Forward declaration for main routine. Makes compiler
 * happy (-Wunused-function, __attribute__((noreturn)))
 */
void synchronize_logical_slots_launcher_main(Datum main_arg) pg_attribute_noreturn();
#endif

static void
synchronize_logical_slots_launcher_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static void
synchronize_logical_slots_launcher_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static void
synchronize_logical_slots_launcher_build_query(StringInfoData *buf)
{
    appendStringInfo(buf,  "SELECT synchronize_logical_slots() "
               "FROM pg_catalog.pg_extension "
               "WHERE extname = 'synchronize_logical_slots';"
           );
}

static void
synchronize_logical_slots_extension_build_query(StringInfoData *buf)
{
    appendStringInfo(buf,  "SELECT CASE WHEN COUNT(1) > 0 THEN true ELSE false END "
               "FROM pg_catalog.pg_extension "
               "WHERE extname = 'synchronize_logical_slots';"
           );
}

void
synchronize_logical_slots_launcher_main(Datum main_arg)
{
    StringInfoData buf;
    StringInfoData check_ext_buf;

    /* Register functions for SIGTERM/SIGHUP management */
    pqsignal(SIGHUP, synchronize_logical_slots_launcher_sighup);
    pqsignal(SIGTERM, synchronize_logical_slots_launcher_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Connect to a database */
    BackgroundWorkerInitializeConnection(databasename, NULL, BGWORKER_SHMEM_ACCESS|BGWORKER_BACKEND_DATABASE_CONNECTION);

    /* Build query for process */
    initStringInfo(&buf);
    synchronize_logical_slots_launcher_build_query(&buf);
    initStringInfo(&check_ext_buf);
    synchronize_logical_slots_extension_build_query(&check_ext_buf);

    while (!got_sigterm)
    {
        int rc = 0;
        int ret, i;
        int sleep_interval;
        bool extn_exists = false;

        if (0 == interval)
            sleep_interval = 10;
        else
            sleep_interval = interval;

        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       sleep_interval * 1000L
                       , PG_WAIT_EXTENSION
            );
        ResetLatch(&MyProc->procLatch);

        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        /* Process signals */
        if (got_sighup)
        {
            int old_interval;
            /* Save old value of kill chance */
            old_interval = interval;

            /* Process config file */
            ProcessConfigFile(PGC_SIGHUP);
            got_sighup = false;
            ereport(LOG, (errmsg("bgworker synchronize_logical_slots_launcher signal: processed SIGHUP")));

            /* Rebuild query if necessary */
            if (old_interval != interval)
            {
                resetStringInfo(&buf);
                resetStringInfo(&check_ext_buf);
                initStringInfo(&buf);
                synchronize_logical_slots_launcher_build_query(&buf);
                initStringInfo(&check_ext_buf);
                synchronize_logical_slots_extension_build_query(&check_ext_buf);
            }
        }

        if (got_sigterm)
        {
            /* Simply exit */
            ereport(LOG, (errmsg("bgworker synchronize_logical_slots_launcher signal: processed SIGTERM")));
            proc_exit(0);
        }

        /*
         * If interval is 0 we should not do anything.
         * This has to be done after sighup and sigterm handling.
         */
        if (0 == interval)
        {
            elog(LOG, "Nothing to do, sleep zzzzZZZZ");
            continue;
        }


        /* Process idle connection kill */
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        pgstat_report_activity(STATE_RUNNING, check_ext_buf.data);

        /* Statement start time */
        SetCurrentStatementStartTimestamp();

        /* Execute query */
        ret = SPI_execute(check_ext_buf.data, false, 0);

        /* Some error handling */
        if (ret != SPI_OK_SELECT)
            elog(FATAL, "Error when trying to synchronize logical slots");

        /* Do some processing and log stuff disconnected */
        for (i = 0; i < SPI_processed; i++)
         {
             bool isnull;

             /* Fetch values */
             extn_exists = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[i],
                                                     SPI_tuptable->tupdesc,
                                                     1, &isnull));
             /* Log what message return */
             if ( extn_exists )
             {
                 elog(DEBUG1, "extension synchronize_logical_slot exists.");
             }
         }
        
        if ( extn_exists )
        {
            pgstat_report_activity(STATE_RUNNING, buf.data);
            SetCurrentStatementStartTimestamp();
             /* Execute query */
            ret = SPI_execute(buf.data, false, 0);
            if (ret != SPI_OK_SELECT)
                elog(FATAL, "Error when trying to synchronize logical slots");
            /* Do some processing and log stuff disconnected */
            for (i = 0; i < SPI_processed; i++)
            {
                bool isnull;
                char *function_message = NULL;
                /* Fetch values */
                function_message = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
                            SPI_tuptable->tupdesc,
                            1, &isnull));
                /* Log what message return */
                if ( ! isnull )
                {
                    elog(DEBUG1, "%s",
                            function_message ? function_message : "none");
                }
            }
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity(STATE_IDLE, NULL);
    }

    /* No problems, so clean exit */
    proc_exit(0);
}

static void
synchronize_logical_slots_launcher_load_params(void)
{
    /*
     * load the database name for synchronization of logical slots
     */
     DefineCustomStringVariable("sync_logical_slot.database",
                             "logical slot database to sync slots.",
                             "Default database is postgres",
                             &databasename,
                             "postgres",
                             PGC_SIGHUP,
                             0,
                             NULL,
                             NULL,
                             NULL);

}

/*
 * Entry point for worker loading
 */
void
_PG_init(void)
{
    BackgroundWorker worker;

    /* Add parameters */
    synchronize_logical_slots_launcher_load_params();

    /* Worker parameter and registration */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;

    snprintf(worker.bgw_library_name, BGW_MAXLEN - 1, "synchronize_logical_slots_launcher");
    snprintf(worker.bgw_function_name, BGW_MAXLEN - 1, "synchronize_logical_slots_launcher_main");

    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
    snprintf(worker.bgw_type, BGW_MAXLEN, "%s", worker_name);
    /* Wait 10 seconds for restart before crash */
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&worker);
}
