/*-------------------------------------------------------------------------
 *
 * sync_slot.c
 *      sync_slot connections of a Postgres server inactive for a given
 *      amount of time.
 *
 * Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      sync_logical_slot/sync_logical_slot.c
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
void sync_logical_slot_main(Datum main_arg) pg_attribute_noreturn();

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* GUC variables */
static int sync_max_idle_time = 1;

/* Worker name */
static char *worker_name = "sync_logical_slot";

/* Worker database name */
static char *databasename = "postgres";
static char *master_fdw = "master_fdw";

static void
sync_logical_slot_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static void
sync_logical_slot_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static void
sync_logical_slot_build_query(StringInfoData *buf)
{
    appendStringInfo(buf, "SELECT standby_update_logical_slots('%s') "
                          "FROM pg_catalog.pg_extension "
                          "WHERE extname = 'slot_timelines';",
                     master_fdw);
}

void
sync_logical_slot_main(Datum main_arg)
{
    StringInfoData buf;

    /* Register functions for SIGTERM/SIGHUP management */
    pqsignal(SIGHUP, sync_logical_slot_sighup);
    pqsignal(SIGTERM, sync_logical_slot_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Connect to a database */
    BackgroundWorkerInitializeConnection(databasename, NULL);

    /* Build query for process */
    initStringInfo(&buf);
    sync_logical_slot_build_query(&buf);

    while (!got_sigterm)
    {
        int rc, ret, i;

        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       sync_max_idle_time * 1000L
                       );
        ResetLatch(&MyProc->procLatch);

        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        /* Process signals */
        if (got_sighup)
        {
            int old_interval;
            /* Save old value of kill interval */
            old_interval = sync_max_idle_time;

            /* Process config file */
            ProcessConfigFile(PGC_SIGHUP);
            got_sighup = false;
            ereport(LOG, (errmsg("bgworker sync_logical_slot signal: processed SIGHUP")));

            /* Rebuild query if necessary */
            if (old_interval != sync_max_idle_time)
            {
                resetStringInfo(&buf);
                initStringInfo(&buf);
                sync_logical_slot_build_query(&buf);
            }
        }

        if (got_sigterm)
        {
            /* Simply exit */
            ereport(LOG, (errmsg("bgworker sync_logical_slot signal: processed SIGTERM")));
            proc_exit(0);
        }

        /* Process idle connection kill */
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        pgstat_report_activity(STATE_RUNNING, buf.data);

        /* Statement start time */
        SetCurrentStatementStartTimestamp();

        /* Execute query */
        ret = SPI_execute(buf.data, false, 0);

        /* Some error handling */
        if (ret != SPI_OK_SELECT)
            elog(FATAL, "Error when trying to sync logical slots");

        /* Do some processing and log stuff disconnected */
        for (i = 0; i < SPI_processed; i++)
        {
            bool isnull;
            char *function_message = NULL;

            /* Fetch values */
            function_message = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
                                                    SPI_tuptable->tupdesc,
                                                    1, &isnull));
                    /* Log what has been disconnected */
            if ( ! isnull )
            {
                elog(LOG, "%s",
                    function_message ? function_message : "none");
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
sync_logical_slot_load_params(void)
{
    /*
     * Kill backends with idle time more than this interval, possible
     * candidates for execution are scanned at the same time interbal.
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
    DefineCustomStringVariable("sync_logical_slot.master_fdw",
                            "master FDW name.",
                            "Default database none",
                            &master_fdw,
                            "none",
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
    sync_logical_slot_load_params();

    /* Worker parameter and registration */
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "sync_logical_slot");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "sync_logical_slot_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
    /* Wait 10 seconds for restart before crash */
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&worker);
}

