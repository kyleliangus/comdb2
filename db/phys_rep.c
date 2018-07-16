#include <cdb2api.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>

#include "comdb2.h"
#include <build/db.h>
#include "phys_rep.h"

/* for replication */
static cdb2_hndl_tp* repl_db;
static char* repl_db_name;

static int do_repl;

/* internal implementation */
typedef struct DB_Connection {
    char *hostname;
    int is_up;
    time_t last_cnct;
} DB_Connection;

static DB_Connection **local_rep_dbs = NULL;
static int cnct_len = 0;
static int idx = 0;

static DB_Connection* get_connect(char* hostname);
static int insert_connect(char* hostname);
static void delete_connect(DB_Connection* cnct);
static void interpret_record();

static pthread_t sync_thread;
/* internal implementation */

/* externs here */
extern struct dbenv* thedb;

/* external API */
int set_repl_db_name(char* name)
{
    int rc;
    repl_db_name = name;

    /* TODO: may want to verify local db exists */
    rc = 0;

    return rc;
}

int add_replicant_host(char *hostname) 
{
    return insert_connect(hostname);
}

int remove_replicant_host(char *hostname) {
    DB_Connection* cnct = get_connect(hostname);
    if (cnct)
    {
        delete_connect(cnct);
        return 0;
    }
    return -1;
}

void cleanup_hosts()
{
    DB_Connection* cnct;

    for (int i = 0; i < idx; i++)
    {
        cnct = local_rep_dbs[i];
        delete_connect(cnct);
    }

    free(repl_db_name);

    cdb2_close(repl_db);
}

const char* start_replication()
{
    int rc;
    DB_Connection* cnct;

    for (int i = 0; i < idx; i++)
    {
        cnct = local_rep_dbs[i];

        if ((rc = cdb2_open(&repl_db, repl_db_name, 
                        cnct->hostname, CDB2_DIRECT_CPU)) == 0)
        {
            fprintf(stdout, "Attached to %s for replication\n", cnct->hostname);
            if (pthread_create(&sync_thread, NULL, keep_in_sync, NULL))
            {
                fprintf(stderr, "Couldn't create thread to sync\n");
                cdb2_close(repl_db);
            }

            return cnct->hostname;
        }

        fprintf(stdout, "%s\n", cnct->hostname);
    }

    fprintf(stderr, "Couldn't find any remote dbs to connect to\n");
    return NULL;
}

void* keep_in_sync(void* args)
{
    /* vars for syncing */
    int rc;
    struct timespec wait_spec;
    struct timespec remain_spec;
    char* sql_cmd = "select * from comdb2_transaction_logs"; 

    do_repl = 1;
    wait_spec.tv_sec = 1;
    wait_spec.tv_nsec = 0;

    while(do_repl)
    {
        rc = cdb2_run_statement(repl_db, sql_cmd);
        // ncols = cdb2_numcolumns(repl_db);

        while((rc = cdb2_next_record(repl_db) == CDB2_OK))
        {
            /* should have ncols as 5 
            for(col = 0; col < ncols; col++)
            {
                val = cdb2_column_value(repl_db, col); 
                fprintf(stderr, "%s, type: %d", 
                        cdb2_column_name(repl_db, col),
                        cdb2_column_type(repl_db, col));
            } */
            interpret_record();

            break;
        }

        if (rc == CDB2_OK_DONE)
        {
            fprintf(stdout, "Finished reading from xsaction logs\n");
        }
        else
        {
            fprintf(stderr, "Had an error %d\n", rc);
        }

        fprintf(stdout, "I sleep for 1 second\n");
        nanosleep(&wait_spec, &remain_spec);
    }

    cdb2_close(repl_db);
    return NULL;
}

void stop_sync()
{
    do_repl = 0;

    if (pthread_join(sync_thread, NULL)) 
    {
        fprintf(stderr, "sync thread didn't join back :(");
    }

}

/* privates */
static void interpret_record()
{
    /* vars for 1 record */
    int ncols;
    int col;
    void* blob;
    int blob_len;
    char* lsn, gen, timestamp;
    int64_t rectype; 
    int rc;

    /* get db internals */
    DB_LOGC* logc;
    DBT logrec;
    DB_LSN last_log_lsn;
    bdb_state_type* bdb_state;

    lsn = (char *) cdb2_column_value(repl_db, 0);
    rectype = *(int64_t *) cdb2_column_value(repl_db, 1);
    gen = (char *) cdb2_column_value(repl_db, 2);
    timestamp = (char *) cdb2_column_value(repl_db, 3);
    blob = cdb2_column_value(repl_db, 4);
    blob_len = cdb2_column_size(repl_db, 4);

    bdb_state = thedb->bdb_env;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        fprintf(stderr, "%s: can't get log cursor rc %d\n", __func__, rc);
        return;
    }
    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_MALLOC;
    rc = logc->get(logc, &first_log_lsn, &logrec, DB_LAST);
    if (rc) {
        fprintf(stderr, "%s: can't get first log record rc %d\n", __func__,
                rc);
        logc->close(logc, 0);
        return;
    }
    if (logrec.data)
        free(logrec.data);
    logc->close(logc, 0);


}

/* data struct implementation */
static DB_Connection* get_connect(char* hostname)
{
    DB_Connection* cnct; 

    for (int i = 0; i < idx; i++) 
    {
        cnct = local_rep_dbs[i];

        if (strcmp(cnct->hostname, hostname) == 0) 
        {
            return cnct;
        }
    }

    return NULL;
}

static int insert_connect(char* hostname)
{
    if (idx >= cnct_len)
    {
        // on initialize
        if (!local_rep_dbs)
        {
            cnct_len = 4;
            local_rep_dbs = malloc(cnct_len * sizeof(DB_Connection*));
        }
        else 
        {
            cnct_len *= 2;
            DB_Connection** temp = realloc(local_rep_dbs, 
                    cnct_len * sizeof(*local_rep_dbs));

            if (temp) 
            {
                local_rep_dbs = temp; 
            }
            else 
            {
                fprintf(stderr, "Failed to realloc hostnames");
                return -1;
            }
        }

    }

    DB_Connection* cnct = malloc(sizeof(DB_Connection));
    cnct->hostname = hostname;
    cnct->last_cnct = 0;
    cnct->is_up = 1;

    local_rep_dbs[idx++] = cnct;

    return 0;
}

static void delete_connect(DB_Connection* cnct)
{
    free(cnct->hostname);
    free(cnct);
}

