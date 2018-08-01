#include <cdb2api.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>

#include "phys_rep_lsn.h"
#include "dbinc/rep_types.h"

#include "comdb2.h"
#include "phys_rep.h"
#include "truncate_log.h"

#include <parse_lsn.h>
#include <logmsg.h>

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
static LOG_INFO handle_record();

static pthread_t sync_thread;
/* internal implementation */

/* externs here */
extern struct dbenv* thedb;
extern int sc_ready(void);

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
            logmsg(LOGMSG_WARN, "Attached to '%s' and db '%s' for replication\n", 
                    cnct->hostname,
                    repl_db_name);
            if (pthread_create(&sync_thread, NULL, keep_in_sync, NULL))
            {
                logmsg(LOGMSG_ERROR, "Couldn't create thread to sync\n");
                cdb2_close(repl_db);
            }

            return cnct->hostname;
        }

        logmsg(LOGMSG_WARN, "Couldn't connect to %s\n", cnct->hostname);
    }

    logmsg(LOGMSG_ERROR, "Couldn't find any remote dbs to connect to\n");
    return NULL;
}

void* keep_in_sync(void* args)
{
    /* vars for syncing */
    int rc;
    int64_t gen;
    struct timespec wait_spec;
    struct timespec remain_spec;
    // char* sql_cmd = "select * from comdb2_transaction_logs('{@file:@offset}')"; 
    size_t sql_cmd_len = 100;
    char sql_cmd[sql_cmd_len];
    LOG_INFO info;
    LOG_INFO prev_info;


    do_repl = 1;
    wait_spec.tv_sec = 1;
    wait_spec.tv_nsec = 0;

    /* cannot call get_last_lsn if thedb is not ready */
    while (!sc_ready())
        nanosleep(&wait_spec, &remain_spec);

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    /* do truncation to start fresh */
    info = get_last_lsn(thedb->bdb_env);
    prev_info = handle_truncation(repl_db, info);
    gen = prev_info.gen;
    fprintf(stderr, "gen: %ld\n", gen);

    while(do_repl)
    {
        info = get_last_lsn(thedb->bdb_env);
        prev_info = info;

        rc = snprintf(sql_cmd, sql_cmd_len, 
                "select * from comdb2_transaction_logs('{%u:%u}')", 
                info.file, info.offset);
        if (rc < 0 || rc >= sql_cmd_len)
            logmsg(LOGMSG_ERROR, "sql_cmd buffer is not long enough!\n");

        if ((rc = cdb2_run_statement(repl_db, sql_cmd)) != CDB2_OK)
        {
            logmsg(LOGMSG_ERROR, "Couldn't query the database, retrying\n");
            nanosleep(&wait_spec, &remain_spec);
            continue;
        }

        /* should verify that this is the record we want */
        if ((rc = cdb2_next_record(repl_db)) != CDB2_OK)
        {
            logmsg(LOGMSG_WARN, "Can't find the next record\n");

            if (rc == CDB2_OK_DONE)
            {
                fprintf(stderr, "Let's do truncation\n");
                prev_info = handle_truncation(repl_db, info);
            }
        }
        else
        {
            int broke_early = 0;

            while((rc = cdb2_next_record(repl_db)) == CDB2_OK)
            {
                int64_t* rec_gen =  (int64_t *) cdb2_column_value(repl_db, 2);
                if (rec_gen && *rec_gen > gen)
                {
                    logmsg(LOGMSG_WARN, "My master changed, do truncation!\n");
                    fprintf(stderr, "gen: %ld, rec_gen: %ld\n", gen, *rec_gen);
                    prev_info = handle_truncation(repl_db, info);
                    gen = *rec_gen;

                    broke_early = 1;
                    break;
                }
                prev_info = handle_record(prev_info);

            }

            /* check we finished correctly */
            if ((!broke_early && rc != CDB2_OK_DONE ) || 
                    (broke_early && rc != CDB2_OK))
            {
                logmsg(LOGMSG_ERROR, "Had an error %d\n", rc);
                fprintf(stderr, "rc=%d\n", rc);
            }

        }

        nanosleep(&wait_spec, &remain_spec);
    }

    cdb2_close(repl_db);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    return NULL;
}

void stop_sync()
{
    do_repl = 0;

    if (pthread_join(sync_thread, NULL)) 
    {
        logmsg(LOGMSG_ERROR, "sync thread didn't join back :(\n");
    }

}

/* privates */
static LOG_INFO handle_record(LOG_INFO prev_info)
{
    /* vars for 1 record */
    void* blob;
    int blob_len;
    unsigned int gen;
    char* lsn, *timestamp, *token;
    const char delim[2] = ":";
    int64_t rectype; 
    int rc; 
    unsigned int file, offset;

    lsn = (char *) cdb2_column_value(repl_db, 0);
    rectype = *(int64_t *) cdb2_column_value(repl_db, 1);
    gen = *(unsigned int *) cdb2_column_value(repl_db, 2);
    timestamp = (char *) cdb2_column_value(repl_db, 3);
    blob = cdb2_column_value(repl_db, 4);
    blob_len = cdb2_column_size(repl_db, 4);

    logmsg(LOGMSG_WARN, "lsn: %s", lsn);

    /* TODO: consider using sqlite/ext/comdb2/tranlog.c */
    token = strtok(lsn, delim);    
    if (token)
    {
        /* assuming it's {#### */
        file = (unsigned int) strtoul(token + 1, NULL, 0);
    }
    else
    {
        file = 0;
    }

    token = strtok(NULL, delim);
    if (token)
    {
        token[strlen(token) - 1] = '\0';
        offset = (unsigned int) strtoul(token, NULL, 0);
    }
    else
    {
        offset = 0;
    }
    
    logmsg(LOGMSG_WARN, ": %u:%u, rectype: %ld\n", file, offset, rectype);    

    /* check if we need to call new file flag */
    if (prev_info.file < file)
    {
        rc = apply_log(thedb->bdb_env->dbenv, prev_info.file, 
                get_next_offset(thedb->bdb_env->dbenv, prev_info), 
                REP_NEWFILE, NULL, 0); 
    }
    rc = apply_log(thedb->bdb_env->dbenv, file, offset, 
            REP_LOG, blob, blob_len); 

    if (rc != 0)
    {
        logmsg(LOGMSG_ERROR, "Something went wrong with applying the logs\n");
    }

    LOG_INFO next_info;
    next_info.file = file;
    next_info.offset = offset;
    next_info.size = blob_len;

    return next_info;
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
                logmsg(LOGMSG_ERROR, "Failed to realloc hostnames");
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

