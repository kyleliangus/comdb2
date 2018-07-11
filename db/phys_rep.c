#include <cdb2api.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include "config.h"
#include "phys_rep.h"

/* for replication */
static cdb2_hndl_tp* repl_db;
static cdb2_hndl_tp* host_db;
static char* host_db_name;

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
/* internal implementation */

/* external API */
int set_host_db(char* name)
{
    int rc;
    host_db_name = name;

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

    free(host_db_name);

    cdb2_close(host_db);
    cdb2_close(repl_db);
}

/* TODO: open a connection with a db */
const char* start_replication()
{
    int rc;
    DB_Connection* cnct;

    for (int i = 0; i < idx; i++)
    {
        cnct = local_rep_dbs[i];
        /* TODO: change to "default" once ready to test */ 
        if ((rc = cdb2_open(&repl_db, cnct->hostname, "local", 0)) == 0)
        {
            fprintf(stdout, "Attached to %s for replication\n", cnct->hostname);
            return cnct->hostname;
        }
    }
    fprintf(stderr, "Couldn't find any remote dbs to connect to\n");
    return NULL;
}

void* keep_in_sync(void* args)
{
    int rc;
    //int64_t
    char* sql_cmd = "select * from comdb2_transaction_logs"; 

    do_repl = 1;

    while(do_repl)
    {
        rc = cdb2_run_statement(repl_db, sql_cmd);

    }

    return NULL;
}

void stop_sync()
{
    do_repl = 0;
}

/* privates */
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

