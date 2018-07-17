#include <build/db.h>
#include <strings.h>

#include "bdb_int.h"
#include "phys_rep_lsn.h"

extern bdb_state_type *bdb_state;

LOG_INFO get_last_lsn(bdb_state_type* bdb_state)
{
    int rc; 

    /* get db internals */
    DB_LOGC* logc;
    DBT logrec;
    DB_LSN last_log_lsn;
    LOG_INFO log_info;
    log_info.file = log_info.offset = log_info.size = 0;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get log cursor rc %d\n", __func__, rc);
        return log_info;
    }
    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_MALLOC;
    rc = logc->get(logc, &last_log_lsn, &logrec, DB_LAST);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get last log record rc %d\n", __func__,
                rc);
        logc->close(logc, 0);
        return log_info;
    }

    /*
    fprintf(stdout, "Data: %.*s", logrec.size, (char *) logrec.data); 
    fprintf(stdout, " size: %u ", logrec.size);
    fprintf(stdout, "ulen %u, dlen %u, doff %u\n", logrec.ulen, logrec.dlen, logrec.doff);
    */
    fprintf(stdout, "LSN %u:%u\n", last_log_lsn.file, last_log_lsn.offset);

    log_info.file = last_log_lsn.file;
    log_info.offset =  last_log_lsn.offset; 
    log_info.size = logrec.size;


    logc->close(logc, 0);

    return log_info;
}

u_int32_t get_last_lsn(bdb_state_type* bdb_state)
{
    LOG_INFO log_info = get_last_lsn(bdb_state);

    return log_info.offset + log_info.size;
}
