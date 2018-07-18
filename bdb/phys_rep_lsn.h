#ifndef PHYS_REP_LSN_H
#define PHYS_REP_LSN_H
#include "bdb_int.h"

typedef struct LOG_INFO LOG_INFO;
struct LOG_INFO 
{
    u_int32_t file;
    u_int32_t offset;
    u_int32_t size;
};

LOG_INFO get_last_lsn(bdb_state_type* bdb_state);
u_int32_t get_next_lsn(bdb_state_type* bdb_state);

int apply_log(DB_ENV* dbenv, int file, int offset, int64_t rectype, 
        void* blob, int blob_len);

#endif
