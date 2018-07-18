#ifndef REP_RECORD_H
#define REP_RECORD_H

#include "db.h"

int __dbenv_apply_log(DB_ENV* dbenv, int file, int offset, int64_t rectype,
        void* blob, int blob_len);

#endif
