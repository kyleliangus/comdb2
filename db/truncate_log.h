#ifndef TRUNCATE_LOG_H
#define TRUNCATE_LOG_H

#include <phys_rep_lsn.h>
#include <time.h>

int truncate_log(unsigned int file, unsigned int offset);
int truncate_timestamp(time_t timestamp);
LOG_INFO handle_truncation(cdb2_hndl_tp* repl_db, LOG_INFO prev_info);

#endif
