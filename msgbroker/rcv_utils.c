#include <librdkafka/rdkafka.h>
#include <stdlib.h>

#include "rcv_utils.h"


struct Rcv_Connection* setup_subscribe(const char* brokers, const char* topic)
{

    struct Rcv_Connection* retCnct = (struct Rcv_Connection *) (malloc(sizeof(struct Rcv_Connection)));

    return retCnct;
}

void rcv_msg(struct Rcv_Connection* cnct, char* buff, size_t buff_len)
{

}

/* Acts as a destructor for Rcv_Connection */
void close_subscribe(struct Rcv_Connection* cnct)
{

}
