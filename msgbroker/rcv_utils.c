#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <sys/time.h>

#include "rcv_utils.h"

static char errstr[512];
/**
 * Kafka logger callback (optional)
 */
static void logger (const rd_kafka_t *rk, int level,
        const char *fac, const char *buf) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stdout, "%d.%03d RDKAFKA-%i-%s: %s: %s\n",
            (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
            level, fac, rd_kafka_name(rk), buf);
}


struct Rcv_Connection* setup_subscribe(const char* brokers, const char* topic)
{
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    const char *debug = NULL;
    int do_conf_dump = 0;
    char tmp[16];
    rd_kafka_resp_err_t err;
    char *group = NULL;
    rd_kafka_topic_partition_list_t *topics;
    int is_subscription;
    int i;

    /* Kafka configuration */
    conf = rd_kafka_conf_new();

    /* Set logger */
    rd_kafka_conf_set_log_cb(conf, logger);

    /* Quick termination */
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();


    /* create a subscription and return to the user */
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
