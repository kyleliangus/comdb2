#define _DEFAULT_SOURCE 

#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <signal.h>

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


static int getIPAddr(char* ipv4Addr)
{
    char hostbuffer[256];
    int hostname;
 
    // To retrieve hostname
    hostname = gethostname(hostbuffer, sizeof(hostbuffer));
    if (hostname == -1)
    {
        return 1;
    }
 
    struct addrinfo hints, *res;
    int status;
    char ipstr[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; // AF_INET or AF_INET6 to force version
    hints.ai_socktype = SOCK_STREAM;

    if ((status = getaddrinfo(hostbuffer, NULL, &hints, &res)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(status));
        return 2;
    }

    struct sockaddr_in *ipv4 = (struct sockaddr_in *)res->ai_addr;
    void *addr = &(ipv4->sin_addr);
    inet_ntop(res->ai_family, addr, ipstr, sizeof ipstr);
    printf("  %s: %s\n", hostbuffer, ipstr);

    memcpy(ipv4Addr, ipstr,sizeof ipstr);
    freeaddrinfo(res); // free the linked list

    return 0;
}

static void print_partition_list (FILE *fp,
        const rd_kafka_topic_partition_list_t
        *partitions) {
    int i;
    for (i = 0 ; i < partitions->cnt ; i++) {
        fprintf(stderr, "%s %s [%"PRId32"] offset %"PRId64,
                i > 0 ? ",":"",
                partitions->elems[i].topic,
                partitions->elems[i].partition,
                partitions->elems[i].offset);
    }
    fprintf(stderr, "\n");

}
static void rebalance_cb (rd_kafka_t *rk,
        rd_kafka_resp_err_t err,
        rd_kafka_topic_partition_list_t *partitions,
        void *opaque) {

    fprintf(stderr, "%% Consumer group rebalanced: ");

    switch (err)
    {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            fprintf(stderr, "assigned:\n");
            print_partition_list(stderr, partitions);
            rd_kafka_assign(rk, partitions);
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            fprintf(stderr, "revoked:\n");
            print_partition_list(stderr, partitions);
            rd_kafka_assign(rk, NULL);
            break;

        default:
            fprintf(stderr, "failed: %s\n",
                    rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
    }
}


static char* msg_consume (rd_kafka_message_t *rkmessage) {
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            fprintf(stderr,
                    "%% Consumer reached end of %s [%"PRId32"] "
                    "message queue at offset %"PRId64"\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition, rkmessage->offset);

            return NULL;
        }

        if (rkmessage->rkt)
            fprintf(stderr, "%% Consume error for "
                    "topic \"%s\" [%"PRId32"] "
                    "offset %"PRId64": %s\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition,
                    rkmessage->offset,
                    rd_kafka_message_errstr(rkmessage));
        else
            fprintf(stderr, "%% Consumer error: %s: %s\n",
                    rd_kafka_err2str(rkmessage->err),
                    rd_kafka_message_errstr(rkmessage));

        if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
            fprintf(stderr, "Unknown partition or topic\n");

        return NULL;
    }

    fprintf(stdout, "%% Message (topic %s [%"PRId32"], "
            "offset %"PRId64", %zd bytes):\n",
            rd_kafka_topic_name(rkmessage->rkt),
            rkmessage->partition,
            rkmessage->offset, rkmessage->len);

    printf("Key: %.*s\n",
            (int)rkmessage->key_len, (char *)rkmessage->key);
    printf("%.*s\n",
            (int)rkmessage->len, (char *)rkmessage->payload);

    return rkmessage->payload;
}


struct Rcv_Connection* setup_subscribe(const char* brokers, 
        const char* topic, const char* group)
{
    rd_kafka_conf_t* conf;
    rd_kafka_topic_conf_t* topic_conf;
    rd_kafka_t* rk;
    char tmp[16];
    rd_kafka_resp_err_t err;
    char* sub_group;

    /* Kafka configuration */
    conf = rd_kafka_conf_new();

    /* Set logger */
    rd_kafka_conf_set_log_cb(conf, logger);

    /* Quick termination */
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();

    // assign groups
    /* Consumer groups require a group id */
    if (!group) {
        //group = "rdkafka_consumer_example";
        sub_group = malloc(INET6_ADDRSTRLEN + 1);
        getIPAddr(sub_group);
    }
    else {
        sub_group = malloc(sizeof strlen(group) + 1);
        memcpy(sub_group, group, strlen(group));
    }
    fprintf(stderr, "Group: %s\n", sub_group);

    if (rd_kafka_conf_set(conf, "group.id", sub_group,
                errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);

        free(sub_group);
        return NULL;
    }

    /* Consumer groups always use broker based offset storage */
    if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method",
                "broker",
                errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);

        free(sub_group);
        return NULL;
    }

    /* Set default topic config for pattern-matched topics. */
    rd_kafka_conf_set_default_topic_conf(conf, topic_conf);

    /* Callback called on partition assignment changes */
    rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

    /* Create Kafka handle */
    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
                    errstr, sizeof(errstr)))) {
        fprintf(stderr,
                "%% Failed to create new consumer: %s\n",
                errstr);

        free(sub_group);
        return NULL;
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(rk, brokers) == 0) {
        fprintf(stderr, "%% No valid brokers specified\n");

        free(sub_group);
        return NULL;
    }

    /* Redirect rd_kafka_poll() to consumer_poll() */
    rd_kafka_poll_set_consumer(rk);

    /* TODO: implement offsets if necessary */
    rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(1);

    rd_kafka_topic_partition_list_add(topics, topic, 1);
    fprintf(stderr, "%% Subscribing to %d topics\n", topics->cnt);

    if ((err = rd_kafka_subscribe(rk, topics))) {
        fprintf(stderr,
                "%% Failed to start consuming topics: %s\n",
                rd_kafka_err2str(err));
        exit(1);
    }

    /* create a subscription and return to the user */
    struct Rcv_Connection* retCnct = (struct Rcv_Connection *) (malloc(sizeof(struct Rcv_Connection)));

    /* save sub_group, rk */
    retCnct->rk = rk;
    retCnct->group = sub_group;
    retCnct->topics = topics;
    
    return retCnct;
}

struct Message rcv_msg(struct Rcv_Connection* cnct)
{
    rd_kafka_t* rk = cnct->rk;

    rd_kafka_message_t *rkmessage;

    struct Message msg;
    msg.buff = NULL;
    msg.len = 0;
    
    rkmessage = rd_kafka_consumer_poll(rk, 1000);
    if (rkmessage) {
        char* msg_buff = msg_consume(rkmessage);
        if (msg_buff) {
            msg.len = rkmessage->len;
            msg.buff = malloc(msg.len);

            memcpy(msg.buff, msg_buff, msg.len);
        }
        rd_kafka_message_destroy(rkmessage);
    }

    return msg;
}

/* Acts as a destructor for Rcv_Connection */
void close_subscribe(struct Rcv_Connection* cnct)
{
    rd_kafka_t* rk = cnct->rk;
    rd_kafka_resp_err_t err;
    rd_kafka_topic_partition_list_t *topics = cnct->topics;

    err = rd_kafka_consumer_close(rk);
    if (err)
        fprintf(stderr, "%% Failed to close consumer: %s\n",
                rd_kafka_err2str(err));
    else
        fprintf(stderr, "%% Consumer closed\n");

    rd_kafka_topic_partition_list_destroy(topics);

    /* Destroy handle */
    rd_kafka_destroy(rk);

    /* Let background threads clean up and terminate cleanly. */
    int run = 5;
    while (run-- > 0 && rd_kafka_wait_destroyed(1000) == -1)
        printf("Waiting for librdkafka to decommission\n");
    if (run <= 0)
        rd_kafka_dump(stdout, rk);

    free(cnct->group);
}

void delete_message(struct Message msg)
{
    free(msg.buff);
}
