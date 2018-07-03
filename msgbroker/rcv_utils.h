#include <librdkafka/rdkafka.h>

struct Rcv_Connection {
    // privates: this can change
    // change this for underlying framework
    rd_kafka_t* rk;         /* Producer instance handle */ 
    rd_kafka_topic_partition_list_t *topics;
    char* group;

} Rcv_Connection;


/* Acts as a constructor for Rcv_Connection */
struct Rcv_Connection* setup_subscribe(const char* brokers, 
        rd_kafka_topic_partition_list_t* topic, const char* group);

/* Is a nonblocking receive */
size_t rcv_msg(struct Rcv_Connection* cnct, char* buff, size_t buff_len);

/* Acts as a destructor for Rcv_Connection */
void close_subscribe(struct Rcv_Connection* cnct);
