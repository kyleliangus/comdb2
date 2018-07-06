#include <librdkafka/rdkafka.h>

struct Rcv_Connection {
    // privates: this can change
    // change this for underlying framework
    rd_kafka_t* rk;         /* Producer instance handle */ 
    rd_kafka_topic_partition_list_t *topics;
    char* group;

} Rcv_Connection;

struct Message {
    char* buff;
    size_t len;
} Message;


/* Acts as a constructor for Rcv_Connection */
struct Rcv_Connection* setup_subscribe(const char* brokers, 
        const char* topic, const char* group);

/* Is a nonblocking receive */
struct Message rcv_msg(struct Rcv_Connection* cnct, int timeout);

/* Acts as a destructor for Rcv_Connection */
void close_subscribe(struct Rcv_Connection* cnct);

void delete_message(struct Message msg); 
