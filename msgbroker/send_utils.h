#include <librdkafka/rdkafka.h>

struct Connection {
    // privates: this can change
    // change this for underlying framework
    rd_kafka_t *rk;         /* Producer instance handle */
    rd_kafka_topic_t *rkt;  /* Topic object */
    char errstr[512];       /* librdkafka API error reporting buffer */

} Connection;


/* Acts as a constructor for Connection */
struct Connection* setup_connection(const char* brokers, const char* topic);

void send_msg(struct Connection* cnct, char* buff, size_t buff_len);

void close_connection(struct Connection* cnct);
