#include <librdkafka/rdkafka.h>

struct Send_Connection {
    // privates: this can change
    // change this for underlying framework
    rd_kafka_t *rk;         /* Producer instance handle */
    rd_kafka_topic_t *rkt;  /* Topic object */

} Send_Connection;


/* Acts as a constructor for Send_Connection */
struct Send_Connection* setup_connection(const char* brokers, const char* topic);

/* Is a blocking send, TODO: Does not attempt to resend if failed to contact server */
void send_msg(struct Send_Connection* cnct, char* buff, size_t buff_len);

/* Acts as a destructor for Send_Connection */
void close_connection(struct Send_Connection* cnct);
