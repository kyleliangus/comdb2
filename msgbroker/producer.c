/**
 * Simple Apache Kafka producer
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include <librdkafka/rdkafka.h>

#include "send_utils.h"

static int run = 1;

/**
 * @brief Signal termination of program
 */
static void stop (int sig) {
    run = 0;
    fclose(stdin); /* abort fgets() */
}



int main (int argc, char **argv) {
    char buf[512];          /* Message value temporary buffer */
    const char *brokers;    /* Argument: broker list */
    const char *topic;      /* Argument: topic to produce to */

    /*
     * Argument validation
     */
    if (argc != 3) {
        fprintf(stderr, "%% Usage: %s <broker> <topic>\n", argv[0]);
        return 1;
    }

    brokers = argv[1];
    topic   = argv[2];

    struct Send_Connection* cnct = setup_connection(brokers, topic);

    /* Signal handler for clean shutdown */
    signal(SIGINT, stop);

    fprintf(stderr,
            "%% Type some text and hit enter to produce message\n"
            "%% Or just hit enter to only serve delivery reports\n"
            "%% Press Ctrl-C or Ctrl-D to exit\n");

    while (run && fgets(buf, sizeof(buf), stdin)) {
        size_t len = strlen(buf);

        if (buf[len-1] == '\n') /* Remove newline */
            buf[--len] = '\0';

        if (len == 0) {
            /* Empty line: only serve delivery reports */
            //rd_kafka_poll(rk, 0/*non-blocking */);
            continue;
        }

        send_msg(cnct, buf, len);
    }

    close_connection(cnct);

    return 0;
}
