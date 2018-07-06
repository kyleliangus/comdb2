/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2018 Magnus Edenhill
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met: 
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer. 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution. 
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#define _DEFAULT_SOURCE 

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <getopt.h>

#include <sys/types.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include <librdkafka/rdkafka.h>  /* for Kafka driver */

#include "rcv_utils.h"

static char addr[256];

static int run = 1;
static rd_kafka_t *rk;
static int exit_eof = 0;
static int wait_eof = 0;  /* number of partitions awaiting EOF */
static int quiet = 0;
static 	enum {
    OUTPUT_HEXDUMP,
    OUTPUT_RAW,
} output = OUTPUT_HEXDUMP;

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

    memcpy(ipv4Addr, ipstr, sizeof ipstr);
    freeaddrinfo(res); // free the linked list

    return 0;
}

static void stop (int sig) {
    if (!run)
        exit(1);
    run = 0;
    fclose(stdin); /* abort fgets() */
}


static void hexdump (FILE *fp, const char *name, const void *ptr, size_t len) {
    const char *p = (const char *)ptr;
    unsigned int of = 0;


    if (name)
        fprintf(fp, "%s hexdump (%zu bytes):\n", name, len);

    for (of = 0 ; of < len ; of += 16) {
        char hexen[16*3+1];
        char charen[16+1];
        int hof = 0;

        int cof = 0;
        int i;

        for (i = of ; i < (int)of + 16 && i < (int)len ; i++) {
            hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
            cof += sprintf(charen+cof, "%c",
                    isprint((int)p[i]) ? p[i] : '.');
        }
        fprintf(fp, "%08x: %-48s %-16s\n",
                of, hexen, charen);
    }
}

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



/**
 * Handle and print a consumed message.
 * Internally crafted messages are also used to propagate state from
 * librdkafka to the application. The application needs to check
 * the `rkmessage->err` field for this purpose.
 */
static void msg_consume (rd_kafka_message_t *rkmessage) {
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            fprintf(stderr,
                    "%% Consumer reached end of %s [%"PRId32"] "
                    "message queue at offset %"PRId64"\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition, rkmessage->offset);

            if (exit_eof && --wait_eof == 0) {
                fprintf(stderr,
                        "%% All partition(s) reached EOF: "
                        "exiting\n");
                run = 0;
            }

            return;
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
            run = 0;
        return;
    }

    if (!quiet)
        fprintf(stdout, "%% Message (topic %s [%"PRId32"], "
                "offset %"PRId64", %zd bytes):\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset, rkmessage->len);

    if (rkmessage->key_len) {
        if (output == OUTPUT_HEXDUMP)
            hexdump(stdout, "Message Key",
                    rkmessage->key, rkmessage->key_len);
        else
            printf("Key: %.*s\n",
                    (int)rkmessage->key_len, (char *)rkmessage->key);
    }

    if (output == OUTPUT_HEXDUMP)
        hexdump(stdout, "Message Payload",
                rkmessage->payload, rkmessage->len);
    else
        printf("%.*s\n",
                (int)rkmessage->len, (char *)rkmessage->payload);
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
            wait_eof += partitions->cnt;
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            fprintf(stderr, "revoked:\n");
            print_partition_list(stderr, partitions);
            rd_kafka_assign(rk, NULL);
            wait_eof = 0;
            break;

        default:
            fprintf(stderr, "failed: %s\n",
                    rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
    }
}


static int describe_groups (rd_kafka_t *rk, const char *group) {
    rd_kafka_resp_err_t err;
    const struct rd_kafka_group_list *grplist;
    int i;

    err = rd_kafka_list_groups(rk, group, &grplist, 10000);

    if (err) {
        fprintf(stderr, "%% Failed to acquire group list: %s\n",
                rd_kafka_err2str(err));
        return -1;
    }

    for (i = 0 ; i < grplist->group_cnt ; i++) {
        const struct rd_kafka_group_info *gi = &grplist->groups[i];
        int j;

        printf("Group \"%s\" in state %s on broker %d (%s:%d)\n",
                gi->group, gi->state,
                gi->broker.id, gi->broker.host, gi->broker.port);
        if (gi->err)
            printf(" Error: %s\n", rd_kafka_err2str(gi->err));
        printf(" Protocol type \"%s\", protocol \"%s\", "
                "with %d member(s):\n",
                gi->protocol_type, gi->protocol, gi->member_cnt);

        for (j = 0 ; j < gi->member_cnt ; j++) {
            const struct rd_kafka_group_member_info *mi;
            mi = &gi->members[j];

            printf("  \"%s\", client id \"%s\" on host %s\n",
                    mi->member_id, mi->client_id, mi->client_host);
            printf("    metadata: %d bytes\n",
                    mi->member_metadata_size);
            printf("    assignment: %d bytes\n",
                    mi->member_assignment_size);
        }
        printf("\n");
    }

    if (group && !grplist->group_cnt)
        fprintf(stderr, "%% No matching group (%s)\n", group);

    rd_kafka_group_list_destroy(grplist);

    
    fprintf(stdout, "This is my dump!\n");
    rd_kafka_dump(stdout, rk);
    return 0;
}



static void sig_usr1 (int sig) {
    rd_kafka_dump(stdout, rk);
}

int main (int argc, char **argv) {
    char mode = 'C';
    char *brokers = "localhost:9092";
    int opt;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    char errstr[512];
    const char *debug = NULL;
    int do_conf_dump = 0;
    char tmp[16];
    rd_kafka_resp_err_t err;
    char *group = NULL;
    rd_kafka_topic_partition_list_t *topics;
    int is_subscription;
    int i;

    quiet = !isatty(STDIN_FILENO);

    while ((opt = getopt(argc, argv, "g:b:qd:eX:ADO")) != -1) {
        switch (opt) {
            case 'b':
                brokers = optarg;
                break;
            case 'g':
                group = optarg;
                break;
            case 'e':
                exit_eof = 1;
                break;
            case 'd':
                debug = optarg;
                break;
            case 'q':
                quiet = 1;
                break;
            case 'A':
                output = OUTPUT_RAW;
                break;
            case 'X':
                {
                    char *name, *val;
                    rd_kafka_conf_res_t res;

                    if (!strcmp(optarg, "list") ||
                            !strcmp(optarg, "help")) {
                        rd_kafka_conf_properties_show(stdout);
                        exit(0);
                    }

                    if (!strcmp(optarg, "dump")) {
                        do_conf_dump = 1;
                        continue;
                    }

                    name = optarg;
                    if (!(val = strchr(name, '='))) {
                        fprintf(stderr, "%% Expected "
                                "-X property=value, not %s\n", name);
                        exit(1);
                    }

                    *val = '\0';
                    val++;

                    res = RD_KAFKA_CONF_UNKNOWN;
                    /* Try "topic." prefixed properties on topic
                     * conf first, and then fall through to global if
                     * it didnt match a topic configuration property. */
                    if (!strncmp(name, "topic.", strlen("topic.")))
                        res = rd_kafka_topic_conf_set(topic_conf,
                                name+
                                strlen("topic."),
                                val,
                                errstr,
                                sizeof(errstr));

                    if (res == RD_KAFKA_CONF_UNKNOWN)
                        res = rd_kafka_conf_set(conf, name, val,
                                errstr, sizeof(errstr));

                    if (res != RD_KAFKA_CONF_OK) {
                        fprintf(stderr, "%% %s\n", errstr);
                        exit(1);
                    }
                }
                break;

            case 'D':
            case 'O':
                mode = opt;
                break;

            default:
                goto usage;
        }
    }


    if (do_conf_dump) {
        const char **arr;
        size_t cnt;
        int pass;

        for (pass = 0 ; pass < 2 ; pass++) {
            if (pass == 0) {
                arr = rd_kafka_conf_dump(conf, &cnt);
                printf("# Global config\n");
            } else {
                printf("# Topic config\n");
                arr = rd_kafka_topic_conf_dump(topic_conf,
                        &cnt);
            }

            for (i = 0 ; i < (int)cnt ; i += 2)
                printf("%s = %s\n",
                        arr[i], arr[i+1]);

            printf("\n");

            rd_kafka_conf_dump_free(arr, cnt);
        }

        exit(0);
    }


    if (strchr("OC", mode) && optind == argc) {
usage:
        fprintf(stderr,
                "Usage: %s [options] <topic[:part]> <topic[:part]>..\n"
                "\n"
                "librdkafka version %s (0x%08x)\n"
                "\n"
                " Options:\n"
                "  -g <group>      Consumer group (%s)\n"
                "  -b <brokers>    Broker address (%s)\n"
                "  -e              Exit consumer when last message\n"
                "                  in partition has been received.\n"
                "  -D              Describe group.\n"
                "  -O              Get commmitted offset(s)\n"
                "  -d [facs..]     Enable debugging contexts:\n"
                "                  %s\n"
                "  -q              Be quiet\n"
                "  -A              Raw payload output (consumer)\n"
                "  -X <prop=name> Set arbitrary librdkafka "
                "configuration property\n"
                "               Properties prefixed with \"topic.\" "
                "will be set on topic object.\n"
                "               Use '-X list' to see the full list\n"
                "               of supported properties.\n"
                "\n"
                "For balanced consumer groups use the 'topic1 topic2..'"
                " format\n"
                "and for static assignment use "
                "'topic1:part1 topic1:part2 topic2:part1..'\n"
                "\n",
            argv[0],
            rd_kafka_version_str(), rd_kafka_version(),
            group, brokers,
            RD_KAFKA_DEBUG_CONTEXTS);
            exit(1);
    }


    signal(SIGINT, stop);
    signal(SIGUSR1, sig_usr1);

    if (debug &&
            rd_kafka_conf_set(conf, "debug", debug, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% Debug configuration failed: %s: %s\n",
                errstr, debug);
        exit(1);
    }

    /*
     * Client/Consumer group
     */

    topics = rd_kafka_topic_partition_list_new(argc - optind);
    is_subscription = 1;
    char* topic = argv[optind];
    for (i = optind ; i < argc ; i++) {
        /* Parse "topic[:part] */
        char *topic = argv[i];
        char *t;
        int32_t partition = -1;

        if ((t = strstr(topic, ":"))) {
            *t = '\0';
            partition = atoi(t+1);
            is_subscription = 0; /* is assignment */
            wait_eof++;
        }

        rd_kafka_topic_partition_list_add(topics, topic, partition);
    }


    struct Rcv_Connection* sscribe = setup_subscribe(brokers, topic, group);

    while (run) {
        struct Message msg = rcv_msg(sscribe);
        delete_message(msg);
    }

    close_subscribe(sscribe);

    return 0;
}
