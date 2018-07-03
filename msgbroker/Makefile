CC = gcc
CFLAGS = -lrdkafka -lz -lpthread -lrt -g

TEST_MAINS ?= consumer producer

all: $(TEST_MAINS)

consumer: consumer.c rcv_utils.c
	$(CC) consumer.c rcv_utils.c $(CFLAGS) -o $@

producer: producer.c send_utils.c
	$(CC) producer.c send_utils.c $(CFLAGS) -o $@

clean: 
	rm -f $(TEST_MAINS)
