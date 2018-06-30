CC = gcc
CFLAGS = -lrdkafka -lz -lpthread -lrt

TEST_MAINS ?= consumer producer

all: $(TEST_MAINS)

consumer: consumer.c 
	$(CC) consumer.c $(CFLAGS) -o $@

producer: producer.c
	$(CC) producer.c $(CFLAGS) -o $@

clean: 
	rm -f $(TEST_MAINS)
