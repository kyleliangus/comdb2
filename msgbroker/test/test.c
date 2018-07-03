// C program to display hostname
// and IP address
#define _DEFAULT_SOURCE 

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>

#include <netinet/in.h>

// Driver code
int getIPAddr(char* ipv4Addr)
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

int main()
{
    char addr[INET6_ADDRSTRLEN];
    getIPAddr(addr);
    printf("Host IP: %s\n", addr);
}
