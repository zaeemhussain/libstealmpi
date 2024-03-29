#ifndef __SOCKET_H__
#define __SOCKET_H__

#define PORT_NUM 3940
#define BIND_TIMES 10
#define SOCKET_BUF_INIT_SIZE 131072 //128 k
#define USE_TCP
//#define DEBUG
#define USE_RDMA

int get_my_ip(char *ip);
int socket_connect();
int socket_send(void *buf, int len);
int socket_recv();
int csocket_send(int val);
int csocket_recv();

extern int sock_fd, csock_fd; 

#endif