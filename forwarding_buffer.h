#ifndef __FORWARDING_BUFFER_H__
#define __FORWARDING_BUFFER_H__

typedef struct forwarding_buf{
    char *buffer;
    int capacity;
    int current;
} forwarding_buf;

#define FORWARDING_BUF_SIZE (1 << 1)

int forwarding_buf_init();
int buffer_or_send(void *buf, int len, int src, int tag);
int send_current_buffer();
int free_forwarding_buffer();

extern forwarding_buf *forwarding_buffer;
extern int forwarding_count, buf_forwarding_count;

#endif