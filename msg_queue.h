#ifndef __MSG_QUEUE_H__
#define __MSG_QUEUE_H__

typedef struct msg_packet{
    int src;
    int tag;
    int length;
    int data; //index into msg buffer
} msg_packet;

typedef struct msg_queue{
    msg_packet *buffer;
    msg_packet *buffer_end;
    int capacity;
    int count;
    msg_packet *head;
    msg_packet *tail;
} msg_queue;

typedef struct msg_buf{
    char *buffer;
    int capacity;
    int count;
    int head;
    int tail;
} msg_buf;

#define MSG_QUEUE_INIT_CAP 102400//msg queue init capacity
#define MSG_DATA_BUF_SIZE (1 << 30)// + (1 << 29) + (1 << 27)
#define MB_LEAP_THRESHOLD 0.995

int mq_init();
int mq_free();
int mq_push(int src, int tag, int length);
int mq_pop(int *src, int *tag, int *length, void *data);
int mb_request(int len, int *first_seg_len);
int mb_read(void *buf);
int mb_reach_leap_threshold(double *utilization);

extern msg_buf *shared_mb;
extern msg_queue *shared_mq;

#endif