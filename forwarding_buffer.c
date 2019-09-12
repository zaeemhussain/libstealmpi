#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <pthread.h>
#include "mpi_override.h"
#include "forwarding_buffer.h"
#include "socket.h"

//#define DEBUG
/*global variables*/
forwarding_buf *forwarding_buffer = NULL;



int forwarding_buf_init(){
    forwarding_buffer = (forwarding_buf *)malloc(sizeof(forwarding_buf));
    forwarding_buffer->capacity = FORWARDING_BUF_SIZE;
    forwarding_buffer->current = 0;
    forwarding_buffer->buffer = (char *)malloc(FORWARDING_BUF_SIZE);
    if(forwarding_buffer->buffer == NULL){
        printf("Error in malloc for message forwarding buffer!\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
#ifdef DEBUG
    printf("[%d] Initialized message forwarding buffer, buffer capacity is %d\n", 
            actual_rank, forwarding_buffer->capacity);
    fflush(stdout);
#endif

    return 0;
} 

int buffer_or_send(void *buf, int len, int src, int tag){
    int header[3];
    header[0] = len;
    header[1] = src;
    header[2] = tag;
    int msgLen[1];
    if(len + 3*sizeof(int) + forwarding_buffer->current >= forwarding_buffer->capacity){
        send_current_buffer();
    }
    if(len + 3*sizeof(int) >= forwarding_buffer->capacity){ //Message won't fit in buffer
        msgLen[0] = len + 3*sizeof(int);
        socket_send(msgLen, sizeof(int));
        socket_send(header, 3*sizeof(int));
        socket_send(buf, len);
    }
    else{
        memcpy((char *)(forwarding_buffer->buffer+forwarding_buffer->current), (char *)header, 3*sizeof(int));
        forwarding_buffer->current += 3*sizeof(int);
        memcpy((char *)(forwarding_buffer->buffer+forwarding_buffer->current), buf, len);
        forwarding_buffer->current += len;
    }

    return 0;
}

int send_current_buffer(){
    if(forwarding_buffer->current > 0){
//#ifdef DEBUG
    printf("[%d] Sending current buffer. Size is %d\n", 
            actual_rank, forwarding_buffer->current);
    fflush(stdout);
//#endif
        int msgLen[1];
        msgLen[0] = forwarding_buffer->current;
//        printf("[%d] Sending length. Byte 1: %d, Byte 2: %d, Byte 3: %d, Byte 4: %d\n", 
//                actual_rank, (*msgLen) & 0xFF, (*(msgLen+1)) & 0xFF, (*(msgLen+2)) & 0xFF, (*(msgLen+3)) & 0xFF);
        socket_send(msgLen, sizeof(int));
        socket_send(forwarding_buffer->buffer, forwarding_buffer->current);
        forwarding_buffer->current = 0;
    }
    return 0;
}

int free_forwarding_buffer(){
    free(forwarding_buffer->buffer);
    free(forwarding_buffer);
}
