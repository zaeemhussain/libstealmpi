#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <mpi.h>
#include <pthread.h>
#include "mpi_override.h"
#include "forwarding_buffer.h"
#include "socket.h"

//#define DEBUG
/*global variables*/
forwarding_buf *forwarding_buffer = NULL;
int buf_forwarding_count = 0, forwarding_count = 0;


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
        //printf("[%d] Sending current buffer.\n", actual_rank);
        send_current_buffer();
        
    }
    if(len + 3*sizeof(int) >= forwarding_buffer->capacity){ //Message won't fit in buffer
        //printf("[%d] Message won't fit, forwarding.\n", actual_rank);
        msgLen[0] = len + 3*sizeof(int);
        socket_send(msgLen, sizeof(int));
        socket_send(header, 3*sizeof(int));
        socket_send(buf, len);
        forwarding_count++;
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
        buf_forwarding_count++;
#ifdef DEBUG
    printf("[%d] Sending current buffer. Size is %d\n", 
            actual_rank, forwarding_buffer->current);
    fflush(stdout);
#endif
/*        struct timeval time_t1, time_t2;
        gettimeofday(&time_t1, 0x0);
        struct rusage usage1, usage2;
        getrusage(RUSAGE_SELF, &usage1);*/
        int msgLen[1];
        msgLen[0] = forwarding_buffer->current;
        socket_send(msgLen, sizeof(int));
        socket_send(forwarding_buffer->buffer, forwarding_buffer->current);
        forwarding_buffer->current = 0;
/*        getrusage(RUSAGE_SELF, &usage2);
        gettimeofday(&time_t2, 0x0);
        double sec = (usage2.ru_utime.tv_sec - usage1.ru_utime.tv_sec);
        double usec = (usage2.ru_utime.tv_usec - usage1.ru_utime.tv_usec) / 1000000.0;
        double stdiff = sec + usec;
        printf("[%d] Spent %.6f seconds in user mode.\n", actual_rank, stdiff);
        sec = (usage2.ru_stime.tv_sec - usage1.ru_stime.tv_sec);
        usec = (usage2.ru_stime.tv_usec - usage1.ru_stime.tv_usec) / 1000000.0;
        stdiff = sec + usec;
        printf("[%d] Spent %.6f seconds in kernel mode.\n", actual_rank, stdiff);
        printf("[%d] Number of voluntary context switches during send is %ld.\n", actual_rank, usage2.ru_nvcsw - usage1.ru_nvcsw);
        printf("[%d] Number of involuntary context switches during send is %ld.\n", actual_rank, usage2.ru_nivcsw - usage1.ru_nivcsw);
        sec = (time_t2.tv_sec - time_t1.tv_sec);
        usec = (time_t2.tv_usec - time_t1.tv_usec) / 1000000.0;
        stdiff = sec + usec;
        printf("[%d] Send time for %d bytes is %.6f.\n", actual_rank, msgLen[0], stdiff);
        printf("[%d] Send started at %.6f and ended at %.6f.\n", actual_rank, time_t1.tv_sec + time_t1.tv_usec / 1000000.0, time_t2.tv_sec + time_t2.tv_usec / 1000000.0);*/
    }
    return 0;
}

int free_forwarding_buffer(){
    free(forwarding_buffer->buffer);
    free(forwarding_buffer);
}
