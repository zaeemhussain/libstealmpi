#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#ifdef USE_RDMA
#include <rdma/rsocket.h>
#endif
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <net/if.h>
#include <errno.h>
#include <ifaddrs.h>
#include <pthread.h>
#include <sched.h>
#include "mpi.h"
#include "socket.h"
#include "mpi_override.h"
#include "msg_queue.h"
#include "leap.h"

/*global variables*/
int sock_fd;

#ifndef USE_TCP
struct sockaddr_in my_addr, re_addr;
#endif

/*retrieve local ip address*/
int get_my_ip(char *ip){
    struct ifaddrs *myaddrs = NULL, *ifa = NULL;
    void *in_addr = NULL;
    struct sockaddr_in *s4 = NULL; 
    if(getifaddrs(&myaddrs) != 0)
    {
        perror("getifaddrs");
        exit(1);
    }

    for (ifa = myaddrs; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (ifa->ifa_addr == NULL)
            continue;
        if (!(ifa->ifa_flags & IFF_UP))
            continue;
        if(ifa->ifa_addr->sa_family != AF_INET)
            continue;

        s4 = (struct sockaddr_in *)ifa->ifa_addr;
        in_addr = &s4->sin_addr;

        if (!inet_ntop(ifa->ifa_addr->sa_family, in_addr, ip, INET_ADDRSTRLEN))
        {
            printf("%s: inet_ntop failed!\n", ifa->ifa_name);
        }
        else if(!strncmp(ip, "10.", 3))
        {
#ifdef DEBUG
            printf("[%d] Found IP starting with 10.: %s\n", actual_rank, ip);
            fflush(stdout);
#endif
#ifdef USE_RDMA
            if(!strncmp(ifa->ifa_name, "ib", 2)){
                break;
            }
            else{
                continue;
            }
#else
            break;
#endif
        }
    }
#ifdef USE_RDMA
    if(strncmp(ifa->ifa_name, "ib", 2)){
        printf("[%d] Did not find ib0 interface!\n", actual_rank);
        fflush(stdout);
    }
#endif
    if(strncmp(ip, "10.", 3)){
        printf("[%d] Error in finding correct IP!\n", actual_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    //printf("[%d] Interface name: %s\n", actual_rank, ifa->ifa_name);
    //fflush(stdout);

    freeifaddrs(myaddrs);
    return 0;
}

#ifdef USE_TCP
int socket_connect(){
    int temp_fd, ctemp_fd;
    int new_fd, cnew_fd;
    int status, cstatus;
    int sin_size, csin_size;
    char ipstr[INET_ADDRSTRLEN];
    struct sockaddr_in my_addr, re_addr;
    int port_inc, port_num;
    int sock_buf_len;

#ifdef USE_RDMA
    if((temp_fd = rsocket(PF_INET, SOCK_STREAM, 0)) == -1){
#else
    if((temp_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1){
#endif
        printf("[%d] Error in socket()!\n", actual_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
#ifdef USE_RDMA
    if((ctemp_fd = rsocket(PF_INET, SOCK_STREAM, 0)) == -1){
#else
    if((ctemp_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1){
#endif
        printf("[%d] Error in socket()!\n", actual_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    if(shStart <= actual_rank && actual_rank <= shEnd){

        if(get_my_ip(ipstr) != 0){
            printf("[%d] Error in finding correct IP!\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }

        my_addr.sin_family = AF_INET;
        my_addr.sin_addr.s_addr = inet_addr(ipstr);
        memset(my_addr.sin_zero, '\0', sizeof my_addr.sin_zero);
        my_addr.sin_port = 0;
#ifdef USE_RDMA
        if(rbind(temp_fd, (struct sockaddr *)&my_addr, sizeof my_addr) == -1){
#else
        if(bind(temp_fd, (struct sockaddr *)&my_addr, sizeof my_addr) == -1){
#endif
#ifdef USE_RDMA
            rclose(temp_fd);
#else
            close(temp_fd);
#endif
            printf("[%d] Error in bind(), giving up and aborting!\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        int sa_len=sizeof(my_addr);
#ifdef USE_RDMA
        if(rgetsockname(temp_fd, &my_addr, &sa_len) == -1) {
#else
        if(getsockname(temp_fd, &my_addr, &sa_len) == -1) {
#endif
            printf("[%d] getsockname() failed!\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        port_num = my_addr.sin_port;
        my_addr.sin_port = 0;
#ifdef USE_RDMA
        if(rlisten(temp_fd, 5) == -1){
#else
        if(listen(temp_fd, 5) == -1){
#endif
            printf("[%d] Error in listen()!\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }

        sin_size = sizeof re_addr;
        /*send my ip and port to shadow*/
        PMPI_Send(ipstr, strlen(ipstr) + 1, MPI_CHAR, actual_rank + (appSize - shStart), SHADOW_IP_TAG, ls_cntr_world_comm);
        PMPI_Send(&port_num, 1, MPI_INT, actual_rank + (appSize - shStart), SHADOW_IP_TAG, ls_cntr_world_comm);
#ifdef DEBUG
            printf("[%d] Here after sending comm info to my shadow. Port number %d, address %s\n", actual_rank, port_num, ipstr);
            fflush(stdout);
#endif
#ifdef USE_RDMA
        sock_fd = raccept(temp_fd, (struct sockaddr *)&re_addr, &sin_size);
#else
        sock_fd = accept(temp_fd, (struct sockaddr *)&re_addr, &sin_size);
#endif
        if(new_fd == -1){
            printf("[%d] Error in accept()!\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
//        inet_ntop(AF_INET, &(re_addr.sin_addr), s, sizeof s);
#ifdef DEBUG
        printf("[%d]: got connection from my shadow\n", actual_rank);
        fflush(stdout);
#endif
#ifdef USE_RDMA
        rclose(temp_fd);
#else
        close(temp_fd);
#endif
        /*main only do send(), so make socket send buffer as large as possible*/
        sock_buf_len = 2 << 22;
#ifdef USE_RDMA
        if(rsetsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &sock_buf_len, sizeof(int)) == -1){
#else
        if(setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &sock_buf_len, sizeof(int)) == -1){
#endif
            printf("[%d] setsockopt: %s\n", actual_rank, strerror(errno));
        }
        sock_buf_len = 2 << 10;
#ifdef USE_RDMA
        if(rsetsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &sock_buf_len, sizeof(int)) == -1){
#else
        if(setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &sock_buf_len, sizeof(int)) == -1){
#endif
            printf("[%d] setsockopt: %s\n", actual_rank, strerror(errno));
        }
//        inet_ntop(AF_INET, &(re_addr.sin_addr), s, sizeof s);

    }
    else if(actual_rank>=appSize){
        /*receive ip and port of main*/
        PMPI_Recv(ipstr, INET_ADDRSTRLEN, MPI_CHAR, actual_rank-(appSize-shStart), SHADOW_IP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
        PMPI_Recv(&port_num, 1, MPI_INT, actual_rank-(appSize-shStart), SHADOW_IP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
        re_addr.sin_port = (short)port_num;
#ifdef DEBUG
        printf("[%d] Received port number %d and address %s from my main.\n", actual_rank, re_addr.sin_port, ipstr);
        fflush(stdout);
#endif
        sock_fd = temp_fd;
        re_addr.sin_family = AF_INET;
        
        re_addr.sin_addr.s_addr = inet_addr(ipstr);
        memset(my_addr.sin_zero, '\0', sizeof my_addr.sin_zero);
#ifdef USE_RDMA
        if(rconnect(sock_fd, (struct sockaddr *)&re_addr, sizeof re_addr) == -1){
#else
        if(connect(sock_fd, (struct sockaddr *)&re_addr, sizeof re_addr) == -1){
#endif
#ifdef USE_RDMA
            rclose(sock_fd);
#else
            close(sock_fd);
#endif
            //printf("%d\n",actual_rank);
            char rankstr[100];
            sprintf(rankstr, "%d", actual_rank);
            strcat(rankstr," client: connect");
            //fflush(stdout);
            perror(rankstr);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
/*#ifdef DEBUG
        printf("[%d] connected to main\n", actual_rank);
        fflush(stdout);
#endif*/
        /*shadow only do recv(), so make socket recv buffer as large as possible*/
        sock_buf_len = 2 << 10;
#ifdef USE_RDMA
        if(rsetsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &sock_buf_len, sizeof(int)) == -1){
#else
        if(setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &sock_buf_len, sizeof(int)) == -1){
#endif
            printf("[%d] setsockopt: %s\n", actual_rank, strerror(errno));
        }
        sock_buf_len = 2 << 22;
#ifdef USE_RDMA
        if(rsetsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &sock_buf_len, sizeof(int)) == -1){
#else
        if(setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &sock_buf_len, sizeof(int)) == -1){
#endif
            printf("[%d] setsockopt: %s\n", actual_rank, strerror(errno));
        }
    }

    return 0;
}

#else

int socket_connect(){
    int temp_fd;
    int new_fd;
    int status;
    int sin_size;
    char my_ipstr[INET_ADDRSTRLEN], re_ipstr[INET_ADDRSTRLEN];
    int port_inc, my_port_num, re_port_num;


    if((temp_fd = socket(PF_INET, SOCK_DGRAM, 0)) == -1){
        printf("[%d] Error in socket()!\n", actual_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    if(get_my_ip(my_ipstr) != 0){
        printf("[%d] Error in finding correct IP!\n", ls_world_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = inet_addr(my_ipstr);
    memset(my_addr.sin_zero, '\0', sizeof my_addr.sin_zero);
    for(port_inc = 0; port_inc < BIND_TIMES; port_inc++){
        my_port_num = PORT_NUM + ls_app_rank + port_inc * 100;
        my_addr.sin_port = htons((short)my_port_num);
        if(bind(temp_fd, (struct sockaddr *)&my_addr, sizeof my_addr) == -1){
            if(port_inc + 1 >= BIND_TIMES){
                close(temp_fd);
                printf("[%d] Error in bind(), giving up and aborting!\n", actual_rank);
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
            else{
                printf("[%d] Error in bind()!\n", actual_rank);
            }
        }
        else{
            break;
        }
    }
    printf("[%d] My ip is %s, port is %d\n", actual_rank, my_ipstr, my_port_num);
    if(2*actual_rank < actual_size){
        /*send my ip and port to shadow*/
        PMPI_Send(my_ipstr, strlen(my_ipstr) + 1, MPI_CHAR, actual_rank + actual_size/2, SHADOW_IP_TAG, ls_cntr_world_comm);
        PMPI_Send(&my_port_num, 1, MPI_INT, actual_rank + actual_size/2, SHADOW_IP_TAG, ls_cntr_world_comm);
        PMPI_Recv(re_ipstr, INET_ADDRSTRLEN, MPI_CHAR, actual_rank + actual_size/2, SHADOW_IP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
        PMPI_Recv(&re_port_num, 1, MPI_INT, actual_rank + actual_size/2, SHADOW_IP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
        printf("[%d] My shadow ip is %s, port is %d\n", actual_rank, re_ipstr, re_port_num);
        sock_fd = temp_fd;
        re_addr.sin_family = AF_INET;
        re_addr.sin_port = htons((short)re_port_num);
        re_addr.sin_addr.s_addr = inet_addr(re_ipstr);
        memset(re_addr.sin_zero, '\0', sizeof re_addr.sin_zero);
    }
    else{
        PMPI_Send(my_ipstr, strlen(my_ipstr) + 1, MPI_CHAR, ls_app_rank , SHADOW_IP_TAG, ls_cntr_world_comm);
        PMPI_Send(&my_port_num, 1, MPI_INT, actual_rank-actual_size/2, SHADOW_IP_TAG, ls_cntr_world_comm);
        PMPI_Recv(re_ipstr, INET_ADDRSTRLEN, MPI_CHAR, actual_rank-actual_size/2, SHADOW_IP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
        PMPI_Recv(&re_port_num, 1, MPI_INT, actual_rank-actual_size/2, SHADOW_IP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
        printf("[%d] My main ip is %s, port is %d\n", actual_rank-actual_size/2, re_ipstr, re_port_num);
        sock_fd = temp_fd;
        re_addr.sin_family = AF_INET;
        re_addr.sin_port = htons((short)re_port_num);
        re_addr.sin_addr.s_addr = inet_addr(re_ipstr);
        memset(re_addr.sin_zero, '\0', sizeof re_addr.sin_zero);
    }
    return 0;
}
#endif


int socket_send(void *buf, int len){
    int sent_bytes = 0, remain_bytes = len;
    int count;

#ifdef DEBUG
    printf("[%d] begin socket_send, len = %d\n", actual_rank, len);
    printf("[%d] Sending length. Byte 1: %d, Byte 2: %d, Byte 3: %d, Byte 4: %d\n", 
                actual_rank, (*(char *)buf) & 0xFF, (*((char *)buf+1)) & 0xFF, (*((char *)buf+2)) & 0xFF, (*((char *)buf+3)) & 0xFF);
    fflush(stdout);
#endif
    
    while(remain_bytes > 0){
#ifdef USE_RDMA
        count = rsend(sock_fd, (char *)buf + sent_bytes, remain_bytes, 0);
#else
        count = send(sock_fd, (char *)buf + sent_bytes, remain_bytes, 0);
#endif
        if(count == -1){
            perror("Error in socket_send()");
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        sent_bytes += count;
        remain_bytes -= count;
#ifdef SDEBUG
        printf("[%d] socket_send, sent %d bytes, %d bytes remaining\n", actual_rank, count, remain_bytes);
        fflush(stdout);
#endif
    }
#ifdef DEBUG
    printf("[%d] socket_send, len = %d\n", actual_rank, len);
    fflush(stdout);
#endif 
    ls_data_msg_count++;  

    return 0;
}



/*shadow receives msg from its main*/
int socket_recv(){
/*    struct timeval time_t1, time_t2;
    gettimeofday(&time_t1, 0x0);
    struct rusage usage1, usage2;
    getrusage(RUSAGE_SELF, &usage1);*/
    int count;
    int msgLen[1];
    int header[3];
    int cur_recv_bytes, recv_bytes, remain_bytes;
    int buf_index, buf_len;
    char *buf = NULL;

    remain_bytes = sizeof(int);
    recv_bytes = 0;
    while(remain_bytes > 0){
#ifdef USE_TCP
#ifdef USE_RDMA
        count = rrecv(sock_fd, (char *)msgLen + recv_bytes, remain_bytes, 0);
#else
        count = recv(sock_fd, (char *)msgLen + recv_bytes, remain_bytes, 0);
#endif
#else
        count = recvfrom(sock_fd, (char *)msgLen + recv_bytes, remain_bytes, 0, NULL, NULL);
#endif
        if(count == -1){
            perror("recvfrom for msg length\n");
            printf("[%d] Error in socket_recv() for msg length!\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        else if(count == 0){
            cterm_flag=1;
#ifdef DEBUG
            printf("[%d] Main has closed connection, now exiting\n", actual_rank);
            fflush(stdout);
#endif            
            pthread_exit(NULL);
            return 0;
        }
        remain_bytes -= count;
        recv_bytes += count;
    }
#ifdef DEBUG
    printf("[%d] socket_recv, got msg length = %d, number of received bytes: %d\n", 
                actual_rank, msgLen[0], recv_bytes);
    printf("[%d] Byte 1: %d, Byte 2: %d, Byte 3: %d, Byte 4: %d\n", 
                actual_rank, (*msgLen) & 0xFF, (*(msgLen+1)) & 0xFF, (*(msgLen+2)) & 0xFF, (*(msgLen+3)) & 0xFF);
    fflush(stdout);
#endif
    
    recv_bytes = 0;
    while(recv_bytes < msgLen[0]){
        remain_bytes = 3 * sizeof(int);
        cur_recv_bytes = 0;
        while(remain_bytes > 0){
#ifdef USE_TCP
#ifdef USE_RDMA
            count = rrecv(sock_fd, (char *)header + cur_recv_bytes, remain_bytes, 0);
#else
            count = recv(sock_fd, (char *)header + cur_recv_bytes, remain_bytes, 0);
#endif
#else
            count = recvfrom(sock_fd, (char *)header + cur_recv_bytes, remain_bytes, 0, NULL, NULL);
#endif
            if(count == -1){
                perror("recvfrom for msg header\n");
                printf("[%d] Error in socket_recv() for msg header!\n", actual_rank);
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
            else if(count == 0){
                cterm_flag=1;
#ifdef DEBUG
                printf("[%d] Main has closed connection, now exiting\n", actual_rank);
                fflush(stdout);
#endif            
                pthread_exit(NULL);
                return 0;
            }
            remain_bytes -= count;
            recv_bytes += count;
            cur_recv_bytes += count;
        }
        buf_index = mb_request(header[0], &buf_len);
        if(buf_index < 0){
            printf("[%d] mb is full! I requested %d bytes\n", actual_rank, buf_len);
            //mq_clear(shared_mq->count);
            //buf_index = mb_request(header[0], &buf_len);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
#ifdef RDEBUG
        else{
            printf("[%d] Requested len = %d, allocated len = %d, buffer index = %d\n", 
                    actual_rank, header[0], buf_len, buf_index);
            fflush(stdout);
        }
#endif
        buf = shared_mb->buffer + buf_index;
        if(buf == NULL){
            printf("[%d] socket_recv, buffer address in mb is NULL\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        remain_bytes = buf_len;
        cur_recv_bytes = 0;
        while(remain_bytes > 0){
#ifdef USE_TCP
#ifdef USE_RDMA
            count = rrecv(sock_fd, buf + cur_recv_bytes, remain_bytes, 0);
#else
            count = recv(sock_fd, buf + cur_recv_bytes, remain_bytes, 0);
#endif
#else
            count = recvfrom(sock_fd, buf + cur_recv_bytes, remain_bytes, 0, NULL, NULL);
#endif
            if(count == -1){
                int error_code = errno;
                printf("%d %d\n", error_code, EFAULT);
                perror("Error in socket_recv() for msg payload");
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
            else if(count == 0){
                cterm_flag=1;
#ifdef DEBUG
                printf("[%d] Main has closed connection, now exiting\n", actual_rank);
                fflush(stdout);
#endif            
                pthread_exit(NULL);
                return 0;
            }
#ifdef RDEBUG
            else{
                printf("[%d] Recv %d bytes, %d bytes remaining\n", actual_rank, count, remain_bytes - count);
                fflush(stdout);
            }
#endif
            remain_bytes -= count;
            recv_bytes += count;
            cur_recv_bytes += count;
        }
        if(buf_len < header[0]){
            buf = shared_mb->buffer;
            remain_bytes = header[0] - buf_len;
            cur_recv_bytes = 0;
            while(remain_bytes > 0){
#ifdef USE_TCP
#ifdef USE_RDMA
                count = rrecv(sock_fd, (char *)buf + cur_recv_bytes, remain_bytes, 0);
#else
                count = recv(sock_fd, (char *)buf + cur_recv_bytes, remain_bytes, 0);
#endif
#else
                count = recvfrom(sock_fd, (char *)buf + cur_recv_bytes, remain_bytes, 0, NULL, NULL);
#endif
                if(count == -1){
                    int error_code = errno;
                    printf("%d %d\n", error_code, EFAULT);
                    perror("Error in socket_recv() for msg payload");
                    MPI_Abort(MPI_COMM_WORLD, -1);
                }
                else if(count == 0){
                    cterm_flag=1;
#ifdef DEBUG
                    printf("[%d] Main has closed connection, now exiting\n", actual_rank);
                    fflush(stdout);
#endif                
                    pthread_exit(NULL);
                    return 0;
                }
#ifdef RDEBUG
                else{
                    printf("[%d] Recv %d bytes, %d bytes remaining\n", actual_rank, count, remain_bytes);
                    fflush(stdout);
                }
#endif
                remain_bytes -= count;
                recv_bytes += count;
                cur_recv_bytes += count;
            }
        }
#ifdef DEBUG
        printf("[%d] socket_recv, src = %d, tag = %d, len = %d\n", actual_rank, header[1], header[2], header[0]);
        fflush(stdout);
#endif
        mq_push(header[1], header[2], header[0]);
    }
    double buf_util;
    if(!need2leap && mb_reach_leap_threshold(&buf_util)){
        need2leap=1;
        MPI_Request request;
        int code = 1;
        printf("[%d] Buffer reached threshold. Notifying my main.\n", actual_rank);
#ifdef DEBUG
        printf("[%d] Buffer reached threshold. Notifying my main.\n", actual_rank);
#endif
#ifdef COORDINATED_LEAPING
        PMPI_Send(&code, 1, MPI_INT, 0, SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm);
#else
        PMPI_Send(&code, 1, MPI_INT, actual_rank-(appSize-shStart), SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm);
#endif      
    }
/*    getrusage(RUSAGE_SELF, &usage2);
    gettimeofday(&time_t2, 0x0);
    double sec = (usage2.ru_utime.tv_sec - usage1.ru_utime.tv_sec);
    double usec = (usage2.ru_utime.tv_usec - usage1.ru_utime.tv_usec) / 1000000.0;
    double rtdiff = sec + usec;
    printf("[%d] Spent %.6f seconds in user mode.\n", actual_rank, rtdiff);
    sec = (usage2.ru_stime.tv_sec - usage1.ru_stime.tv_sec);
    usec = (usage2.ru_stime.tv_usec - usage1.ru_stime.tv_usec) / 1000000.0;
    rtdiff = sec + usec;
    printf("[%d] Spent %.6f seconds in kernel mode.\n", actual_rank, rtdiff);
    printf("[%d] Number of voluntary context switches during receive is %ld.\n", actual_rank, usage2.ru_nvcsw - usage1.ru_nvcsw);
    printf("[%d] Number of involuntary context switches during receive is %ld.\n", actual_rank, usage2.ru_nivcsw - usage1.ru_nivcsw);
    sec = (time_t2.tv_sec - time_t1.tv_sec);
    usec = (time_t2.tv_usec - time_t1.tv_usec) / 1000000.0;
    rtdiff = sec + usec;
    printf("[%d] Receive time for %d bytes is %.6f\n", actual_rank, msgLen[0], rtdiff);
    printf("[%d] Receive started at %.6f and ended at %.6f.\n", actual_rank, time_t1.tv_sec + time_t1.tv_usec / 1000000.0, time_t2.tv_sec + time_t2.tv_usec / 1000000.0);*/
    
    return 0;
}



/*monitor_thread runs this function and listen for incoming msg from main*/
int wait_for_msg(){
    fd_set read_fds;
    fd_set master;
    int ret;
    struct sched_param param;
    param.sched_priority = 2;
    ret=sched_setscheduler(0, SCHED_RR, &param);
    if(ret==-1)
        printf("[%d]: Set scheduler returned error %d!\n", actual_rank, errno);
    FD_ZERO(&master);
    FD_ZERO(&read_fds);
    FD_SET(sock_fd, &master);

    while(1){
        read_fds = master;
#ifdef USE_RDMA
        if(rselect(sock_fd + 1, &read_fds, NULL, NULL, NULL) == -1){
#else
        if(select(sock_fd + 1, &read_fds, NULL, NULL, NULL) == -1){
#endif
            printf("[%d] Error in select()!\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        if(FD_ISSET(sock_fd, &read_fds)){
            socket_recv();
        }
        else{
            printf("[%d] Error: sock_fd NOT in read_fds after select()\n", actual_rank);
            MPI_Abort(MPI_COMM_WORLD, -1);
        } 
    }
    return 0;
}