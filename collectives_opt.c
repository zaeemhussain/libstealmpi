#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sched.h>
#include "mpi_override.h"
#include "request_list.h"
#include "forwarding_buffer.h"
#include <math.h>
//#define SLEEPDEBUG

struct timeval time_1, time_2;
int allred_itercount = 0;

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
    long long length;
    MPI_Aint lb, extent;

#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Bcast\n", actual_rank);
    fflush(stdout);
#endif
    MPI_Request request;
    int bflag=0;
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Ibcast(buffer, count, datatype, root, ls_data_world_comm, &request);
        }
        else{
            PMPI_Ibcast(buffer, count, datatype, root, comm, &request);
        }
        PMPI_Test(&request, &bflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!bflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &bflag, MPI_STATUS_IGNORE);
        }
/*        bcastPrCount += prCount;
        bcastCallCount++;*/
        /*forward result to shadow*/
        if(shStart <= actual_rank && actual_rank <= shEnd){
        MPI_Type_get_extent(datatype, &lb, &extent);
        length = extent * (long long)count;
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(buffer, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, buffer);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Bcast, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Bcast\n", actual_rank);
    fflush(stdout);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
    return MPI_SUCCESS; 
}

int MPI_Reduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
    long long length;
    MPI_Aint lb, extent;
    int rc;
    int comm_rank, comm_size;
    MPI_Request request;
    int rdflag=0;
    MPI_Comm_rank(comm, &comm_rank);
    MPI_Comm_size(comm, &comm_size);
 
#ifdef DEBUG
    printf("[%d] begin Reduce\n", actual_rank);
#endif
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            rc = PMPI_Ireduce(sendbuf, recvbuf, count, datatype, op, root, ls_data_world_comm, &request);
        }
        else{
            rc = PMPI_Ireduce(sendbuf, recvbuf, count, datatype, op, root, comm, &request);
        }
        PMPI_Test(&request, &rdflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!rdflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &rdflag, MPI_STATUS_IGNORE);
        }
/*        reducePrCount += prCount;
        reduceCallCount++;*/
        /*forward result to shadow*/
        if(comm_rank == root){
            if(shStart <= actual_rank && actual_rank <= shEnd){
            MPI_Type_get_extent(datatype, &lb, &extent);
            length = extent * (long long)count;
            ls_data_msg_counter++;
            ls_dat_msg_len+=length;
            buffer_or_send(recvbuf, length, 0, 0); 
            }
        }
    }
    else if(comm_rank == root){
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
    printf("[%d] end Reduce\n", actual_rank);
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/ 
    if(3*actual_rank<2*actual_size)
        return rc;
    return MPI_SUCCESS;
}

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
    long long length;
    MPI_Aint lb, extent;
    int rc;
    MPI_Request request;
    int ardflag=0;

#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Allreduce\n", actual_rank);
#endif
    double sec;
    double usec;
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Iallreduce(sendbuf, recvbuf, count, datatype, op, ls_data_world_comm, &request);
        }
        else{
            PMPI_Iallreduce(sendbuf, recvbuf, count, datatype, op, comm, &request);
        }
        PMPI_Test(&request, &ardflag, MPI_STATUS_IGNORE);
        gettimeofday(&time_1, 0x0);
        if(allred_itercount > 0){
            sec = (time_1.tv_sec -time_2.tv_sec);
            usec = (time_1.tv_usec - time_2.tv_usec) / 1000000.0;
        }

        int prCount = 0;
        while(!ardflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == 0)*/
            //if(allred_itercount > 0 /*&& allred_itercount < 3800 /*&& (sec + usec) < 0.001*/)
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &ardflag, MPI_STATUS_IGNORE);
        }
/*        allreducePrCount += prCount;
        allreduceCallCount++;*/
        /*if(comm == MPI_COMM_WORLD){
            PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, ls_data_world_comm);
        }
        else{
            PMPI_Allreduce(sendbuf, recvbuf, count, datatype, op, comm);
        }*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        MPI_Type_get_extent(datatype, &lb, &extent);
        length = extent * (long long)count;
#ifdef DEBUG
        printf("[%d] Forwarding result to my shadow in allreduce. Length is: %d. Current time : %.3f\n", actual_rank, length, MPI_Wtime());
        fflush(stdout);
#endif
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;
#ifdef DEBUG
        printf("[%d] In allreduce. Going to check what my main left in msg queue. Current time : %.3f\n", actual_rank, MPI_Wtime());
        fflush(stdout);
#endif

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Allreduce, time is %.6f\n", actual, diff);
#else
    printf("[%d] end Allreduce\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
    gettimeofday(&time_2, 0x0);
    allred_itercount++;
    return MPI_SUCCESS;
}

int MPI_Alltoall(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Alltoall\n", actual_rank);
#endif
    MPI_Request request;
    int aflag=0;
    int rc;
    MPI_Aint lb, data_size;
    long long length;
    int size;

    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Ialltoall(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, ls_data_world_comm, &request);
        }
        else{
            PMPI_Ialltoall(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm, &request);
        }
        PMPI_Test(&request, &aflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!aflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &aflag, MPI_STATUS_IGNORE);
        }
/*        alltoallPrCount += prCount;
        alltoallCallCount++;*/
        /*if(comm == MPI_COMM_WORLD){
            PMPI_Alltoall(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, ls_data_world_comm);
        }
        else{
            PMPI_Alltoall(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm);
        }*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        MPI_Type_get_extent(recvtype, &lb, &data_size);
        MPI_Comm_size(comm, &size);
        length = data_size * (long long)recvcount * size;
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
    
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Alltoall, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Alltoall\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}

int MPI_Alltoallv(const void *sendbuf, const int sendcounts[], const int sdispls[], MPI_Datatype sendtype, void *recvbuf, const int recvcounts[], const int rdispls[], MPI_Datatype recvtype, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Alltoallv\n", actual_rank);
#endif

    MPI_Request request;
    int avflag=0;
    int rc;
    MPI_Aint lb, data_size;
    int i, max_i, count;
    int comm_size;
    int length;
    double myt1, myt2;

    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Ialltoallv(sendbuf, sendcounts, sdispls, sendtype, recvbuf, recvcounts, rdispls, recvtype, ls_data_world_comm, &request);
        }
        else{
            PMPI_Ialltoallv(sendbuf, sendcounts, sdispls, sendtype, recvbuf, recvcounts, rdispls, recvtype, comm, &request);
        }
        PMPI_Test(&request, &avflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!avflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &avflag, MPI_STATUS_IGNORE);
        }
/*        alltoallvPrCount += prCount;
        alltoallvCallCount++;*/
        /*if(comm == MPI_COMM_WORLD){
            PMPI_Alltoallv(sendbuf, sendcounts, sdispls, sendtype, recvbuf, recvcounts, rdispls, recvtype, ls_data_world_comm);
        }
        else{
            PMPI_Alltoallv(sendbuf, sendcounts, sdispls, sendtype, recvbuf, recvcounts, rdispls, recvtype, comm);
        }*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        MPI_Type_get_extent(recvtype, &lb, &data_size);
        MPI_Comm_size(comm, &comm_size);
        max_i = comm_size - 1;
        /*assuming no overlap of buffer writing, find the max displacement*/
        for(i = 0; i < comm_size; i++){
            if(rdispls[i] > rdispls[max_i])
                max_i = i;
        }
        count = rdispls[max_i] + recvcounts[max_i];
        length = data_size * (long long)count;
        //printf("[%d] msg length in alltoallv is %d bytes.\n", actual_rank, length);
        myt1 = MPI_Wtime();
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0);
        myt2 = MPI_Wtime();
        //printf("[%d] msg length is %d bytes, took %.2f seconds\n", actual_rank, length, myt2 - myt1); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Alltoallv, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Alltoallv\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    if(actual_rank<appSize)
        return rc;
    return MPI_SUCCESS;
}

int MPI_Barrier(MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
    struct timeval time_1, time_2;
/*    if(actual_rank==(2*actual_size/3)){
        gettimeofday(&time_1, 0x0);
        printf("[%d] begin Barrier\n", actual_rank);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Barrier\n", actual_rank);
#endif

    int rt;
    
    if(comm == MPI_COMM_WORLD){
        rt=PMPI_Barrier(ls_data_world_comm);
    }
    else{
        rt=PMPI_Barrier(comm);
    }
/*    if(actual_rank==0 || actual_rank==appSize){
        gettimeofday(&time_2, 0x0);
        double sec = (time_2.tv_sec - time_1.tv_sec);
        double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
        double diff = sec + usec;
        printf("[%d] end Barrier, time is %.6f\n", actual_rank, diff);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Barrier, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Barrier\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
    return rt;
}

int MPI_Sendrecv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
            int dest, int sendtag, void *recvbuf, int recvcount,
                MPI_Datatype recvtype, int source, int recvtag,
                    MPI_Comm comm, MPI_Status *status){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
    int rt;
    int rflag=0, sflag=0;
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Sendrecv, dest is %d, src is %d\n", actual_rank, dest, source);
#endif
        MPI_Request temp_reqs[2];
        
        rt = MPI_Isend(sendbuf, sendcount, sendtype, dest, sendtag, comm, &temp_reqs[0]);
        if(rt != MPI_SUCCESS){
            return rt;
        }
        rt = MPI_Irecv(recvbuf, recvcount, recvtype, source, recvtag, comm, &temp_reqs[1]);
    
        MPI_Wait(&temp_reqs[0], MPI_STATUS_IGNORE);
        MPI_Wait(&temp_reqs[1], status);
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Sendrecv, time is %.6f\n", ls_world_rank, diff);
#else
    printf("[%d] end Sendrecv\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
    return rt;
}

int MPI_Gather(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
            void *recvbuf, int recvcount, MPI_Datatype recvtype, int root,
                MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Gather\n", actual_rank);
#endif

    int rc = MPI_SUCCESS;//initialized to be success, so that if comm only has one process rc is automatically set to success
    long long length;
    MPI_Aint lb, extent;
    int comm_rank, comm_size;
    MPI_Request request;
    int gaflag=0;
    MPI_Comm_rank(comm, &comm_rank);
    MPI_Comm_size(comm, &comm_size);
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Igather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, ls_data_world_comm, &request);
        }
        else{
            PMPI_Igather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, &request);
        }
        PMPI_Test(&request, &gaflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!gaflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &gaflag, MPI_STATUS_IGNORE);
        }
/*        gatherPrCount += prCount;
        gatherCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        if(comm_rank == root){
            MPI_Type_get_extent(recvtype, &lb, &extent);
            length = extent * (long long)recvcount * comm_size;
            ls_data_msg_counter++;
            ls_dat_msg_len+=length;
            buffer_or_send(recvbuf, length, 0, 0); 
        }
        }
    }
    else if(comm_rank == root){
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Gather, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Gather\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return rc;
}

int MPI_Allgather(const void *sendbuf, int  sendcount,
             MPI_Datatype sendtype, void *recvbuf, int recvcount,
                  MPI_Datatype recvtype, MPI_Comm comm)
{
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Allgather\n", actual_rank);
#endif

    int rc = MPI_SUCCESS;//initialized to be success, so that if comm only has one process rc is automatically set to success
    long long length;
    MPI_Aint lb, extent;
    int comm_size;
    MPI_Request request;
    int agflag=0;

    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Iallgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, ls_data_world_comm, &request);
        }
        else{
            PMPI_Iallgather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, comm, &request);
        }
        PMPI_Test(&request, &agflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!agflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &agflag, MPI_STATUS_IGNORE);
        }
/*        allgatherPrCount += prCount;
        allgatherCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
            MPI_Type_get_extent(recvtype, &lb, &extent);
            MPI_Comm_size(comm, &comm_size);
            length = extent * (long long)recvcount * comm_size;
            ls_data_msg_counter++;
            ls_dat_msg_len+=length;
            buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Allgather, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Allgather\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
    return rc;
}

int MPI_Gatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
            void *recvbuf, const int recvcounts[], const int displs[], MPI_Datatype
            recvtype, int root, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Gatherv\n", actual_rank);
#endif
    int rc;
    MPI_Aint lb, data_size;
    int i, max_i, count;
    int comm_size, comm_rank;
    int length;
    MPI_Request request;
    int gvflag=0;
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Igatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, root, ls_data_world_comm, &request);
        }
        else{
            PMPI_Igatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, root, comm, &request);
        }
        PMPI_Test(&request, &gvflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!gvflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &gvflag, MPI_STATUS_IGNORE);
        }
/*        gathervPrCount += prCount;
        gathervCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        if(comm_rank == root){
            MPI_Type_get_extent(recvtype, &lb, &data_size);
            max_i = comm_size - 1;
            /*assuming no overlap of buffer writing, find the max displacement*/
            for(i = 0; i < comm_size; i++){
                if(displs[i] > displs[max_i])
                    max_i = i;
            }
            count = displs[max_i] + recvcounts[max_i];
            length = data_size * (long long)count;
            ls_data_msg_counter++;
            ls_dat_msg_len+=length;
            buffer_or_send(recvbuf, length, 0, 0); 
        }
        }
    }
    else if(comm_rank == root){
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Gatherv, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Gatherv\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}

int MPI_Allgatherv(const void *sendbuf, int sendcount,
            MPI_Datatype sendtype, void *recvbuf, const int recvcounts[],
                const int displs[], MPI_Datatype recvtype, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Allgatherv\n", actual_rank);
#endif
    int rc;
    MPI_Aint lb, data_size;
    int i, max_i, count;
    int comm_size, comm_rank;
    int length;
    MPI_Request request;
    int agvflag=0;
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Iallgatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, ls_data_world_comm, &request);
        }
        else{
            PMPI_Iallgatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs, recvtype, comm, &request);
        }
        PMPI_Test(&request, &agvflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!agvflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &agvflag, MPI_STATUS_IGNORE);
        }
/*        allgathervPrCount += prCount;
        allgathervCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        MPI_Type_get_extent(recvtype, &lb, &data_size);
        max_i = comm_size - 1;
        /*assuming no overlap of buffer writing, find the max displacement*/
        for(i = 0; i < comm_size; i++){
            if(displs[i] > displs[max_i])
                max_i = i;
        }
        count = displs[max_i] + recvcounts[max_i];
        length = data_size * (long long)count;
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Allgatherv, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Allgatherv\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}

int MPI_Scatter(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                               void *recvbuf, int recvcount, MPI_Datatype recvtype,
                               int root, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Scatter\n", actual_rank);
#endif
    int rc;
    MPI_Aint lb, data_size;
    int comm_size, comm_rank;
    int length;
    MPI_Request request;
    int sflag=0;
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Iscatter(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, ls_data_world_comm, &request);
        }
        else{
            PMPI_Iscatter(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, root, comm, &request);
        }
        PMPI_Test(&request, &sflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!sflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &sflag, MPI_STATUS_IGNORE);
        }
/*        scatterPrCount += prCount;
        scatterCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        MPI_Type_get_extent(recvtype, &lb, &data_size);
        length = data_size * (long long)recvcount;
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Scatter, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Scatter\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}

int MPI_Scatterv(const void *sendbuf, const int sendcounts[], const int displs[],
                                MPI_Datatype sendtype, void *recvbuf, int recvcount,
                                MPI_Datatype recvtype, int root, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Scatterv\n", actual_rank);
#endif
    int rc;
    MPI_Aint lb, data_size;
    int comm_size, comm_rank;
    int length;
    MPI_Request request;
    int svflag=0;
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Iscatterv(sendbuf, sendcounts, displs, sendtype, recvbuf, recvcount, recvtype, root, ls_data_world_comm, &request);
        }
        else{
            PMPI_Iscatterv(sendbuf, sendcounts, displs, sendtype, recvbuf, recvcount, recvtype, root, comm, &request);
        }
        PMPI_Test(&request, &svflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!svflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &svflag, MPI_STATUS_IGNORE);
        }
/*        scattervPrCount += prCount;
        scattervCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){/*forward result to shadow*/
        MPI_Type_get_extent(recvtype, &lb, &data_size);
        length = data_size * (long long)recvcount;
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Scatterv, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Scatterv\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}

int MPI_Scan(const void *sendbuf, void *recvbuf, int count,
                            MPI_Datatype datatype, MPI_Op op, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Scan\n", actual_rank);
#endif
    int rc;
    MPI_Aint lb, data_size;
    int length;
    MPI_Request request;
    int scflag=0;
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Iscan(sendbuf, recvbuf, count, datatype, op, ls_data_world_comm, &request);
        }
        else{
            PMPI_Iscan(sendbuf, recvbuf, count, datatype, op, comm, &request);
        }
        PMPI_Test(&request, &scflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!scflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &scflag, MPI_STATUS_IGNORE);
        }
/*        scanPrCount += prCount;
        scanCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){
        /*forward result to shadow*/
        MPI_Type_get_extent(datatype, &lb, &data_size);
        length = data_size * (long long)count;
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Scan, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Scan\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}

int MPI_Reduce_scatter(const void *sendbuf, void *recvbuf, const int recvcounts[],
                                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Reduce_scatter\n", actual_rank);
#endif
    int rc;
    MPI_Aint lb, data_size;
    int length;
    int comm_rank;
    MPI_Request request;
    int rsflag=0;
    MPI_Comm_rank(comm, &comm_rank);
 
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Ireduce_scatter(sendbuf, recvbuf, recvcounts, datatype, op, ls_data_world_comm, &request);
        }
        else{
            PMPI_Ireduce_scatter(sendbuf, recvbuf, recvcounts, datatype, op, comm, &request);
        }
        PMPI_Test(&request, &rsflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!rsflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &rsflag, MPI_STATUS_IGNORE);
        }
/*        reducescatterPrCount += prCount;
        reducescatterCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){
        /*forward result to shadow*/
        MPI_Type_get_extent(datatype, &lb, &data_size);
        length = data_size * (long long)recvcounts[comm_rank];
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Reduce_scatter, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Reduce_scatter\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}

int MPI_Reduce_scatter_block(const void *sendbuf, void *recvbuf, int recvcount,
                                             MPI_Datatype datatype, MPI_Op op, MPI_Comm comm){
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }*/
#ifdef DEBUG
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Reduce_scatter_block\n", actual_rank);
#endif
    int rc;
    MPI_Aint lb, data_size;
    int length;
    MPI_Request request;
    int rsbflag=0;
    if(actual_rank<appSize){
        if(comm == MPI_COMM_WORLD){
            PMPI_Ireduce_scatter_block(sendbuf, recvbuf, recvcount, datatype, op, ls_data_world_comm, &request);
        }
        else{
            PMPI_Ireduce_scatter_block(sendbuf, recvbuf, recvcount, datatype, op, comm, &request);
        }
        PMPI_Test(&request, &rsbflag, MPI_STATUS_IGNORE);
        int prCount = 0;
        while(!rsbflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
/*            if(prCount == sleepiters)*/
                usleep(mainsleeptime);
/*            else
                prCount++;*/
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            sleeptime += tbefore;
            sleepcount++;
//            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
//            fflush(stdout);
#endif
            PMPI_Test(&request, &rsbflag, MPI_STATUS_IGNORE);
        }
/*        reducescatterblockPrCount += prCount;
        reducescatterblockCallCount++;*/
        if(shStart <= actual_rank && actual_rank <= shEnd){
        /*forward result to shadow*/
        MPI_Type_get_extent(datatype, &lb, &data_size);
        length = data_size * (long long)recvcount;
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(recvbuf, length, 0, 0); 
        }
    }
    else{
        int src, tag, length;

        mq_pop(&src, &tag, &length, recvbuf);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Reduce_scatter_block, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Reduce_scatter_block\n", actual_rank);
#endif
#endif
/*    if( actual_rank >= actual_size/2 ){
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGALRM);
        pthread_sigmask(SIG_UNBLOCK, &set, NULL);
    }*/
 
    return MPI_SUCCESS;
}