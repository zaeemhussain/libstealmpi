#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <mpi.h>
#include <signal.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <sched.h>
#include <linux/prctl.h>
#include <sched.h>
#include "mpi_override.h"
#include "monitor_thread.h"
#include "socket.h"
#include "request_list.h"
#include "shared_mem.h"
#include "forwarding_buffer.h"
//#define DEBUG

struct timeval time_t1, time_t2;
double tdiff;
double mpicommtime=0.0;
double sockcommtime=0.0;
int actual_rank;                  // 'this' process rank
int actual_size;
int appSize, shStart, shEnd, cStart, cEnd;
int mainsleeptime=0;
int *flags_ptr;
int exflag=0;
int full_speed_flag, term_flag=0, cterm_flag=0;
int ls_cntr_msg_count=0, ls_recv_counter=0, ls_data_msg_count=0;
int need2leap=0;
long long ls_data_msg_counter=0;
long long ls_dat_msg_len=0;
int fsflag=0;
MPI_Comm ls_data_world_comm, ls_cntr_world_comm, ls_failure_world_comm;
unsigned int cpu1,cpu2;// *node1,*node2;
long long intcount=0;
struct itimerval ttimer;



int MPI_Comm_size(MPI_Comm comm, int *size){

    if(comm == MPI_COMM_WORLD){
        *size=appSize;
    }
    else{
        PMPI_Comm_size(comm, size);     
    }

    return MPI_SUCCESS;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank){

    if(comm == MPI_COMM_WORLD){
        if( actual_rank < appSize ) 
            *rank = actual_rank;
        else
            *rank = actual_rank - appSize + shStart;
    }
    else{
        PMPI_Comm_rank(comm, rank);
    }

    return MPI_SUCCESS;
}

int MPI_Init(int* argc, char*** argv){
    int ret;
    
    gettimeofday(&time_t1, 0x0);
    ret = PMPI_Init(argc, argv);
    
    PMPI_Comm_size(MPI_COMM_WORLD, &actual_size);
    PMPI_Comm_rank(MPI_COMM_WORLD, &actual_rank);    
    PMPI_Comm_dup(MPI_COMM_WORLD, &ls_cntr_world_comm);
    PMPI_Comm_dup(MPI_COMM_WORLD, &ls_failure_world_comm);

    /*int i = 0;
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    printf("[%d] PID %d on %s ready for attach\n", actual_rank, getpid(), hostname);
    fflush(stdout);
    while (0 == i)
        sleep(5);*/

    FILE *configF = fopen("/ihome/rmelhem/zah20/configs/config.txt","r");
    fscanf(configF,"%d %d %d %d %d",&appSize, &shStart, &shEnd, &cStart, &cEnd);
    if((appSize + shEnd - shStart + 1) != actual_size)
        printf("[%d] Config does not match number of processes.\n", actual_rank);
    fclose(configF);
    PMPI_Comm_split(MPI_COMM_WORLD, actual_rank>=appSize, actual_rank, &ls_data_world_comm);

    if(cStart < cEnd){
        if(cStart <= actual_rank && actual_rank <= cEnd)
            create_shared_mem();
    }
    else{
        if((cStart <= actual_rank && actual_rank < appSize) || actual_rank <= cEnd)
            create_shared_mem();
    }

    if(shStart <= actual_rank && actual_rank <= shEnd){
        forwarding_buf_init();
        socket_connect();
    }
    else if(actual_rank >= appSize){
        mq_init();
        socket_connect();
        //create_shared_mem();
        launch_monitor_thread(0);
        while(!cterm_flag){ //this part would need to be fixed later
            usleep(10000);
        }
        /*pthread_join(ls_monitor_thread, NULL);
        free_thread_resources();
        mq_free();
#ifdef USE_RDMA
        rclose(sock_fd);
#else
        close(sock_fd);
#endif
        PMPI_Comm_free(&ls_cntr_world_comm);
        PMPI_Comm_free(&ls_failure_world_comm);
        PMPI_Comm_free(&ls_data_world_comm);   
        PMPI_Finalize();
        exit(0);*/
    }
    return ret;
}


int MPI_Finalize(){
    int ret;
    int code;
#ifdef DEBUG
    printf("[%d]: In finalize\n", actual_rank);
    fflush(stdout);
#endif
    if(actual_rank>=appSize)
        printf("[%d] My time is %.6f\n", actual_rank, tdiff);
    double maxtime,avgtime;
    if(cStart < cEnd){
        if(cStart <= actual_rank && actual_rank <= cEnd)
            *(flags_ptr+sizeof(int))=1;
    }
    else{
        if((cStart <= actual_rank && actual_rank < appSize) || actual_rank <= cEnd)
            *(flags_ptr+sizeof(int))=1;
    }
    if(shStart <= actual_rank && actual_rank <= shEnd){
        send_current_buffer();
        free_forwarding_buffer();
    }
    gettimeofday(&time_t2, 0x0);
    double sec = (time_t2.tv_sec - time_t1.tv_sec);
    double usec = (time_t2.tv_usec - time_t1.tv_usec) / 1000000.0;
    tdiff = sec + usec;
    PMPI_Allreduce(&tdiff, &maxtime, 1, MPI_DOUBLE, MPI_MAX, ls_data_world_comm);
    PMPI_Allreduce(&tdiff, &avgtime, 1, MPI_DOUBLE, MPI_SUM, ls_data_world_comm);
    cpu2=sched_getcpu();
    if(actual_rank==0){
        printf("[%d] Total time is %.6f\n", actual_rank, maxtime);
        printf("[%d] Average time is %.6f\n", actual_rank, avgtime/appSize);
        if(actual_rank==0){
            printf("[%d] Number of data messages is %d\n", actual_rank, ls_data_msg_counter);
            printf("[%d] Total message length is %d\n", actual_rank, ls_dat_msg_len);
        }
    }
    if(actual_rank==0)// || actual_rank>=(actual_size/2))
        printf("[%d] My time is %.6f\n", actual_rank, tdiff);

/*    int finval[1];
    if(2*actual_rank>=actual_size){
        
#ifdef DEBUG
        printf("[%d]: shadow in finalize\n", actual_rank);
        fflush(stdout);
#endif
        while(!(*(flags_ptr+sizeof(int)))){
            ;
        }
        finval[0]=1;
        PMPI_Send(finval,1,MPI_INT,actual_rank-(actual_size/2),SHADOW_MAIN_FINALIZE_TAG, ls_cntr_world_comm);
    }*/
    
    if(actual_rank>=appSize){
#ifdef DEBUG
        printf("[%d]: shadow in finalize\n", actual_rank);
        fflush(stdout);
#endif
        /*printf("[%d]: Entered interrupt %d times.\n", actual_rank, intcount);
        fflush(stdout);*/

        /*terminate monitor thread first*/
        pthread_join(ls_monitor_thread, NULL);
#ifdef DEBUG
        printf("[%d]: monitor thread joined.\n", actual_rank);
        printf("[%d]: shadow full_speed_flag: %d, terminate_flag: %d.\n", actual_rank, *(flags_ptr+sizeof(int)), term_flag);
        fflush(stdout);
#endif
        free_thread_resources();
        mq_free();
    }
    
    if(actual_rank>=appSize || (shStart <= actual_rank && actual_rank <= shEnd)){
#ifdef USE_RDMA
    rclose(sock_fd);
#else
    close(sock_fd);
#endif
    }
    /*if(2*actual_rank < actual_size){
        int finflag=0;
        PMPI_Iprobe(actual_rank+(actual_size/2), SHADOW_MAIN_FINALIZE_TAG, ls_cntr_world_comm, &finflag, MPI_STATUS_IGNORE);
        while(!finflag){
            usleep(USLEEP_TIME);
            PMPI_Iprobe(actual_rank+(actual_size/2), SHADOW_MAIN_FINALIZE_TAG, ls_cntr_world_comm, &finflag, MPI_STATUS_IGNORE);
        }
        PMPI_Recv(finval,1,MPI_INT,actual_rank+(actual_size/2),SHADOW_MAIN_FINALIZE_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
    }*/
    PMPI_Comm_free(&ls_cntr_world_comm);
    PMPI_Comm_free(&ls_failure_world_comm);
    PMPI_Comm_free(&ls_data_world_comm);
    if(actual_rank>=appSize){
        close_shared_mem();
    }
    if(cStart < cEnd){
        if(cStart <= actual_rank && actual_rank <= cEnd)
            close_shared_mem();
    }
    else{
        if((cStart <= actual_rank && actual_rank < appSize) || actual_rank <= cEnd)
            close_shared_mem();
    }
#ifdef DEBUG
    printf("[%d]: Time spent in mpi recv is %.3f.\n", actual_rank, mpicommtime);
    printf("[%d]: Time spent in socket send is %.3f.\n", actual_rank, sockcommtime);
    printf("[%d]: Calling actual finalize now.\n", actual_rank);
    fflush(stdout);
#endif
            
    ret=PMPI_Finalize();
#ifdef DEBUG
        printf("[%d]: After finalize call.\n", actual_rank);
        fflush(stdout);
#endif
    return ret;
}

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{

#ifdef DEBUG
        printf("[%d] Entered MPI send.\n", actual_rank);
        fflush(stdout);
#endif
    int ret;   
    if(actual_rank<appSize){
#ifdef DEBUG
        printf("[%d] Entered main MPI send.\n", actual_rank);
        fflush(stdout);
#endif
        MPI_Request request;
        int myflag=0;
        //PMPI_Send(buf, count, datatype, dest, tag, comm);
        PMPI_Isend(buf, count, datatype, dest, tag, comm,&request);  // actual SEND!!
        PMPI_Test(&request, &myflag, MPI_STATUS_IGNORE);
        while(!myflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
            usleep(mainsleeptime);
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            printf("[%d] Current sleep in send took %.6f seconds.\n", actual_rank, tbefore);
            fflush(stdout);
#endif
            PMPI_Test(&request, &myflag, MPI_STATUS_IGNORE);
        }
#ifdef DEBUG
        printf("[%d] End send. Current time : %.3f\n", actual_rank, MPI_Wtime());
        fflush(stdout);
#endif
    }

    return 0;
}


int MPI_Recv(void *buf, int count, MPI_Datatype datatype,
       int source, int tag, MPI_Comm comm, MPI_Status *status)
{

#ifdef DEBUG
        printf("[%d] entered receive routine\n", actual_rank);
        fflush(stdout);
#endif
    int length;
    MPI_Status temp_status;                         
    int myflag=0;
    double time1, time2;
    if(actual_rank<appSize){
        PMPI_Iprobe(source, tag, comm, &myflag, status);
        while(!myflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in recv. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
            usleep(mainsleeptime);
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            printf("[%d] Current sleep in recv took %.6f seconds.\n", actual_rank, tbefore);
            fflush(stdout);
#endif
            PMPI_Iprobe(source, tag, comm, &myflag, status);
        }
        if(status == MPI_STATUS_IGNORE){
            status = &temp_status;
        }
        time1=MPI_Wtime();
        PMPI_Recv(buf, count, datatype, source, tag, comm, status);
        time2=MPI_Wtime();
#ifdef DEBUG
        printf("[%d] end MPI_Recv, src=%d, tag=%d, length=%d, time=%.3f, %.3f\n", actual_rank, status->MPI_SOURCE, status->MPI_TAG, length, time1, time2);
        fflush(stdout);
#endif
        mpicommtime+=time2-time1;
        if(shStart <= actual_rank && actual_rank <= shEnd){
        time1=MPI_Wtime();
        PMPI_Get_count(status, MPI_CHAR, &length);
        ls_data_msg_counter++;
        ls_dat_msg_len+=length;
        buffer_or_send(buf, length, status->MPI_SOURCE, status->MPI_TAG);
        time2=MPI_Wtime();
        sockcommtime+=time2-time1; 
        }
    }
    else{
        int temp_src;
        int temp_tag;

        mq_pop(&temp_src, &temp_tag, &length, buf);
        if(status != MPI_STATUS_IGNORE){
            status->MPI_SOURCE = temp_src;
            status->MPI_TAG = temp_tag;
            status->_ucount = length;
        }
#ifdef DEBUG
        printf("[%d] end MPI_Recv, src=%d, tag=%d, length=%d\n", actual_rank, temp_src, temp_tag, length);
        fflush(stdout);
#endif   
    }

    return 0;
}

int MPI_Probe(int source, int tag, MPI_Comm comm, MPI_Status *status){

    int rc;
    int buf[3];

#ifdef DEBUG
    printf("[%d] begin MPI_Probe()\n", actual_rank);
    fflush(stdout);
#endif
    if(actual_rank<appSize){
        int pflag=0;
        rc = PMPI_Iprobe(source, tag, comm, &pflag, status);
        while(!pflag){
#ifdef DEBUG
        printf("[%d] Signalling its colocated shadow in probe. Current time : %.3f\n", actual_rank, MPI_Wtime());
        fflush(stdout);
#endif
//            *flags_ptr=1;
            usleep(mainsleeptime);
            rc = PMPI_Iprobe(source, tag, comm, &pflag, status);
        }
        if(shStart <= actual_rank && actual_rank <= shEnd){
        if(status != MPI_STATUS_IGNORE){
            buf[0] = status->MPI_SOURCE;
            buf[1] = status->MPI_TAG;
            buf[2] = status->_ucount;
        }
        buffer_or_send(buf, 3*sizeof(int), buf[0], SHADOW_PROBE_TAG);
        }
        ls_cntr_msg_count++;
    }
    else{
        int temp_src;
        int temp_tag;

        mq_pop(&temp_src, &temp_tag, &rc, buf);

        ls_cntr_msg_count++;
        if(status != MPI_STATUS_IGNORE){
            status->MPI_SOURCE = buf[0];
            status->MPI_TAG = buf[1];
            status->_ucount = buf[2];
        }
    }

#ifdef DEBUG
    printf("[%d] end MPI_Probe\n", actual_rank);
    fflush(stdout);
#endif

    if(3*actual_rank<2*actual_size)
        return rc;
    return MPI_SUCCESS;
}

int MPI_Iprobe(int source, int tag, MPI_Comm comm, int *flag, MPI_Status *status){

    int rc;
    int buf[4];

#ifdef DEBUG
    printf("[%d] begin MPI_Iprobe()\n", actual_rank);
    fflush(stdout);
#endif
    if(actual_rank<appSize){
        rc = PMPI_Iprobe(source, tag, comm, flag, status);
        buf[0] = *flag;
        if(shStart <= actual_rank && actual_rank <= shEnd){
        if(*flag && status != MPI_STATUS_IGNORE){
            buf[1] = status->MPI_SOURCE;
            buf[2] = status->MPI_TAG;
            buf[3] = status->_ucount;
        }
        buffer_or_send(buf, 4*sizeof(int), buf[1], SHADOW_PROBE_TAG);
        }
#ifdef DEBUG
    printf("[%d] in MPI_Iprobe() sent flag %d\n", actual_rank, buf[0]);
    fflush(stdout);
#endif

        ls_cntr_msg_count++;
    }
    else{
        int temp_src;
        int temp_tag;

        mq_pop(&temp_src, &temp_tag, &rc, buf);

        ls_cntr_msg_count++;
        *flag = buf[0];
        if(*flag && status != MPI_STATUS_IGNORE){
            status->MPI_SOURCE = buf[1];
            status->MPI_TAG = buf[2];
            status->_ucount = buf[3];
        }
#ifdef DEBUG
    printf("[%d] in MPI_Iprobe() got flag %d\n", actual_rank, *flag);
    fflush(stdout);
#endif
    }

#ifdef DEBUG
    printf("[%d] end MPI_Iprobe()\n", actual_rank);
    fflush(stdout);
#endif

    if(actual_rank<appSize)
        return rc;
    return MPI_SUCCESS;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request){

#ifdef DEBUG
    long long length;
    MPI_Aint lb, extent;
    MPI_Type_get_extent(datatype, &lb, &extent);
    length = extent * (long long)count;
#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
    printf("[%d] begin Isend, message length is %lld\n", actual_rank, length);
#endif
 
    if(actual_rank<appSize){ 
        PMPI_Isend(buf, count, datatype, dest, tag, comm, request);
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Isend, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Isend\n", actual_rank);
#endif
#endif

    return MPI_SUCCESS; 
} 

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request){

#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
 
    int rc = 0;
#ifdef DEBUG
    printf("[%d] begin Irecv\n", actual_rank);
#endif

    if(actual_rank<appSize){
        PMPI_Irecv(buf, count, datatype, source, tag, comm, request);
    }
    if((shStart <= actual_rank && actual_rank <= shEnd) || actual_rank >= appSize){
    rl_add(request, buf);}
    ls_recv_counter++;

#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Irecv, time is %.6f, recv count = %d\n", actual_rank, diff, ls_recv_counter);
#else
    printf("[%d] end Irecv, recv count = %d\n", actual_rank, ls_recv_counter);
#endif
#endif
 
    return MPI_SUCCESS;
}

int MPI_Wait(MPI_Request *request, MPI_Status *status){

    req_list *p = NULL;
    MPI_Status temp_status;
    int length;

#ifdef DEBUG
    printf("[%d] begin MPI_Wait\n", actual_rank);
    fflush(stdout);
#endif

    if(actual_rank<appSize){
        if(status == MPI_STATUS_IGNORE){
            status = &temp_status;
        }
        int wflag=0;
        PMPI_Test(request, &wflag, status);
        while(!wflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in wait. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
            usleep(mainsleeptime);
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            printf("[%d] Current sleep in wait took %.6f seconds.\n", actual_rank, tbefore);
            fflush(stdout);
#endif
            PMPI_Test(request, &wflag, status);
        }

        if((p = rl_find(request)) != NULL){
            /*This requset is for MPI_Irecv()*/
            MPI_Get_count(status, MPI_CHAR, &length);
            ls_data_msg_counter++;
            ls_dat_msg_len+=length;
            buffer_or_send(p->buf, length, status->MPI_SOURCE, status->MPI_TAG);
            rl_remove(p);
            p = NULL;
#ifdef DEBUG
            printf("[%d] end MPI_Wait, msg received with src = %d, tag = %d, length = %d \n", 
                    actual_rank, status->MPI_SOURCE, status->MPI_TAG, length);
            fflush(stdout);
#endif
        }
#ifdef DEBUG
        else{
            printf("[%d] end MPI_Wait, send completed\n", actual_rank);
            fflush(stdout);
        }
#endif
    }
    else{
        if((p = rl_find(request)) != NULL){
            /*This request is for MPI_Irecv()*/
            int temp_src;
            int temp_tag;
    
            mq_pop(&temp_src, &temp_tag, &length, p->buf);
            if(status != MPI_STATUS_IGNORE){
                status->MPI_SOURCE = temp_src;
                status->MPI_TAG = temp_tag;
                status->_ucount = length;
            }
            rl_remove(p);
            p = NULL;
#ifdef DEBUG
            printf("[%d] end MPI_Wait, msg received with src = %d, tag = %d, length = %d \n", 
                    actual_rank, temp_src, temp_tag, length);
            fflush(stdout);
#endif
        }
        else{
            //status=MPI_STATUS_IGNORE;
#ifdef DEBUG
            printf("[%d] end MPI_Wait, send completed\n", actual_rank);
            fflush(stdout);
#endif
        }
    }

    return MPI_SUCCESS; 
}

int MPI_Waitany(int count, MPI_Request array_of_requests[],
            int *index, MPI_Status *status){

    req_list *p = NULL;
    MPI_Status temp_status;
    int length, i;

#ifdef DEBUG
    printf("[%d] begin MPI_Waitany\n", actual_rank);
    fflush(stdout);
#endif

    if(actual_rank<appSize){
        if(status == MPI_STATUS_IGNORE){
            status = &temp_status;
        }
        int waflag=0;
        PMPI_Testany(count, array_of_requests, index, &waflag, status);
        while(!waflag){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in waitany. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
            usleep(mainsleeptime);
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            printf("[%d] Current sleep in waitany took %.6f seconds.\n", actual_rank, tbefore);
            fflush(stdout);
#endif
            PMPI_Testany(count, array_of_requests, index, &waflag, status);
        }
        if(shStart <= actual_rank && actual_rank <= shEnd){
        buffer_or_send(index, sizeof(int), 0, SHADOW_WAITANY_TAG);}
        ls_cntr_msg_count++;
        if(*index != MPI_UNDEFINED && (p = rl_find(&array_of_requests[*index])) != NULL){
            MPI_Get_count(status, MPI_CHAR, &length);
            ls_data_msg_counter++;
            ls_dat_msg_len+=length;
            buffer_or_send(p->buf, length, status->MPI_SOURCE, status->MPI_TAG);
            rl_remove(p);
            p = NULL;
#ifdef DEBUG
            printf("[%d] end MPI_Waitany, index = %d, msg received with src = %d, tag = %d, length = %d\n", 
                   actual_rank, *index, status->MPI_SOURCE, status->MPI_TAG, length);
            fflush(stdout);
#endif 
        }
#ifdef DEBUG
        else if(*index == MPI_UNDEFINED){
            printf("[%d] end MPI_Waitany, index == MPI_UNDEFINED\n", actual_rank);
            fflush(stdout);
        }
        else{
            printf("[%d] end MPI_Waitany, index = %d, send completed\n", actual_rank, *index);
            fflush(stdout);
        }
#endif
    }
    else{
        int temp_src1;
        int temp_tag1;
        int length1;
    
        mq_pop(&temp_src1, &temp_tag1, &length1, index);

        ls_cntr_msg_count++;
        if(*index != MPI_UNDEFINED && (p = rl_find(&array_of_requests[*index])) != NULL){
            int temp_src;
            int temp_tag;

            mq_pop(&temp_src, &temp_tag, &length, p->buf);
            if(status != MPI_STATUS_IGNORE){
                status->MPI_SOURCE = temp_src;
                status->MPI_TAG = temp_tag;
                status->_ucount = length;
            }
            rl_remove(p);
            p = NULL;
#ifdef DEBUG
            printf("[%d] end MPI_Waitany, index = %d, msg received with src = %d, tag = %d, length = %d\n", 
                   actual_rank, *index, temp_src, temp_tag, length);
            fflush(stdout);
#endif 
        }
#ifdef DEBUG
        else if(*index == MPI_UNDEFINED){
            printf("[%d] end MPI_Waitany, index == MPI_UNDEFINED\n", actual_rank);
            fflush(stdout);
        }
        else{
            printf("[%d] end MPI_Waitany, index = %d, send completed\n", actual_rank, *index);
            fflush(stdout);
        }
#endif
    }

    return MPI_SUCCESS;
}

int MPI_Waitsome(int incount, MPI_Request array_of_requests[],
            int *outcount, int array_of_indices[], MPI_Status array_of_statuses[]){

    req_list *p = NULL;
    int status_flag;
    int length;
    int *temp_buf = NULL; 
    int i;

#ifdef DEBUG
    printf("[%d] begin MPI_Waitsome\n", actual_rank);
    fflush(stdout);
#endif

    if(actual_rank<appSize){
        if(array_of_statuses == MPI_STATUSES_IGNORE){
            status_flag = 1;
            array_of_statuses = (MPI_Status *)malloc(incount * sizeof(MPI_Status));
        }
        else{
            status_flag = 0;
        }
        PMPI_Testsome(incount, array_of_requests, outcount, array_of_indices, array_of_statuses);
        while(*outcount==0){
#ifdef DEBUG
        //printf("[%d] Signalling its colocated shadow in waitsome. Current time : %.3f\n", actual_rank, MPI_Wtime());
        //fflush(stdout);
#endif
//            *flags_ptr=1;
#ifdef SLEEPDEBUG
            double tbefore=MPI_Wtime();
#endif
            usleep(mainsleeptime);
#ifdef SLEEPDEBUG
            tbefore=MPI_Wtime()-tbefore;
            printf("[%d] Current sleep in waitsome took %.6f seconds.\n", actual_rank, tbefore);
            fflush(stdout);
#endif
            PMPI_Testsome(incount, array_of_requests, outcount, array_of_indices, array_of_statuses);
        }
        if(*outcount == MPI_UNDEFINED){
            if(shStart <= actual_rank && actual_rank <= shEnd){
            buffer_or_send(outcount, sizeof(int), 0, SHADOW_WAITSOME_TAG);}
            ls_cntr_msg_count++;
#ifdef DEBUG
            printf("[%d] end MPI_Waitsome, outcount == MPI_UNDEFINED\n", actual_rank);
            fflush(stdout);
#endif
        }
        else{
            if(shStart <= actual_rank && actual_rank <= shEnd){
            temp_buf = (int *)malloc((*outcount + 1) * sizeof(int));
            temp_buf[0] = *outcount;
            for(i = 0; i < *outcount; i++){
                temp_buf[i+1] = array_of_indices[i];
            }
            buffer_or_send(temp_buf, (*outcount + 1)*sizeof(int), 0, SHADOW_WAITSOME_TAG);}
            ls_cntr_msg_count++;
            for(i = 0; i < *outcount; i++){
                int index = array_of_indices[i];

                if((p = rl_find(&array_of_requests[index])) != NULL){
                    MPI_Get_count(&array_of_statuses[i], MPI_CHAR, &length);
                    ls_data_msg_counter++;
                    ls_dat_msg_len+=length;
                    buffer_or_send(p->buf, length, array_of_statuses[i].MPI_SOURCE, array_of_statuses[i].MPI_TAG);
                    rl_remove(p);
                    p = NULL;
#ifdef DEBUG
                    printf("[%d] MPI_Waitsome, outcount = %d, index = %d, msg_received with src = %d, tag = %d, length = %d\n", 
                        actual_rank, *outcount, index, array_of_statuses[i].MPI_SOURCE, array_of_statuses[i].MPI_TAG,
                        length);
                    fflush(stdout);
#endif
                }                    
            }
            free(temp_buf); 
            if(status_flag)
                free(array_of_statuses);
        }
    }
    else{
        temp_buf = (int *)malloc((incount + 1) * sizeof(int));
        int temp_src1;
        int temp_tag1;
        int length1;
        mq_pop(&temp_src1, &temp_tag1, &length1, temp_buf);
        ls_cntr_msg_count++;
        *outcount = temp_buf[0];
        if(temp_buf[0] == MPI_UNDEFINED){
#ifdef DEBUG
            printf("[%d] end MPI_Waitsome, outcount = MPI_UNDEFINED\n", actual_rank);
            fflush(stdout);
#endif
        }
        else{
            int index, temp_src, temp_tag;

            for(i = 0; i < *outcount; i++){
                index = temp_buf[i+1];
                array_of_indices[i] = index;
                if((p = rl_find(&array_of_requests[index])) != NULL){
                    mq_pop(&temp_src, &temp_tag, &length, p->buf);
#ifdef DEBUG
                    printf("[%d] MPI_Waitsome, outcount = %d, index = %d, msg_received with src = %d, tag = %d, length = %d\n", 
                        actual_rank, *outcount, index, temp_src, temp_tag, length);
                    fflush(stdout);
#endif
                    if(array_of_statuses != MPI_STATUSES_IGNORE){
                        array_of_statuses[i].MPI_SOURCE = temp_src;
                        array_of_statuses[i].MPI_TAG = temp_tag;
                        array_of_statuses[i]._ucount = length;
                    }
                    rl_remove(p);
                    p = NULL;
                }
            }
        }    
    }

#ifdef DEBUG
    printf("[%d] end MPI_Waitsome\n", actual_rank);
    fflush(stdout);
#endif

    return MPI_SUCCESS;
}

int MPI_Waitall(int count, MPI_Request array_of_requests[], MPI_Status *array_of_statuses){

#ifdef TIME_DEBUG
    struct timeval time_1, time_2;
    gettimeofday(&time_1, 0x0);
#endif
 
    int i;
#ifdef DEBUG
    printf("[%d] begin MPI_Waitall, %d requests in total\n", actual_rank, count);
#endif
    for(i = 0; i < count; i++){
        if(array_of_statuses == MPI_STATUSES_IGNORE){
#ifdef DEBUG
            printf("[%d] MPI_Waitall using MPI_STATUSES_IGNORE\n", actual_rank);
            fflush(stdout);
#endif
            MPI_Wait(&array_of_requests[i], MPI_STATUS_IGNORE);
        }
        else{
#ifdef DEBUG
            printf("[%d] MPI_Waitall with MPI_Statuses\n", actual_rank);
            fflush(stdout);
#endif
            MPI_Wait(&array_of_requests[i], &array_of_statuses[i]);
        }
    }
#ifdef DEBUG
#ifdef TIME_DEBUG
    gettimeofday(&time_2, 0x0);
    double sec = (time_2.tv_sec - time_1.tv_sec);
    double usec = (time_2.tv_usec - time_1.tv_usec) / 1000000.0;
    double diff = sec + usec;
    printf("[%d] end Waitall, time is %.6f\n", actual_rank, diff);
#else
    printf("[%d] end Waitall\n", actual_rank);
#endif
#endif

    return MPI_SUCCESS;
}

int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status){

    req_list *p = NULL;
    MPI_Status temp_status;
    int length;

#ifdef DEBUG
    printf("[%d] begin MPI_Test\n", actual_rank);
    fflush(stdout);
#endif

    if(actual_rank<appSize){
        if(status == MPI_STATUS_IGNORE){
            status = &temp_status;
        }
        PMPI_Test(request, flag, status); 
        if(shStart <= actual_rank && actual_rank <= shEnd){
        buffer_or_send(flag, sizeof(int), 0, SHADOW_TEST_TAG);}
        ls_cntr_msg_count++;
        if(*flag && ((p = rl_find(request)) != NULL)){
            /*This requset is for MPI_Irecv()*/
            MPI_Get_count(status, MPI_CHAR, &length);
            ls_data_msg_counter++;
            ls_dat_msg_len+=length;
            buffer_or_send(p->buf, length, status->MPI_SOURCE, status->MPI_TAG);
            rl_remove(p);
            p = NULL;
#ifdef DEBUG
            printf("[%d] end MPI_Test, msg received with src = %d, tag = %d, length = %d \n", 
                actual_rank, status->MPI_SOURCE, status->MPI_TAG, length);
            fflush(stdout);
#endif
        }
#ifdef DEBUG
        else if(*flag){
            printf("[%d] end MPI_Test, send completed\n", actual_rank);
            fflush(stdout);
        }
        else if((p = rl_find(request)) != NULL){
            printf("[%d] end MPI_Test, msg not received yet\n", actual_rank);
            fflush(stdout);
        }
        else{
            printf("[%d] end MPI_Test, send not completed yet\n", actual_rank);
            fflush(stdout);
        }
#endif
    }
    else{
        int temp_src1, temp_tag1, length1;
        mq_pop(&temp_src1, &temp_tag1, &length1, flag);
        ls_cntr_msg_count++;
        if(*flag && ((p = rl_find(request)) != NULL)){
            /*This request is for MPI_Irecv()*/
            int temp_src;
            int temp_tag;
    
            mq_pop(&temp_src, &temp_tag, &length, p->buf);
            if(status != MPI_STATUS_IGNORE){
                status->MPI_SOURCE = temp_src;
                status->MPI_TAG = temp_tag;
                status->_ucount = length;
            }
            rl_remove(p);
            p = NULL;
#ifdef DEBUG
            printf("[%d] end MPI_Test, msg received with src = %d, tag = %d, length = %d \n", 
                actual_rank, status->MPI_SOURCE, status->MPI_TAG, length);
            fflush(stdout);
#endif
        }
#ifdef DEBUG
        else if(*flag){
            printf("[%d] end MPI_Test, send completed\n", actual_rank);
            fflush(stdout);
        }
        else if((p = rl_find(request)) != NULL){
            printf("[%d] end MPI_Test, msg not received yet\n", actual_rank);
            fflush(stdout);
        }
        else{
            printf("[%d] end MPI_Test, send not completed yet\n", actual_rank);
            fflush(stdout);
        }
#endif
    }


    return MPI_SUCCESS; 
}

int MPI_Testall(int count, MPI_Request array_of_requests[],
            int *flag, MPI_Status array_of_statuses[]){

    req_list *p = NULL;
    int length;
    int status_flag;
    int i;

#ifdef DEBUG
    printf("[%d] begin MPI_Testall\n", actual_rank);
    fflush(stdout);
#endif

    if(actual_rank<appSize){
        if(array_of_statuses == MPI_STATUSES_IGNORE){
            status_flag = 1;
            array_of_statuses = (MPI_Status *)malloc(count * sizeof(MPI_Status));
        }
        else{
            status_flag = 0;
        }
        PMPI_Testall(count, array_of_requests, flag, array_of_statuses);
        if(shStart <= actual_rank && actual_rank <= shEnd){
        buffer_or_send(flag, sizeof(int), 0, SHADOW_TESTALL_TAG);}
        ls_cntr_msg_count++;
        if(*flag){
            for(i = 0; i < count; i++){
                if((p = rl_find(&array_of_requests[i])) != NULL){
                    MPI_Get_count(&array_of_statuses[i], MPI_CHAR, &length);
                    ls_data_msg_counter++;
                    ls_dat_msg_len+=length;
                    buffer_or_send(p->buf, length, array_of_statuses[i].MPI_SOURCE, array_of_statuses[i].MPI_TAG);
                    rl_remove(p);
                    p = NULL;
#ifdef DEBUG
                    printf("[%d] MPI_Testall, msg received with src = %d, tag = %d, length = %d\n", 
                            actual_rank, array_of_statuses[i].MPI_SOURCE, array_of_statuses[i].MPI_TAG, length);
                    fflush(stdout);
#endif
                }
#ifdef DEBUG
                else{
                    printf("[%d] MPI_Testall, send completed\n", actual_rank);
                    fflush(stdout);
                }
#endif
            }
        } 
#ifdef DEBUG
        else{
            printf("[%d] end MPI_Testall, not completed yet\n", actual_rank);
            fflush(stdout);
        }
#endif
        if(status_flag)
            free(array_of_statuses);

    }
    else{
        int temp_src1;
        int temp_tag1, length1;
        mq_pop(&temp_src1, &temp_tag1, &length1, flag);
        ls_cntr_msg_count++;
        if(*flag){
            for(i = 0; i < count; i++){
                if((p = rl_find(&array_of_requests[i])) != NULL){
                    int temp_src;
                    int temp_tag;
                    
                    mq_pop(&temp_src, &temp_tag, &length, p->buf);
                    if(array_of_statuses != MPI_STATUSES_IGNORE){
                        array_of_statuses[i].MPI_SOURCE = temp_src;
                        array_of_statuses[i].MPI_TAG= temp_tag;
                        array_of_statuses[i]._ucount = length;
                    }
                    rl_remove(p);
                    p = NULL;
#ifdef DEBUG
                    printf("[%d] MPI_Testall, msg received with src = %d, tag = %d, length = %d\n", 
                            actual_rank, temp_src, temp_tag, length);
                    fflush(stdout);
#endif
                }
#ifdef DEBUG
                else{
                    printf("[%d] MPI_Testall, send completed\n", actual_rank);
                    fflush(stdout);
                }
#endif
            }
        }
#ifdef DEBUG
        else{
            printf("[%d] end MPI_Testall, not completed yet\n", actual_rank);
            fflush(stdout);
        }
#endif
    }

    return MPI_SUCCESS;
}

int MPI_Group_size(MPI_Group group, int *size){

    int retval = PMPI_Group_size(group, size);

    return retval;
}

int MPI_Group_rank(MPI_Group group, int *rank){

    int retval = PMPI_Group_rank(group, rank);

    return retval;
}

int MPI_Group_compare(MPI_Group group1, MPI_Group group2, int *result){

    int retval = PMPI_Group_compare(group1, group2, result);

    return retval;
}

int MPI_Group_translate_ranks(MPI_Group group1, int n, const int ranks1[], MPI_Group group2, int ranks2[]){

    int retval =PMPI_Group_translate_ranks(group1, n, ranks1, group2, ranks2);

    return retval;
}

int MPI_Comm_group(MPI_Comm comm, MPI_Group *group){

    int retval;
    if(comm == MPI_COMM_WORLD){
        retval = PMPI_Comm_group(ls_data_world_comm, group);
    }
    else{
        retval = PMPI_Comm_group(comm, group);
    }

    return retval;
}

int MPI_Group_union(MPI_Group group1, MPI_Group group2, MPI_Group *newgroup){

    int retval = PMPI_Group_union(group1, group2, newgroup);

    return retval;
}

int MPI_Group_intersection(MPI_Group group1, MPI_Group group2, MPI_Group *newgroup){

    int retval = PMPI_Group_intersection(group1, group2, newgroup);

    return retval;
}

int MPI_Group_difference(MPI_Group group1, MPI_Group group2, MPI_Group *newgroup){

    int retval = PMPI_Group_difference(group1, group2, newgroup);

    return retval;
}

int MPI_Group_incl(MPI_Group group, int n, const int ranks[], MPI_Group *newgroup){

    int retval = PMPI_Group_incl(group, n, ranks, newgroup); 

    return retval;
}

int MPI_Group_excl(MPI_Group group, int n, const int ranks[], MPI_Group *newgroup){

    int retval = PMPI_Group_excl(group, n, ranks, newgroup); 

    return retval;
}

int MPI_Group_range_incl(MPI_Group group, int n, int ranges[][3], MPI_Group *newgroup){

    int retval = PMPI_Group_range_incl(group, n, ranges, newgroup);

    return retval;
}

int MPI_Group_range_excl(MPI_Group group, int n, int ranges[][3], MPI_Group *newgroup){

    int retval = PMPI_Group_range_excl(group, n, ranges, newgroup);

    return retval;
}

int MPI_Group_free(MPI_Group *group){

    int retval = PMPI_Group_free(group);

    return retval;
}


int MPI_Comm_split(MPI_Comm comm, int color, int key, MPI_Comm *newcomm){

#ifdef DEBUG
    printf("[%d] begin Comm_split\n", actual_rank);
    fflush(stdout);
#endif
    int retval;
    if(comm == MPI_COMM_WORLD){
        retval = PMPI_Comm_split(ls_data_world_comm, color, key, newcomm);
    }
    else{
        retval = PMPI_Comm_split(comm, color, key, newcomm);
    }
#ifdef DEBUG
    if(newcomm==MPI_COMM_NULL){
        printf("[%d] Comm_split returned null communicator.\n", actual_rank);
        fflush(stdout);
    }
    printf("[%d] end Comm_split.\n", actual_rank);
    fflush(stdout);
#endif

    return retval;
} 


int MPI_Comm_dup(MPI_Comm comm, MPI_Comm *newcomm){

    int retval;
    if(comm == MPI_COMM_WORLD)
        retval = PMPI_Comm_dup(ls_data_world_comm, newcomm);
    else
        retval = PMPI_Comm_dup(comm, newcomm);

    return retval;
}


int MPI_Comm_compare(MPI_Comm comm1, MPI_Comm comm2, int *result){

    if(comm1 == MPI_COMM_WORLD)
        comm1 = ls_data_world_comm;
    if(comm2 == MPI_COMM_WORLD)
        comm2 = ls_data_world_comm;
    int retval = PMPI_Comm_compare(comm1, comm2, result);

    return retval;
}
    

int MPI_Comm_create(MPI_Comm comm, MPI_Group group, MPI_Comm *newcomm){

    int retval;
    if(comm == MPI_COMM_WORLD)
        retval = PMPI_Comm_create(ls_data_world_comm, group, newcomm);
    else
        retval = PMPI_Comm_create(comm, group, newcomm);

    return retval;
}

int MPI_Comm_free(MPI_Comm *comm){

    int retval = PMPI_Comm_free(comm);

    return retval; 
}

int MPI_Dims_create(int nnodes, int ndims, int dims[]){

    int retval = PMPI_Dims_create(nnodes, ndims, dims);

    return retval;
}


int MPI_Cart_create(MPI_Comm comm_old, int ndims, const int dims[],
            const int periods[], int reorder, MPI_Comm *comm_cart){

    int retval;
    if(comm_old == MPI_COMM_WORLD)
        retval = PMPI_Cart_create(ls_data_world_comm, ndims, dims, periods, reorder, comm_cart);
    else
        retval = PMPI_Cart_create(comm_old, ndims, dims, periods, reorder, comm_cart);

    return retval;
}


int MPI_Topo_test(MPI_Comm comm, int *status){

    int retval = PMPI_Topo_test(comm, status);

    return retval;
}


int MPI_Cartdim_get(MPI_Comm comm, int *ndims){

    int retval = PMPI_Cartdim_get(comm, ndims);

    return retval;
}


int MPI_Cart_get(MPI_Comm comm, int maxdims, int dims[], int periods[],
            int coords[]){

    int retval = PMPI_Cart_get(comm, maxdims, dims, periods, coords);

    return retval;
}
        

int MPI_Cart_rank(MPI_Comm comm, const int coords[], int *rank){

    int retval = PMPI_Cart_rank(comm, coords, rank);

    return retval;
}


int MPI_Cart_coords(MPI_Comm comm, int rank, int maxdims,
            int coords[]){

    int retval = PMPI_Cart_coords(comm, rank, maxdims, coords);

    return retval;
}


int MPI_Cart_sub(MPI_Comm comm, const int remain_dims[], MPI_Comm *comm_new){

    int retval = PMPI_Cart_sub(comm, remain_dims, comm_new);

    return retval;
}


int MPI_Cart_shift(MPI_Comm comm, int direction, int disp,
            int *rank_source, int *rank_dest){

    int retval = PMPI_Cart_shift(comm, direction, disp, rank_source, rank_dest);

    return retval;
}

int MPI_Graph_create(MPI_Comm comm_old, int nnodes, const int index[],
            const int edges[], int reorder, MPI_Comm *comm_graph){

    int retval;
    if(comm_old == MPI_COMM_WORLD)
        retval = PMPI_Graph_create(ls_data_world_comm, nnodes, index, edges, reorder, comm_graph);
    else
        retval = PMPI_Graph_create(comm_old, nnodes, index, edges, reorder, comm_graph);

    return retval;
}

int MPI_Graphdims_get(MPI_Comm comm, int *nnodes, int *nedges){

    int retval = PMPI_Graphdims_get(comm, nnodes, nedges);

    return retval;
}

int MPI_Graph_get(MPI_Comm comm, int maxindex, int maxedges, int *index, int *edges){

    int retval = PMPI_Graph_get(comm, maxindex, maxedges, index, edges);

    return retval;
}

int MPI_Graph_neighbors_count(MPI_Comm comm, int rank, int *nneighbors){

    int retval = PMPI_Graph_neighbors_count(comm, rank, nneighbors);

    return retval;
}

int MPI_Graph_neighbors(MPI_Comm comm, int rank, int maxneighbors,
            int neighbors[]){

    int retval = PMPI_Graph_neighbors(comm, rank, maxneighbors, neighbors);

    return retval;
}