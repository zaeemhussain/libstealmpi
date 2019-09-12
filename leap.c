#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <signal.h>
#include <string.h>
#include "mpi.h"
#include "mpi_override.h"
#include "leap.h"
#include "msg_queue.h"
#include <unistd.h> 
#define DEBUG

int leap_loop_count = -1;
int leap_state_count = 0;
state_data *leap_list_head = NULL;
state_data *leap_list_tail = NULL;

void shadow_leap(int type){
    int code;
    double start_t, end_t;

    if(type == 0){
#ifdef DEBUG
        printf("[%d] Leap the main to shadow's state, loop counter = %d\n", actual_rank, leap_loop_count);
        fflush(stdout);
#endif
        start_t = MPI_Wtime();
        state_data *p = leap_list_head;
        if(actual_rank >= actual_size/2){
            while(p){
                if(p->is_pointer){
                    PMPI_Send(p->count_addr, 1, MPI_INT, actual_rank - actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm);
                    PMPI_Send(*(p->p_addr), *(p->count_addr), p->dt, actual_rank - actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm);
#ifdef VDEBUG
            printf("[%d] Sent a pointer variable to process %d\n", actual_rank, actual_rank - actual_size/2);
            fflush(stdout);
#endif
                }
                else{
                    PMPI_Send(p->addr, p->count, p->dt, actual_rank - actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm);
#ifdef VDEBUG
            printf("[%d] Sent a variable to process %d\n", actual_rank, actual_rank - actual_size/2);
            fflush(stdout);
#endif
                }
                p = p->next;
            }
#ifdef VDEBUG
            printf("[%d] Sent state to process %d\n", actual_rank, actual_rank - actual_size/2);
            fflush(stdout);
#endif
        } 
        else{
            while(p){    
                if(p->is_pointer){
                    PMPI_Recv(p->count_addr, 1, MPI_INT, actual_rank + actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
                    free(*(p->p_addr));
                    MPI_Aint lb, extent;
                    MPI_Type_get_extent(p->dt, &lb, &extent);
                    *(p->p_addr)=malloc((*p->count_addr)*extent);
                    PMPI_Recv(*(p->p_addr), *(p->count_addr), p->dt, actual_rank + actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
#ifdef VDEBUG
            printf("[%d] Received a pointer variable from process %d\n", actual_rank, actual_rank + actual_size/2);
            fflush(stdout);
#endif
                }
                else{
                    PMPI_Recv(p->addr, p->count, p->dt, actual_rank + actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
#ifdef VDEBUG
            printf("[%d] Received a variable from process %d\n", actual_rank, actual_rank + actual_size/2);
            fflush(stdout);
#endif
                }
                p = p->next;
            }
#ifdef VDEBUG
            printf("[%d] Recv state from process %d\n", actual_rank, actual_rank + actual_size/2);
            fflush(stdout);
#endif
        }
        end_t = MPI_Wtime();
#ifdef DEBUG
        printf("[%d] leaping took %.2f seconds\n", actual_rank, end_t - start_t);
#endif
    }
    else{ /*leaping the shadow to main's state*/
#ifdef DEBUG
        printf("[%d] shadow_leap to main's state, loop counter = %d, data msg counter = %d, cntr msg counter = %d\n", actual_rank, leap_loop_count, 0,0);//ls_data_msg_count, ls_cntr_msg_count);
        fflush(stdout);
#endif
        start_t = MPI_Wtime();
        //firstly, transfer state
        state_data *p = leap_list_head;
        if(actual_rank < actual_size/2){
            while(p){
                if(p->is_pointer){
                    PMPI_Send(p->count_addr, 1, MPI_INT, actual_rank + actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm);
                    PMPI_Send(*(p->p_addr), *(p->count_addr), p->dt, actual_rank + actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm);
#ifdef VDEBUG
            printf("[%d] Sent a pointer variable to process %d\n", actual_rank, actual_rank + actual_size/2);
            fflush(stdout);
#endif
                }
                else{
                    PMPI_Send(p->addr, p->count, p->dt, actual_rank + actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm);
#ifdef VDEBUG
            printf("[%d] Sent a variable to process %d\n", actual_rank, actual_rank + actual_size/2);
            fflush(stdout);
#endif
                }
                p = p->next;
            }
#ifdef VDEBUG
            printf("[%d] Sent state to process %d\n", actual_rank, actual_rank + actual_size/2);
            fflush(stdout);
#endif
        } 
        else{
            while(p){    
                if(p->is_pointer){
                    PMPI_Recv(p->count_addr, 1, MPI_INT, actual_rank - actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
                    free(*(p->p_addr));
                    MPI_Aint lb, extent;
                    MPI_Type_get_extent(p->dt, &lb, &extent);
                    *(p->p_addr)=malloc((*p->count_addr)*extent);
                    PMPI_Recv(*(p->p_addr), *(p->count_addr), p->dt, actual_rank - actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
#ifdef VDEBUG
            printf("[%d] Received a pointer variable from process %d\n", actual_rank, actual_rank - actual_size/2);
            fflush(stdout);
#endif
                }
                else{
                    PMPI_Recv(p->addr, p->count, p->dt, actual_rank - actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
#ifdef VDEBUG
            printf("[%d] Received a variable from process %d\n", actual_rank, actual_rank - actual_size/2);
            fflush(stdout);
#endif
                }
                p = p->next;
            }
        }
        //secondly, consume obsolete msgs
        if(actual_rank < actual_size/2){
            int temp[2];
            temp[0] = leap_loop_count;
            temp[1] = ls_data_msg_count;
            //temp[2] = ls_cntr_msg_count;
            PMPI_Send(temp, 2, MPI_INT, actual_rank + actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm);
            ls_data_msg_count = 0;
        }
        else{
            int temp[2];
            int count_diff = 0;
            int i;
            MPI_Status temp_status;
            char buffer[128];
            int length;

            PMPI_Recv(temp, 2, MPI_INT, actual_rank - actual_size/2, SHADOW_LEAP_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
#ifdef DEBUG
            printf("[%d] main loop count = %d, data msg count = %d, cntr msg count = %d, shadow loop count = %d, data msg count = %d, cntr msg count = %d\n", actual_rank, temp[0], temp[1], 0, leap_loop_count, ls_data_msg_count, 0);
#endif 
            /*consume data msg*/
            while(shared_mq->count < temp[1] - ls_data_msg_count){
                printf("[%d] Waiting for more messages, data msg count at main = %d, data msg count at shadow = %d, mq count = %d\n", actual_rank, temp[1], ls_data_msg_count, shared_mq->count);
                struct timespec reboot_time;
                reboot_time.tv_sec = 0;
                reboot_time.tv_nsec = USLEEP_TIME*1000;
                nanosleep(&reboot_time, NULL);
            }
            mq_clear(temp[1] - ls_data_msg_count);
/*            for(i = 0; i < temp[2] - ls_cntr_msg_count; i++){
                
                PMPI_Probe(ls_app_rank, MPI_ANY_TAG, ls_cntr_world_comm, &temp_status);
                PMPI_Get_count(&temp_status, MPI_CHAR, &length); 
                if(length < 128){
                    PMPI_Recv(buffer, length, MPI_CHAR, ls_app_rank, temp_status.MPI_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
                }
                else{
                    char *buffer = malloc(length);
                    
                    PMPI_Recv(buffer, length, MPI_CHAR, ls_app_rank, temp_status.MPI_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
                    free(buffer);
                } 
            }
#ifdef DEBUG
            printf("[%d] Cleared control messages\n", ls_world_rank);
            fflush(stdout);
#endif
*/
            leap_loop_count = temp[0]; //update shadow's loop count
            ls_data_msg_count = 0;
//            ls_cntr_msg_count = temp[2];
        }
#ifdef DEBUG
        end_t = MPI_Wtime();
        printf("[%d] leaping took %.2f seconds\n", actual_rank, end_t - start_t);
#endif
    }
}

void ls_reset_recv_counter(){
    leap_loop_count++;
    if( actual_rank >= actual_size/2 ){
        //recv_log_cur_count=0;
        if(*(flags_ptr+2*sizeof(int))){
            sigset_t set;
            /*block timer signal*/
/*            sigemptyset(&set);
            sigaddset(&set, SIGALRM);
            pthread_sigmask(SIG_BLOCK, &set, NULL);*/
/*            struct timespec reboot_time;
            reboot_time.tv_sec = REBOOT_TIME;
            reboot_time.tv_nsec = 0;
            nanosleep(&reboot_time, NULL);*/
/*#ifdef DEBUG
                printf("[%d] I failed because my colocated main has failed. Going to leap to my main's state.\n", actual_rank);
                fflush(stdout);
#endif
            //mq_clear();
            shadow_leap(1);*/
            *(flags_ptr+2*sizeof(int))=0;
//            pthread_sigmask(SIG_UNBLOCK, &set, NULL);            
        }
        if(need2leap){
            /*Receive main's handshake message*/
            int code;
            sigset_t set;
            /*block timer signal*/
            sigemptyset(&set);
            sigaddset(&set, SIGALRM);
            pthread_sigmask(SIG_BLOCK, &set, NULL);
            PMPI_Send(&code, 1, MPI_INT, actual_rank - actual_size/2, SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm);
            shadow_leap(1);
            need2leap=0;
            pthread_sigmask(SIG_UNBLOCK, &set, NULL);
            /*struct itimerval ttimer;
            ttimer.it_value.tv_sec = 0;
            ttimer.it_value.tv_usec = 10;
            setitimer (ITIMER_REAL, &ttimer, NULL);  */
        }
    }
    else{
        int ret;
        int msg_arrived = 0;
        int code;
        int parent = (actual_rank - 1) / 2;
        int lchild = 2 * actual_rank + 1;
        int rchild = 2 * actual_rank + 2;
        MPI_Status status;
#ifdef COORDINATED_LEAPING
        PMPI_Iprobe(MPI_ANY_SOURCE, SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm, &msg_arrived, &status);
#else
        PMPI_Iprobe(actual_rank + actual_size/2, SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm, &msg_arrived, &status);
#endif
        if(msg_arrived){
            PMPI_Recv(&code, 1, MPI_INT, status.MPI_SOURCE, SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
#ifdef DEBUG
            printf("[%d] Got message from %d to initiate buffer forced leap.\n", actual_rank, status.MPI_SOURCE);
#endif
            msg_arrived = 0;
            /*trigger forced leaping*/
#ifdef DEBUG
            printf("[%d] Got notification from shadow's monitor thread to start buffer forced leap.\n", actual_rank);
            fflush(stdout);
#endif
            /*make sure the compute thread has caught up*/
            double start_t, end_t;
            start_t = MPI_Wtime();
            msg_arrived = 0;
            while(!msg_arrived){
                PMPI_Iprobe(actual_rank + actual_size/2, SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm, &msg_arrived, MPI_STATUS_IGNORE);
                usleep(0);
            }
            PMPI_Recv(&code, 1, MPI_INT, actual_rank + actual_size/2, SHADOW_FORCE_LEAPING_TAG, ls_cntr_world_comm, MPI_STATUS_IGNORE);
            end_t = MPI_Wtime();
            printf("[%d] Waited %.2f seconds for shadow's compute thread to catch up.\n", actual_rank, end_t - start_t);
            shadow_leap(1);
        }    
    }
}

void leap_register_state_pointer(void **p_addr, int *count_addr, MPI_Datatype dt){
/*#ifdef DEBUG
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);
#endif*/
    state_data *temp = (state_data *)malloc(sizeof(state_data));
    temp->p_addr = p_addr;
    temp->count_addr = count_addr;
    temp->dt = dt;
    temp->is_pointer=1;
    temp->next = NULL;
    if(leap_state_count == 0){
        leap_list_head = leap_list_tail = temp;
    }
    else{
        leap_list_tail->next = temp;
        leap_list_tail = temp;
    }
/*#ifdef DEBUG
    if(ls_world_rank < 2){
        printf("[%d] registered 1 data (size = %d)\n", actual_rank, count*extent);
        fflush(stdout);
#endif*/
    leap_state_count++;
}


void leap_register_state(void *addr, int count, MPI_Datatype dt){
/*#ifdef DEBUG
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);
#endif*/
    state_data *temp = (state_data *)malloc(sizeof(state_data));
    temp->addr = addr;
    temp->count = count;
    temp->dt = dt;
    temp->is_pointer=0;
    temp->next = NULL;
    if(leap_state_count == 0){
        leap_list_head = leap_list_tail = temp;
    }
    else{
        leap_list_tail->next = temp;
        leap_list_tail = temp;
    }
/*#ifdef DEBUG
    if(ls_world_rank < 2){
        printf("[%d] registered 1 data (size = %d)\n", actual_rank, count*extent);
        fflush(stdout);
#endif*/
    leap_state_count++;
}

void ls_inject_failure(int rank, int iter){
    /*if(leap_loop_count==iter){
        printf("[%d]: Entered inject failure routine.\n", actual_rank);
        fflush(stdout);
    }*/
    if(actual_rank < actual_size/2){
        if(leap_loop_count == iter){
            if(actual_rank == rank){
                printf("[%d]: I am going to have a failure!\n", actual_rank);
                fflush(stdout);
                *(flags_ptr+2*sizeof(int))=1;
                //simulate reboot time
                struct timespec reboot_time;
                reboot_time.tv_sec = REBOOT_TIME;
                reboot_time.tv_nsec = 0;
                nanosleep(&reboot_time, NULL);
#ifdef DEBUG
                printf("[%d] Finished rebooting (%d secs)\n", actual_rank, reboot_time.tv_sec);
                fflush(stdout);
#endif
                shadow_leap(0); // shadow to main leaping
/*                int doneval=1;
                PMPI_Send(&doneval, 1, MPI_INT, (actual_rank+1)%(actual_size/2), MAIN_RECOVERED_TAG, ls_failure_world_comm);*/
/*                int scntr;
                for(scntr=0;scntr<(actual_size/2);scntr++){
                    if(scntr!=rank)
                        PMPI_Send(&doneval, 1, MPI_INT, scntr, MAIN_RECOVERED_TAG, ls_failure_world_comm);
                }*/
//                PMPI_Bcast(&doneval, 1, MPI_INT, rank, ls_data_world_comm);
            }
/*            else if(((actual_rank+1)%(actual_size/2))==rank){//(actual_rank+rank)==(actual_size/2)
#ifdef DEBUG
                printf("[%d] Detected failure of main %d (which also implies my shadow failed). Transferring state to my shadow.\n", actual_rank, rank);
                fflush(stdout);
#endif
                shadow_leap(1);
            }*/
/*            else if((rank+1)%(actual_size/2)==actual_rank){
                 int doneval;
                 int myflag=0;
                 PMPI_Iprobe(rank, MAIN_RECOVERED_TAG, ls_failure_world_comm, &myflag, MPI_STATUS_IGNORE);
                 while(!myflag){*/
/*#ifdef DEBUG
                     printf("[%d] Signalling its colocated shadow while waiting for failing main to recover. Current time : %.3f\n", actual_rank, MPI_Wtime());
                     fflush(stdout);
#endif
                     *flags_ptr=1;*/
/*                     usleep(100);
                     PMPI_Iprobe(rank, MAIN_RECOVERED_TAG, ls_failure_world_comm, &myflag, MPI_STATUS_IGNORE);
                 }
                 PMPI_Recv(&doneval, 1, MPI_INT, rank, MAIN_RECOVERED_TAG, ls_failure_world_comm, MPI_STATUS_IGNORE);
            }*/
/*            else{
                int doneval;
                int myflag=0;
                MPI_Request request;
                PMPI_Ibcast(&doneval, 1, MPI_INT, rank, ls_data_world_comm, &request);
                PMPI_Test(&request, &myflag, MPI_STATUS_IGNORE);
                while(!myflag){*/
/*#ifdef DEBUG
                     printf("[%d] Signalling its colocated shadow while waiting for failing main to recover. Current time : %.3f\n", actual_rank, MPI_Wtime());
                     fflush(stdout);
#endif
                     *flags_ptr=1;*/
/*                     usleep(100);
                     PMPI_Test(&request, &myflag, MPI_STATUS_IGNORE);
                 }
//                 PMPI_Recv(&doneval, 1, MPI_INT, rank, MAIN_RECOVERED_TAG, ls_failure_world_comm, MPI_STATUS_IGNORE);
            }*/
        }
    }
    else{
        if(leap_loop_count == iter){
            if(actual_rank == rank+actual_size/2){
#ifdef DEBUG
                printf("[%d] I am a shadow going to transfer my state to the failing main.\n", actual_rank);
                fflush(stdout);
#endif
                shadow_leap(0);
            }
        }
    }
} 