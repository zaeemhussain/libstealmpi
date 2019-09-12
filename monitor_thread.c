#include "mpi.h"
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include "monitor_thread.h"
#include "mpi_override.h"


pthread_t ls_monitor_thread, ls_compute_thread;
pthread_attr_t ls_monitor_thread_attr;



void free_thread_resources(){
    pthread_attr_destroy(&ls_monitor_thread_attr);
}


/*monitor thread for shadow process*/
void* monitor_thread_shadow(void* arg){
    
    wait_for_msg();
}


void launch_monitor_thread(int is_main){
    int rc;
    sigset_t set;

    pthread_attr_init(&ls_monitor_thread_attr);
    pthread_attr_setdetachstate(&ls_monitor_thread_attr, PTHREAD_CREATE_JOINABLE);
    ls_compute_thread = pthread_self();
    
    /*block signals for monitor thread*/
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    sigaddset(&set, SIGUSR2);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGALRM);
    if(is_main){
        //rc = pthread_create(&ls_monitor_thread, &ls_monitor_thread_attr, monitor_thread_main, NULL);
    }
    else{
        pthread_sigmask(SIG_BLOCK, &set, NULL);
        rc = pthread_create(&ls_monitor_thread, &ls_monitor_thread_attr, monitor_thread_shadow, NULL);
    }
    if(rc){
        printf("[%d]: ERROR: return code from pthread_create() is %d!\n", actual_rank, rc);
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
#ifdef DEBUG
    printf("[%d] Launched monitor thread\n", actual_rank);
    fflush(stdout);
#endif
    /*unblock signals for compute thread*/
    pthread_sigmask(SIG_UNBLOCK, &set, NULL);
}


