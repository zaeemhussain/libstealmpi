#ifndef __MPI_OVERRIDE_H__
#define __MPI_OVERRIDE_H__

#define SHADOW_LEAP_TAG 1
#define SHADOW_SRC_TAG 2
#define SHADOW_RECV_POINT_TAG 3
#define SHADOW_RECV_COUNT_TAG 4
#define SHADOW_SYNC_TAG 5
#define SHADOW_CART_NEWRANK_TAG 6
#define SHADOW_IP_TAG 7
#define SHADOW_PROBE_TAG 8
#define SHADOW_TEST_TAG 9
#define SHADOW_WAITANY_TAG 10
#define SHADOW_WAITSOME_TAG 11
#define SHADOW_TESTALL_TAG 12
#define SHADOW_MAIN_FINALIZE_TAG 13
#define MAIN_RECOVERED_TAG 14
#define SHADOW_FORCE_LEAPING_TAG 15
#define SHADOW_FORCE_LEAPING_LOGCOUNT_TAG 16
#define SHADOW_LEAPING 
#define LS_MSG_SRC_BUF_LENGTH 65536 

#define USLEEP_TIME 50

extern int actual_rank, actual_size, term_flag, full_speed_flag, exflag, cterm_flag, mainsleeptime;
extern int shStart, appSize, shEnd, cStart, cEnd;
extern int sleepcount, sleepiters;
extern int bcastCallCount, reduceCallCount, allreduceCallCount, alltoallCallCount, alltoallvCallCount, gatherCallCount;
extern int bcastPrCount, reducePrCount, allreducePrCount, alltoallPrCount, alltoallvPrCount, gatherPrCount;
extern int allgatherCallCount, gathervCallCount, allgathervCallCount, scatterCallCount, scanCallCount, scattervCallCount;
extern int allgatherPrCount, gathervPrCount, allgathervPrCount, scatterPrCount, scanPrCount, scattervPrCount;
extern int reducescatterCallCount, reducescatterblockCallCount;
extern int reducescatterPrCount, reducescatterblockPrCount;
extern int ls_data_msg_count, need2leap;
extern long long ls_data_msg_counter;
extern long long ls_dat_msg_len;
extern MPI_Comm ls_data_world_comm, ls_cntr_world_comm, ls_failure_world_comm;
extern double mpicommtime, sockcommtime, sleeptime;
extern int *flags_ptr;

#endif