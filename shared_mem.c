#include <stdio.h>
#include <stdlib.h>
#include "shared_mem.h"
#include "mpi_override.h"
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>

int fd;

int create_shared_mem(){
    int myrank = actual_rank;
    if( myrank >= appSize){
        myrank = (actual_rank - appSize + cStart) % appSize;
    }
    int length = snprintf( NULL, 0, "/procrank%d", myrank );
    char* str = malloc( length + 1 );
    snprintf( str, length + 1, "/procrank%d", myrank );
    fd = shm_open(str, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    
    if (fd == -1){
        /* Handle error */;
        printf("[%d]: Failed to open shared memory object. Error number is %d.\n", actual_rank, errno);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if (ftruncate(fd, 3*sizeof(int)) == -1){
        /* Handle error */;
        printf("[%d]: Failed in ftruncate.\n", actual_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    flags_ptr = mmap(NULL, 3*sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (flags_ptr == MAP_FAILED){
        /* Handle error */;
        printf("[%d]: Failed in mmap.\n", actual_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if( actual_rank < appSize ){    
        *flags_ptr = 0;
        *(flags_ptr+sizeof(int)) = 0;
        *(flags_ptr+2*sizeof(int)) = 0;
    }
    free(str);
    return 0;
}

int close_shared_mem(){
    int olderror;
    if (munmap(flags_ptr, 3*sizeof(int)) == -1) {
        printf("[%d]: Failed in munmap.\n", actual_rank);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    olderror=errno;
    errno=0;
    if (close(fd) == -1) {
        printf("[%d]: Failed to close shared object file. Error number is %d. Old value of errno was %d.\n", actual_rank, errno, olderror);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
    if( actual_rank >= appSize ){
        int myrank = (actual_rank - appSize + cStart) % appSize;
        int length = snprintf( NULL, 0, "/procrank%d", myrank );
        char* str = malloc( length + 1 );
        snprintf( str, length + 1, "/procrank%d", myrank );
        olderror=errno;
        errno=0;
        if (shm_unlink(str) == -1) {
            printf("[%d]: Failed to remove file from filesystem. Error number is %d. Old value of errno was %d.\n", actual_rank, errno, olderror);
            //MPI_Abort(MPI_COMM_WORLD, -1);
        }
        free(str);
    }
    return 0;
}