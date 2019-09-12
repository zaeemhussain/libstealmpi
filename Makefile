FLAGS= -w -fPIC -g -c #-DDEBUG
CFLAGS= -g -shared -Wl,-soname,libstealmpi.so.1 -lrdmacm
_OBJ = collectives_opt.o mpi_override.o monitor_thread.o forwarding_buffer.o socket.o msg_queue.o request_list.o shared_mem.o leap.o
LDIR=$(shell pwd)/bin
OBJ= $(patsubst %,$(LDIR)/%,$(_OBJ))

all: $(LDIR)/libstealmpi.so.1.0

$(LDIR)/libstealmpi.so.1.0: $(OBJ)
	gcc $(CFLAGS) -o $@ $^ 
	ln -sf $@ $(LDIR)/libstealmpi.so.1
	ln -sf $@ $(LDIR)/libstealmpi.so

$(LDIR)/mpi_override.o: mpi_override.c mpi_override.h
	mpicc $(FLAGS) -o $@  $<

$(LDIR)/collectives_opt.o: collectives_opt.c mpi_override.h 
	mpicc $(FLAGS) -o $@  $<

$(LDIR)/request_list.o: request_list.c request_list.h mpi_override.h
	mpicc $(FLAGS) -o $@  $<

$(LDIR)/monitor_thread.o: monitor_thread.c monitor_thread.h mpi_override.h
	mpicc $(FLAGS) -o $@ $<

$(LDIR)/socket.o: socket.c socket.h mpi_override.h
	mpicc $(FLAGS) -o $@ $<

$(LDIR)/msg_queue.o: msg_queue.c msg_queue.h mpi_override.h
	mpicc $(FLAGS) -o $@ $<

$(LDIR)/shared_mem.o: shared_mem.c shared_mem.h mpi_override.h
	mpicc $(FLAGS) -o $@ $<

$(LDIR)/leap.o: leap.c leap.h mpi_override.h
	mpicc $(FLAGS) -o $@  $<

$(LDIR)/forwarding_buffer.o: forwarding_buffer.c forwarding_buffer.h mpi_override.h $(LDIR)/socket.o
	mpicc $(FLAGS) -o $@  $<


clean:
	rm -f $(LDIR)/*