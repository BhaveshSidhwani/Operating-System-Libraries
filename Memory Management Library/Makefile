CC = gcc
CFLAGS = -g -c -m32 -lm
CFLAGS_64 = -g -c -lm
AR = ar -rc
RANLIB = ranlib

all: my_vm.a my_vm64.a

my_vm.a: my_vm.o
	$(AR) libmy_vm.a my_vm.o
	$(RANLIB) libmy_vm.a

my_vm64.a: my_vm64.o
	$(AR) libmy_vm64.a my_vm64.o
	$(RANLIB) libmy_vm64.a

my_vm.o: my_vm.h

	$(CC)	$(CFLAGS)  my_vm.c

my_vm64.o: my_vm.h

	$(CC)	$(CFLAGS_64)  my_vm64.c

clean:
	rm -rf *.o *.a
