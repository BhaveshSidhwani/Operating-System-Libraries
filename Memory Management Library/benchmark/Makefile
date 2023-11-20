
all : test
test: ../my_vm.h
	gcc test.c -L../ -lmy_vm -m32 -lm -o test
	gcc multi_test.c -L../ -lmy_vm -m32 -lm -o mtest -lpthread
	gcc test.c -L../ -lmy_vm64 -lm -o test64
	gcc multi_test.c -L../ -lmy_vm64 -lm -o mtest64 -lpthread

clean:
	rm -rf test mtest test64 mtest64
