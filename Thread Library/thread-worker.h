// File:	worker_t.h

/* List all group member's name:
 * Bhavesh Sidhwani (bs1061)
 * Shobhit Singh (ss4363)
*/
// username of iLab: 
// iLab Server: ilab1.cs.rutgers.edu

#ifndef WORKER_T_H
#define WORKER_T_H

#define _GNU_SOURCE
#define QUANTUM 10
#define RESET_QUANTUM 500

/* To use Linux pthread Library in Benchmark, you have to comment the USE_WORKERS macro */
#define USE_WORKERS 1

/* include lib header files that you need here: */
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <ucontext.h>

typedef unsigned int worker_t;

typedef enum thread_status {
	READY,
	RUNNING,
	BLOCKED,
	TERMINATED
} thread_status;

typedef struct TCB {
	/* add important states in a thread control block */
	pthread_t t_id;						// thread Id
	thread_status t_status;				// thread status
	ucontext_t t_ctx;					// thread context
	int t_priority;						// thread priority
	int elapsed;						// time quantum elapsed counter
	struct timespec start, end, resp;
	void *return_val;
	// And more ...

	// YOUR CODE HERE

} tcb; 

/* mutex struct definition */
typedef struct worker_mutex_t {
	/* add something here */

	// YOUR CODE HERE
	int is_lock;			// lock status of mutex
	tcb *t_info;			// thread that locked the mutex
	struct queue *waiting_queue; 	// waiting queue
} worker_mutex_t;

typedef struct queue_node {
	tcb *t_info;
	struct queue_node *next;
} queue_node;

typedef struct queue {
	queue_node *front, *rear;
} queue;
/* define your data structures here: */
// Feel free to add your own auxiliary data structures (linked list or queue etc...)

// YOUR CODE HERE


/* Function Declarations: */

/* create a new thread */
int worker_create(worker_t * thread, pthread_attr_t * attr, void
    *(*function)(void*), void * arg);

/* give CPU pocession to other user level worker threads voluntarily */
int worker_yield();

/* terminate a thread */
void worker_exit(void *value_ptr);

/* wait for thread termination */
int worker_join(worker_t thread, void **value_ptr);

/* initial the mutex lock */
int worker_mutex_init(worker_mutex_t *mutex, const pthread_mutexattr_t *mutexattr);

/* aquire the mutex lock */
int worker_mutex_lock(worker_mutex_t *mutex);

/* release the mutex lock */
int worker_mutex_unlock(worker_mutex_t *mutex);

/* destroy the mutex */
int worker_mutex_destroy(worker_mutex_t *mutex);

/* Create Queue */
queue *create_queue();

/* Create queue node */
queue_node *create_node(tcb *thread);

/* add a thread on the queue rear */
void en_queue(queue *q, tcb *thread);

/* remove a thread from the queue front */
tcb *de_queue(queue *q);


/* Function to print global statistics. Do not modify this function.*/
void print_app_stats(void);

#ifdef USE_WORKERS
#define pthread_t worker_t
#define pthread_mutex_t worker_mutex_t
#define pthread_create worker_create
#define pthread_exit worker_exit
#define pthread_join worker_join
#define pthread_mutex_init worker_mutex_init
#define pthread_mutex_lock worker_mutex_lock
#define pthread_mutex_unlock worker_mutex_unlock
#define pthread_mutex_destroy worker_mutex_destroy
#endif

#endif
