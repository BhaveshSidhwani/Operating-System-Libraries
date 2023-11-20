// File:	thread-worker.c

/* List all group member's name:
 * Bhavesh Sidhwani (bs1061)
 * Shobhit Singh (ss4363)
*/
// username of iLab: 
// iLab Server: ilab1.cs.rutgers.edu

#include <ucontext.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "thread-worker.h"

#define STACK_SIZE SIGSTKSZ
#define QUEUE_NUM 4

//Global counter for total context switches and 
//average turn around and response time
long tot_cntx_switches=0;
double avg_turn_time=0;
double avg_resp_time=0;


// INITAILIZE ALL YOUR OTHER VARIABLES HERE
// YOUR CODE HERE
static int thread_id = 0;
static int resp_threads = 0;
static int term_threads = 0;
static int is_init = 0;
static int is_timer_interrupt = 0;
static int is_terminated[1000];

static tcb *running_thread;
static tcb *exit_thread;
static tcb *scheduler_thread;

static struct sigaction timer_handler;
static struct itimerval timer;

static queue *run_queue[QUEUE_NUM];
static queue *threads;

static struct timespec start, end;

static void schedule();
static void sched_psjf();
static void sched_mlfq();

void timer_interrupt(int signum) {
	running_thread->t_status = READY;
	is_timer_interrupt = 1;
	tot_cntx_switches ++;
	swapcontext(&running_thread->t_ctx, &scheduler_thread->t_ctx);
}

void one_time_init() {
	/* Queue creation */
	for (int i=0; i<QUEUE_NUM; i++) {
		run_queue[i] = create_queue();
	}
	threads = create_queue();

	/* Scheduler context */
	scheduler_thread = (tcb*) malloc(sizeof(tcb));
	if (getcontext(&(scheduler_thread->t_ctx)) < 0) {
		perror("getcontext");
		exit(EXIT_FAILURE);
	}
	scheduler_thread->t_id = thread_id;
	thread_id++;
	scheduler_thread->t_ctx.uc_link = NULL;
	scheduler_thread->t_ctx.uc_stack.ss_sp = malloc(STACK_SIZE);
	scheduler_thread->t_ctx.uc_stack.ss_size = STACK_SIZE;
	scheduler_thread->t_ctx.uc_stack.ss_flags = 0;
	makecontext(&(scheduler_thread->t_ctx), (void *)&schedule, 0);

	// printf("Scheduler id: %d ...\n", scheduler_thread->t_id);
	tcb *main_thread = (tcb*) malloc(sizeof(tcb));
	// - create and initialize the context of this worker thread
	if (getcontext(&(main_thread->t_ctx)) < 0) {
		perror("getcontext");
		exit(EXIT_FAILURE);
	}

	main_thread->t_id = thread_id;
	thread_id++;
	main_thread->t_status = RUNNING;
	main_thread->t_priority = 0;
	main_thread->elapsed = 0;
	main_thread->t_ctx.uc_link = NULL;
	main_thread->t_ctx.uc_stack.ss_sp = malloc(STACK_SIZE);
	main_thread->t_ctx.uc_stack.ss_size = STACK_SIZE;
	main_thread->t_ctx.uc_stack.ss_flags = 0;
	running_thread = main_thread;
	en_queue(run_queue[0], main_thread);
	en_queue(threads, main_thread);

	exit_thread = (tcb*) malloc(sizeof(tcb));
	if (getcontext(&(exit_thread->t_ctx)) < 0) {
		perror("getcontext");
		exit(EXIT_FAILURE);
	}

	exit_thread->t_ctx.uc_link = NULL;
	exit_thread->t_ctx.uc_stack.ss_sp = malloc(STACK_SIZE);
	exit_thread->t_ctx.uc_stack.ss_size = STACK_SIZE;
	exit_thread->t_ctx.uc_stack.ss_flags = 0;
	makecontext(&(exit_thread->t_ctx), (void *) &worker_exit, 0);

	/* Timer initialize */
	memset(&timer_handler, 0, sizeof(timer_handler));
	timer_handler.sa_handler = &timer_interrupt;
	sigaction(SIGPROF, &timer_handler, NULL);
	timer.it_interval.tv_usec = QUANTUM * 1000;
	timer.it_interval.tv_sec = 0;
	timer.it_value.tv_usec = QUANTUM * 1000;
	timer.it_value.tv_sec = 0;

	setitimer(ITIMER_PROF, &timer, NULL);
	
	is_init = 1;
}

/* create a new thread */
int worker_create(worker_t * thread, pthread_attr_t * attr, 
					void *(*function)(void*), void * arg) {

	if (is_init == 0) {
		one_time_init();
		clock_gettime(CLOCK_REALTIME, &start);
	}
	
	// - create Thread Control Block (TCB)
	tcb *new_thread = (tcb*) malloc(sizeof(tcb));
	// - create and initialize the context of this worker thread
	if (getcontext(&(new_thread->t_ctx)) < 0) {
		perror("getcontext");
		exit(EXIT_FAILURE);
	}

	*thread = thread_id;

	new_thread->t_id = thread_id;
	thread_id++;
	new_thread->t_status = READY;
	new_thread->t_priority = 0;
	new_thread->elapsed = 0;
	new_thread->t_ctx.uc_link = &exit_thread->t_ctx;
	new_thread->t_ctx.uc_stack.ss_sp = malloc(STACK_SIZE);
	new_thread->t_ctx.uc_stack.ss_size = STACK_SIZE;
	new_thread->t_ctx.uc_stack.ss_flags = 0;
	makecontext(&new_thread->t_ctx, (void *) function, 1, arg);

	clock_gettime(CLOCK_REALTIME, &(new_thread->start));

	en_queue(run_queue[0], new_thread);
	en_queue(threads, new_thread);

	// - allocate space of stack for this thread to run
	// after everything is set, push this thread into run queue and 
	// - make it ready for the execution.

	// YOUR CODE HERE
	
	return 0;
};

/* give CPU possession to other user-level worker threads voluntarily */
int worker_yield() {
	
	// - change worker thread's state from Running to Ready
	running_thread->t_status = READY;
	// - save context of this thread to its thread control block
	// - switch from thread context to scheduler context
	tot_cntx_switches ++;
	if (swapcontext(&running_thread->t_ctx, &scheduler_thread->t_ctx)) {
		printf("Context Swapping Error\n");
		return -1;
	}

	// YOUR CODE HERE
	
	return 0;
};

/* terminate a thread */
void worker_exit(void *value_ptr) {
	// - de-allocate any dynamic memory created when starting this thread
	running_thread->t_status = TERMINATED;
	running_thread->return_val = value_ptr;
	is_terminated[(int)(running_thread->t_id)] = 1;
	clock_gettime(CLOCK_REALTIME, &(running_thread->end));
	struct timespec temp_end = running_thread->end;
	struct timespec temp_start = running_thread->start;
	unsigned long int diff = ((temp_end.tv_sec-temp_start.tv_sec)*1000) + ((temp_end.tv_nsec-temp_start.tv_nsec)/1000000);
	avg_turn_time = ((avg_turn_time*term_threads) + diff);
	term_threads++;
	avg_turn_time = avg_turn_time / term_threads;

	free(running_thread->t_ctx.uc_stack.ss_sp);
	tot_cntx_switches ++;
	setcontext(&scheduler_thread->t_ctx);

	// YOUR CODE HERE
};


/* Wait for thread termination */
int worker_join(worker_t thread, void **value_ptr) {
	queue_node *temp = threads->front;
	while (temp->t_info->t_id == thread) {
		temp = temp->next;
	}

	while (is_terminated[(int)(thread)] != 1) {}
	if (value_ptr) {
		*value_ptr = temp->t_info->return_val;
	}
	
	// - wait for a specific thread to terminate
	// - de-allocate any dynamic memory created by the joining thread

	// YOUR CODE HERE
	return 0;
};

/* initialize the mutex lock */
int worker_mutex_init(worker_mutex_t *mutex, 
						const pthread_mutexattr_t *mutexattr) {
	//- initialize data structures for this mutex

	// YOUR CODE HERE
	mutex->is_lock = 0;
	mutex->t_info = NULL;
	mutex->waiting_queue = create_queue();
	return 0;
};

/* aquire the mutex lock */
int worker_mutex_lock(worker_mutex_t *mutex) {

	// - use the built-in test-and-set atomic function to test the mutex
	// - if the mutex is acquired successfully, enter the critical section
	// - if acquiring mutex fails, push current thread into block list and
	// context switch to the scheduler thread

	// YOUR CODE HERE
	while (__sync_lock_test_and_set(&(mutex->is_lock), 1) == 1) {
		en_queue(mutex->waiting_queue, running_thread);
		running_thread->t_status = BLOCKED;
		tot_cntx_switches ++;
		swapcontext(&running_thread->t_ctx, &scheduler_thread->t_ctx);
	}
	mutex->is_lock = 1;
	mutex->t_info = running_thread;

	return 0;
};

/* release the mutex lock */
int worker_mutex_unlock(worker_mutex_t *mutex) {
	// - release mutex and make it available again. 
	// - put threads in block list to run queue 
	// so that they could compete for mutex later.

	// YOUR CODE HERE
	if (mutex->waiting_queue->front != NULL) {
		queue_node *temp = mutex->waiting_queue->front;
		while (temp != NULL) {
			temp->t_info->t_status = READY;
			int p = temp->t_info->t_priority;
			en_queue(run_queue[p], temp->t_info);
		}
	}
	__sync_lock_release(&(mutex->is_lock));
	mutex->t_info = NULL;

	return 0;
};


/* destroy the mutex */
int worker_mutex_destroy(worker_mutex_t *mutex) {
	// - de-allocate dynamic memory created in worker_mutex_init
	free(mutex->waiting_queue);

	return 0;
};


queue* create_queue() {
	queue *q = (queue*) malloc(sizeof(queue));
	q->front = NULL;
	q->rear = NULL;

	return q;
}

queue_node *create_node(tcb *thread) {
	queue_node *new_node = (queue_node*) malloc(sizeof(queue_node));
	new_node->t_info = thread;
	new_node->next = NULL;

	return new_node;
}

/* add a thread on the queue rear */
void en_queue(queue *q, tcb *thread) {
	queue_node *new_node = create_node(thread);

	if (q->rear == NULL) {
		q->front = new_node;
		q->rear = new_node;
	} else {
		q->rear->next = new_node;
		q->rear = new_node;
	}
}

/* remove a thread from the queue front */
tcb* de_queue(queue *q) {
	tcb *temp;
	if (q->front == NULL) {
		return NULL;
	} else if (q->front == q->rear) {
		temp = q->front->t_info;
		q->rear = NULL;
		q->front = NULL;
	} else {
		temp = q->front->t_info;
		q->front = q->front->next;
	}

	return temp;
}

/* scheduler */
static void schedule() {
	// - every time a timer interrupt occurs, your worker thread library 
	// should be contexted switched from a thread context to this 
	// schedule() function

	// - invoke scheduling algorithms according to the policy (PSJF or MLFQ)

	// if (sched == PSJF)
	//		sched_psjf();
	// else if (sched == MLFQ)
	//		sched_mlfq();

	// YOUR CODE HERE

// - schedule policy
#ifndef MLFQ
	// Choose PSJF
	sched_psjf();
#else 
	// Choose MLFQ
	sched_mlfq();
#endif

}

/* Pre-emptive Shortest Job First (POLICY_PSJF) scheduling algorithm */
static void sched_psjf() {
	// - your own implementation of PSJF
	// (feel free to modify arguments and return types)

	// YOUR CODE HERE
	running_thread->elapsed++;
	// En-queuing the current thread if it has not yet completed execution
	if (running_thread->t_status == READY) {
		en_queue(run_queue[0], running_thread);
	}

	if (run_queue[0]->front == NULL) {
		return;
	}
	
	// Getting the thread that has minimum time elapsed
	queue_node *temp = run_queue[0]->front;
	queue_node *min = temp;
	while (temp != NULL) {
		if ((min->t_info->elapsed > temp->t_info->elapsed) && (temp->t_info->t_status == READY)) {
			min = temp;
		}
		temp = temp->next;
	}

	// De queuing the thread with the minimum time elapsed
	temp = run_queue[0]->front;	
	if (run_queue[0]->front->next == NULL) {
		run_queue[0]->front = NULL;
		run_queue[0]->rear = NULL;
	} else if (temp == min) {
		run_queue[0]->front = temp->next;
	} else {
		while (temp->next != min) {
			temp = temp->next;
		}
		temp->next = min->next;
		if (min == run_queue[0]->rear) {
			run_queue[0]->rear = temp;
		}
	}

	// Updating Running thread
	running_thread = min->t_info;
	running_thread->t_status = RUNNING;

	// Calculating response time
	if (running_thread->elapsed == 0) {
		clock_gettime(CLOCK_REALTIME, &(running_thread->resp));
		struct timespec temp_resp = running_thread->resp;
		struct timespec temp_start = running_thread->start;
		unsigned long int diff = ((temp_resp.tv_sec-temp_start.tv_sec)*1000) + ((temp_resp.tv_nsec-temp_start.tv_nsec)/1000000);
		avg_resp_time = ((avg_resp_time*resp_threads) + diff);
		resp_threads++;
		avg_resp_time = avg_resp_time / resp_threads;
	}

	// Setting context of the thread with minimum time elapsed
	tot_cntx_switches ++;
	setcontext(&running_thread->t_ctx);
}


/* Preemptive MLFQ scheduling algorithm */
static void sched_mlfq() {
	// - your own implementation of MLFQ
	// (feel free to modify arguments and return types)

	// YOUR CODE HERE

	// Get end time to check if the reset time quantum is up or not
	clock_gettime(CLOCK_REALTIME, &end);
	unsigned long int diff = ((end.tv_sec-start.tv_sec)*1000) + ((end.tv_nsec-start.tv_nsec)/1000000);

	// if Reset time quantum is up then move every thread to the top priority queue
	if (diff >= RESET_QUANTUM) {
		queue_node *temp = threads->front;
		while (temp != NULL) {
			temp->t_info->t_priority = 0;
			temp = temp->next;
		}
		clock_gettime(CLOCK_REALTIME, &start);
	} else {
		/* If scheduler context is switched due to timer interrupt 
		* then lower the priority of the current thread
		*/
		if (is_timer_interrupt == 1) {
			int p = running_thread->t_priority;
			int new_p = running_thread->t_priority+1;
			// Bottom most priority remains the same
			if (running_thread->t_priority == QUEUE_NUM-1) {
				new_p = running_thread->t_priority;
			}
			// En-queue the current thread to the rear of the lower priority queue
			if (running_thread->t_status == READY) {
				en_queue(run_queue[new_p], de_queue(run_queue[p]));
				running_thread->t_priority = new_p;
			}
		is_timer_interrupt = 0;
		} else {
			// If thread has yielded then priority stays the same
			int p = running_thread->t_priority;
			if (running_thread->t_status == READY) {
				en_queue(run_queue[p], de_queue(run_queue[p]));
			}
		}
	}

	/* Iterate through queue from top most to bottom most priority
	*  and select the first available thread having hghest priority
	*/
	for (int i=0; i<QUEUE_NUM; i++) {
		if (run_queue[i]->front != NULL) {
			running_thread = run_queue[i]->front->t_info;
			running_thread->t_status = RUNNING;

			// Calculate response time
			if (running_thread->elapsed == 0) {
				clock_gettime(CLOCK_REALTIME, &(running_thread->resp));
				struct timespec temp_resp = running_thread->resp;
				struct timespec temp_start = running_thread->start;
				unsigned long int diff = ((temp_resp.tv_sec-temp_start.tv_sec)*1000) + ((temp_resp.tv_nsec-temp_start.tv_nsec)/1000000);
				avg_resp_time = ((avg_resp_time*resp_threads) + diff);
				resp_threads++;
				avg_resp_time = avg_resp_time / resp_threads;
			}

			// Context switch to the selected thread
			tot_cntx_switches ++;
			setcontext(&running_thread->t_ctx);
		}
	}
}

//DO NOT MODIFY THIS FUNCTION
/* Function to print global statistics. Do not modify this function.*/
void print_app_stats(void) {

	   fprintf(stderr, "Total context switches %ld \n", tot_cntx_switches);
	   fprintf(stderr, "Average turnaround time %lf \n", avg_turn_time);
	   fprintf(stderr, "Average response time  %lf \n", avg_resp_time);
}


// Feel free to add any other functions you need

// YOUR CODE HERE
