/*
 * blqueue.h
 *
 *  Created on: Jan 26, 2015
 *      Author: Arne Tossens
 */

#ifndef BLQUEUE_H_
#define BLQUEUE_H_

#include <semaphore.h>
#include <time.h>
#include <sys/time.h>

struct _blqueue {
	pthread_mutex_t mutex;
	sem_t allowPush;
	sem_t allowPop;
	void **data;
	int capacity;
	int readIndex;
	int writeIndex;
	int interrupted;
};

#define BLQUEUE_INTERRUPTED (-2)

int blqueue_init(struct _blqueue *queue, int capacity);
int blqueue_destroy(struct _blqueue *queue);

int blqueue_push(struct _blqueue *queue, void *data);
int blqueue_push_trywait(struct _blqueue *queue, void *data);
int blqueue_push_timedwait(struct _blqueue *queue, void *data, struct timeval *timeout);

int blqueue_pop(struct _blqueue *queue, void **data);
int blqueue_pop_trywait(struct _blqueue *queue, void **data);
int blqueue_pop_timedwait(struct _blqueue *queue, void **data, struct timeval *timeout);

int blqueue_get_count(struct _blqueue *queue, int *count);

int blqueue_push_batch(struct _blqueue *queue, void **batch, int size, int *pushed);
int blqueue_push_batch_trywait(struct _blqueue *queue, void **batch, int size, int *pushed);
int blqueue_push_batch_timedwait(struct _blqueue *queue, void **batch, int size, int *pushed, struct timeval *timeout);

int blqueue_pop_batch(struct _blqueue *queue, void **batch, int size, int *popped);
int blqueue_pop_batch_trywait(struct _blqueue *queue, void **batch, int size, int *popped);
int blqueue_pop_batch_timedwait(struct _blqueue *queue, void **batch, int size, int *popped, struct timeval *timeout);

int blqueue_interrupt(struct _blqueue *queue, void **data, int *nrData);
int blqueue_interrupt_reset(struct _blqueue *queue);

#endif /* BLQUEUE_H_ */
