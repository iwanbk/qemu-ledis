/*
 * blqueue.c
 *
 *  Created on: Jan 26, 2015
 *      Author: Arne Tossens
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "blqueue.h"
#include "timeval.h"

// Initialization, finalization

int blqueue_init(struct _blqueue *queue, int capacity) {
	memset(queue, 0, sizeof(struct _blqueue));
	queue->capacity = capacity;
	size_t size = sizeof(void *) * capacity;
	queue->data = (void **) malloc(size);
	memset(queue->data, 0, size);
	pthread_mutex_init(&queue->mutex, NULL);
	sem_init(&queue->allowPop, 0, 0);
	sem_init(&queue->allowPush, 0, capacity);
	return 0;
}

int blqueue_destroy(struct _blqueue *queue) {
	sem_destroy(&queue->allowPush);
	sem_destroy(&queue->allowPop);
	pthread_mutex_destroy(&queue->mutex);
	free(queue->data);
	memset(queue, 0, sizeof(struct _blqueue));
	return 0;
}

// Push

static int _push(struct _blqueue *queue, void *data) {
	if (queue->interrupted) {
		return BLQUEUE_INTERRUPTED;
	}
	pthread_mutex_lock(&queue->mutex);
	if (queue->interrupted) {
		pthread_mutex_unlock(&queue->mutex);
		return BLQUEUE_INTERRUPTED;
	}
	queue->data[queue->writeIndex++] = data;
	if (queue->writeIndex == queue->capacity) {
		queue->writeIndex = 0;
	}
	pthread_mutex_unlock(&queue->mutex);
	sem_post(&queue->allowPop);
	return 0;
}

int blqueue_push(struct _blqueue *queue, void *data) {
	int retval = sem_wait(&queue->allowPush);
	if (retval != 0) {
		return retval;
	}
	return _push(queue, data);
}

int blqueue_push_trywait(struct _blqueue *queue, void *data) {
	int retval = sem_trywait(&queue->allowPush);
	if (retval != 0) {
		return retval;
	}
	return _push(queue, data);
}

static int _push_abstimedwait(struct _blqueue *queue, void *data, struct timespec *abstimeout) {
	int retval = sem_timedwait(&queue->allowPush, abstimeout);
	if (retval != 0) {
		return retval;
	}
	return _push(queue, data);
}

int blqueue_push_timedwait(struct _blqueue *queue, void *data, struct timeval *timeout) {
	struct timespec abstimeout = { timeout->tv_sec, timeout->tv_usec * 1000 };
	return _push_abstimedwait(queue, data, &abstimeout);
}

// Pop

static int _pop(struct _blqueue *queue, void **data) {
	if (queue->interrupted) {
		return BLQUEUE_INTERRUPTED;
	}
	pthread_mutex_lock(&queue->mutex);
	if (queue->interrupted) {
		pthread_mutex_unlock(&queue->mutex);
		return BLQUEUE_INTERRUPTED;
	}
	*data = queue->data[queue->readIndex++];
	if (queue->readIndex == queue->capacity) {
		queue->readIndex = 0;
	}
	pthread_mutex_unlock(&queue->mutex);
	sem_post(&queue->allowPush);
	return 0;
}

int blqueue_pop(struct _blqueue *queue, void **data) {
	int retval = sem_wait(&queue->allowPop);
	if (retval != 0) {
		return retval;
	}
	return _pop(queue, data);
}

int blqueue_pop_trywait(struct _blqueue *queue, void **data) {
	int retval = sem_trywait(&queue->allowPop);
	if (retval != 0) {
		return retval;
	}
	return _pop(queue, data);
}

static int _pop_abstimedwait(struct _blqueue *queue, void **data, struct timespec *abstimeout) {
	int retval = sem_timedwait(&queue->allowPop, abstimeout);
	if (retval != 0) {
		return retval;
	}
	return _pop(queue, data);
}

int blqueue_pop_timedwait(struct _blqueue *queue, void **data, struct timeval *timeout) {
	struct timespec abstimeout = { timeout->tv_sec, timeout->tv_usec * 1000 };
	return _pop_abstimedwait(queue, data, &abstimeout);
}

// Count

int blqueue_get_count(struct _blqueue *queue, int *count) {
	return sem_getvalue(&queue->allowPop, count);
}

// Push batch

int blqueue_push_batch(struct _blqueue *queue, void **batch, int size, int *pushed) {
	int i, retval;
	for (i = 0; i < size; i++) {
		retval = blqueue_push(queue, batch[i]);
		if (retval != 0) {
			break;
		}
	}
	*pushed = i;
	return retval;
}

int blqueue_push_batch_trywait(struct _blqueue *queue, void **batch, int size, int *pushed) {
	int i, retval;
	for (i = 0; i < size; i++) {
		retval = blqueue_push_trywait(queue, batch[i]);
		if (retval != 0) {
			break;
		}
	}
	*pushed = i;
	return retval;
}

int blqueue_push_batch_timedwait(struct _blqueue *queue, void **batch, int size, int *pushed, struct timeval *timeout) {
	struct timespec abstimeout;
	struct timeval end;
	int i, retval;
	gettimeofday(&end, NULL);
	timeval_add(&end, timeout);
	abstimeout.tv_sec = end.tv_sec;
	abstimeout.tv_nsec = end.tv_usec * 1000;
	for (i = 0; i < size; i++) {
		retval = _push_abstimedwait(queue, batch[i], &abstimeout);
		if (retval != 0) {
			break;
		}
	}
	*pushed = i;
	return retval;
}

// Pop batch

int blqueue_pop_batch(struct _blqueue *queue, void **batch, int size, int *popped) {
	int i, retval;
	for (i = 0; i < size; i++) {
		retval = blqueue_pop(queue, &batch[i]);
		if (retval != 0) {
			break;
		}
	}
	*popped = i;
	return retval;
}

int blqueue_pop_batch_trywait(struct _blqueue *queue, void **batch, int size, int *popped) {
	int i, retval;
	for (i = 0; i < size; i++) {
		retval = blqueue_pop_trywait(queue, &batch[i]);
		if (retval != 0) {
			break;
		}
	}
	*popped = i;
	return retval;
}

int blqueue_pop_batch_timedwait(struct _blqueue *queue, void **batch, int size, int *popped, struct timeval *timeout) {
	struct timespec abstimeout;
	struct timeval end;
	int i, retval;
	gettimeofday(&end, NULL);
	timeval_add(&end, timeout);
	abstimeout.tv_sec = end.tv_sec;
	abstimeout.tv_nsec = end.tv_usec * 1000;
	for (i = 0; i < size; i++) {
		retval = _pop_abstimedwait(queue, &batch[i], &abstimeout);
		if (retval != 0) {
			break;
		}
	}
	*popped = i;
	return retval;
}

// Interrupt

int blqueue_interrupt(struct _blqueue *queue, void **data, int *nrData) {
	int count, i;
	pthread_mutex_lock(&queue->mutex);
	queue->interrupted = 1;

	// save messages first
	i = 0;
	sem_getvalue(&queue->allowPop, &count);
	while (count > 0) {
		data[i++] = queue->data[queue->readIndex++];
		if (queue->readIndex == queue->capacity) {
			queue->readIndex = 0;
		}
		count--;
	}
	*nrData = i;

	// unblock push waiters - so they see the interrupt
	sem_getvalue(&queue->allowPush, &count);
	while (count < queue->capacity) {
		sem_post(&queue->allowPush);
		count++;
	}
	// unblock pop waiters - so they see the interrupt
	sem_getvalue(&queue->allowPop, &count);
	while (count < queue->capacity) {
		sem_post(&queue->allowPop);
		count++;
	}

	pthread_mutex_unlock(&queue->mutex);
	return 0;
}

int blqueue_interrupt_reset(struct _blqueue *queue) {
	int count;
	pthread_mutex_lock(&queue->mutex);
	if (!queue->interrupted) {
		pthread_mutex_unlock(&queue->mutex);
		return 0;
	}
	// allowPush is already at capacity
	// restore allowPop to 0
	sem_getvalue(&queue->allowPop, &count);
	while (count > 0) {
		sem_wait(&queue->allowPop);
		count--;
	}
	queue->interrupted = 0;
	pthread_mutex_unlock(&queue->mutex);
	return 0;
}
