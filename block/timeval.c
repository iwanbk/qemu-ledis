/*
 * timeval.c
 *
 *  Created on: Jan 29, 2015
 *      Author: arne
 */

#include <sys/time.h>
#include "timeval.h"

void timeval_subtract(struct timeval *from, struct timeval *subtract) {
	from->tv_sec -= subtract->tv_sec;
	if (subtract->tv_usec > from->tv_usec) {
		from->tv_sec--;
		from->tv_usec += 1000000;
	}
	from->tv_usec -= subtract->tv_usec;
}

void timeval_add(struct timeval *to, struct timeval *add) {
	to->tv_sec += add->tv_sec;
	to->tv_usec += add->tv_usec;
	if (to->tv_usec >= 1000000) {
		to->tv_sec++;
		to->tv_usec -= 1000000;
	}
}

int timeval_eq(struct timeval *val1, struct timeval *val2) {
	return val1->tv_sec == val2->tv_sec && val1->tv_usec == val2->tv_usec;
}

int timeval_neq(struct timeval *val1, struct timeval *val2) {
	return !(val1->tv_sec == val2->tv_sec && val1->tv_usec == val2->tv_usec);
}

int timeval_lt(struct timeval *val1, struct timeval *val2) {
	if (val1->tv_sec < val2->tv_sec) {
		return 1;
	}
	return val1->tv_usec < val2->tv_usec;
}

int timeval_lte(struct timeval *val1, struct timeval *val2) {
	if (val1->tv_sec <= val2->tv_sec) {
		return 1;
	}
	return val1->tv_usec <= val2->tv_usec;
}

int timeval_gt(struct timeval *val1, struct timeval *val2) {
	if (val1->tv_sec > val2->tv_sec) {
		return 1;
	}
	return val1->tv_usec > val2->tv_usec;
}

int timeval_gte(struct timeval *val1, struct timeval *val2) {
	if (val1->tv_sec >= val2->tv_sec) {
		return 1;
	}
	return val1->tv_usec >= val2->tv_usec;
}

