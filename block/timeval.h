/*
 * timeval.h
 *
 *  Created on: Jan 29, 2015
 *      Author: Arne Tossens
 */

#ifndef TIMEVAL_H_
#define TIMEVAL_H_

#include <sys/time.h>

void timeval_subtract(struct timeval *from, struct timeval *subtract);
void timeval_add(struct timeval *to, struct timeval *add);
int timeval_eq(struct timeval *val1, struct timeval *val2);
int timeval_neq(struct timeval *val1, struct timeval *val2);
int timeval_lt(struct timeval *val1, struct timeval *val2);
int timeval_lte(struct timeval *val1, struct timeval *val2);
int timeval_gt(struct timeval *val1, struct timeval *val2);
int timeval_gte(struct timeval *val1, struct timeval *val2);

#endif /* TIMEVAL_H_ */
