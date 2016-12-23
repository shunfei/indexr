/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#ifndef _BHQSORT_H_
#define _BHQSORT_H_

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef int (*comp_func_ib) (const void *, const void *);

inline void __ib_swap_local(char *a, char* b, int len)
{
  register int j;
  register char t;
  for (j=0; j<len; j++)
  {
    t = a[j];
    a[j] = b[j];
    b[j] = t;
  } 
}

static void __bubble_sort(char* b, int lo, int hi, int s, comp_func_ib cmpsrt)
{
	bool swapped = true;
	int i;

	while (swapped)
	{
		// forward direction
		swapped = false;
		for (i = lo; i < hi; i++)
		{
			if (cmpsrt((void *)(b + i*s), (void *)(b + (i+1)*s)) > 0)
			{
				__ib_swap_local(b + i*s, b + (i+1)*s, s);
				swapped = true;
			}
		}

		// no swap - means sorted.
		if (!swapped)
			break;

		// backward direction
		swapped = false;
		hi --;

		for (i = hi; i > lo; i--)
		{
			if (cmpsrt((void *)(b + i*s), (void *)(b + (i-1)*s)) < 0)
			{
				__ib_swap_local(b + i*s, b + (i-1)*s, s);
				swapped = true;
			}
		}
		lo ++;
	}
}

#define PIVOT_SIZE 32

static void __quicksort_ib(char* b, int lo, int hi, int s, comp_func_ib cmpsrt, int call_seq)
{
	if (call_seq > 8192)
	{
		__bubble_sort(b, lo, hi, s, cmpsrt);
	  return;
	}
    register int i = lo;
    register int j = hi;
    register int pv = (lo + hi)/2;

    char _space[PIVOT_SIZE];
    register char* pivot = _space;

    // actual byte position for elements where i, j are the index position.
    register int ii = i*s;
    register int jj = j*s;

    if (s > PIVOT_SIZE)
    {
      pivot = (char *)malloc(s);
      if (!pivot) return;
    }
	memcpy(pivot, b + pv*s, s);

    while (i <= j)
    {
      while (cmpsrt((void *)(b + ii), (void *)pivot) < 0) { i++; ii += s; };
      while (cmpsrt((void *)(b + jj), (void *)pivot) > 0) { j--; jj -= s; };
      if (i <= j)
      {
        __ib_swap_local(b + ii, b + jj, s);
		i++; ii += s;
		j--; jj -= s;
      }
    }
	if (lo < j) __quicksort_ib(b, lo, j, s, cmpsrt, call_seq + 1);
	if (i < hi) __quicksort_ib(b, i, hi, s, cmpsrt, call_seq + 1);

    if (s > PIVOT_SIZE)
      free(pivot);
}

// buf, total elements, element size, compare function
static void qsort_ib(void* b, int l, int s, comp_func_ib cmpsrt)
{
  __quicksort_ib((char*) b, 0, l - 1, s, cmpsrt, 0);
}

// buf, total elements, element size, compare function
static void bubblesort_ib(void* b, int l, int s, comp_func_ib cmpsrt)
{
	__bubble_sort((char*) b, 0, l - 1, s, cmpsrt);
}

#endif  //_BHQSORT_H_
