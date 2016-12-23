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

#ifndef _CIRCBUF_H_
#define _CIRCBUF_H_

#include <vector>
#include <assert.h>

template <class T>
class FixedSizeBuffer
{
public:
	FixedSizeBuffer(int size = 15) : size(size) { buf.resize(size); Reset(); }
	void Put(T& v) { assert(elems < size); iin = (iin + 1) % size; buf[iin] = v; ++elems; }
	T& Get() { assert(elems > 0); T& v = buf[iout]; --elems; iout = (iout + 1) % size; return v; }
	T& GetLast() { assert(elems > 0); return buf[iin]; }
	T& Nth(int n) { assert(elems >= n - 1); return buf[(iout + n) % size]; }
	int Elems() { return elems; }
	bool Empty() { return elems == 0; }
	void Reset() { iin = size - 1; iout = 0; elems = 0; }
private:
	std::vector<T> buf;
	int iin, iout;
	int elems;
	int size;
};


#endif // CIRCBUF_H_
