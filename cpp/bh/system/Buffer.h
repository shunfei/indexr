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

#ifndef BUFFER_H_
#define BUFFER_H_

#include "common/bhassert.h"
#include <boost/shared_ptr.hpp>
#include "system/MemoryManagement/TrackableObject.h"

class Buffer
{
public:

	virtual ~Buffer() {};

	virtual char* BufAppend(uint len) = 0;
	virtual char* SeekBack(uint len) = 0;

	virtual int BufSize() = 0;

	// Note: we allow n=buf_used, although it is out of declared limits. Fortunately "buf" is one character longer.
	virtual char* Buf(int n) = 0;

	// in case of incomplete loads (READ mode) - load the next data block into buffer
	virtual int BufFetch(int unused_bytes = 0, int to_read = 0) = 0;

	virtual int BufStatus() = 0;

	virtual void BufFlush() {}

	// write an uchar to the buffer
	virtual int Write(uchar c);
	virtual int WriteIfNonzero(uchar c);

protected:
	int buf_used;			// number of bytes loaded or reserved so far
	char* buf;				//current buf in bufs
	int size;
};

typedef boost::shared_ptr<Buffer> BufferPtr;
#endif /*BUFFER_H_*/
