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

#ifndef _SYSTEM_LARGEBUF_H_
#define _SYSTEM_LARGEBUF_H_

#include <boost/shared_ptr.hpp>
#pragma warning( disable : 4267 )
#include <boost/thread.hpp>
#pragma warning( default : 4267 )

#include "common/CommonDefinitions.h"
#include "MemoryManagement/TrackableObject.h"
#include "system/Buffer.h"

class IOParameters;
class IBStream;
//////////////////////////////////////////////////////////////////////////////////////////
///////////////////  LargeBuffer:   /////////////////////////////////////////////////////////
//
//	Handles the large 64 MB buffer for fast file access
//


enum BufOpenType {READ,OVERWRITE,APPEND};

#ifndef __GNUC__
#define PIPE_BUF                    1024
#endif

#define	DEFAULT_BUFFERS_SIZE		(64 << 20)
#define	PIPE_BUFFER_SIZE			(64 << 10)
#define	WRITE_TO_PIPE_NO_BYTES		PIPE_BUF
#define WRITE_TO_FILE_NO_BYTES		(4 << 20)
#define PIPE_TIMEOUT				10000

#ifdef _MSC_VER
#define INVALID_THREAD INVALID_HANDLE_VALUE
#else
#define INVALID_THREAD -1
#endif

class LargeBuffer : public Buffer, public TrackableObject
{
public:
	LargeBuffer(int no_of_bufs = 3, int size = DEFAULT_BUFFERS_SIZE);
	virtual ~LargeBuffer();

	//////////////////////////////////////////////////////////////////////////////////////
	///////////////////  File/pipe handling   ////////////////////////////////////////////
	//void InitLargeBuffer(int size = DEFAULT_BUFFERS_SIZE);
	bool BufOpen(const IOParameters& iop, BufOpenType mode);	// open file (closing the previous one) in mode:
													//	- READ:			...
													//	- OVERWRITE:	...
													//	- APPEND:		...
	bool StartReadingThread();
	bool StartPipeReadingThread();
	int BufFetch(int unused_bytes = 0, int to_read = 0);			// in case of incomplete loads (READ mode) - load the next data block into buffer
	void BufFlush();		// save the data to the file (in writing mode)
	void BufClose();		// close the buffer; warning: does not flush data in writing mode
	void FlushAndClose();
	int BufStatus()			// 0 - closed, 1 - read, 2 - overwrite, 3 - append
	{ return buf_status; }
	bool IsAllocated();
	int BufSize()			{ return buf_used; }
	int Size()				{return size;}
    char* BufAppend(unsigned int len);
    char* SeekBack(uint len);
	int IsCompleted() {return !buf_incomplete;};
	// direct access to the buffer (declared part only):
	char *Buf(int n)		{ assert(n>=0 && n<=buf_used); return buf+n; }			// Note: we allow n=buf_used, although it is out of declared limits. Fortunately "buf" is one character longer.
	int Read(IBStream *file, char* buffer, int bytes_to_read);

	void MarkUseOf(int buf_no);
	void ReleaseFromUse(int buf_no);
	int GetCurrentBufNo() {return curr_buf_no;};
	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_TEMPORARY;}

private:
	// File/pipe management
	char** bufs;				// data buffers; undefined outside [0..(buf_used-1)]
	int curr_buf_no;				//buf = bufs + currBufNo
	char* buf2;					//buf to be used next
	int curr_buf2_no;
	int* usage_of_bufs;
	int num_of_bufs;
	char* read_thread_buffer;
	int bytes_in_read_thread_buffer;
	int buf_status;			// 0 - not used (closed or failed to open file), 1 - in use, file loaded to read, 2 - in use for overwriting, 3 - in use for appending
	int buf_incomplete;		// 0 - buf_status!=1 or the file is completely loaded, 1 - the file is loaded incompletely, should be read again
	//IBFile curfile;			// file handler - used for incomplete reads or writing
	std::auto_ptr<IBStream> curfile;			// file handler - used for incomplete reads or writing

    IBMutex read_mutex;
    IBCond  buffer_cond;
	//pthread_mutex_t bufUsageMutex;
	//pthread_mutex_t synch_read_thread;
	//pthread_cond_t isFreeBuf;

    boost::thread flush_thread;
    boost::thread read_thread;
    bool stop_reading_thread;
	bool failed;
	//pthread_t bufFlushThread;
	//pthread_t reading_thread;

private:
	void ReadThread(void);
	void UseNextBuf();
	int FindUnusedBuf();

public:
	struct Params
	{
#ifdef _MSC_VER
		HANDLE pipe;
#endif
		char* path;
		unsigned long dw_desired_access;
		void* obj;
		bool finish;		
	} params;
	void BufFlushThread(IBStream* file, char* buf_ptr, int len, bool* failed);
};

typedef boost::shared_ptr<LargeBuffer> LargeBufferPtr;
#endif //_SYSTEM_LARGEBUF_H_

