#ifndef _IBMemStream_H_
#define _IBMemStream_H_

#include <vector>
#include <string>
#include "system/RCException.h"
#include "system/IBStream.h"

#define MEMSTREAM_UNDEF -1
#define MEMSTREAM_WRITE 0
#define MEMSTREAM_READ 1

class MemStreamBlock
{
	uchar *data;	// pointer to buf

public:	
	int size;	// size of allocated buffer
	int wpos;   // write pointer
	int rpos;   // read pointer

	MemStreamBlock();
	~MemStreamBlock();
	bool IsFull() const;
	bool IsEmpty() const;
	bool Create(uint s);
	uint Write(const uchar* buf, uint count);
	uint Read(uchar* buf, uint count);
};

//
// MemoryStream maintains a vector of memory blocks. Each block
// has a buffer and counters. Write stores data in the current block's buffer. When
// the buffer is full, it creates a new block and write to new one's buffer
//

class IBMemStream: public IBStream {

	int openmode;
public:
	std::vector<MemStreamBlock* > blocks;
	int curblock;
	int blocksize;
	void SetMemBlockSize(uint s);
	long GetSize();
	
	IBMemStream();
	~IBMemStream();
	
	int OpenReadOnly(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	int OpenCreateEmpty(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	uint WriteExact(const void* buf, uint count) throw(DatabaseRCException);
	uint ReadExact(void* buf, uint count) throw(DatabaseRCException);
	uint Read(void* buf, uint count) throw(DatabaseRCException);

	void FlushTo(IBStream& out_stream);

	int Close();
	
	bool IsOpen() const { assert(false); return true; };
	int OpenReadWrite(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException) { assert(false); return 0; };
	int OpenWriteAppend(std:: string const&, int create = 0 , uint64 timeout = 0) throw(DatabaseRCException) { assert(false); return 0; };

	std::string PublishInfo();
	static int utest_IBMemStream();

private:
	uint Write(const void* buf, uint count) throw(DatabaseRCException);
};


#endif //_IBMemStream_H_
