#include "IBMemStream.h"
#include <sstream>
#include <iostream>

#include <boost/scoped_array.hpp>

// ========================= MemStreamBlock =====================/
// ========================= =========== ========================/

MemStreamBlock::MemStreamBlock()
{
	data = NULL;
	wpos = 0;
	rpos = 0;
	size = 0;
}

MemStreamBlock::~MemStreamBlock()
{
	if (data) free(data);
	data = NULL;
}

bool MemStreamBlock::Create(uint s)
{
	size = s;
	assert(size > 0);
	data = (uchar *)malloc(size);
	return (data != NULL);
}

bool MemStreamBlock::IsFull() const
{
	return (size == wpos);
}

bool MemStreamBlock::IsEmpty() const
{
	return (wpos == 0);
}

uint MemStreamBlock::Write(const uchar* buf, uint count)
{
	assert (data != NULL);
	assert (!IsFull());
	
	uint unused = size - wpos;
	uint nbytes = unused > count ? count : unused;
	memcpy(data + wpos, buf , nbytes);
	wpos += nbytes;
	return nbytes;
}

uint MemStreamBlock::Read(uchar* buf, uint count)
{
	assert (data != NULL);
	uint unread = wpos - rpos;
	uint nbytes = unread > count ? count : unread;
	if (nbytes)
		memcpy(buf, data + rpos , nbytes);
	rpos += nbytes;
	return nbytes;
}

// ========================= IBMemStream =====================/
// ========================= =========== =====================/

IBMemStream::IBMemStream()
{
	blocks.empty();
	curblock = 0;
	blocksize = 5*1024*1024;

	openmode = MEMSTREAM_UNDEF;
}

int IBMemStream::Close()
{
	uint n = uint(blocks.size());
	for(uint i = 0; i < n; i++)
		delete blocks[i];
	blocks.clear();
	return 0;
};

IBMemStream::~IBMemStream()
{
	if (blocks.size() > 0)
		Close();
}

void IBMemStream::SetMemBlockSize(uint s)
{
	blocksize = s;
}

long IBMemStream::GetSize()
{
	uint length = 0;
	uint n = uint(blocks.size());
	for(uint i = 0; i < n; i++)
		length += blocks[i]->wpos;
	return length;
}

int IBMemStream::OpenCreateEmpty(std::string const& filename, int create, uint64 timeout) throw(DatabaseRCException)
{
	assert (openmode == MEMSTREAM_UNDEF);
	// Must be empty before for creating.
	assert (blocks.size() == 0);
	MemStreamBlock *memblock = new MemStreamBlock();
	if (!memblock->Create(blocksize)) {
		delete memblock;
		std::stringstream ss;
		ThrowError( static_cast<std::stringstream&>( ss << "MemStream::OpenCreateEmpty insufficient memory, request size: " << blocksize << " bytes.").str());
		return 1;
	}
	curblock = 0;
	blocks.push_back(memblock);
	openmode = MEMSTREAM_WRITE;
	return 0;
}

int IBMemStream::OpenReadOnly(std::string const& filename, int create, uint64 timeout) throw(DatabaseRCException)
{
	// MemStream must be opened as write before reading from it.
	assert (openmode == MEMSTREAM_WRITE);
	uint n = uint(blocks.size());
	// Must be opened before for writing.
	assert (n > 0);
	for(uint i = 0; i < n; i++)
		blocks[i]->rpos = 0;
	curblock = 0;
	openmode = MEMSTREAM_READ;
	return 0;
}

uint IBMemStream::Write(const void* buf, uint count) throw(DatabaseRCException)
{
	assert (openmode == MEMSTREAM_WRITE);
	uint current_write = 0;

	while (current_write < count) {
		if (blocks[curblock]->IsFull()) {
			MemStreamBlock *memblock = new MemStreamBlock();
			if (!memblock || !memblock->Create(blocksize)) {
				delete memblock;
				std::stringstream ss;
				ThrowError( static_cast<std::stringstream&>( ss << "MemStream::Write insufficient memory, request size: " << blocksize << " bytes.").str());
				return 1;
			}
			blocks.push_back(memblock);
			curblock ++;
		}
		uint write_bytes = blocks[curblock]->Write((uchar*)buf + current_write, count - current_write);
		current_write += write_bytes;
	}
	return current_write;
}

uint IBMemStream::WriteExact(const void* buf, uint count) throw(DatabaseRCException)
{
	uint current_write = 0;
	current_write = Write(buf, count);
	if (count && (int) current_write < count) {
		std::stringstream ss;
		ThrowError( static_cast<std::stringstream&>( ss << "MemStream::WriteExact failed, expected/actual wrote: " << count << "/" << current_write).str() );
	}
	return current_write;
}

uint IBMemStream::Read(void* buf, uint count) throw(DatabaseRCException)
{
	assert (openmode == MEMSTREAM_READ);
	uint current_read = 0;

	while (current_read < count && curblock < blocks.size()) {
		uint read_bytes = blocks[curblock]->Read((uchar*)buf + current_read, count - current_read);
		if (read_bytes > 0)
			current_read += read_bytes;
		else {
			// Move to next block
			curblock ++;
			continue;
		}
	}
	return current_read;
}

uint IBMemStream::ReadExact(void* buf, uint count) throw(DatabaseRCException)
{
	uint current_read = 0;
	current_read = Read(buf, count);
	if (count && (int) current_read < count) {
		std::stringstream ss;
		ThrowError( static_cast<std::stringstream&>( ss << "MemStream::ReadExact failed, expected/actual read: " << count << "/" << current_read ).str() );
	}
	return current_read;
}

void IBMemStream::FlushTo(IBStream& out_stream)
{
	OpenReadOnly("none");
	long size = GetSize();
	long ntransferred = 0, nbytes, bufsize = 512*1024;
	boost::scoped_array<uchar> tempbuf(new uchar [bufsize]);

	// Read from memstream and write to it
	while (ntransferred < size) {
		nbytes = Read(tempbuf.get(), bufsize);
		if (nbytes == 0)
			ThrowError("MemStream::FlushTo failed, unexpected internal error, nbytes==0");
		out_stream.WriteExact(tempbuf.get(), nbytes);
		ntransferred += nbytes;
	}
}

std::string IBMemStream::PublishInfo()
{
	std::stringstream sinfo;
	long totalallocated = 0;
	long totallength = 0;
	uint n = uint(blocks.size());
	sinfo << "===============IBMemStream Info ====================>>\n";
	sinfo << "Number of allocated blocks: " << n << ", blocksize= " << blocksize << "\n";
	sinfo << "===================================\n";
	for(uint i = 0; i < n; i++) {
		sinfo << " block[" << i << "], allocated/written/read="<<  blocks[i]->size << "/" << blocks[i]->wpos << "/" << blocks[i]->rpos << "\n";
		totallength += blocks[i]->wpos;
		totalallocated += blocks[i]->size;
	}
	sinfo << "Total allocated/written= " << totalallocated << "/" << totallength << "\n";
	sinfo << "<<===============IBMemStream Info ====================";
	return sinfo.str();
}
