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

#include <algorithm>
#include <boost/bind.hpp>

#include "RCSystem.h"
#include "LargeBuffer.h"
#include "core/WinTools.h"
#include "common/bhassert.h"
#include "core/tools.h"
#include "system/IOParameters.h"

using namespace std;
using namespace boost;
/////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////  LargeBuffer   //////////////////////////////////////////////////////////////

static int const D_OVERRUN_GUARDIAN = -66;

LargeBuffer::LargeBuffer(int num_of_bufs, int requested_size)
{
	this->num_of_bufs = num_of_bufs;
	bufs= new char*[num_of_bufs];
	usage_of_bufs = new int[num_of_bufs];
	failed = false;

	params.path = 0;
	//bufFlushThread = INVALID_THREAD;
	read_thread_buffer = 0;
	//reading_thread = 0;

	size = requested_size;

	if (bufs == NULL || usage_of_bufs == NULL) {
		rclog << lock << "Error: out of memory small malloc failed (42a)" << unlock;
		return;
	}

	for (int i = 0; i< num_of_bufs; i++)
		bufs[i] = NULL;

	for (int i = 0; i< num_of_bufs; i++) {
		bufs[i] = (char*)alloc((size + 2)*sizeof(char), BLOCK_TEMPORARY, true);// additional 2 bytes for safety and terminating '\0';
		if(!bufs[i]) {
			rclog << lock << "Error: out of memory (" << size + 2 << " bytes failed). (42)" << unlock;
			return;
		}
		usage_of_bufs[i] = 0;
		bufs[i][size]='\0';		//maybe unnecessary
		bufs[i][size+1]=D_OVERRUN_GUARDIAN; //maybe unnecessary
	}

	curr_buf_no = 0;
	buf = bufs[curr_buf_no];
	curr_buf2_no = 1;
	buf2= bufs[curr_buf2_no];

	buf_status=0;
	buf_used=0;
	buf_incomplete=0;
	bytes_in_read_thread_buffer = 0;
	stop_reading_thread = true;

	//pthread_mutexattr_t mattr;
	//pthread_mutexattr_init(&mattr);
	//pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
	//pthread_mutex_init(&synch_read_thread, &mattr);
	//pthread_mutex_init(&bufUsageMutex, NULL);
	//pthread_cond_init(&isFreeBuf, NULL);
}

LargeBuffer::~LargeBuffer()
{
	if(read_thread.joinable()) {
		read_mutex.Lock();
		stop_reading_thread = true;
		read_mutex.Unlock();
		read_thread.join();
	}

	if(flush_thread.joinable())
        flush_thread.join();
	if(curfile.get())
		curfile->Close();
	for (int i = 0; i< num_of_bufs; i++)
		if (bufs[i]) dealloc(bufs[i]);
	if (bufs) delete [] bufs;
	if (usage_of_bufs) delete [] usage_of_bufs;
	if (params.path) delete [] params.path;
	if(read_thread_buffer)
		dealloc(read_thread_buffer);
}

bool LargeBuffer::BufOpen(const IOParameters& iop, BufOpenType mode)
{
	bool result = false;
	
	if(buf_status != 0) {		
		return 0;
	}
	BufClose(); 

	try {
//		DebugBreak();
		curfile = IBStream::CreateOpenIBStream(iop, mode);
		if(!curfile.get() || !curfile->IsOpen()) {
			BufClose();
			return false;
		}		
	} catch ( ... ) {
		buf_status = 0;
		return false;
	}

	if(mode == READ) {
		buf_used = Read(curfile.get(), buf, size);
		if(buf_used == -1) {
			curfile->Close();
			buf_incomplete = 0;
			buf_status = 0;
			return false;
		} else if(buf_used == size) {
			if (!StartReadingThread()) {
				buf_status = 0;
				return 0;
			}
			buf_incomplete = 1;
		} else {
			curfile->Close();
			buf_incomplete = 0;
		}
		buf_status = 1;
		result = true;
	} else if(mode == OVERWRITE || mode == APPEND) {
			if(mode == OVERWRITE) {
				buf_used = 0;
				buf_status = 2;
				result = true;
				chmod( iop.Path(), 0666 );
			} else {
				buf_used = 0;
				buf_status = 3;
				result = true;
			}
	} else
		rclog << lock << "BufOpen warning: mode not implemented!" << unlock;
	return result;
}

bool LargeBuffer::StartReadingThread()
{
	if(BufferingLevel > 0)
	{
		if(!read_thread_buffer) {
			read_thread_buffer  = (char*)alloc((size+1)*sizeof(char), BLOCK_TEMPORARY, true);
			if (!read_thread_buffer) {
				rclog << lock << "Error: out of memory (" << size + 1 << " bytes failed). (42)" << unlock;
				return false;
			}
		}
		bytes_in_read_thread_buffer = 0;

		boost::thread::id threadId;
		stop_reading_thread = false;
        read_thread = thread( bind( &LargeBuffer::ReadThread, this) );
		threadId = read_thread.get_id();

		return true;
	}
	return false;
}

void LargeBuffer::BufFlush()		// save the data to the file (in write mode)
{
	if(buf_used>size)
		rclog << lock << "LargeBuffer error: Buffer overrun (Flush)" << unlock;
	if(flush_thread.joinable()) {
     flush_thread.join();
	}
	if(failed) {
		rclog << lock << "ERROR: Write operation to file or pipe failed." << unlock;	
		throw FileRCException("Write operation to file or pipe failed.");
	}
	boost::thread::id threadId;
	flush_thread = thread( bind( &LargeBuffer::BufFlushThread, this, curfile.get(), buf, buf_used, &failed ) );
	threadId = flush_thread.get_id();
	UseNextBuf();
}

void LargeBuffer::BufFlushThread(IBStream* file, char* buf_ptr, int len, bool* failed)
{
	if(file->IsOpen())
	{
		int start = 0, end = 0;
		while(end != len)
		{
			start = end;
			end += WRITE_TO_PIPE_NO_BYTES;
			if(end > len)
				end = len;			
			try {
				file->WriteExact(buf_ptr+start, end-start);
			} catch(DatabaseRCException&)
			{				
				*failed = true;
				break;
			}
		}
	}
	return;
}

void LargeBuffer::BufClose()		// close the buffer; warning: does not flush data on disk (write mode)
{	
	if(flush_thread.joinable())
		flush_thread.join();

	if(read_thread.joinable()) {
		read_mutex.Lock();
		stop_reading_thread = true;
		read_mutex.Unlock();
		read_thread.join();
	}

	if(curfile.get())
		curfile->Close();

	buf_status=0;
	buf_used=0;
	buf_incomplete=0;
	if(buf[size+1]!=D_OVERRUN_GUARDIAN) {
		rclog << lock << "INTERNAL WARNING: buffer overrun detected in LargeBuffer::BufClose." << unlock;
		assert(0);
	}
}

void LargeBuffer::FlushAndClose()
{
	BufFlush();
	BufClose();
}

bool LargeBuffer::IsAllocated()
{
	if (bufs == NULL || usage_of_bufs == NULL)
		return false;
	for (int i = 0; i< num_of_bufs; i++) {
		if (!bufs[i]) return false;
	}
	return true; 
}

char* LargeBuffer::BufAppend(unsigned int len)
{
	if((int)len>size)
	{
		rclog << lock << "Error: LargeBuffer buffer overrun (BufAppend)" << unlock;
		return NULL;
	}
	int buf_pos = buf_used;
	if(size > (int)(buf_used + len))
		buf_used += len;
	else
	{
		BufFlush();
		buf_status = 3;
		buf_pos = 0;
		buf_used = len;
	}
	return this->Buf(buf_pos);	// NOTE: will point to the first undeclared byte in case of len=0.
								// Fortunately we always assume one additional byte for '\0' to be insertable at the end of system buffer
}

char* LargeBuffer::SeekBack(uint len)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((uint)buf_used >= len);
	buf_used -= len;
	return this->Buf(buf_used);
}

int LargeBuffer::BufFetch(int unused_bytes, int to_read)
{
	if(to_read == 0)
		to_read = size;
	if(buf_status == 1) {
		IBGuard guard(read_mutex);

		if(bytes_in_read_thread_buffer == -1) {
			curfile->Close();
			buf_incomplete = 0;
			throw FileRCException("Unable to read from the input file.");
		}

		if((!curfile->IsOpen() && bytes_in_read_thread_buffer == 0) || to_read==unused_bytes)
			return 0;

		for(int i = 0; i < unused_bytes; i++)
			buf2[i] = *(buf + ((buf_used - unused_bytes) + i));
		to_read -= unused_bytes;
		int to_read_from_th_buf = min(to_read, bytes_in_read_thread_buffer);

		memcpy(buf2 + unused_bytes, read_thread_buffer, to_read_from_th_buf);

		for(int i = 0; i < bytes_in_read_thread_buffer - to_read_from_th_buf; i++)
			read_thread_buffer[i] = read_thread_buffer[i+to_read_from_th_buf];

		bytes_in_read_thread_buffer -= to_read_from_th_buf;
		unused_bytes += to_read_from_th_buf;
		to_read -= to_read_from_th_buf;

		buf_used = unused_bytes;
		if(curfile->IsOpen()) {
			ushort no_steps = 0;
			int r = 0;
			while(no_steps < 15 && to_read > 0) {
				try {
					r = curfile->Read(buf2+unused_bytes, to_read);		// Read to buffer and set '\0' on the end of file
				} catch(DatabaseRCException&) {
					r = -1;
				}
				if(r != -1) {
					buf_used += r;
					if(r == 0) {
						curfile->Close();
						buf_incomplete = 0;
						break;
					} else {
						buf_incomplete = 1;
					}
					if(r != to_read) {
						unused_bytes += r;
						to_read -= r;
					} else
						break;
				} else {
					no_steps++;
					rclog << lock << "Reading from the input file error: " << DisplayError().c_str() << unlock;
					SleepNow(500 + no_steps*500);
				}

			}
			if(r == -1) {
				curfile->Close();
				buf_incomplete = 0;
				throw FileRCException("Unable to read from the input file.");
			}
		}
		buf_status=1;
	}
	UseNextBuf();
	return buf_used;
}

int LargeBuffer::Read(IBStream *file, char* buffer, int bytes_to_read)
{
	int requested_size = bytes_to_read;
	int no_attempts = 0;
	int bytes_read = 0;
	int read_b = -1;
	bool do_stop = false;
	bytes_read = 0;
	while(no_attempts < 16 && bytes_read != requested_size) {
		no_attempts++;
		do_stop = false;
		while(!do_stop) {
			try {
			read_b = file->Read(buffer + bytes_read , min(bytes_to_read, requested_size - bytes_read));
			} catch(DatabaseRCException&) {
				read_b = -1;
			}
			if(read_b == -1)
				do_stop = true;
			else if(read_b == 0)
				return bytes_read;
			else
				bytes_read += read_b;
		}
		bytes_to_read /= 2;
	}
	return read_b != -1 ? bytes_read : read_b;
}

void LargeBuffer::ReadThread()
{
	int to_read = 0;
	int to_read_in_one_loop = size / BufferingLevel;
	bool do_sleep = false;
	bool do_stop = false;

	int no_read_bytes = -1;

	while(!do_stop) {
		no_read_bytes = 0;
        read_mutex.Lock();
		if(buf_incomplete != 1 || !curfile->IsOpen() || stop_reading_thread) {
            read_mutex.Unlock();
			break;
		}

		if(bytes_in_read_thread_buffer != size) {
			to_read = to_read_in_one_loop;
			if(size - bytes_in_read_thread_buffer < to_read_in_one_loop)
				to_read = size - bytes_in_read_thread_buffer;
			ushort no_steps = 0;
			while(no_steps < 15  && to_read > 0) {
				try {
				no_read_bytes = curfile->Read(read_thread_buffer + bytes_in_read_thread_buffer, to_read);
				} catch(DatabaseRCException&) {
					no_read_bytes = -1;
				}				
				if(no_read_bytes == -1) {
					no_steps++;
					rclog << lock << "Reading from the input file error: " << DisplayError().c_str() << unlock;
					SleepNow(500 + no_steps*500);
					do_stop = true;
				} else {
					do_stop = false;
					bytes_in_read_thread_buffer += no_read_bytes;
					if(no_read_bytes != 0)
						buf_incomplete = 1;
					else {
						curfile->Close();
						buf_incomplete = 0;
						do_stop = true;
					}
					break;
				}
			}

			if(no_read_bytes == -1) {
				bytes_in_read_thread_buffer = -1;
				do_stop = true;
			}
		}

		if(bytes_in_read_thread_buffer == size)
			do_sleep = true;
		else
			do_sleep = false;
        read_mutex.Unlock();
		if(do_sleep)
			SleepNow(20);
		if(no_read_bytes != -1)
			SleepNow(20);
	}
	return;
}


void LargeBuffer::UseNextBuf()
{
	int tmp = curr_buf2_no;
	curr_buf2_no = FindUnusedBuf();
	buf2 = bufs[curr_buf2_no];		//to be loaded with data
	curr_buf_no = tmp;
	buf =  bufs[curr_buf_no];			//to be parsed
}

int LargeBuffer::FindUnusedBuf()
{
    buffer_cond.Lock();
	while (true) {
		for (int i = 0; i< num_of_bufs; i++) {
			if (usage_of_bufs[i] == 0 && i != curr_buf2_no) { //not used and not just loaded
                buffer_cond.Unlock();
				return i;
			}
		}
        buffer_cond.Wait();
	}
	return -1; //to pacify compilation warnings
}

void LargeBuffer::MarkUseOf(int buf_no)
{
	BHASSERT(buf_no < num_of_bufs, " marking use of too big bufNo");
	buffer_cond.Lock();
	usage_of_bufs[buf_no]++;
	buffer_cond.Unlock();
}

void LargeBuffer::ReleaseFromUse(int buf_no)
{
	BHASSERT(buf_no < num_of_bufs,"releasing too big bufNo");
	buffer_cond.Lock();
	usage_of_bufs[buf_no]--;
	if (usage_of_bufs[buf_no] == 0) {
		buffer_cond.Signal();
	}
	buffer_cond.Unlock();
}

#ifdef _XMK
#ifdef _MSC_VER
bool createSecurityDescritpor(SECURITY_ATTRIBUTES &sa) 
{
	//returns true on success
	class tmpException {};

	unsigned long dwRes;
	PSID pEveryoneSID = NULL;
	PACL pACL = NULL;
	PSECURITY_DESCRIPTOR pSD = NULL;
	EXPLICIT_ACCESS ea;
	SID_IDENTIFIER_AUTHORITY SIDAuthWorld = SECURITY_WORLD_SID_AUTHORITY;
	HKEY hkSub = NULL;

	try {


		// Create a well-known SID for the Everyone group.
		if(!AllocateAndInitializeSid(&SIDAuthWorld, 1,
			SECURITY_WORLD_RID,
			0, 0, 0, 0, 0, 0, 0,
			&pEveryoneSID)) throw tmpException();

		// Initialize an EXPLICIT_ACCESS structure for an ACE.
		// The ACE will allow Everyone read access to the key.
		ZeroMemory(&ea, sizeof(EXPLICIT_ACCESS));
		ea.grfAccessPermissions = GENERIC_READ | GENERIC_WRITE;
		ea.grfAccessMode = SET_ACCESS;
		ea.grfInheritance= NO_INHERITANCE;
		ea.Trustee.TrusteeForm = TRUSTEE_IS_SID;
		ea.Trustee.TrusteeType = TRUSTEE_IS_COMPUTER;
		ea.Trustee.ptstrName  = (LPTSTR) pEveryoneSID;

		// Create a new ACL that contains the new ACEs.
		dwRes = SetEntriesInAcl(1, &ea, NULL, &pACL);
		if (ERROR_SUCCESS != dwRes) throw tmpException();

		// Initialize a security descriptor.  
		pSD = (PSECURITY_DESCRIPTOR) LocalAlloc(LPTR, 
			SECURITY_DESCRIPTOR_MIN_LENGTH); 
		if (NULL == pSD) throw tmpException();

		if (!InitializeSecurityDescriptor(pSD, SECURITY_DESCRIPTOR_REVISION)) throw tmpException();


		// Add the ACL to the security descriptor. 
		if (!SetSecurityDescriptorDacl(pSD, 
			TRUE,     // bDaclPresent flag   
			pACL, 
			FALSE))   // not a default DACL 
			throw tmpException();

		// Initialize a security attributes structure.
		sa.nLength = sizeof (SECURITY_ATTRIBUTES);
		sa.lpSecurityDescriptor = pSD;
		sa.bInheritHandle = FALSE;

		// Use the security attributes to set the security descriptor 
	}
	catch (tmpException &) {
		if (pEveryoneSID) 
			FreeSid(pEveryoneSID);
		if (pACL) 
			LocalFree(pACL);
		if (pSD) 
			LocalFree(pSD);

		return false;
	}

	return true;
}
#endif
#endif
