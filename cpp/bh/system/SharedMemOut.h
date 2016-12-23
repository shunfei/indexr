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

#ifndef _SYSTEM_SHAREDMEM_OUT_H_
#define _SYSTEM_SHAREDMEM_OUT_H_

#include <sstream>
#include <sys/types.h>
#ifdef __GNUC__
#include <sys/ipc.h>
#include <sys/sem.h>
#include <semaphore.h>
#endif

#include "ChannelOut.h"
#define IPC_WAIT 0

class SharedMemOut : public ChannelOut
{
public:
	//SharedMemOut(char* msg_buf, unsigned buf_size, const char* event_name);
	SharedMemOut(char* msg_buf, unsigned buf_size, const char* event_name, const char *shpath, int &status);
	~SharedMemOut();

	ChannelOut& operator<<(short value) { (*m_out) << value; return *this; };
	ChannelOut& operator<<(int value) { (*m_out) << value; return *this; };
	ChannelOut& operator<<(long value) { (*m_out) << value; return *this; };

	ChannelOut& operator<<(float value) { (*m_out) << value; return *this; };
	ChannelOut& operator<<(double value) { (*m_out) << value; return *this; };
	ChannelOut& operator<<(long double value) { (*m_out) << value; return *this; };

	ChannelOut& operator<<(unsigned short value) { (*m_out) << value; return *this; };
	ChannelOut& operator<<(unsigned int value) { (*m_out) << value; return *this; };

	ChannelOut& operator<<(unsigned long value) { (*m_out) << value; return *this; };

	ChannelOut& operator<<(int long long value) { (*m_out) << value; return *this; };
	ChannelOut& operator<<(int long long unsigned value) { (*m_out) << value; return *this; };

	ChannelOut& operator<<(char c)  { (*m_out) << c; return *this; };
	ChannelOut& operator<<(const char* buffer)  { (*m_out) << buffer; return *this; };
	ChannelOut& operator<<(const wchar_t* buffer)  { (*m_out) << buffer; return *this; };
	ChannelOut& operator<<(const std::string& str)  { (*m_out) << str.c_str(); return *this; };

	void setf(std::ios_base::fmtflags _Mask) { (*m_out).setf(_Mask); };
	void precision(std::streamsize prec) { (*m_out).precision(prec); };

	ChannelOut& flush();
	ChannelOut& fixed() { (*m_out).setf(std::ios_base::fixed, std::ios_base::floatfield); return *this; };

	void close() { };

	ChannelOut& operator<<(ChannelOut& (*_Pfn)(ChannelOut&)) { return _Pfn(*this); };

private:
	std::ostringstream* m_out;
	char* m_msg_buf;
	unsigned m_buf_size;
#ifdef _MSC_VER
	HANDLE m_event_for_loader;
	HANDLE m_event_for_server;
#else
	key_t sem_ipc_key;
	int sem_ipc;
#endif
};

#endif  //_SYSTEM_SHAREDMEM_OUT_H_
