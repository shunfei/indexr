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
#ifndef _SYSTEM_CHANNELOUT_H_
#define _SYSTEM_CHANNELOUT_H_

#include <iomanip>
#include "types/RCDataTypes.h"
#include "common/CommonDefinitions.h"

class ChannelOut
{
#ifdef __WIN__
public:
    ChannelOut() {}  // VC++ compiler requires explicit definition of a constructor in this case
#endif

public:
	virtual ChannelOut& operator<<(short value) = 0;
	virtual ChannelOut& operator<<(int value) = 0;
	virtual ChannelOut& operator<<(long value) = 0;

	virtual ChannelOut& operator<<(float value) = 0;
	virtual ChannelOut& operator<<(double value) = 0;
	virtual ChannelOut& operator<<(long double value) = 0;

	virtual ChannelOut& operator<<(unsigned short value) = 0;
	virtual ChannelOut& operator<<(unsigned int value) = 0;
	virtual ChannelOut& operator<<(unsigned long value) = 0;

	virtual ChannelOut& operator<<(int long long value) = 0;
	virtual ChannelOut& operator<<(int long long unsigned value) = 0;

	virtual ChannelOut& operator<<(char c) = 0;
	virtual ChannelOut& operator<<(const char* buffer) = 0;
	virtual ChannelOut& operator<<(const wchar_t* buffer) = 0;
	virtual ChannelOut& operator<<(const std::string& str) = 0;
	ChannelOut& operator<<(RCBString& rcbs)
	{
		for(ushort i = 0; i < rcbs.len; i++)
			(*this) << (char)(rcbs[i]);
		return *this;
	}
	ChannelOut& operator<<(const std::exception& exc) { (*this) << exc.what(); return *this; };

	virtual void setf(std::ios_base::fmtflags _Mask) = 0;
	virtual void precision(std::streamsize prec) = 0;

	virtual ChannelOut& flush() = 0;
	virtual ChannelOut& fixed() = 0;
	virtual void close() = 0;

	virtual ChannelOut& operator<<(ChannelOut& (*_Pfn)(ChannelOut&)) = 0;
};


//inline ChannelOut& flush(ChannelOut& inout)
//{
//	return inout.flush();
//}

inline ChannelOut& endl(ChannelOut& inout)
{
	inout << '\n';
	return inout.flush();
}

inline ChannelOut& fixed(ChannelOut& inout)
{
	return inout.fixed();
}


#endif //_SYSTEM_CHANNELOUT_H_

