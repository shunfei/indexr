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

#ifndef _SYSTEM_BHSTDOUT_H_
#define _SYSTEM_BHSTDOUT_H_

#include "ChannelOut.h"

class StdOut : public ChannelOut
{
public:
	ChannelOut& operator<<(short value) { std::cout << value; return *this; };
	ChannelOut& operator<<(int value) { std::cout << value; return *this; };
	ChannelOut& operator<<(long value) { std::cout << value; return *this; };

	ChannelOut& operator<<(float value) { std::cout << value; return *this; };
	ChannelOut& operator<<(double value) { std::cout << value; return *this; };
	ChannelOut& operator<<(long double value) { std::cout << value; return *this; };

	ChannelOut& operator<<(unsigned short value) { std::cout << value; return *this; };
	ChannelOut& operator<<(unsigned int value) { std::cout << value; return *this; };
	ChannelOut& operator<<(unsigned long value) { std::cout << value; return *this; };

	ChannelOut& operator<<(int long long value) { std::cout << value; return *this; };
	ChannelOut& operator<<(int long long unsigned value) { std::cout << value; return *this; };

	ChannelOut& operator<<(char c)  { std::cout << c; return *this; };
	ChannelOut& operator<<(const char* buffer)  { std::cout << buffer; return *this; };
	ChannelOut& operator<<(const wchar_t* buffer)  { std::wcout << buffer; return *this; };
	ChannelOut& operator<<(const std::string& str)  { std::cout << str; return *this; };

	void setf(std::ios_base::fmtflags _Mask) { std::cout.setf(_Mask); };
	void precision(std::streamsize prec) { std::cout.precision(prec); };

	ChannelOut& flush() { std::cout.flush(); return *this; };
	ChannelOut& fixed() { std::cout.setf(std::ios_base::fixed, std::ios_base::floatfield); return *this; };

	void close() { };

	ChannelOut& operator<<(ChannelOut& (*_Pfn)(ChannelOut&)) { return _Pfn(*this); };
};

#endif //_SYSTEM_BHSTDOUT_H_

