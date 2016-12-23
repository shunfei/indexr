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

#ifndef _SYSTEM_FILEOUT_H_
#define _SYSTEM_FILEOUT_H_

#include <fstream>
#include <string>
#include "ChannelOut.h"
#include "RCException.h"

#ifdef __WIN__
#undef open
#undef close
#endif

class FileOut : public ChannelOut
{
public:
	FileOut(std::string const& filepath)
		:	m_out(filepath.c_str(), std::ios_base::out | std::ios_base::app)
	{
		if(!m_out.is_open())
			throw FileRCException(std::string("Unable to open ") + std::string(filepath));
	}

	~FileOut()
	{ }

	ChannelOut& operator<<(short value) { m_out << value; return *this; };
	ChannelOut& operator<<(int value)   { m_out << value; return *this; };
	ChannelOut& operator<<(long value)  { m_out << value; return *this; };

	ChannelOut& operator<<(float value)  { m_out << value; return *this; };
	ChannelOut& operator<<(double value) { m_out << value; return *this; };
	ChannelOut& operator<<(long double value) { m_out << value; return *this; };

	ChannelOut& operator<<(unsigned short value) { m_out << value; return *this; };
	ChannelOut& operator<<(unsigned int value)   { m_out << value; return *this; };
	ChannelOut& operator<<(unsigned long value)  { m_out << value; return *this; };

	ChannelOut& operator<<(int long long value)  { m_out << value; return *this; };
	ChannelOut& operator<<(int long long unsigned value) { m_out << value; return *this; };

	ChannelOut& operator<<(char c)  { m_out << c; return *this; };
	ChannelOut& operator<<(const char* buffer)     { m_out << buffer; return *this; };
	ChannelOut& operator<<(const wchar_t* buffer)  { m_out << buffer; return *this; };
	ChannelOut& operator<<(const std::string& str)      { m_out << str.c_str(); return *this; };

	void setf(std::ios_base::fmtflags _Mask)  { m_out.setf(_Mask); };
	void precision(std::streamsize prec) { m_out.precision(prec); };

	ChannelOut& flush() { m_out.flush(); return *this; };
	ChannelOut& fixed() { m_out.setf(std::ios_base::fixed, std::ios_base::floatfield); return *this; };

	void close() { if (m_out.is_open()) m_out.close(); };

	ChannelOut& operator<<(ChannelOut& (*_Pfn)(ChannelOut&)) { return _Pfn(*this); };

private:
	std::wofstream m_out;
};

#endif //_SYSTEM_FILEOUT_H_
