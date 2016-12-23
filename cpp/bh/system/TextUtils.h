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

#ifndef _TEXTUTILS_H
#define _TEXTUTILS_H

#include "RCException.h"
#include "compress/defs.h"

//bool TcharToAscii(const TCHAR* src, char* dest, unsigned long destSize);  // Converts TCHAR string to char string
//const char* DecodeError(int ErrorCode, char* msg_buffer, unsigned long size);

void Convert2Hex(const unsigned char * src, int src_size, char * dest, int dest_size, bool zero_term = true);

bool EatWhiteSigns(char*& ptr, int& len);
bool EatDTSeparators(char*& ptr, int& len);
BHReturnCode EatUInt(char*& ptr, int& len, uint& out_value);
BHReturnCode EatInt(char*& ptr, int& len, int& out_value);
BHReturnCode EatUInt64(char*& ptr, int& len, _uint64& out_value);
BHReturnCode EatInt64(char*& ptr, int& len, _int64& out_value);
bool CanBeDTSeparator(char c);
#endif //_TEXTUTILS_H

