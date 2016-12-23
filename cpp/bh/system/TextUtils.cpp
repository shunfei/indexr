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

#include "TextUtils.h"

using namespace std;

// to use it, please test this function first
//bool TcharToAscii(const TCHAR* src, char* dest, size_t destSize)
//{
//#ifdef _MSC_VER
////TODO: why CHAR instead of char? after testing we can use char instead of CHAR
//	const codecvt<TCHAR, CHAR, char_traits<CHAR>::state_type> & cvt
//		= use_facet<std::codecvt<TCHAR, CHAR, char_traits<CHAR>::state_type>>(std::locale());
//#else
//	const codecvt<TCHAR, char, char_traits<char>::state_type> & cvt
//		= use_facet<std::codecvt<TCHAR, char, char_traits<char>::state_type> >(std::locale());
//#endif
//	mbstate_t state((mbstate_t()));
//	TCHAR const *esrc;
//	char *edest;
//	if(cvt.out(state, src, src + char_traits<TCHAR>::length(src), esrc, dest, dest + destSize - 1, edest) != cvt.ok)
//		return false;
//	*edest  = '\0';
//	return true;
//}

#ifdef __NOT_IN_USE__
const char* DecodeError(int ErrorCode, char* msg_buffer, unsigned long size)
{
#ifdef _MSC_VER
       FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS |
                  FORMAT_MESSAGE_MAX_WIDTH_MASK,
                  NULL, ErrorCode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                  msg_buffer, size, NULL);
#else
	strncpy(msg_buffer, strerror(errno), size);
#endif
	return msg_buffer;
}
#endif // __NOT_IN_USE__

inline char Convert2Hex(int index)
{
	static const char tab[]={'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
#ifdef _MSC_VER
	assert(index>=0 && index<_countof(tab) && "Index must be between 0 and 15 in 2hex conversion");
	__assume(index >= 0 && index < _countof(tab));
#endif
	return tab[index];
};

void Convert2Hex(const unsigned char * src, int src_size, char * dest, int dest_size, bool zero_term)
{
	assert(dest_size>1 && "At least 2 bytes needed for hexadecimal representation");

	dest_size = min(src_size * 2 + 1, dest_size);
	if(zero_term)
	{
		if (dest_size%2)
			dest_size--;
		else
			dest_size -= 2;
	}

	for (int i = 0; i < dest_size/2; i++) {
		dest[2*i]=Convert2Hex((src[i])/16);
		dest[2*i+1]=Convert2Hex((src[i])%16);
	};
	if(zero_term)
		dest[dest_size] = '\0';
};


bool EatWhiteSigns(char*& ptr, int& len)
{
	bool vs = false;
	while(len > 0 && isspace((unsigned char)*ptr))
	{
		len--;
		ptr++;
		vs = true;
	}
	return vs;
}

bool EatDTSeparators(char*& ptr, int& len)
{
	bool vs = false;
	while(len > 0 && CanBeDTSeparator(*ptr))
	{
		len--;
		ptr++;
		vs = true;
	}
	return vs;
}

BHReturnCode EatInt(char*& ptr, int& len, int& out_value)
{
	out_value = 0;
	_int64 out_v_tmp = 0;
	BHReturnCode rc = BHRC_FAILD;
	short sign = 1;

	if(len > 0 && *ptr == '+') // WINPORT extension
	{
		ptr++;
	}

	if(len > 0 && *ptr == '-')
	{
		sign = -1;
		ptr++;
	}

	while(len > 0 && isdigit((unsigned char)*ptr))
	{
		if(rc != BHRC_OUT_OF_RANGE_VALUE)
		{
			out_v_tmp = out_v_tmp * 10 + ((ushort)*ptr - '0');
			if(out_v_tmp * sign > INT_MAX)
			{
				out_v_tmp = INT_MAX;
				rc = BHRC_OUT_OF_RANGE_VALUE;
			}
			else if(out_v_tmp * sign < INT_MIN)
			{
				out_v_tmp = INT_MIN;
				rc = BHRC_OUT_OF_RANGE_VALUE;
			}
			else
				rc = BHRC_SUCCESS;
		}
		len--;
		ptr++;
	}

	if(rc == BHRC_SUCCESS)
		out_v_tmp *= sign;
	out_value = (int)out_v_tmp;
	return rc;
}

BHReturnCode EatUInt(char*& ptr, int& len, uint& out_value)
{
	if(len > 0 && *ptr == '+') {
		ptr++;
		len--;
	}

	out_value = 0;
	uint out_v_tmp = 0;
	BHReturnCode rc = BHRC_FAILD;
	while(len > 0 && isdigit((unsigned char) *ptr)) {
		out_v_tmp = out_value;
		out_value = out_value * 10 + ((ushort) *ptr - '0');
		if(rc != BHRC_OUT_OF_RANGE_VALUE) {
			if(out_v_tmp > out_value)
				rc = BHRC_OUT_OF_RANGE_VALUE;
			else
				rc = BHRC_SUCCESS;
		}
		len--;
		ptr++;
	}
	return rc;
}

BHReturnCode EatInt64(char*& ptr, int& len, _int64& out_value)
{
	out_value = 0;
	_int64 out_v_tmp = 0;
	BHReturnCode rc = BHRC_FAILD;
	bool sing = false;
	if(len > 0 && *ptr == '-') {
		sing = true;
		ptr++;
		len--;
	}
	else if(len > 0 && *ptr == '+') {
		ptr++;
		len--;
	}

	while(len > 0 && isdigit((unsigned char)*ptr)) {
		out_v_tmp = out_value * 10 + ((ushort)*ptr - '0');
		if(rc != BHRC_OUT_OF_RANGE_VALUE) {
			if(out_v_tmp < out_value)
				rc = BHRC_OUT_OF_RANGE_VALUE;
			else{
				rc = BHRC_SUCCESS;
				out_value = out_v_tmp;
			}
		}
		len--;
		ptr++;
	}

	if(sing)
		out_value *= -1;

	return rc;
}

BHReturnCode EatUInt64(char*& ptr, int& len, _uint64& out_value)
{
	if(len > 0 && *ptr == '+') {
		ptr++;
		len--;
	}

	out_value = 0;
	_uint64 out_v_tmp = 0;
	BHReturnCode rc = BHRC_FAILD;
	while(len > 0 && isdigit((unsigned char)*ptr))
	{
		out_v_tmp = out_value;
		out_value = out_value * 10 + ((ushort)*ptr - '0');
		if(rc != BHRC_OUT_OF_RANGE_VALUE)
		{
			if(out_v_tmp > out_value)
				rc = BHRC_OUT_OF_RANGE_VALUE;
			else
				rc = BHRC_SUCCESS;
		}
		len--;
		ptr++;
	}
	return rc;
}

bool CanBeDTSeparator(char c)
{
	if(
		c == '-' || c == ':' || c == '~' || c == '!' || c == '@' || c == '#' || c == '$' || c == '%' ||
		c == '^' || c == '&' || c == '*' || c == '(' || c == ')' || c == '{' || c == '}' || c == '[' ||
		c == ']' || c == '/' ||	c == '?' || c == '.' || c == ',' || c == '<' || c == '>' || c == '_' ||
		c == '=' || c == '+' || c == '|' || c == '`'
	)
		return true;
	return false;
}

