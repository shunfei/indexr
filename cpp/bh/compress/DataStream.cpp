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

#include "DataStream.h"

void BitStream::ZeroBits(uint beg, uint end)
{
	uint byte1 = beg >> 3, bit1 = 8 - (beg & 7),
		 byte2 = end >> 3, bit2 = end & 7;

	if(byte1 < byte2) {
		(buf[byte1] _SHL_ASSIGN_ bit1) _SHR_ASSIGN_ bit1;			// clear 'bit1' upper bits of 'byte1'
		if(bit2)
			(buf[byte2] >>= bit2) <<= bit2;		// clear 'bit2' lower bits of 'byte2'
		memset(buf + byte1 + 1, 0, byte2 - byte1 - 1);	// clear the rest of bytes, in the middle
	}
	else if(beg < end) {		// beggining and end are in the same byte
		uchar t = buf[byte1];
		bit1 = 8 - bit1; bit2 = 8 - bit2;
		t _SHR_ASSIGN_ bit1;
		t _SHL_ASSIGN_ (bit1 + bit2);
		t >>= bit2;
		buf[byte1] -= t;
	}
}

void BitStream::ClearBits()
{
	clrlen = 2 * pos + 64;
	clrlen = (clrlen / 8) * 8;		// round to the whole byte
	if((clrlen <= pos) || (clrlen > len))
		clrlen = len;
	if(clrlen <= pos) BufOverrun();
	ZeroBits(pos, clrlen);
}
