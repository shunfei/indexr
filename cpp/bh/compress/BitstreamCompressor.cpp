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

#include "BitstreamCompressor.h"
#include "common/bhassert.h"
#include "system/fet.h"

const double log_of_2 = log(2.0);

double BitstreamCompressor::Entropy(double p)
{
	if((p <= 0.0) || (p >= 1.0)) return 0.0;
	double q = 1.0 - p;
	return -(p*log(p) + q*log(q)) / log_of_2;
}

void BitstreamCompressor::GetSumTable(unsigned short* sum, unsigned int num0, unsigned int num1, bool patch)
{
	unsigned int cnt0 = num0, cnt1 = num1;

	uint maxtotal;
	if(patch) maxtotal = ArithCoder::MAX_TOTAL;
	else maxtotal = USHRT_MAX;

	// reduce cnt0 and cnt1 so that their 'total' is small enough for the coder
	while(cnt0 + cnt1 > maxtotal) {
		cnt0 >>= 1;
		cnt1 >>= 1;
		// repair if count = 0
		if((cnt0 == 0) && (num0 > 0))   cnt0 = 1;
		if((cnt1 == 0) && (num1 > 0))   cnt1 = 1;
	}

	sum[0] = 0;
	sum[1] = cnt0;
	sum[2] = (unsigned short)(cnt0 + cnt1);
}

uint BitstreamCompressor::Differ(BitStream* dest, BitStream* src)
{
	// note: an exception is thrown if 'src' contains more bits than 'numbits' (length of 'dest')
	uint num1 = 0;
	uchar prev = 0, next = 0, bit = 0;
	while(src->CanRead()) {
		next = src->GetBit();
		dest->PutBit(bit = prev ^ next);
		prev = next;
		if(bit) num1++;
	}
	return num1;
}

void BitstreamCompressor::Integrt(BitStream* dest, BitStream* src)
{
	uchar bit = 0, next = 0;
	while(src->CanRead()) {
		next = src->GetBit();
		dest->PutBit(bit = bit ^ next);
	}
}

CprsErr BitstreamCompressor::CompressData(BitStream* dest, BitStream* src, unsigned int numbits, unsigned int num1)
{
	unsigned short sum[3];
	GetSumTable(sum, numbits - num1, num1, true);
	try {
		//dest->PutBit0();    // version indicator
		ArithCoder ac;
		CprsErr err = ac.CompressBits(dest, src, sum, sum[2]);
		return err;
	} catch(ErrBufOverrun&) { 
		return CPRS_ERR_BUF; 
	}
}

CprsErr BitstreamCompressor::Compress(BitStream* dest, BitStream* src, unsigned int numbits, unsigned int num1)
{
	// Format:
	//   <ver>       v+1 bits	sequence of v ones and 0 after it; 'v' is the number of version
	//   <diff>      1 bit		1 if the original data were differentiated, 0 otherwise
	//
	//  If <diff> = 0:
	//   <data>      ...
	//  Else (<diff> = 1):
	//   <islong>    1 bit			1 if <dnum1> is stored on 32 bits, 0 otherwise (16 bits)
	//   <dnum1>     16/32 bits		ushort/uint - number of ones in differentiated stream
	//   <data>      ...
	//
	// Versions:
	//   v = 0 -  version with a bug in GetSumTable()
	//   v = 1 -  fixed bug

	try {
		dest->PutBit1();
		dest->PutBit0();	// version indicator
	} catch(ErrBufOverrun&) { 
		return CPRS_ERR_BUF; 
	}

	// try to differentiate the 'src' stream and check if the compressed data would be shorter
	char* tab = new char[(numbits+7)/8];
	BitStream diff(tab, numbits);
	uint pos = src->GetPos();

	try {
		uint dnum1 = Differ(&diff, src);

		int size_plain = (int)(Entropy((double)num1/numbits) * numbits);
		int size_diff  = (int)(Entropy((double)dnum1/numbits) * numbits)
						+ 1 + (dnum1 <= USHRT_MAX ? 16 : 32);  // no. of bits needed to save 'dnum1'

		if(size_plain <= size_diff) {
			src->SetPos(pos);
			dest->PutBit0();	// the stream is not differentiated
		}
		else {
			diff.Reset();
			src = &diff;
			num1 = dnum1;
			dest->PutBit1();	// the stream is differentiated

			if(dnum1 <= USHRT_MAX) {
				dest->PutBit0();			// 'dnum1' is stored as ushort
				dest->PutUInt(dnum1, 16);
			}
			else {
				dest->PutBit1();			// 'dnum1' is stored as uint
				dest->PutUInt(dnum1, 32);
			}
		}
	} catch(ErrBufOverrun&) { 
		delete[] tab; 
		return CPRS_ERR_BUF; 
	}

	CprsErr err = CompressData(dest, src, numbits, num1);
	delete[] tab;
	return err;
}

CprsErr BitstreamCompressor::DecompressData(BitStream* dest, BitStream* src, unsigned int numbits, unsigned int num1, bool patch)
{
	unsigned short sum[3];
	GetSumTable(sum, numbits - num1, num1, patch);
	try {
		//if(src->GetBit() != 0) return CPRS_ERR_VER;		// version indicator
		ArithCoder ac;
		CprsErr err = ac.DecompressBits(dest, src, sum, sum[2]);
		return err;
	} catch(ErrBufOverrun&) { 
		return CPRS_ERR_BUF; 
	}
}

CprsErr BitstreamCompressor::Decompress(BitStream* dest, BitStream* src, unsigned int numbits, unsigned int num1)
{
	try {
		// version indicator
		bool patch = true;
		if(src->GetBit() == 0) patch = false;
		else if(src->GetBit() != 0) return CPRS_ERR_VER;

		uchar isdiff = src->GetBit();					// is the data differentiated?
		if(isdiff) {
			uchar islong = src->GetBit();				// 'num1' is stored as uint or ushort?
			num1 = src->GetUInt(islong ? 32 : 16);

			char* tab = new char[(numbits+7)/8];
			try {
				BitStream diff(tab, numbits);
				CprsErr err = DecompressData(&diff, src, numbits, num1, patch);
				if(err != CPRS_SUCCESS) { delete[] tab; return err; }

				diff.Reset();
				Integrt(dest, &diff);
			} catch(ErrBufOverrun&) { 
				delete[] tab; 
				return CPRS_ERR_BUF; 
			}
			delete[] tab;
			return CPRS_SUCCESS;
		}
		else return DecompressData(dest, src, numbits, num1, patch);
	} catch(ErrBufOverrun&) { 
		return CPRS_ERR_BUF; 
	}
}

CprsErr BitstreamCompressor::Compress(unsigned int *in, unsigned int *&out, int &outpos, int &outlen, int numbits, int num1)
{
	MEASURE_FET("BitstreamCompressor::Compress(...)");
	BitStream src((char*)in, numbits),
			  dest((char*)out, outlen, outpos);

	CprsErr err = Compress(&dest, &src, numbits, num1);

	int move = dest.GetPos() / (8*sizeof(*out));
	out += move;
	outpos -= move * (8*sizeof(*out));
	outlen = dest.GetPos();
	return err;
}

CprsErr BitstreamCompressor::Decompress(unsigned int *in, int inpos, int inlen, unsigned int *out, int numbits, int num1)
{
	MEASURE_FET("BitstreamCompressor::Decompress(...)");
	BitStream src((char*)in, inlen, inpos),
			  dest((char*)out, numbits);
	return Decompress(&dest, &src, numbits, num1);
}

//////////////////////////////////////////////////////////////////////

CprsErr BitstreamCompressor::Compress(char* dest, uint& len, char* src, uint numbits, uint num1)
{
	MEASURE_FET("BitstreamCompressor::Compress(...)");
	BitStream ssrc(src, numbits),
			  sdest(dest, len*8);
	CprsErr err = Compress(&sdest, &ssrc, numbits, num1);
	len = (sdest.GetPos()+7) / 8;
	return err;
}

CprsErr BitstreamCompressor::Decompress(char* dest, uint len, char* src, uint numbits, uint num1)
{
	MEASURE_FET("BitstreamCompressor::Decompress(...)");
	BitStream ssrc(src, len*8),
			  sdest(dest, numbits);
	return Decompress(&sdest, &ssrc, numbits, num1);
}

