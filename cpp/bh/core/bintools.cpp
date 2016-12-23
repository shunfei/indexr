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

#if !defined(__sun__)
#include <boost/math/special_functions.hpp>
#endif

#include "bintools.h"
#include "system/RCSystem.h"

using namespace std;

const int bin_sums[256]={	0,	1,	1,	2,	1,	2,	2,	3,		// 00000***
							1,	2,	2,	3,	2,	3,	3,	4,		// 00001***
							1,	2,	2,	3,	2,	3,	3,	4,		// 00010***
							2,	3,	3,	4,	3,	4,	4,	5,		// 00011***
							1,	2,	2,	3,	2,	3,	3,	4,		// 00100***
							2,	3,	3,	4,	3,	4,	4,	5,		// 00101***
							2,	3,	3,	4,	3,	4,	4,	5,		// 00110***
							3,	4,	4,	5,	4,	5,	5,	6,		// 00111***
							1,	2,	2,	3,	2,	3,	3,	4,		// 01000***
							2,	3,	3,	4,	3,	4,	4,	5,		// 01001***
							2,	3,	3,	4,	3,	4,	4,	5,		// 01010***
							3,	4,	4,	5,	4,	5,	5,	6,		// 01011***
							2,	3,	3,	4,	3,	4,	4,	5,		// 01100***
							3,	4,	4,	5,	4,	5,	5,	6,		// 01101***
							3,	4,	4,	5,	4,	5,	5,	6,		// 01110***
							4,	5,	5,	6,	5,	6,	6,	7,		// 01111***
							1,	2,	2,	3,	2,	3,	3,	4,		// 10000***
							2,	3,	3,	4,	3,	4,	4,	5,		// 10001***
							2,	3,	3,	4,	3,	4,	4,	5,		// 10010***
							3,	4,	4,	5,	4,	5,	5,	6,		// 10011***
							2,	3,	3,	4,	3,	4,	4,	5,		// 10100***
							3,	4,	4,	5,	4,	5,	5,	6,		// 10101***
							3,	4,	4,	5,	4,	5,	5,	6,		// 10110***
							4,	5,	5,	6,	5,	6,	6,	7,		// 10111***
							2,	3,	3,	4,	3,	4,	4,	5,		// 11000***
							3,	4,	4,	5,	4,	5,	5,	6,		// 11001***
							3,	4,	4,	5,	4,	5,	5,	6,		// 11010***
							4,	5,	5,	6,	5,	6,	6,	7,		// 11011***
							3,	4,	4,	5,	4,	5,	5,	6,		// 11100***
							4,	5,	5,	6,	5,	6,	6,	7,		// 11101***
							4,	5,	5,	6,	5,	6,	6,	7,		// 11110***
							5,	6,	6,	7,	6,	7,	7,	8};		// 11111***

int CalculateBinSum(unsigned int n)
{
	//		NOTE: there is a potentially better method (to be tested and extended to 32 and 64 bits)
	//		The problem is known as "population function".
	//
	//		x = input (8 bit);
	//		x = x&55h + (x>>1)&55h;		x = x&33h + (x>>2)&33h;		x = x&0fh + (x>>4)&0fh
	//		result = x
	//

	int s=0;
	s+=bin_sums[n%256];
	n=n>>8;
	s+=bin_sums[n%256];
	n=n>>8;
	s+=bin_sums[n%256];
	n=n>>8;
	s+=bin_sums[n%256];
	return s;
}

int  CalculateBinSize(unsigned int n)			// 000 -> 0, 001 -> 1, 100 -> 3 etc.
{
	int s = 0;
	while(n > 255) {
		n = n>>8;
		s += 8;
	} 
	while(n > 0) {
		n = n>>1;
		s++;
	}
	return s;
}

int CalculateBinSize(_uint64 n)			// 000 -> 0, 001 -> 1, 100 -> 3 etc.
{
	if(((uint)(n>>32)) == 0) return CalculateBinSize((uint)n);		// >> safe, as n is 64-bit
	return CalculateBinSize(((uint)(n>>32))) + 32;
}

int CalculateByteSize(_uint64 n)			// 0 -> 0, 1 -> 1, 255 -> 1, 256 -> 2 etc.
{
	int res = 0;
	while(n != 0) {
		n >>= 8;
		res++;
	}
	return res;
}

_int64 SafeMultiplication(_int64 x, _int64 y)	// return a multiplication of two numbers or NULL_VALUE_64 in case of overflow
{
	if(x == NULL_VALUE_64 || y == NULL_VALUE_64 ||
		((x > 0x7FFFFFFF || y > 0x7FFFFFFF) &&
		  CalculateBinSize(_uint64(x)) + CalculateBinSize(_uint64(y)) > 63 ))
		return NULL_VALUE_64;
	return x * y;
}


RSValue Or(RSValue f, RSValue s)
{
	if(f == RS_ALL || s == RS_ALL)
		return RS_ALL;
	if(f == RS_SOME || s == RS_SOME)
		return RS_SOME;
	return RS_NONE;
}

RSValue And(RSValue f, RSValue s)
{
	if(f == RS_ALL && s == RS_ALL)
		return RS_ALL;
	if(f == RS_NONE || s == RS_NONE)
		return RS_NONE;
	return RS_SOME;
}

/////////////////////////////////////////////////////////////////////////////////////

_int64  MonotonicDouble2Int64(_int64 d)	// encode double value (bitwise stored as _int64) into _int64 to prevent comparison directions
{
#if !defined(__sun__)
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!(boost::math::isnan)(*(double*)&d));
#endif
	if(*((double*)(&d)) < 0)
		return ~d;							// for negative - reverse all bits (including the sign 1)
	else
		return (d | 0x8000000000000000LL);			// for positive - change the sign 0 bit into 1
}

_int64  MonotonicInt642Double(_int64 v)	// decode a value encoded by MonotonicDouble2Int64
{
	if((v & 0x8000000000000000LL) != 0)
		return (v & 0x7FFFFFFFFFFFFFFFLL);	// reset the last bit, if it is 1
	return ~v;
}

///////////////////////////////////////

// Format of float: <s><exp><mantissa>,
// where <s> is one-bit sign, <exp> is 8-bit signed exponent, <mantissa> is unsigned 23 bit.
// Note that the same bit is a sign in double and in int64.

/*inline*/ _int64 FloatPlusEpsilon(_int64 v)		// transform v (as a float value) to the first larger value
{
	union { double d; _int64 i;} u;
	union {float f; int i;} uf;
	u.i = v;
	//if((v & 0x7F800000) == 0x7F800000)	// special value: NaN etc.
	if((v & 0x7FF0000000000000LL) == 0x7FF0000000000000LL)	// special value: NaN etc.
		return v;

	if(v < 0) {// negative number?
		union { double d; _int64 i;} uneg;
		uf.f = (float)u.d;
		uneg.d  = -uf.f;
		u.i = FloatMinusEpsilon(uneg.i);
		uf.f = (float)u.d;
		u.d = -uf.f;
		return u.i;
	}
	uf.f = (float)u.d;
	uf.i++;
	u.d = uf.f;
//	cerr << "end Plus epsil\n" ;
	return u.i;

}

/*inline*/ _int64 FloatMinusEpsilon(_int64 v)		// transform v (as a float value) to the first larger value
{
	union { double d; _int64 i;} u;
	union {float f; int i;} uf;
	//cerr << "start Minus epsil\n" ;
	if((v & 0x7FF0000000000000LL) == 0x7FF0000000000000LL)	// special value: NaN etc.
		return v;

	u.i=v;
	if(v < 0) { // negative number?
		union { double d; _int64 i;} uneg;
		uf.f = (float)u.d;
		uneg.d  = -uf.f;				// transform to positive
		u.i = FloatPlusEpsilon(uneg.i);
		uf.f = (float)u.d;
		u.d = -uf.f;					// and back to negative
		return u.i;
	}
	if(v == 0) {
		//int x = 0x80400001;
		//float f = *(float*)&x;
		//double d = f;
		//return *(int64 *)&d;					// very small negative
	  return  0x8000000000400001LL ;
	}
	if(v ==   0x0000000000400001LL)
		return 0;					// very small positive
	uf.f = (float)u.d;
	if ((uf.i & 0x7FFFFF) == 0x000001) {		// mantissa is minimal
		uf.i = (uf.i >> 23) - 1;			// decrease exponent by one
		uf.i = ((uf.i << 23) | 0x000001);	// leave the minimal mantissa
		u.d = uf.f;
		return u.i;
	}
	uf.i--;
	u.d = uf.f;
	return u.i;
//	double d = *(float *)&iv;
//	return *(_int64 *)&d;
}

////////////////////////////////////////////

// Format of double: <s><exp><mantissa>,
// where <s> is one-bit sign, <exp> is 11-bit signed exponent, <mantissa> is unsigned 52 bit.
// Note that the same bit is a sign in double and in _int64.

/*inline*/ _int64  DoublePlusEpsilon(_int64 v)		// transform v (as a double value) to the first larger value
{
	union { double d; _int64 i;} u;

	if((v & 0x7FF0000000000000LL)==0x7FF0000000000000LL)	// special value: NaN etc.
		return v;
	if(v < 0)	// negative number?
	{
		u.i = v;
		u.d = -u.d;									// transform to positive
		u.i = DoubleMinusEpsilon(u.i);
		u.d = -u.d;										// and back to negative
		return u.i;

	}
	if((v & 0x000FFFFFFFFFFFFFLL)==0x000FFFFFFFFFFFFFLL)	// mantissa is maximal
	{
		v = v + 1;			// effectively enlarge exponent by one
		return (v | 0x0008000000000000LL);	// set the highest bit of mantissa (currently zeroed)
	}
	return v + 1;
}

/*inline*/ _int64  DoubleMinusEpsilon(_int64 v)		// transform v (as a double value) to the first larger value
{
	union { double d; _int64 i;} u;

	if((v & 0x7FF0000000000000LL)==0x7FF0000000000000LL)	// special value: NaN etc.
		return v;

	if(v < 0)	// negative number?
	{
		u.i = v;
		u.d = -u.d;							// transform to positive
		u.i = DoublePlusEpsilon(u.i);
		u.d = -u.d;							// and back to negative
		return u.i;

	}
	if(v == 0) return 0x8000000000000001LL;					// very small negative
	if(v == 0x0000000000000001LL) return 0;					// very small positive
	if((v & 0x000FFFFFFFFFFFFFLL) == 0x0000000000000001LL)	// mantissa is minimal
	{
		v = (v >> 52) - 1;			// decrease exponent by one
		return ((v<<52) | 0x0000000000000001LL);	// leave the minimal mantissa
	}
	return v - 1;
}
///////////////////////////////////////////////////////////////////////////////////////////


uint HashValue(const char unsigned* data, int len)		// a simple hash function similar to the one used in boost::hash
{
	uint val = 0;
	int i = 0;
	uint *d = (uint*)data;
	int len4 = (len / 4) * 4;			// whole 4-byte chunks - it is faster to operate on uint as long as possible
	if(len4 == 4) {
		val = *d + 0x9e3779b9;
	} else {
		for(; i < len4; i += 4)
			val ^= *(d++) + 0x9e3779b9 + (val << 10) + (val >> 2);
	}
	for(; i < len; i++)
		val ^= uint(data[i]) + 0x9e3779b9 + (val << 10) + (val >> 2);	// in boost... there is "<< 6" here, but it lead to too many collisions
	val ^= (val >> 11) + (val << 21);					// more hashing to prevent collisions
	return val;
}

//////////////////////////////////////////////////////////////////////////////////////
/*
 **********************************************************************
 ** md5.c                                                            **
 ** RSA Data Security, Inc. MD5 Message Digest Algorithm             **
 ** Created: 2/17/90 RLR                                             **
 ** Revised: 1/91 SRD,AJ,BSK,JT Reference C Version                  **
 **********************************************************************

 **********************************************************************
 ** Copyright (C) 1990, RSA Data Security, Inc. All rights reserved. **
 **                                                                  **
 ** License to copy and use this software is granted provided that   **
 ** it is identified as the "RSA Data Security, Inc. MD5 Message     **
 ** Digest Algorithm" in all material mentioning or referencing this **
 ** software or this function.                                       **
 **                                                                  **
 ** License is also granted to make and use derivative works         **
 ** provided that such works are identified as "derived from the RSA **
 ** Data Security, Inc. MD5 Message Digest Algorithm" in all         **
 ** material mentioning or referencing the derived work.             **
 **                                                                  **
 ** RSA Data Security, Inc. makes no representations concerning      **
 ** either the merchantability of this software or the suitability   **
 ** of this software for any particular purpose.  It is provided "as **
 ** is" without express or implied warranty of any kind.             **
 **                                                                  **
 ** These notices must be retained in any copies of any part of this **
 ** documentation and/or software.                                   **
 **********************************************************************
 */

/* Data structure for MD5 (Message Digest) computation */
typedef struct {
  _uint64 bits_handled;                   /* number of _bits_ handled mod 2^64 */
  uint buf[4];                                    /* scratch buffer */
  unsigned char in[64];                              /* input buffer */
  unsigned char* digest;     /* actual digest after MD5Final call */
} MD5_CTX_bt;

void MD5Update(MD5_CTX_bt*, unsigned char const*, unsigned int);
void MD5Final(MD5_CTX_bt*);

/* Computes the message digest for string inString.
   Prints out message digest, a space, the string (in quotes) and a
   carriage return.
 */
void HashMD5(unsigned char const* buf, int len, unsigned char* hash)
{
	MD5_CTX_bt mdContext;
	mdContext.digest = hash;
	mdContext.bits_handled = 0;
	/* Load magic initialization constants. */
	mdContext.buf[0] = (uint)0x67452301;
	mdContext.buf[1] = (uint)0xefcdab89;
	mdContext.buf[2] = (uint)0x98badcfe;
	mdContext.buf[3] = (uint)0x10325476;

	MD5Update( &mdContext, buf, len );
	MD5Final( &mdContext );
}

static void Transform( uint*, uint* );

static unsigned char PADDING[64] = {
  0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};

/* F, G and H are basic MD5 functions: selection, majority, parity */
#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (z)) | ((y) & (~z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define I(x, y, z) ((y) ^ ((x) | (~z)))

/* ROTATE_LEFT rotates x left n bits */
#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32-(n))))

/* FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4 */
/* Rotation is separate from addition to prevent recomputation */
#define FF(a, b, c, d, x, s, ac) \
  {(a) += F ((b), (c), (d)) + (x) + (uint)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define GG(a, b, c, d, x, s, ac) \
  {(a) += G ((b), (c), (d)) + (x) + (uint)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define HH(a, b, c, d, x, s, ac) \
  {(a) += H ((b), (c), (d)) + (x) + (uint)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }
#define II(a, b, c, d, x, s, ac) \
  {(a) += I ((b), (c), (d)) + (x) + (uint)(ac); \
   (a) = ROTATE_LEFT ((a), (s)); \
   (a) += (b); \
  }

void MD5Update( MD5_CTX_bt* mdContext, unsigned char const* inBuf, unsigned int inLen )
{
  uint in[16];
  int mdi;

  /* compute number of bytes mod 64 */
  mdi = (int)((mdContext->bits_handled >> 3) & 0x3F);

  /* update number of bits */
  mdContext->bits_handled += ((_uint64)inLen) << 3;
  while (inLen--) {
    /* add new character to buffer, increment mdi */
    mdContext->in[mdi++] = *inBuf++;

    /* transform if necessary */
    if (mdi == 0x40) {
    	memcpy(in, mdContext->in, 64);
    	Transform (mdContext->buf, in);
    	mdi = 0;
    }
  }
}

void MD5Final( MD5_CTX_bt* mdContext )
{
  uint in[16];
  int mdi;
  unsigned int padLen;

  /* save number of bits */
  *(_uint64*)(in+14) = mdContext->bits_handled;

  /* compute number of bytes mod 64 */
  mdi = (int)((mdContext->bits_handled >> 3) & 0x3F);

  /* pad out to 56 mod 64 */
  padLen = (mdi < 56) ? (56 - mdi) : (120 - mdi);
  MD5Update (mdContext, PADDING, padLen);

  /* append length in bits and transform */
	memcpy(in, mdContext->in, 56);
	Transform (mdContext->buf, in);

  /* store buffer in digest */
	memcpy(mdContext->digest, mdContext->buf, 16);
}

/* Basic MD5 step. Transform buf based on in.
 */
static void Transform( uint* buf, uint* in )
{
  uint a = buf[0], b = buf[1], c = buf[2], d = buf[3];

  /* Round 1 */
#define S11 7
#define S12 12
#define S13 17
#define S14 22
  FF ( a, b, c, d, in[ 0], S11, 3614090360); /* 1 */
  FF ( d, a, b, c, in[ 1], S12, 3905402710); /* 2 */
  FF ( c, d, a, b, in[ 2], S13,  606105819); /* 3 */
  FF ( b, c, d, a, in[ 3], S14, 3250441966); /* 4 */
  FF ( a, b, c, d, in[ 4], S11, 4118548399); /* 5 */
  FF ( d, a, b, c, in[ 5], S12, 1200080426); /* 6 */
  FF ( c, d, a, b, in[ 6], S13, 2821735955); /* 7 */
  FF ( b, c, d, a, in[ 7], S14, 4249261313); /* 8 */
  FF ( a, b, c, d, in[ 8], S11, 1770035416); /* 9 */
  FF ( d, a, b, c, in[ 9], S12, 2336552879); /* 10 */
  FF ( c, d, a, b, in[10], S13, 4294925233); /* 11 */
  FF ( b, c, d, a, in[11], S14, 2304563134); /* 12 */
  FF ( a, b, c, d, in[12], S11, 1804603682); /* 13 */
  FF ( d, a, b, c, in[13], S12, 4254626195); /* 14 */
  FF ( c, d, a, b, in[14], S13, 2792965006); /* 15 */
  FF ( b, c, d, a, in[15], S14, 1236535329); /* 16 */

  /* Round 2 */
#define S21 5
#define S22 9
#define S23 14
#define S24 20
  GG ( a, b, c, d, in[ 1], S21, 4129170786); /* 17 */
  GG ( d, a, b, c, in[ 6], S22, 3225465664); /* 18 */
  GG ( c, d, a, b, in[11], S23,  643717713); /* 19 */
  GG ( b, c, d, a, in[ 0], S24, 3921069994); /* 20 */
  GG ( a, b, c, d, in[ 5], S21, 3593408605); /* 21 */
  GG ( d, a, b, c, in[10], S22,   38016083); /* 22 */
  GG ( c, d, a, b, in[15], S23, 3634488961); /* 23 */
  GG ( b, c, d, a, in[ 4], S24, 3889429448); /* 24 */
  GG ( a, b, c, d, in[ 9], S21,  568446438); /* 25 */
  GG ( d, a, b, c, in[14], S22, 3275163606); /* 26 */
  GG ( c, d, a, b, in[ 3], S23, 4107603335); /* 27 */
  GG ( b, c, d, a, in[ 8], S24, 1163531501); /* 28 */
  GG ( a, b, c, d, in[13], S21, 2850285829); /* 29 */
  GG ( d, a, b, c, in[ 2], S22, 4243563512); /* 30 */
  GG ( c, d, a, b, in[ 7], S23, 1735328473); /* 31 */
  GG ( b, c, d, a, in[12], S24, 2368359562); /* 32 */

  /* Round 3 */
#define S31 4
#define S32 11
#define S33 16
#define S34 23
  HH ( a, b, c, d, in[ 5], S31, 4294588738); /* 33 */
  HH ( d, a, b, c, in[ 8], S32, 2272392833); /* 34 */
  HH ( c, d, a, b, in[11], S33, 1839030562); /* 35 */
  HH ( b, c, d, a, in[14], S34, 4259657740); /* 36 */
  HH ( a, b, c, d, in[ 1], S31, 2763975236); /* 37 */
  HH ( d, a, b, c, in[ 4], S32, 1272893353); /* 38 */
  HH ( c, d, a, b, in[ 7], S33, 4139469664); /* 39 */
  HH ( b, c, d, a, in[10], S34, 3200236656); /* 40 */
  HH ( a, b, c, d, in[13], S31,  681279174); /* 41 */
  HH ( d, a, b, c, in[ 0], S32, 3936430074); /* 42 */
  HH ( c, d, a, b, in[ 3], S33, 3572445317); /* 43 */
  HH ( b, c, d, a, in[ 6], S34,   76029189); /* 44 */
  HH ( a, b, c, d, in[ 9], S31, 3654602809); /* 45 */
  HH ( d, a, b, c, in[12], S32, 3873151461); /* 46 */
  HH ( c, d, a, b, in[15], S33,  530742520); /* 47 */
  HH ( b, c, d, a, in[ 2], S34, 3299628645); /* 48 */

  /* Round 4 */
#define S41 6
#define S42 10
#define S43 15
#define S44 21
  II ( a, b, c, d, in[ 0], S41, 4096336452); /* 49 */
  II ( d, a, b, c, in[ 7], S42, 1126891415); /* 50 */
  II ( c, d, a, b, in[14], S43, 2878612391); /* 51 */
  II ( b, c, d, a, in[ 5], S44, 4237533241); /* 52 */
  II ( a, b, c, d, in[12], S41, 1700485571); /* 53 */
  II ( d, a, b, c, in[ 3], S42, 2399980690); /* 54 */
  II ( c, d, a, b, in[10], S43, 4293915773); /* 55 */
  II ( b, c, d, a, in[ 1], S44, 2240044497); /* 56 */
  II ( a, b, c, d, in[ 8], S41, 1873313359); /* 57 */
  II ( d, a, b, c, in[15], S42, 4264355552); /* 58 */
  II ( c, d, a, b, in[ 6], S43, 2734768916); /* 59 */
  II ( b, c, d, a, in[13], S44, 1309151649); /* 60 */
  II ( a, b, c, d, in[ 4], S41, 4149444226); /* 61 */
  II ( d, a, b, c, in[11], S42, 3174756917); /* 62 */
  II ( c, d, a, b, in[ 2], S43,  718787259); /* 63 */
  II ( b, c, d, a, in[ 9], S44, 3951481745); /* 64 */

  buf[0] += a;
  buf[1] += b;
  buf[2] += c;
  buf[3] += d;
}

////////////////////////////////////////////////////////////////////////////////////////////
