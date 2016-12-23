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

#ifndef __QUICKMATH_H
#define __QUICKMATH_H

#include <math.h>
#include <assert.h>
#include "../common/CommonDefinitions.h"
#include "../compress/tools.h"
//#include "compress/defs.h"


// Precomputed values of some mathematical formulas, used to speed up computations
class QuickMath {
public:
	static const int MAX_NLOG2N = 65536;
	static const int MAX_LOG2   = 65536;
	static const int MAX_POW10  = 18;

private:
	// array of precomputed values of n*log2(n), for n=0,...,2^16  - for fast computation of entropy
	static double tab_nlog2n[MAX_NLOG2N + 1];
	static double tab_log2[MAX_LOG2 + 1];			// precomputed values of log2(n)
	static double tab_pow10f[MAX_POW10 + 1];
	static _int64  tab_pow10i[MAX_POW10 + 1];
	static void Init();

public:
	QuickMath()		{}
	QuickMath(int)	{ Init(); }		// should be invoked exactly once during program execution, to force initialization

	static const double logof2;
	static double nlog2n(uint n)		{ assert(n <= MAX_NLOG2N); return tab_nlog2n[n]; }
	//static double log(uint n)			{ assert(n <= MAX_LOG); return tab_log[n]; }
	static double log2(uint n)			{ assert(n <= MAX_LOG2); return tab_log2[n]; }
	static double log2(double x)		{ return ::log(x)/logof2; }

	// returns 10^n
	static double power10f(uint n)		{ assert(n <= MAX_POW10); return tab_pow10f[n]; }
	static _int64 power10i(uint n)		{ assert(n <= MAX_POW10); return tab_pow10i[n]; }

	// number of digits in decimal notation of 'n'
	static uint precision10(_uint64 n);

	// number of bits in binary notation of 'n'
	static uint precision2(_uint64 n)	{ return GetBitLen(n); }
};


#endif
