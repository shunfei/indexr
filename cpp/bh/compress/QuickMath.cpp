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

#include "QuickMath.h"

const double QuickMath::logof2 = log(2.0);
double QuickMath::tab_nlog2n[QuickMath::MAX_NLOG2N + 1];
double QuickMath::tab_log2[QuickMath::MAX_LOG2 + 1];
double QuickMath::tab_pow10f[QuickMath::MAX_POW10 + 1];
_int64  QuickMath::tab_pow10i[QuickMath::MAX_POW10 + 1];

void QuickMath::Init() {
	tab_nlog2n[0] = tab_log2[0] = 0.0;
	for(uint n = 1; n <= MAX_LOG2; n++)
		tab_log2[n] = log2((double)n);
	for(uint n = 1; n <= MAX_NLOG2N; n++)
		tab_nlog2n[n] = n * log2(n);

	tab_pow10f[0] = 1.0;
	tab_pow10i[0] = 1;
	for(uint n = 1; n <= MAX_POW10; n++)  {
		tab_pow10i[n] = tab_pow10i[n-1] * 10;
		tab_pow10f[n] = (double)tab_pow10i[n];
	}
}

uint QuickMath::precision10(_uint64 n)
{
	uint e = 0;
	while( (e <= MAX_POW10) && (n >= _uint64(tab_pow10i[e])) )
		e++;
	return e;
}

QuickMath ___math___(1);	// force initialization of static members

