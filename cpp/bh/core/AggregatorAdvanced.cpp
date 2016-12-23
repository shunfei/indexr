/* Copyright (C)  2005-2009 Infobright Inc.

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

#include "AggregatorAdvanced.h"
#include <math.h>

void AggregatorStat64::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	// efficient implementation from WIKI
	// http://en.wikipedia.org/wiki/Standard_deviation
	stats_updated = false;
	if(v != NULL_VALUE_64) {
		if(NoObj(buf) == 0) {
			NoObj(buf) += 1;
			A(buf) = double(v); // m
			Q(buf) = 0; // s
			factor--;
		}
		for(int i = 0; i < factor; i++) {
			NoObj(buf) += 1;
			double vd = double(v);
			double A_prev = A(buf);
			A(buf) = A_prev + (vd - A_prev) / (double)NoObj(buf);
			Q(buf) += (vd - A_prev) * (vd - A(buf));
		}
	}
}

void AggregatorStatD::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	// efficient implementation from WIKI
	// http://en.wikipedia.org/wiki/Standard_deviation
	stats_updated = false;
	if(v != NULL_VALUE_64) {
		if(NoObj(buf) == 0) {
			NoObj(buf) += 1;
			A(buf) = *((double*)(&v)); // m
			Q(buf) = 0; // s
			factor--;
		}
		for(int i = 0; i < factor; i++) {
			NoObj(buf) += 1;
			double vd = *((double*)(&v));
			double A_prev = A(buf);
			A(buf) = A_prev + (vd - A_prev) / (double)NoObj(buf);
			Q(buf) += (vd - A_prev) * (vd - A(buf));
		}
	}
}

void AggregatorStat::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(NoObj(src_buf) == 0)
		return;
	stats_updated = false;
	if(NoObj(buf) == 0)
		memcpy(buf, src_buf, BufferByteSize());
	else {
		_int64 n = NoObj(buf);
		_int64 m = NoObj(src_buf);
		// n*var(X) = Q(X)
		// var(X+Y) = (n*var(X) + m*var(Y)) / (n+m) + nm / (n+m)^2 * (avg(X) - avg(Y))^2
		Q(buf) = Q(buf) + Q(src_buf) + n * m / double(n + m) * (A(buf) - A(src_buf)) * (A(buf) - A(src_buf));

		// avg(X+Y) = (avg(X)*n + avg(Y)*m) / (n+m)
		A(buf) = (A(buf) * n + A(src_buf) * m) / double(n + m);
		NoObj(buf) = n + m;
	}
}

void AggregatorStatD::PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)
{
	stats_updated = false;
	RCNum val(RC_REAL);
	if(!v.IsEmpty() && RCNum::ParseReal(v, val, RC_REAL) == BHRC_SUCCESS && !val.IsNull()) {
		double d_val = double(val);
		PutAggregatedValue(buf, *((_int64*)(&d_val)), factor);
	}
}

_int64 AggregatorVarPop64::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 1)
		return NULL_VALUE_64;
	double vd = VarPop(buf) / prec_factor / double(prec_factor);
	return *(_int64*)(&vd);
}

_int64 AggregatorVarSamp64::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 2)
		return NULL_VALUE_64;
	double vd = VarSamp(buf) / prec_factor / double(prec_factor);
	return *(_int64*)(&vd);
}

_int64 AggregatorStdPop64::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 1)
		return NULL_VALUE_64;
	double vd = sqrt(VarPop(buf)) / prec_factor;
	return *(_int64*)(&vd);
}

_int64 AggregatorStdSamp64::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 2)
		return NULL_VALUE_64;
	double vd = sqrt(VarSamp(buf)) / prec_factor;
	return *(_int64*)(&vd);
}

_int64 AggregatorVarPopD::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 1)
		return NULL_VALUE_64;
	double vd = VarPop(buf);
	return *(_int64*)(&vd);
}

_int64 AggregatorVarSampD::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 2)
		return NULL_VALUE_64;
	double vd = VarSamp(buf);
	return *(_int64*)(&vd);
}

_int64 AggregatorStdPopD::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 1)
		return NULL_VALUE_64;
	//double vd = Q(buf) / NoObj(buf);
	//vd = sqrt(vd);
	double vd = sqrt(VarPop(buf));
	return *(_int64*)(&vd);
}

_int64 AggregatorStdSampD::GetValue64(unsigned char *buf)
{
	if(NoObj(buf) < 2)
		return NULL_VALUE_64;
	//double vd = Q(buf) / (NoObj(buf) - 1);
	//vd = sqrt(vd);
	double vd = sqrt(VarSamp(buf));
	return *(_int64*)(&vd);
}

///////////////////////////////////////////////////////////////////////////////////////////////

void AggregatorBitAnd::PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)
{
	stats_updated = false;
	RCNum val(RC_BIGINT);
	if(!v.IsEmpty() && RCNum::Parse(v, val, RC_BIGINT) == BHRC_SUCCESS && !val.IsNull()) {
		PutAggregatedValue(buf, _int64(val), factor);
	}
}

void AggregatorBitOr::PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)
{
	stats_updated = false;
	RCNum val(RC_BIGINT);
	if(!v.IsEmpty() && RCNum::Parse(v, val, RC_BIGINT) == BHRC_SUCCESS && !val.IsNull()) {
		PutAggregatedValue(buf, _int64(val), factor);
	}
}

void AggregatorBitXor::PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)
{
	stats_updated = false;
	RCNum val(RC_BIGINT);
	if(!v.IsEmpty() && RCNum::Parse(v, val, RC_BIGINT) == BHRC_SUCCESS && !val.IsNull()) {
		PutAggregatedValue(buf, _int64(val), factor);
	}
}
