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

#ifndef AGGREGATOR_ADVANCED_H_
#define AGGREGATOR_ADVANCED_H_

#include "Aggregator.h"

/*! \brief A generalization of aggregation algorithms (counters) to be used in GROUP BY etc.
 * See "Aggregator.h" for more details.
 *
 * \note The aim of this class hierarchy is to make aggregators more pluggable,
 * as well as to replace multiple ifs and switch/cases by virtual methods.
 * Therefore it is suggested to implement all variants as separate subclasses,
 * e.g. to distinguish 32- and 64-bit counters.
 */

///////////////////////////////////////////////////////////////////////////////////////////////
//! Abstract class for all "second central moment"-like statistical operations,
//  i.e. VAR_POP, VAR_SAMP, STD_POP, STD_SAMP
class AggregatorStat : public Aggregator
{
public:
	// Buffer contents for all functions, 8+8+8 bytes:
	//	<no_obj> <A> <Q>  - int64, double, double
	AggregatorStat() : Aggregator() { }
	AggregatorStat(AggregatorStat &sec) : Aggregator(sec) { }

	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual int BufferByteSize() 				{ return 24; }
	virtual void Reset(unsigned char *buf)			{ *((_int64*)buf) = 0; *((double*)(buf + 8)) = 0; *((double*)(buf + 16)) = 0; }

protected:
	_int64& NoObj(unsigned char *buf)				{ return *((_int64*)buf); }

	// efficient implementation from WIKI
	// http://en.wikipedia.org/wiki/Standard_deviation
	double& A(unsigned char * buf) { return *((double*)(buf + 8)); }
	double& Q(unsigned char * buf) { return *((double*)(buf + 16)); }

	double VarPop(unsigned char *buf) {	return Q(buf) / NoObj(buf); }
	double VarSamp(unsigned char *buf) { return Q(buf) / (NoObj(buf) - 1); }
};

class AggregatorStat64 : public AggregatorStat 
{
public:
	AggregatorStat64(int precision) : prec_factor(PowOfTen(precision)) {}
	AggregatorStat64(AggregatorStat64 &sec) : AggregatorStat(sec), prec_factor(sec.prec_factor) { }

	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);

protected:
	double prec_factor;			// precision factor: the values must be divided by it
};

class AggregatorStatD : public AggregatorStat 
{
public:
	AggregatorStatD() : AggregatorStat() { }
	AggregatorStatD(AggregatorStatD &sec) : AggregatorStat(sec) { }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor);

	virtual double GetValueD(unsigned char *buf)			{ _int64 v = GetValue64(buf); return *(double*)(&v); }
};

///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorVarPop64 : public AggregatorStat64
{
public:
	AggregatorVarPop64(int precision) : AggregatorStat64(precision) {}
	AggregatorVarPop64(AggregatorVarPop64 &sec) : AggregatorStat64(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorVarPop64(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};

///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorVarSamp64 : public AggregatorStat64
{
public:
	AggregatorVarSamp64(int precision) : AggregatorStat64(precision) {}
	AggregatorVarSamp64(AggregatorVarSamp64 &sec) : AggregatorStat64(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorVarSamp64(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};
///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorStdPop64 : public AggregatorStat64
{
public:
	AggregatorStdPop64(int precision) : AggregatorStat64(precision) {}
	AggregatorStdPop64(AggregatorStdPop64 &sec) : AggregatorStat64(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorStdPop64(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};
///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorStdSamp64 : public AggregatorStat64
{
public:
	AggregatorStdSamp64(int precision) : AggregatorStat64(precision) {}
	AggregatorStdSamp64(AggregatorStdSamp64 &sec) : AggregatorStat64(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorStdSamp64(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};

///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorVarPopD : public AggregatorStatD
{
public:
	AggregatorVarPopD() : AggregatorStatD() {}
	AggregatorVarPopD(AggregatorVarPopD &sec) : AggregatorStatD(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorVarPopD(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};

///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorVarSampD : public AggregatorStatD
{
public:
	AggregatorVarSampD() : AggregatorStatD() {}
	AggregatorVarSampD(AggregatorVarSampD &sec) : AggregatorStatD(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorVarSampD(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};

///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorStdPopD : public AggregatorStatD
{
public:
	AggregatorStdPopD() : AggregatorStatD() {}
	AggregatorStdPopD(AggregatorStdPopD &sec) : AggregatorStatD(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorStdPopD(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};

///////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorStdSampD : public AggregatorStatD
{
public:
	AggregatorStdSampD() : AggregatorStatD() {}
	AggregatorStdSampD(AggregatorStdSampD &sec) : AggregatorStatD(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorStdSampD(*this); }

	virtual _int64 GetValue64(unsigned char *buf);
};

//////////////////////////////////////////////////////////////////////////////////////////////

class AggregatorBitAnd : public Aggregator
{
public:
	AggregatorBitAnd() : Aggregator() {}
	AggregatorBitAnd(AggregatorBitAnd &sec) : Aggregator(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorBitAnd(*this); }

	virtual int BufferByteSize() 				{ return 8; }
	virtual void Reset(unsigned char *buf)			{ *((_int64*)buf) = 0xFFFFFFFFFFFFFFFFULL; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
	{ *((_int64*)buf) = (*((_int64*)buf) & v); }
	virtual void Merge(unsigned char *buf, unsigned char *src_buf)
	{ *((_int64*)buf) = (*((_int64*)buf) & *((_int64*)src_buf)); }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor);
	virtual _int64 GetValue64(unsigned char *buf)	{ return *((_int64*)buf); }

	virtual bool FactorNeeded()						{ return false; }
	virtual bool IgnoreDistinct()					{ return true; }
};

class AggregatorBitOr : public Aggregator
{
public:
	AggregatorBitOr() : Aggregator() {}
	AggregatorBitOr(AggregatorBitOr &sec) : Aggregator(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorBitOr(*this); }

	virtual int BufferByteSize() 				{ return 8; }
	virtual void Reset(unsigned char *buf)			{ *((_int64*)buf) = 0; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
	{ *((_int64*)buf) = (*((_int64*)buf) | v); }
	virtual void Merge(unsigned char *buf, unsigned char *src_buf)
	{ *((_int64*)buf) = (*((_int64*)buf) | *((_int64*)src_buf)); }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor);
	virtual _int64 GetValue64(unsigned char *buf)	{ return *((_int64*)buf); }

	virtual bool FactorNeeded()						{ return false; }
	virtual bool IgnoreDistinct()					{ return true; }
};

class AggregatorBitXor : public Aggregator
{
public:
	AggregatorBitXor() : Aggregator() {}
	AggregatorBitXor(AggregatorBitXor &sec) : Aggregator(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorBitXor(*this); }

	virtual int BufferByteSize() 				{ return 8; }
	virtual void Reset(unsigned char *buf)			{ *((_int64*)buf) = 0; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
	{ if(factor%2 == 1) *((_int64*)buf) = (*((_int64*)buf) ^ v); }
	virtual void Merge(unsigned char *buf, unsigned char *src_buf)
	{ *((_int64*)buf) = (*((_int64*)buf) ^ *((_int64*)src_buf)); }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor);
	virtual _int64 GetValue64(unsigned char *buf)	{ return *((_int64*)buf); }
};

#endif
