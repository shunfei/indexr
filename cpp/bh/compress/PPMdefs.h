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

#ifndef __COMPRESS_PPMDEFS_H
#define __COMPRESS_PPMDEFS_H

#include <stdio.h>
#include <limits.h>
#include "defs.h"
#include "common/bhassert.h"

typedef ushort Count;
const Count COUNT_MAX = USHRT_MAX;
const Count MAX_ESC_COUNT = 500;

struct Range {
	Count low, high;
};

struct PPMParam
{
	double valid_count;		// currently not used! (method Prune() is recursive and unsafe)
	Count esc_count;
	double suf_ratio;		// in SuffixTree: minimum ratio of total count of the node and its suffix node
	double esc_exp, esc_coef;	// for modeling esc count as exponential function of total count of the node

	void Check()		{ BHASSERT(esc_count <= MAX_ESC_COUNT, "should be 'esc_count <= MAX_ESC_COUNT'"); }
	void SetDefault()	{ esc_count = 70; valid_count = 0.0; suf_ratio = 15;
						  esc_exp = 0.0; esc_coef = 0.0;
						  //esc_exp = 0.65; esc_coef = 3.5; esc_count = 5;
						  Check(); }
	PPMParam()			{ SetDefault(); }
};

// interface of models used for PPM compression (like suffix tree or CDAWG)
class PPMModel
{
protected:
	typedef uchar Symb;
	static const int NSymb = 256;

public:
	virtual void TransformForPPM(PPMParam param_ = PPMParam()) = 0;

	virtual void InitPPM() = 0;

	// compression: [str,len_total] -> [len_of_edge,rng,total]
	virtual void Move(Symb* str, int& len, Range& rng, Count& total) = 0;

	// decompression: [c,str,len_max] -> [str,len_of_edge,rng]+returned_error
	virtual CprsErr Move(Count c, Symb* str, int& len, Range& rng) = 0;
	virtual Count GetTotal() = 0;

	// statistics
	virtual int GetNNodes() = 0;
	virtual int GetMemUsage() = 0;		// real number of bytes used, without wasted space in 'vector'
	virtual int GetMemAlloc() = 0;		// total number of bytes used
	virtual void PrintStat(FILE* f) {}

	// file for making logs during compression or decompression
	FILE* logfile;

	PPMModel() : logfile(NULL) {}
	virtual ~PPMModel() {}
};


#endif
