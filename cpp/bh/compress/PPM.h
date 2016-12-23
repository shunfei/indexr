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

#ifndef __COMPRESS_PPM_H
#define __COMPRESS_PPM_H

#include <stdio.h>
#include <iostream>
#include "ArithCoder.h"
#include "PPMdefs.h"


class PPM
{
	typedef uchar Symb;
	
	PPMModel* model;

	// compression and decompression using ArithCoder
	CprsErr CompressArith(char* dest, int& dlen, Symb* src, int slen);
	CprsErr DecompressArith(Symb* dest, int dlen, char* src, int slen);

public:
	enum ModelType { ModelNull, ModelSufTree, ModelWordGraph };
	//enum CoderType { CoderArith, CoderRange };

	static FILE* dump;
	static bool printstat;

	// if data=NULL or dlen=0, the model will be null - compression will simply copy the data;
	// 'method' - which version of compression will be used in Compress/Decompress
	PPM(const Symb* data, int dlen, ModelType mt, PPMParam param = PPMParam(), uchar method = 2);
	~PPM();

	// 'dlen' - max size of 'dest'; upon exit: actual size of 'dest'.
	// 'dlen' should be at least slen+1 - in this case buffer overflow will never occur
	// Otherwise the return value should be checked against CPRS_ERR_BUF.
	// The last symbol of 'src' must be '\0'.
	CprsErr Compress(char* dest, int& dlen, Symb* src, int slen);

	// 'dlen' - actual size of decompressed data ('slen' from Compress())
	// 'slen' - size of compressed data ('dlen' returned from Compress())
	// (so both sizes - of compressed and uncompressed data - must be stored outside of these routines)
	CprsErr Decompress(Symb* dest, int dlen, char* src, int slen);

	void PrintInfo(std::ostream& str);
};


#endif

