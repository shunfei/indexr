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

#include "IncWGraph.h"
#include "TextCompressor.h"
#include "common/bhassert.h"
#include "system/fet.h"

// Compressed data format:		<head_ver> <body>
//
// <head_ver>:		<encoded>[1B] [<size_encod>[4B]] <version>[1B] [<level>[1B]]
//		<encoded> = 1 - data is encoded (CompressVer2 routine, only versions <= 2), 0 - not encoded;
//				<encoded> is stored only in CompressVer2 and Compress;
//				CompressPlain and CompressCopy start from <version>
//		<level> exists only if version > 0
//
// <body> in versions 1 and 2:		<pos_part1>[4B]<pos_part2>[4B]...<pos_part_n>[4B] <part1><part2>...<part_n>
//		<pos_part_i> - position of i'th part, from the beginning of the buffer
//


TextCompressor::TextCompressor() : BLD_START(0), BLD_RATIO(0.0)
{
	graph = new IncWGraph();
}
TextCompressor::~TextCompressor()
{
	delete graph;
}
void TextCompressor::ReleaseMem()
{
	delete graph;
	graph = new IncWGraph();
}


void TextCompressor::SetSplit(int len)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(len > 0);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(BLD_RATIO > 1.01);
	double ratio = len / (double)BLD_START;
	if(ratio < 1.0) ratio = 1.0;

	int n = (int) (log(ratio) / log(BLD_RATIO)) + 1;		// no. of models (rounded downwards)
	if(n < 1) n = 1;

	double bld_ratio = 0.0;
	if(n > 1) bld_ratio = pow(ratio, 1.0/(n-1));		// quotient of 2 consecutive splits

	split.resize(n + 1);
	split[0] = 0;
	double next = BLD_START;
	for(int i = 1; i < n; i++) {
		split[i] = (int) next;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(split[i] > split[i-1]);
		next *= bld_ratio;
	}
	split[n] = len;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(split[n] > split[n-1]);
}

void TextCompressor::SetParams(PPMParam& p, int ver, int lev, int len)
{
	p.SetDefault();
	BLD_START = 64;

	switch(ver) {
		case 1:		p.esc_count = 25;
					BLD_RATIO = 2.5;
					break;
		case 2:		p.esc_count = 70;
					BLD_RATIO = 2.0;
					break;
		default:	BHERROR("not implemented");
	}

	SetSplit(len);
}

CprsErr TextCompressor::CompressCopy(char* dest, int& dlen, char* src, int slen)
{
	if(dlen <= slen) return CPRS_ERR_BUF;
	dest[0] = 0;		// method: no compression
	memcpy(dest + 1, src, slen);
	dlen = slen + 1;
	return CPRS_SUCCESS;
}

CprsErr TextCompressor::CompressCopy(char* dest, int& dlen, char** index, const ushort* lens, int nrec)
{
	dest[0] = 0;		// method: no compression
	int dpos = 1;
	for(int i = 0; i < nrec; i++) {
		if(dpos + lens[i] > dlen) return CPRS_ERR_BUF;
		memcpy(dest + dpos, index[i], lens[i]);
		dpos += lens[i];
	}
	dlen = dpos;
	return CPRS_SUCCESS;
}

CprsErr TextCompressor::DecompressCopy(char* dest, int dlen, char* src, int slen, char** index, const ushort* lens, int nrec)
{
	// version number is already read
	int sumlen = 0;
	for(int i = 0; i < nrec; i++) {
		index[i] = dest + sumlen;
		sumlen += lens[i];
	}
	if(sumlen > dlen) return CPRS_ERR_BUF;
	memcpy(dest, src, sumlen);
	return CPRS_SUCCESS;
}

//---------------------------------------------------------------------------------------------------------

CprsErr TextCompressor::CompressPlain(char* dest, int& dlen, char* src, int slen, int ver, int lev)
{
	if((dest == NULL) || (src == NULL) || (dlen <= 0) || (slen <= 0))
		return CPRS_ERR_PAR;
	if((ver < 0) || (ver > 2) || (lev < 1) || (lev > 9))
		return CPRS_ERR_VER;

	if(ver == 0) return CompressCopy(dest, dlen, src, slen);

	// save version and level of compression
	dest[0] = (char)ver;
	dest[1] = (char)lev;
	int dpos = 2;
	CprsErr err = CPRS_SUCCESS;

	// PPM
	if((ver == 1) || (ver == 2)) {
		int clen;
		PPMParam param;
		SetParams(param, ver, lev, slen);

		// leave place in 'dest' for the array of 'dpos' of data parts
		int n = (int)split.size() - 1;
		int* dpos_tab = (int*) (dest + dpos);
		dpos += n * sizeof(int);

		PPM* ppm = NULL;
		PPM::ModelType mt = ((ver == 1) ? PPM::ModelSufTree : PPM::ModelWordGraph);

		// loop: build next PPM model, compress next part of the data
		for(int i = 0; i < n; i++) {
			ppm = new PPM((uchar*)src, split[i], mt, param);

			clen = dlen - dpos;
			err = ppm->Compress(dest + dpos, clen, (uchar*)src + split[i], split[i+1] - split[i]);
			delete ppm;
			if(err) break;

			dpos_tab[i] = dpos;
			dpos += clen;
		}
	}

	// is it better to simply copy the source data?
	if(((err == CPRS_ERR_BUF) || (dpos >= slen)) && (dlen >= slen + 1))
		return CompressCopy(dest, dlen, src, slen);

	dlen = dpos;
	return err;
}

CprsErr TextCompressor::DecompressPlain(char* dest, int dlen, char* src, int slen)
{
	if((dest == NULL) || (src == NULL) || (dlen <= 0) || (slen <= 0)) return CPRS_ERR_PAR;
	if(slen < 2) return CPRS_ERR_BUF;

	char ver = src[0],
	     lev = (ver > 0) ? src[1] : 1;
	int spos = (ver > 0) ? 2 : 1;

	if((ver < 0) || (ver > 2) || (lev < 1) || (lev > 9))
		return CPRS_ERR_VER;

	// are the data simply copied, without compression?
	if(ver == 0) {
		if(dlen != slen - 1) return CPRS_ERR_PAR;
		memcpy(dest, src + 1, dlen);
		return CPRS_SUCCESS;
	}

	PPMParam param;
	SetParams(param, ver, lev, dlen);
	int n = (int)split.size() - 1;

	// read array of parts' positions in 'src'
	std::vector<int> parts;
	parts.resize(n + 1);
	for(int j = 0; j < n; j++, spos += sizeof(int))
		parts[j] = *(int*)(src + spos);
	parts[n] = slen;
	if(parts[n] < parts[n-1]) return CPRS_ERR_BUF;

	PPM* ppm;
	PPM::ModelType mt = ((ver == 1) ? PPM::ModelSufTree : PPM::ModelWordGraph);
	CprsErr err;

	// loop: build next PPM model, decompress next part of the data
	for(int i = 0; i < n; i++) {
		ppm = new PPM((uchar*)dest, split[i], mt, param, (uchar)src[parts[i]]);

		err = ppm->Decompress((uchar*)dest + split[i], split[i+1] - split[i],
			                           src + parts[i], parts[i+1] - parts[i]);
		delete ppm;
		if(err) return err;
	}

	return CPRS_SUCCESS;
}

CprsErr TextCompressor::CompressVer2(char* dest, int& dlen, char** index, const ushort* lens, int nrec,
									 int ver, int lev)
{
	// encoding:
	//  '0' -> '1'     (zero may occur frequently and should be encoded as a single symbol)
	//  '1' -> '2' '1'
	//  '2' -> '2' '2'

	if((!dest) || (!index) || (!lens) || (dlen <= 0) || (nrec <= 0)) return CPRS_ERR_PAR;
	if(dlen < 5) return CPRS_ERR_BUF;

	// how much memory to allocate for encoded data
	int mem = 0, /*stop,*/ i, j;
	char s;
	/*for(i = 0; i < nrec; i++) {
		stop = index[i] + lens[i];
		for(j = index[i]; j < stop; j++) {
			s = src[j];
			if((s == '\1') || (s == '\2'))
				mem += 2;
			else mem++;
		}
		mem++;		// separating '\0'
	}*/
	for(i = 0; i < nrec; i++) {
		for(j = 0; j < lens[i]; j++) {
			s = index[i][j];
			if((s == '\1') || (s == '\2'))
				mem += 2;
			else mem++;
		}
		mem++;		// separating '\0'
	}

	char* data = new char[mem];
	int sdata = 0;

	// encode, with permutation
	i = PermFirst(nrec);
	/*for(int p = 0; p < nrec; p++) {
		data[sdata++] = '\0';
		stop = index[i] + lens[i];
		for(j = index[i]; j < stop; j++) {
			s = src[j];
			if((uint)s > 2)
				data[sdata++] = s;
			else switch(s) {
				case '\0':   data[sdata++] = '\1'; break;
				case '\1':   data[sdata++] = '\2'; data[sdata++] = '\1'; break;
				case '\2':   data[sdata++] = '\2'; data[sdata++] = '\2';
			}
		}
		PermNext(i, nrec);
	}*/
	for(int p = 0; p < nrec; p++) {
		data[sdata++] = '\0';
		for(j = 0; j < lens[i]; j++) {
			s = index[i][j];
			if((uint)s > 2)
				data[sdata++] = s;
			else switch(s) {
				case '\0':   data[sdata++] = '\1'; break;
				case '\1':   data[sdata++] = '\2'; data[sdata++] = '\1'; break;
				case '\2':   data[sdata++] = '\2'; data[sdata++] = '\2';
			}
		}
		PermNext(i, nrec);
	}
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(sdata == mem);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i == PermFirst(nrec));

	// header
	dest[0] = 1;					// data is encoded
	*(int*)(dest + 1) = sdata;		// store size of encoded data
	int dpos = 5;

	// compress
	dlen -= dpos;
	CprsErr err = CompressPlain(dest + dpos, dlen, data, sdata, ver, lev);
	dlen += dpos;

	delete[] data;
	return err;
}

CprsErr TextCompressor::DecompressVer2(char* dest, int dlen, char* src, int slen, char** index, const ushort* /*lens*/,
									   int nrec)
{
	if((!dest) || (!src) /*|| (!index) */|| (slen <= 0) || (nrec <= 0)) return CPRS_ERR_PAR;
	if(slen < 5) return CPRS_ERR_BUF;

	// is data encoded?
	if(src[0] != 1) return CPRS_ERR_VER;
	int spos = 1;

	// get size of encoded data
	if(slen < spos + 4) return CPRS_ERR_BUF;
	int sdata = *(int*)(src + spos);
	spos += 4;

	char* data = new char[sdata];

	// decompress
	CprsErr err = DecompressPlain(data, sdata, src + spos, slen - spos);
	if(err) { delete[] data; return err; }

	// decode
	int i = PermFirst(nrec);
	int pdat = 0, pdst = 0;
	char s;

	while(pdat < sdata) {
		s = data[pdat++];
		if(s == '\0') {
			index[i] = dest + pdst;
			PermNext(i, nrec);
			continue;
		}

		if(pdst >= dlen) { err = CPRS_ERR_BUF; break; }
		if((uint)s > 2) dest[pdst++] = s;
		else if(s == '\1') dest[pdst++] = '\0';
		else {
			if(pdat >= sdata) { err = CPRS_ERR_OTH; break; }
			s = data[pdat++];
			if((s == '\1') || (s == '\2')) dest[pdst++] = s;
			else { err = CPRS_ERR_OTH; break; }
		}
	}
	delete[] data;

	if(err) return err;
	if(i != PermFirst(nrec)) return CPRS_ERR_OTH;
	return CPRS_SUCCESS;
}

CprsErr TextCompressor::Compress(char* dest, int& dlen, char** index, const ushort* lens, int nrec, uint& packlen,
								 int ver, int lev)
{
	if((!dest) || (!index) || (!lens) || (dlen <= 0) || (nrec <= 0))	return CPRS_ERR_PAR;
	if((ver < 0) || (ver > MAXVER) || (lev < 1) || (lev > 9))			return CPRS_ERR_VER;

	// cumulative length of records
	int slen = 0;
	for(int i = 0; i < nrec; i++)
		slen += lens[i];
	packlen = (uint)slen;

	if((ver == 1) || (ver == 2)) return CompressVer2(dest, dlen, index, lens, nrec, ver, lev);

	dest[0] = 0;				// not encoded
	int dpos = 1;
	CprsErr err = CPRS_SUCCESS;

	if(ver == 0) {
		dlen -= dpos;
		err = CompressCopy(dest+dpos, dlen, index, lens, nrec);
		dlen += dpos;
		return err;
	}

	// Version 3
	dest[dpos++] = (char)ver;		// version and level of compression
	dest[dpos++] = (char)lev;

	// compress with IncWGraph
	try {
		RangeCoder coder;
		coder.InitCompress(dest, dlen, dpos);
		graph->Encode(&coder, index, lens, nrec, packlen);
		coder.EndCompress();
		dpos = coder.GetPos();
	} catch(CprsErr& e) { 
		err = e; 
	}

	// check if copy compression is better
	if(((err == CPRS_ERR_BUF) || (dpos >= slen)) && (dlen >= slen + 2)) {
		dpos = 1;		// leave first byte of the header
		dlen -= dpos;
		err = CompressCopy(dest+dpos, dlen, index, lens, nrec);
		dlen += dpos;
		packlen = (uint)slen;
	}
	else dlen = dpos;

	return err;
}

CprsErr TextCompressor::Compress(char* dest, int& dlen, char** index, const ushort* lens, int nrec,
										int ver, int lev)
{
	MEASURE_FET("TextCompressor::Compress(...)");
	uint packlen;
	return Compress(dest, dlen, index, lens, nrec, packlen, ver, lev);
}


CprsErr TextCompressor::Decompress(char* dest, int dlen, char* src, int slen, char** index, const ushort* lens,
								   int nrec)
{
	MEASURE_FET("TextCompressor::Decompress(...)");
	if((!dest) || (!src) || (!index) || (slen <= 0) || (nrec <= 0)) return CPRS_ERR_PAR;
	if(slen < 2) return CPRS_ERR_BUF;

	// old versions
	if(src[0]) return DecompressVer2(dest, dlen, src, slen, index, lens, nrec);
	int spos = 1;

	// copy compress
	char ver = src[spos++];
	if(ver == 0) return DecompressCopy(dest, dlen, src+spos, slen-spos, index, lens, nrec);

	// Version 3
	if(slen <= spos) return CPRS_ERR_BUF;
	char lev = src[spos++];
	if((ver != 3) || (lev < 1) || (lev > 9)) return CPRS_ERR_VER;

	// check if 'dlen' is large enough
	//int sumlen = 0;
	//for(int i = 0; i < nrec; i++)
	//	sumlen += lens[i];
	//if(sumlen > dlen) return CPRS_ERR_BUF;

	// decompress with IncWGraph
	try {
		RangeCoder coder;
		coder.InitDecompress(src, slen, spos);
		graph->Decode(&coder, index, lens, nrec, dest, dlen);
	} catch(CprsErr& e) { 
		return e; 
	}
	return CPRS_SUCCESS;
}
