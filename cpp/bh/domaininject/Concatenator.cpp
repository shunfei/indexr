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

#include "Concatenator.h"

//#include "stdafx.h"
//#include <iostream>
//#include <vector>
//#include <string>


using namespace std;

bool Concatenator::IsValid(const std::string& rule)
{
	int off = 0;
	int old_pos = -1;
	int pos;
	while ((pos = int( rule.find("%s", off)) ) != string::npos) {
		if (old_pos != -1 && old_pos + 2 == pos)
			return false;
		if (pos == 0 || rule.at(pos - 1) != '%')
			old_pos = pos;
		off = pos + 2;
	}
	off = 0;
	old_pos = -1;
	while ((pos = int( rule.find("%d", off)) ) != string::npos) {
		if (old_pos != -1 && old_pos + 2 == pos)
			return false;
		if (pos == 0 || rule.at(pos - 1) != '%')
			old_pos = pos;
		off = pos + 2;
	}
	return true;
}

Concatenator::Concatenator(const std::string& s) 
{
	mask = s;
	string res = "";
	string spec = ".\\*?^$[{+()|";
	no_bl_n = 0;
	no_bl_s = 0;
	for(int i = 0; i < s.length(); i++) {
		if (s[i] == '%') {
			if (i < (s.length() - 1)) {
				if(s[i+1] == 'd') {
					res += "(\\d{1,18})";
					m_block_type.push_back(MASK_BLOCK_N);	
					no_bl_n++;
					i++;
				} else if (s[i+1] == 's') {
					res += "(.*?)";
					m_block_type.push_back(MASK_BLOCK_S);		
					no_bl_s++;
					i++;
				} else if (s[i+1] == '%') {
					res += "\\%";
					i++;
				} else {					
					res += "\\%";
				}
			} else {
				res += "\\%";
			}
		} else {
			if (spec.find(s[i]) != string::npos) {
				res += "\\"; 
				res += s[i];			
			}
			else
				res += s[i];			
		}
	}
	re = boost::regex(res);	
};

//Concatenator::Concatenator(const std::string& m) 
//{
//	mask = m;
//	int i = 0;
//	no_bl_n = 0;
//	no_bl_s = 0;
//	while (i < mask.length()) {
//		if (mask[i] == '%' && i < mask.length() - 1) {
//			if( mask[i + 1] == 'd') {
//				m_block_type.push_back(MASK_BLOCK_N);	
//				no_bl_n++;
//				i++;
//			} else if (mask[i + 1] == 's') {
//				m_block_type.push_back(MASK_BLOCK_S);		
//				no_bl_s++;
//				i++;
//			}
//		}
//		i++;
//	}		
//};

std::auto_ptr<DomainInjectionDecomposer> Concatenator::Clone() const
{
	return std::auto_ptr<DomainInjectionDecomposer> (new Concatenator(*this));
}

inline uint64 parseNumber(char* src, uint len)
{
	uint64 val = 0;
	for(uint p = 0; p < len; p++) {
		val = val * 10 + (src[p] - '0');
	}
	return val;
}

void Concatenator::Decompose(StringDataBlock& in, std::vector<boost::shared_ptr<DataBlock> >& out, CompressionStatistics& stats)
{	
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(stats.new_no_outliers == stats.previous_no_outliers);
	int nobj = in.GetNObj();
	boost::shared_ptr<StringDataBlock> outliers =  boost::shared_ptr<StringDataBlock>( new StringDataBlock(nobj));
	boost::shared_ptr<NumericDataBlock<uchar> > outl_mask = boost::shared_ptr<NumericDataBlock<uchar> >( new NumericDataBlock<uchar>(nobj));
	vector<boost::shared_ptr<NumericDataBlock<_uint64> > > block_n(no_bl_n);
	vector<boost::shared_ptr<StringDataBlock> > block_s(no_bl_s);
	for(int bn = 0; bn < no_bl_n; bn++) {
		block_n[bn] =  boost::shared_ptr<NumericDataBlock<_uint64> >( new NumericDataBlock<_uint64>(nobj));
	}
	for(int bs = 0; bs < no_bl_s; bs++) {
		block_s[bs] =  boost::shared_ptr<StringDataBlock>( new StringDataBlock(nobj));
	}

	boost::match_results<char*> subgr; 

	int outl_cnt = 0;	
	ushort bl_n_cnt, bl_s_cnt;	

	for(int o = 0; o < nobj; o++) {		
		bl_n_cnt = 0;
		bl_s_cnt = 0;
		bool outlier = true;
		if (boost::regex_match<char*>(in.GetIndex(o), in.GetIndex(o)+in.GetLens(o), subgr, re)) {
			outlier = false;
			for(int bi = 0; !outlier && bi < no_bl_n + no_bl_s; bi++)
				if (m_block_type.at(bi) == MASK_BLOCK_N && *(subgr[bi + 1].first)=='0' && subgr[bi + 1].length()>1)
					outlier = true;
		}
		if (outlier) { 
			outliers->Add(in.GetIndex(o), in.GetLens(o));
			outl_cnt++;
			outl_mask->Add(1);
			if(uint(o) >= stats.previous_no_obj)
				stats.new_no_outliers++;
		} else {
			for(int bi = 0; bi < no_bl_n + no_bl_s; bi++) {
				if (m_block_type.at(bi) == MASK_BLOCK_N) {
					block_n.at(bl_n_cnt++)->Add(parseNumber(subgr[bi + 1].first, uint(subgr[bi + 1].length())));
				} else {
					block_s.at(bl_s_cnt++)->Add(subgr[bi + 1].first, ushort(subgr[bi + 1].length()));
				}				
			}
			outl_mask->Add(0);
		}
	}		
	if(outl_cnt > 0) {
		out.push_back(outl_mask);
		out.push_back(outliers);		
	}
	bl_n_cnt = 0;
	bl_s_cnt = 0;
	for(int bi = 0; bi < no_bl_n + no_bl_s; bi++) {
		if (m_block_type.at(bi) == MASK_BLOCK_N) {
			out.push_back(block_n.at(bl_n_cnt++));
		} else {
			out.push_back(block_s.at(bl_s_cnt++));
		}
	}
};

/*uchar NoDigits(uint x) 
{
	if(x > 9999999999)
		return 11;
	else if(x > 999999999)
		return 10;
	else if(x > 99999999)
		return 9;
	else if(x > 9999999)
		return 8;
	else if(x > 999999)
		return 7;
	else if(x > 99999)
		return 6;
	else if(x > 9999)
		return 5;
	else if(x > 999)
		return 4;
	else if(x > 99)
		return 3;
	else if(x > 9)
		return 2;
	else
		return 1;
}*/

char* itoa(int value, char* result, int base) {
	// check that the base if valid
	if (base < 2 || base > 36) { *result = '\0'; return result; }

	char* ptr = result, *ptr1 = result, tmp_char;
	int tmp_value;

	do {
		tmp_value = value;
		value /= base;
		*ptr++ = "zyxwvutsrqponmlkjihgfedcba9876543210123456789abcdefghijklmnopqrstuvwxyz" [35 + (tmp_value - value * base)];
	} while ( value );

	// Apply negative sign
	if (tmp_value < 0) *ptr++ = '-';
	*ptr-- = '\0';
	while(ptr1 < ptr) {
		tmp_char = *ptr;
		*ptr--= *ptr1;
		*ptr1++ = tmp_char;
	}
	return result;
}


/*void Concatenator::AddParsedValueToBuf(char* & buf, std::vector<boost::shared_ptr<DataBlock> >& in, int o, bool with_outl, uint data_size) {
	ushort no_bl = no_bl_s + no_bl_n;
	int mask_cnt = 0;
	int block_cnt = 0;
	int shift_block_cnt = (with_outl ? 2 : 0 );	
	int shift = 0;
	while (mask_cnt < mask.length()) {
		if (mask[mask_cnt] != '%') {
			buf[shift] = mask[mask_cnt];
			shift++;
			mask_cnt++;			
		} else {			
			if (m_block_type.at(block_cnt) == MASK_BLOCK_N) {
				uint licz  =((NumericDataBlock<uint>*) (in[block_cnt + shift_block_cnt].get()))->GetValue(o);
				char tmp_s[20];
				itoa(licz, tmp_s, 10);
				uchar dig = strlen(tmp_s);
				memcpy(buf + shift, tmp_s, dig);
				shift += dig;				
			} else { // MASK_BLOCK_S
				memcpy(buf + shift, ((StringDataBlock*) (in[block_cnt + shift_block_cnt].get()))->GetIndex(o), ((StringDataBlock*) (in[block_cnt + shift_block_cnt].get()))->GetLens(o));
				shift += ((StringDataBlock*) (in[block_cnt + shift_block_cnt].get()))->GetLens(o);
			}				
			block_cnt++;
			mask_cnt += 2;
		}
	}		
	buf += shift;
}*/

void Concatenator::Compose(std::vector<boost::shared_ptr<DataBlock> >& in, StringDataBlock& out, char* data_, uint data_size, uint& outliers)
{
	ushort no_bl = no_bl_s + no_bl_n;
	int nobj = ((NumericDataBlock<uchar>*) (in[0].get()))->GetNObj();
	const size_t tmp_s_size = 20;
	char tmp_s[tmp_s_size];
	outliers = 0;
	if(in.size() == (no_bl + 2)) { // outliers existing

		int outl_cnt = 0;
		int parsed_cnt = 0;		
		int shift = 0;
		ushort len = 0;

		for(int o = 0; o < nobj; o++) {	
			if( ((NumericDataBlock<uchar>*) (in[0].get()))->GetValue(o) == 0 ) { // not outlier	
				len = 0;
				int mask_cnt = 0;
				int block_cnt = 0;								
				while (mask_cnt < mask.length()) {
					if (mask[mask_cnt] != '%' || mask_cnt+1==mask.length() || (mask[mask_cnt+1]!='s' && mask[mask_cnt+1]!='d')) {
						data_[shift + len] = mask[mask_cnt];						
						len++;
						mask_cnt++;			
						if (mask[mask_cnt-1] == '%' && mask_cnt<mask.length() && mask[mask_cnt]=='%')
							mask_cnt++;
					} else {			
						if (m_block_type.at(block_cnt) == MASK_BLOCK_N) {
							uint64 licz  = ((NumericDataBlock<_uint64>*) (in[block_cnt + 2].get()))->GetValue(parsed_cnt);
							int dig = snprintf(tmp_s, tmp_s_size, "%llu", licz);
							memcpy(data_ + shift + len, tmp_s, dig);							
							len += dig;
						} else { // MASK_BLOCK_S
							int x = ((StringDataBlock*) (in[block_cnt + 2].get()))->GetLens(parsed_cnt);	
							memcpy(data_ + shift + len, ((StringDataBlock*) (in[block_cnt + 2].get()))->GetIndex(parsed_cnt), x);																				
							len += x;
						}				
						block_cnt++;
						mask_cnt += 2;
					}
				}						
				//AddParsedValueToBuf(data_ + shift, in, parsed_cnt, true, data_size);				
				out.Add(data_ + shift, len);
				shift += len;	
				parsed_cnt++;

			} else { //outlier

				len =  ((StringDataBlock*) (in[1].get()))->GetLens(outl_cnt);		
				out.Add(data_ + shift, len);
				memcpy(data_ + shift, ((StringDataBlock*) (in[1].get()))->GetIndex(outl_cnt), len);
				shift += len;				
				outl_cnt++;
			}
		}
		outliers = outl_cnt;
	} else if (in.size() == no_bl) { // no outliers
		ushort len = 0;
		int shift = 0;
		for(int o = 0; o < nobj; o++) {	
			len = 0;
			int mask_cnt = 0;
			int block_cnt = 0;								
			while (mask_cnt < mask.length()) {
				if (mask[mask_cnt] != '%' || mask_cnt+1==mask.length() || (mask[mask_cnt+1]!='s' && mask[mask_cnt+1]!='d')) {
					data_[shift + len] = mask[mask_cnt];
					len++;
					mask_cnt++;
					if (mask[mask_cnt-1] == '%' && mask_cnt<mask.length() && mask[mask_cnt]=='%')
						mask_cnt++;
				} else {			
					if (m_block_type.at(block_cnt) == MASK_BLOCK_N) {
						uint64 licz  = ((NumericDataBlock<_uint64>*) (in[block_cnt].get()))->GetValue(o);
						int dig = snprintf(tmp_s, tmp_s_size, "%llu", licz);
						memcpy(data_ + shift + len, tmp_s, dig);
						len += dig;
					} else { // MASK_BLOCK_S
						int x = ((StringDataBlock*) (in[block_cnt].get()))->GetLens(o);	
						memcpy(data_ + shift + len, ((StringDataBlock*) (in[block_cnt].get()))->GetIndex(o), x);																			
						len += x;
					}				
					block_cnt++;
					mask_cnt += 2;
				}
			}						
			//AddParsedValueToBuf(data_ + shift, in, parsed_cnt, true, data_size);				
			out.Add(data_ + shift, len);
			shift += len;	
		}
	}
	return;
};
