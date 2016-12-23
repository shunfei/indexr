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

#include "IPv4_Decomposer2.h"

inline static uchar NoDigits(uchar x) 
{
	if(x > 99)
		return 3;
	else if(x > 9)
		return 2;
	else
		return 1;
}

bool ParseIp(char* src, uint len, uint& res) 
{
	res = 0;
	uint p = 0;
	ushort tmp_val;
	uchar val;
	for (uchar part=0; part<4; part++) {
		// parse the number
		if (p==len || !isdigit(src[p]))
			return false;
		if (src[p]=='0') {
			p++;
			val = 0;
		}
		else {
			tmp_val = src[p++]-'0';
			if (p<len && isdigit(src[p])) {
				tmp_val = tmp_val*10+(src[p++]-'0');
				if (p<len && isdigit(src[p]))
					tmp_val = tmp_val*10+(src[p++]-'0');
			}
			if (tmp_val>255)
				return false;
			val = (uchar)tmp_val;
		}
		// test whether the dot is next
		if ((part<3 && (p==len || src[p]!='.'))
			|| (part==3 && p!=len))
			return false;
		if (part<3)
			p++;
		// add the number
		res = (res << 8) | val;
	}
	if (p!=len)
		return false;
	return true;

/*	//DebugBreak();
	unsigned char i = 0;

	char* it = ip;	

	bool go_i = true;
	std::string act = "";
	try {
		while (*it != '\0') {		
			go_i = true;
			while ( *it != '\0' && *it != '.') {			
				if(go_i)
					if(*it == '0')
						lzer[i]++;
					else 
						go_i = false;

				act += *it;			
				it++;
			}		
			licz[i] = atoi(act.c_str());
			if(licz[i] == 0)
				lzer[i]--;
			act = "";
			i++;	
			if(*it != '\0')
				it++;
		}
	} catch (...) {
		return -1;
	}
	return 1;*/
}


std::auto_ptr<DomainInjectionDecomposer> IPv4_Decomposer2::Clone() const
{
	return std::auto_ptr<DomainInjectionDecomposer> (new IPv4_Decomposer2(*this));
}

void IPv4_Decomposer2::Decompose(StringDataBlock& in, std::vector<boost::shared_ptr<DataBlock> >& out, CompressionStatistics& stats)
{	
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(stats.new_no_outliers == stats.previous_no_outliers);
	int nobj = in.GetNObj();
	NumericDataBlock<uint>* ips = new NumericDataBlock<uint>(nobj);
	NumericDataBlock<uchar>* outl_mask = new NumericDataBlock<uchar>(nobj);
	StringDataBlock* outliers =  new StringDataBlock(nobj);
	uint ip;
	bool is_outlier = false;	

	for(int o = 0; o < nobj; o++) {
		if (!ParseIp(in.GetIndex(o), in.GetLens(o), ip)) {
			is_outlier = true;
			outliers->Add(in.GetIndex(o), in.GetLens(o));
			outl_mask->Add(1);
			if(uint(o) >= stats.previous_no_obj)
				stats.new_no_outliers++;
		} else {
			ips->Add(ip);
			outl_mask->Add(0);
		} 			
	} 

	out.push_back(boost::shared_ptr<DataBlock>(ips));
	if(is_outlier) {
		out.push_back(boost::shared_ptr<DataBlock>(outl_mask));
		out.push_back(boost::shared_ptr<DataBlock>(outliers));
	}
	else {
		delete outl_mask;
		delete outliers;
	}
}
//uint IPv4_Decomposer::GetComposedSize(std::vector<boost::shared_ptr<DataBlock> >& in)
//{
//
//	uint size = 0;
//	for (int b=0; b<in.size(); b++)
//		size += ((StringDataBlock*)in[b].get())->GetDataSize();
//	return size;
//}




void IPv4_Decomposer2::Compose(std::vector<boost::shared_ptr<DataBlock> >& in, StringDataBlock& out, char* data_, uint data_size, uint& outliers_count)
{	
	int nobj;
	NumericDataBlock<uint>* ips = (NumericDataBlock<uint>*) in[0].get();
	NumericDataBlock<uchar>* outl_mask = NULL;
	StringDataBlock* outliers = NULL;
	if (in.size()==1)
		nobj = ips->GetNObj();
	else {
		outl_mask = (NumericDataBlock<uchar>*) in[1].get();
		outliers = (StringDataBlock*) in[2].get();
		nobj = outl_mask->GetNObj();
	}

	uint outl_cnt = 0;
	uint ip_cnt = 0;
	uint shift = 0;
	ushort len;

	for(int o = 0; o < nobj; o++) {											
		len = 0;			
		if (outl_mask==NULL || outl_mask->GetValue(o) == 0) { // not outlier				

			uint ip = ips->GetValue(ip_cnt);
			for(int i = 0; i < 4; i++) {
				uchar val = (uchar)((ip>>((3-i)*8)) & 255);
				if(i != 0) {
					*(data_ + len + shift) = '.';													
					len++;
				}
				if (val>=100) {
					*(data_ + len + shift) = '0' + val / 100;
					len++;
				}
				if (val>=10) {
					*(data_ + len + shift) = '0' + (val % 100) / 10;
					len++;
				}
				*(data_ + len + shift) = '0' + val % 10;
				len++;
			}			
			ip_cnt++;

		} else { // outlier

			len =  outliers->GetLens(outl_cnt);		
			memcpy(data_ + shift, outliers->GetIndex(outl_cnt), len);
			outl_cnt++;

		}			
		out.Add(data_ + shift, len);
		shift += len;
	}
	outliers_count = outl_cnt;
}
