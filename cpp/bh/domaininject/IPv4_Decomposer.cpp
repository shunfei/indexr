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

#include "IPv4_Decomposer.h"

uchar NoDigits(uchar x) 
{
	if(x > 99)
		return 3;
	else if(x > 9)
		return 2;
	else
		return 1;
}

bool ParseIp(char* src, uint len, uchar licz[], uchar lzer[]) 
{
	uint p = 0;
	char tmp[4];
	for (uchar part=0; part<4; part++) {
		// count zeros
		lzer[part] = 0;
		while (p<len && src[p]=='0') {
			lzer[part]++;
			p++;
		}
		// copy number but max. 3 digits
		int tmp_len = 0;
		while (p<len && tmp_len<3 && isdigit(src[p]))
			tmp[tmp_len++] = src[p++];
		// test whether the dot is next
		if ((part<3 && (p==len || src[p]!='.'))
			|| (part==3 && p!=len))
			return false;
		if (part<3)
			p++;
		// parse the number
		if (tmp_len==0) {
			if (lzer[part]==0)
				return false;
			lzer[part]--;
			licz[part] = 0;
		}
		else {
			tmp[tmp_len] = 0;
			ulong val = strtoul(tmp, NULL, 0);
			if (val==0 || val>255)
				return false;
			licz[part] = (uchar)val;
		}
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
};

std::auto_ptr<DomainInjectionDecomposer> IPv4_Decomposer::Clone() const
{
	return std::auto_ptr<DomainInjectionDecomposer> (new IPv4_Decomposer(*this));
}

void IPv4_Decomposer::Decompose(StringDataBlock& in, std::vector<boost::shared_ptr<DataBlock> >& out, CompressionStatistics& stats)
{	
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(stats.new_no_outliers == stats.previous_no_outliers);
	int nobj = in.GetNObj();
	boost::shared_ptr<NumericDataBlock<uchar> > h[4];
	boost::shared_ptr<NumericDataBlock<uchar> > h_lzer[4];
	boost::shared_ptr<StringDataBlock> outliers =  boost::shared_ptr<StringDataBlock>( new StringDataBlock(nobj));
	boost::shared_ptr<NumericDataBlock<uchar> > outl_mask = boost::shared_ptr<NumericDataBlock<uchar> >( new NumericDataBlock<uchar>(nobj));
	for(int i = 0; i < 4; i++) {
		h[i] =  boost::shared_ptr<NumericDataBlock<uchar> >( new NumericDataBlock<uchar>(nobj));
		h_lzer[i] =  boost::shared_ptr<NumericDataBlock<uchar> >( new NumericDataBlock<uchar>(nobj));
	}

	uchar d[4];
	uchar lz[4];
	uint outl_cnt = 0;	
	for(int o = 0; o < nobj; o++) {
		if (!ParseIp(in.GetIndex(o), in.GetLens(o), d, lz)) {
			outliers->Add(in.GetIndex(o), in.GetLens(o));
			outl_cnt++;
			outl_mask->Add(1);
			if(uint(o) >= stats.previous_no_obj)
				stats.new_no_outliers++;
		} else {
			for(int i = 0; i < 4; i++) {
				h[i]->Add(d[i]);
				h_lzer[i]->Add(lz[i]);
			} 
			outl_mask->Add(0);
		} 			
	} 
	
	if(outl_cnt > 0) {
		out.push_back(outl_mask);
		out.push_back(outliers);
	}
	for(int i = 0; i < 4; i++) {
		out.push_back(h[i]);
		out.push_back(h_lzer[i]);
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




void IPv4_Decomposer::Compose(std::vector<boost::shared_ptr<DataBlock> >& in, StringDataBlock& out, char* data_, uint data_size, uint& outliers)
{	
	int bl_no = int(in.size());
	int nobj;
	if(bl_no < 8) {
		//cerr << lock << "Wrong number of data blocks read in. Composition of AttrPackS failed." << unlock;
		return;
	} else if (bl_no == 8) { // no outliers		
		outliers = 0;
		uint shift = 0;
		nobj = ((NumericDataBlock<uchar>*) (in[0].get()))->GetNObj();			
		
		uchar len;
		uchar licz, l_zer;
		
		for(int o = 0; o < nobj; o++) {					
			len = 0;			
			for(int i = 0; i < 4; i++) {				
				uchar z_cnt = 0;
				licz = ((NumericDataBlock<uchar>*) (in[2 * i].get()))->GetValue(o);	// h
				l_zer = ((NumericDataBlock<uchar>*) (in[2 * i + 1].get()))->GetValue(o);

				if(i != 0) {
					*(data_ + len + shift) = '.';													
					len++;
				}

				uchar digs = NoDigits(licz);

				// leading zeros of part number
				while (z_cnt < l_zer) {
					*(data_ + len + shift) = '0';													
					len++;
					z_cnt++;
				}
				// part number				
				switch(digs){
					case 3 : 	
						memset(data_ + shift + len, '0' + licz / 100, 1); 
						len++;
					case 2 :
						memset(data_ + shift + len, '0' + ( (licz % 100) / 10 ), 1);
						len++;
					case 1 : 
						memset(data_ + shift + len, '0' + licz % 10, 1); 
						len++;
					default : break;
				}
														
			}			
			out.Add(data_ + shift, len);			
			shift += len;
		}

	} else { // existing outliers

		uint outl_cnt = 0;
		uint ip_cnt = 0;
		uint shift = 0;
		nobj = ((NumericDataBlock<uchar>*) (in[0].get()))->GetNObj();	
		uchar licz, l_zer;
		ushort len;
		for(int o = 0; o < nobj; o++) {											
			len = 0;			
			if( ((NumericDataBlock<uchar>*) (in[0].get()))->GetValue(o) == 0 ) { // not outlier				
				for(int i = 0; i < 4; i++) {				
					uchar z_cnt = 0;
					licz = ((NumericDataBlock<uchar>*) (in[2 * i + 2].get()))->GetValue(ip_cnt);	
					l_zer = ((NumericDataBlock<uchar>*) (in[2 * i + 3].get()))->GetValue(ip_cnt);

					if(i != 0) {
						*(data_ + len + shift) = '.';													
						len++;
					}

					uchar digs = NoDigits(licz);

					// leading zeros of part number
					while (z_cnt < l_zer) {
						*(data_ + len + shift) = '0';													
						len++;
						z_cnt++;
					}
					// part number				
					switch(digs){
					case 3 : 	
						memset(data_ + shift + len, '0' + licz / 100, 1); 
						len++;
					case 2 :
						memset(data_ + shift + len, '0' + ( (licz % 100) / 10 ), 1);
						len++;
					case 1 : 
						memset(data_ + shift + len, '0' + licz % 10, 1); 
						len++;
					default : break;
					}

				}			
				out.Add(data_ + shift, len);			
				shift += len;		
				ip_cnt++;

			} else { // outlier

				len =  ((StringDataBlock*) (in[1].get()))->GetLens(outl_cnt);		
				out.Add(data_ + shift, len);
				memcpy(data_ + shift, ((StringDataBlock*) (in[1].get()))->GetIndex(outl_cnt), len);
				shift += len;
				outl_cnt++;

			}			
		}
		outliers = outl_cnt;
	}
}
