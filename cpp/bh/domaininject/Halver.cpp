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

#include "Halver.h"

std::auto_ptr<DomainInjectionDecomposer> Halver::Clone() const
{
	return std::auto_ptr<DomainInjectionDecomposer> (new Halver(*this));
}

void Halver::Decompose(StringDataBlock& in, std::vector<boost::shared_ptr<DataBlock> >& out, CompressionStatistics& stats)
{	
	int nobj = in.GetNObj();
	boost::shared_ptr<StringDataBlock> h1 =  boost::shared_ptr<StringDataBlock>( new StringDataBlock(nobj));
	boost::shared_ptr<StringDataBlock> h2 =  boost::shared_ptr<StringDataBlock>( new StringDataBlock(nobj));
	ushort x;
	for(int o = 0; o < nobj; o++) {
		x = (in.GetLens(o)+1)/2;
		h1->Add(in.GetIndex(o), x);
		h2->Add(in.GetIndex(o) + x, in.GetLens(o)/2);
	}
	out.push_back(h1);
	out.push_back(h2);
}

void Halver::Compose(std::vector<boost::shared_ptr<DataBlock> >& in, StringDataBlock& out, char* data_, uint data_size, uint& outliers)
{	
	int nobj = ((StringDataBlock*) (in[0].get()))->GetNObj();	
	ushort len0, len1;
	uint shift = 0;
	for(int o = 0; o < nobj; o++) {
		len0 =  ((StringDataBlock*) (in[0].get()))->GetLens(o);
		len1 =  ((StringDataBlock*) (in[1].get()))->GetLens(o);		
		out.Add(data_ + shift, len0 + len1);
		memcpy(data_ + shift, ((StringDataBlock*) (in[0].get()))->GetIndex(o), len0);
		shift += len0;
		memcpy(data_ + shift, ((StringDataBlock*) (in[1].get()))->GetIndex(o), len1);
		shift += len1;		
	}
	outliers = 0;
}
