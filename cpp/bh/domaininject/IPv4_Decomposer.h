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
#ifndef IPV4_H_INCLUDED
#define IPV4_H_INCLUDED

#include "DomainInjection.h"

class IPv4_Decomposer : public DomainInjectionDecomposer
{
public:
	virtual std::auto_ptr<DomainInjectionDecomposer> Clone() const;

	void Decompose(StringDataBlock& in, std::vector<boost::shared_ptr<DataBlock> >& out, CompressionStatistics& stats);
	//uint GetComposedSize(std::vector<boost::shared_ptr<DataBlock> >& in);
	void Compose(std::vector<boost::shared_ptr<DataBlock> >& in, StringDataBlock& out, char* data_, uint data_size, uint& outliers);

	std::string GetRule() const { return "IPv4_2"; }

	static std::string GetName() { return "IPv4_2"; }
};

#endif
