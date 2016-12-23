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

#ifndef CONCATENATOR_H_INCLUDED
#define CONCATENATOR_H_INCLUDED

#include "DomainInjection.h"
#pragma warning( disable : 4267 )
#include <boost/regex.hpp>
#pragma warning( default : 4267 )

class Concatenator : public DomainInjectionDecomposer
{
public:
	Concatenator(const std::string& s);
	virtual std::auto_ptr<DomainInjectionDecomposer> Clone() const;

	void Decompose(StringDataBlock& in, std::vector<boost::shared_ptr<DataBlock> >& out, CompressionStatistics& stats);
	//uint GetComposedSize(std::vector<boost::shared_ptr<DataBlock> >& in);
	void Compose(std::vector<boost::shared_ptr<DataBlock> >& in, StringDataBlock& out, char* data_, uint data_size, uint& outliers);

	//void AddParsedValueToBuf(char* & buf, std::vector<boost::shared_ptr<DataBlock> >& in, int o, bool with_outl, uint data_size);

	std::string GetRule() const { return mask; }

	static bool IsValid(const std::string& rule);

private:
	enum mask_block_type { MASK_BLOCK_S, MASK_BLOCK_N };
	std::string mask;	
	std::vector<mask_block_type> m_block_type;
	ushort no_bl_n, no_bl_s;
	boost::regex re;
};

#endif
