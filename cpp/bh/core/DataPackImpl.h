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

#ifndef DATAPACKIMPL_H_
#define DATAPACKIMPL_H_

#include <vector>

#include "common/CommonDefinitions.h"
#include "DataPack.h"

class RCDataType;
class FTree;
struct DPN;
class RCAttr_packN;

template<typename T>
class DataPackImpl : public DataPack
{
	std::vector<T> vec;

public:
	//DataPackImpl(const DPN& dpn);
	DataPackImpl(const int size);
	DataPackImpl(const int size, const T val);
	//DataPackImpl(const RCAttr_packN& attr_pack, const DPN& dpn);
	//DataPackImpl(const RCAttr_packS& attr_pack, const DPN& attr_pack);
	//DataPackImpl(const RCAttr_packN& attr_pack, const DPN& attr_pack, const FTree& dict);
	~DataPackImpl();
	size_t size() {return vec.size();}

public:
	virtual RCDataType& operator[](ushort id) const;
};

#endif /*DATAPACKIMPL_H_*/
