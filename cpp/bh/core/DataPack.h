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

#ifndef DATAPACK_H_
#define DATAPACK_H_

#include <boost/shared_ptr.hpp>

#include "common/CommonDefinitions.h"


class RCDataType;
class DataPackLock;

class DataPack
{
public:
	DataPack() : decomposer_id(0), outliers(0) {};
	virtual ~DataPack() {};
public:
	virtual RCDataType& operator[](ushort id) const = 0;
	virtual size_t size() = 0;

	void SetDecomposerID(uint decomposer_id) { this->decomposer_id = decomposer_id; }
	uint GetDecomposerID() const { return decomposer_id; }

public:
	boost::shared_ptr<DataPackLock> dp_lock;
	uint decomposer_id;
	uint outliers;
};

#endif /*DATAPACK_H_*/
