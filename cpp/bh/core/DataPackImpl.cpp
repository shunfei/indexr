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

#include "DataPackImpl.h"
#include "DPN.h"
#include "types/RCDataTypes.h"
#include "RCAttrPack.h"

using namespace std;

//template<typename T>
//DataPackImpl<T>::DataPackImpl(const DPN& dpn)
//	:	vec(dpn.GetNoObj())
//{
//}

template<typename T>
DataPackImpl<T>::DataPackImpl(const int size)
	:	vec(size)
{ // size null elements
}

template<typename T>
DataPackImpl<T>::DataPackImpl(const int size, const T val)
	:	vec(size)
{
	vec.assign(size, val);
}
template<typename T>
DataPackImpl<T>::~DataPackImpl()
{
}

template<typename T>
RCDataType& DataPackImpl<T>::operator[](ushort id) const
{
	return (T&)vec[id];
}

template class DataPackImpl<RCBString>;
template class DataPackImpl<RCNum>;
template class DataPackImpl<RCDateTime>;
