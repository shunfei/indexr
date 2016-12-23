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

#ifndef DATALOADER_H_
#define DATALOADER_H_

#include <boost/utility.hpp>
#include <vector>

#include "core/bintools.h"
#include "common/CommonDefinitions.h"
#include "compress/tools.h"
#include "system/IOParameters.h"
#include "common/bhassert.h"
#include "ValueCache.h"

class RCAttrLoad;
class DataParser;
class Buffer;
class NewValuesSetCopy;

class DataLoaderImpl : public boost::noncopyable
{
	typedef std::vector<std::pair<int, RCAttrLoad*> > ThreadsHandles;
public:
	typedef std::vector<std::pair<uint,bool> > SortOrderType;

	virtual _uint64	Proceed();
	//virtual _uint64	Proceed2();
	virtual ~DataLoaderImpl();
	uint GetPackrowSize() const { return packrow_size; }
	bool DoSort() const { return enable_sort; }
	uint GetSortSize() const { return sort_size; }
	int64 GetNoRejectedRows() const { return no_rejected_rows; };
	
public:
	DataLoaderImpl(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop);

protected:
	virtual int		LoadData();
	virtual void	Abort();
	virtual std::vector<uint>*  RowOrder(NewValuesSetCopy **nvs, uint size) { return NULL; }
	virtual void	OmitAttrs( NewValuesSetCopy **) {return;}
	virtual void	RowOrder2(std::vector<boost::shared_ptr<ValueCache> >& value_buffers, uint size, order_t& order) { return; }
	virtual void	OmitAttrs2(std::vector<boost::shared_ptr<ValueCache> >& value_buffers) { return; }
	
public:
	static std::auto_ptr<DataLoader>
			CreateDataLoader(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop);
	static std::auto_ptr<DataLoader>
			CreateDataLoader(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, 
								SortOrderType &, int, std::vector<unsigned int> &);
protected:

	std::vector<RCAttrLoad*> 	attrs;
	Buffer*		 				buffer;
	IOParameters 				iop;
	std::auto_ptr<DataParser> 	parser;
	_uint64 					no_loaded_rows;
	int64 					no_rejected_rows;
	uint						packrow_size;
	uint                        sort_size;
	SortOrderType               sort_order;
	bool						enable_sort;
};

#endif /*DATALOADER_H_*/
