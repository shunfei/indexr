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

#include <vector>
#include <algorithm>
#include <boost/lexical_cast.hpp>

#include "system/LargeBuffer.h"
#include "system/IOParameters.h"
#include "DataLoader.h"
#include "NewValueSet.h"
#include "edition/loader/RCAttr_load.h"
#include "DataParser.h"
#include "DataParser2.h"
#include "system/LargeBuffer.h"
#include "system/IOParameters.h"
#include "loader/NewValueSetCopy.h"
#include "loader/NewValueSubSet.h"

using namespace std;
using namespace boost;

DataLoaderImpl::DataLoaderImpl(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop)
	:	attrs(attrs), buffer(&buffer), iop(iop), packrow_size(MAX_PACK_ROW_SIZE)
{
	no_loaded_rows = 0;
	no_rejected_rows = 0;
	sort_size = 0;
	enable_sort = false;
	if(iop.ATIs().size() == 0) {
		std::vector<ATI> atis;
		for(int i = 0; i < attrs.size(); ++i)
			atis.push_back(AttributeTypeInfo(attrs[i]->TypeName(), attrs[i]->Type().GetNullsMode() == NO_NULLS, attrs[i]->Type().GetPrecision(), attrs[i]->Type().GetScale()));
		this->iop.SetATIs(atis);
}
}

DataLoaderImpl::~DataLoaderImpl()
{
}

//_uint64 DataLoaderImpl::Proceed2()
//{
//	NewValuesSetCopy **nvses;
//	boost::shared_ptr<NewValuesSetBase> dummy_value_set_pointer;
//	no_loaded_rows = 0;
//	int chunk_rows_loaded;
//	int to_prepare;
//	if( DoSort() )
//		to_prepare = GetSortSize();
//	else
//		to_prepare = iop.GetPackrowSize() - (int)(attrs[0]->NoObj() % iop.GetPackrowSize());
//
//	if (process_type == ProcessType::DATAPROCESSOR)
//		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::ExpandDPNArrays, _1));
//	for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::SetPackrowSize, _1, iop.GetPackrowSize()));
//
//	try {
//		parser->no_prepared = 0;
//		int count=0;
//		nvses = new NewValuesSetCopy * [attrs.size()];
//		for(uint i = 0; i < attrs.size(); i++)
//			nvses[i] = 0;
//
//		while(true) {
//			parser->row_incomplete = false;
//			parser->Prepare(to_prepare);
//			if (parser->no_prepared == 0) {
//				if (LoadData() == 0) {
//					if (parser->row_incomplete)
//						throw FormatRCException(BHERROR_DATA_ERROR, parser->error_value.first, parser->error_value.second);
//					if (parser->no_prepared == 0)
//						break;
//				}
//				else
//					continue;
//			}
//			for(uint i = 0; i < attrs.size(); i++) {
//				parser->PrepareNextCol();
//				nvses[i] = new NewValuesSetCopy( parser.get(), i, 0, attrs[i]->PackType(), attrs[i]->Type().GetCollation() );
//			}
//			std::vector<uint> * order( RowOrder( nvses, parser->no_prepared ) );
//			OmitAttrs(nvses);
//
//			if( order == NULL ) {
//				for( uint i=0; i<attrs.size(); i++ ) {
//#ifndef __BH_COMMUNITY__
//					attrs[i]->LoadData(nvses[i], dummy_value_set_pointer, (process_type != ProcessType::BHLOADER && process_type != ProcessType::DATAPROCESSOR) || true, false);
//#else
//					attrs[i]->LoadData(nvses[i], dummy_value_set_pointer, false);
//#endif
//			}
//				to_prepare = iop.GetPackrowSize() - (int)(attrs[0]->NoObj() % iop.GetPackrowSize());
//			} else {
//				int current_pkrow_size=0;
//				for( chunk_rows_loaded=0;
//					chunk_rows_loaded<parser->no_prepared;
//					chunk_rows_loaded+=current_pkrow_size ) {
//					current_pkrow_size = std::min(iop.GetPackrowSize() - (uint)(attrs[0]->NoObj() % iop.GetPackrowSize()),
//												(uint)(parser->no_prepared - chunk_rows_loaded));
//					for( uint i=0; i<attrs.size(); i++ ) {
//						NewValuesSetBase *sub;
//						sub = new NewValuesSubSet(nvses[i],
//													*order,
//													chunk_rows_loaded,
//													current_pkrow_size);
//#ifndef __BH_COMMUNITY__
//						attrs[i]->LoadData(sub, dummy_value_set_pointer, (process_type != ProcessType::BHLOADER && process_type != ProcessType::DATAPROCESSOR) || parser->DoPreparedValuesHasToBeCoppied(), false);
//#else
//						attrs[i]->LoadData(nvses[i], dummy_value_set_pointer, false);
//#endif
//						delete sub;
//					}
//				}
//				to_prepare = GetSortSize();
//			}
//			no_loaded_rows += parser->no_prepared;
//
//
//			parser->no_prepared = 0;
//			count++;
//			for(uint i = 0; i < attrs.size(); i++) {
//				delete nvses[i];
//				nvses[i] = 0;
//			}
//			delete order;
//		}
//	} catch(FormatRCException& frce) {
//		frce.m_row_no += no_loaded_rows;
//		Abort();
//		for(uint i = 0; i < attrs.size(); i++) {
//			delete nvses[i];
//			nvses[i] = 0;
//		}
//		throw;
//	} catch(DataTypeConversionRCException&) {
//		for(uint i = 0; i < attrs.size(); i++) {
//			delete nvses[i];
//			nvses[i] = 0;
//		}
//		Abort();
//		throw FormatRCException(BHERROR_DATA_ERROR, no_loaded_rows + parser->error_value.first, parser->error_value.second);
//	} catch(...) {
//		for(uint i = 0; i < attrs.size(); i++) {
//			delete nvses[i];
//			nvses[i] = 0;
//		}
//		Abort();
//		throw InternalRCException(BHERROR_UNKNOWN);
//	}
//
//	if(no_loaded_rows == 0)
//		throw FormatRCException(BHERROR_DATA_ERROR, no_loaded_rows + parser->error_value.first, parser->error_value.second);
//
//#ifndef __BH_COMMUNITY__
//	if (process_type == ProcessType::DATAPROCESSOR) {
//		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::FinalizeStream, _1));
//		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::WaitForSaveThreads, _1));
//	} else
//#endif
//	for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::Save, _1));
//	delete [] nvses;
//	return no_loaded_rows;
//}

_uint64 DataLoaderImpl::Proceed()
{
	DataParser2 parser(attrs, *buffer, iop);
	no_loaded_rows = 0;
	no_rejected_rows = 0;

	if(process_type == ProcessType::DATAPROCESSOR)
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::ExpandDPNArrays, _1));
	for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::SetPackrowSize, _1, iop.GetPackrowSize()));

	std::vector<boost::shared_ptr<ValueCache> > sorted_block;
	order_t order;
	uint sorted_block_size = 0, sorted_block_used = 0;
	uint to_prepare, no_of_rows_returned;
	try {
		do {
			to_prepare = iop.GetPackrowSize() - (int)(attrs[0]->NoObj() % iop.GetPackrowSize());
			std::vector<boost::shared_ptr<NewValuesSetBase> > value_buffers;
			if(DoSort()) {
				if(sorted_block_used == sorted_block_size) {
					// load new sorted block
					sorted_block.clear();
					sorted_block = parser.GetPackrow(GetSortSize(), sorted_block_size);
					RowOrder2(sorted_block, sorted_block_size, order);
					OmitAttrs2(sorted_block);
					sorted_block_used = 0;
				}
				if(sorted_block_size > 0) {
					// get the next packrow from the sorted block
					to_prepare = std::min(to_prepare, sorted_block_size - sorted_block_used);
					for(int buf = 0; buf < sorted_block.size(); buf++)
						value_buffers.push_back(boost::shared_ptr<NewValuesSetBase>(new NewValuesSubSet(sorted_block[buf].get(), order, sorted_block_used, to_prepare)));
					sorted_block_used += to_prepare;
					no_of_rows_returned = to_prepare;
				}
				else
					no_of_rows_returned = 0;
			} else {
				std::vector<boost::shared_ptr<ValueCache> > value_cache_set( parser.GetPackrow(to_prepare, no_of_rows_returned) );
				for(int buf = 0; buf < value_cache_set.size(); buf++)
					value_buffers.push_back(value_cache_set[buf]);
			}
			if(no_of_rows_returned > 0) {
				for(uint att = 0; att < attrs.size(); ++att) {
#ifndef __BH_COMMUNITY__
					attrs[att]->LoadData(value_buffers[att].get(), DoSort() ? sorted_block[att] : value_buffers[att], (process_type != ProcessType::BHLOADER && process_type != ProcessType::DATAPROCESSOR));
#else
					attrs[att]->LoadData(value_buffers[att].get(), value_buffers[att], false);
#endif
				}
				no_loaded_rows += no_of_rows_returned;
			}
		} while(no_of_rows_returned == to_prepare);
	} catch(...) {
		Abort();
		throw;
	}

	no_rejected_rows = parser.GetNoRejectedRows();

	if(parser.ThresholdExceeded(no_loaded_rows + no_rejected_rows))
		throw FormatRCException("Rejected rows threshold exceeded. " + lexical_cast<string>(no_rejected_rows) + " out of " + lexical_cast<string>(no_loaded_rows + no_rejected_rows) + " rows rejected.");

	if(no_loaded_rows == 0 && no_rejected_rows == 0)
		throw FormatRCException(BHERROR_DATA_ERROR, -1, -1);


#ifndef __BH_COMMUNITY__
	if (process_type == ProcessType::DATAPROCESSOR) {
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::FinalizeStream, _1));
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::WaitForSaveThreads, _1));
	} else
#endif
	{
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::SavePacks, _1));
		for_each(attrs.begin(), attrs.end(), boost::bind(&RCAttrLoad::Save, _1));
	}
	return no_loaded_rows;
}

int DataLoaderImpl::LoadData()
{
	MEASURE_FET("DataLoader::LoadData()");
	int ret = 0;
	try	{
		ret = buffer->BufFetch(int(parser->buf_end - parser->buf_ptr));
	} catch (FileRCException&) {
		Abort();
		throw;
	}

	if(ret)	{
		parser->buf_ptr = parser->buf_start = buffer->Buf(0);
		parser->buf_end = parser->buf_start + buffer->BufSize();
	}
	return ret;
}


void DataLoaderImpl::Abort()
{
}

auto_ptr<DataLoader>
DataLoaderImpl::CreateDataLoader(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop)
{
	auto_ptr<DataLoader> data_loader(new DataLoader(attrs, buffer, iop));
	data_loader->parser = DataFormat::GetDataFormat(iop.GetEDF())->CreateDataParser(attrs, buffer, iop);
    //data_loader->parser = DataParserFactory::CreateDataParser(attrs, buffer, iop);
	return data_loader;
}

#ifndef __BH_COMMUNITY__
auto_ptr<DataLoader>
DataLoaderImpl::CreateDataLoader(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, SortOrderType &o, int s, std::vector<unsigned int> &om)
{
	auto_ptr<DataLoader> data_loader(new DataLoader(attrs, buffer, iop, o, s, om));
	if( o.size() == 0 )
		data_loader->parser = DataFormat::GetDataFormat(iop.GetEDF())->CreateDataParser(attrs, buffer, iop, iop.GetPackrowSize());
	else
		data_loader->parser = DataFormat::GetDataFormat(iop.GetEDF())->CreateDataParser(attrs, buffer, iop, s);

	//data_loader->parser = DataParserFactory::CreateDataParser(attrs, buffer, iop);
	return data_loader;
}
#endif
