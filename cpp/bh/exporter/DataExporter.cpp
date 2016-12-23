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

#include <boost/shared_ptr.hpp>

#include "exporter/DataExporter.h"
#include "core/RCAttr.h"
#include "common/bhassert.h"
#include "system/Buffer.h"
#include "core/RCEngine.h"

using namespace std;
using namespace boost;

void DataExporter::Init(boost::shared_ptr<Buffer> buffer, std::vector<ATI> source_deas, fields_t const& fields, std::vector<ATI>& result_deas)
{
#ifndef PURE_LIBRARY
	_fields = fields;
	buf = buffer;

	this->source_deas = source_deas;
	this->deas = result_deas;
	this->no_attrs = int(deas.size());

	for(int i = 0; i < deas.size(); ++i) {
		AttributeType f_at = rceng->GetCorrespondingType(fields[i]);		
		if(ATI::IsStringType(deas[i].Type()) && !ATI::IsStringType(f_at))		
			this->deas[i] = ATI(f_at, deas[i].NotNull());
	}

	cur_attr = 0;
	row = NULL;
	row_ptr = NULL;
	nulls_indicator = 0;
#else
	BHERROR("NOT IMPLEMENTED");
#endif
}

DataExporter::~DataExporter(void)
{
}
//
//void DataExporter::FlushBuffer()
//{
//	if(buf.get())
//		buf->BufFlush();
//}
//
//void DataExporter::ShowProgress(int no_eq)
//{
//	if(!progressout)
//		return;
//	if((no_eq % 10) == 0)
//		(*progressout) << "#";
//	else
//		(*progressout) << "=";
//	(*progressout) << flush;
//}

