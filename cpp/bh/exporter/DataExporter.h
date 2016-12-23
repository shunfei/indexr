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

#ifndef _CORE_RCDATAEXP_H_
#define _CORE_RCDATAEXP_H_

#include <boost/shared_ptr.hpp>

#include "core/bintools.h"
#include "common/CommonDefinitions.h"
#include "compress/tools.h"
#include "system/ChannelOut.h"
#include "system/TextUtils.h"
#include "types/DataConverter.h"
#include "system/Buffer.h"

class DataExporter
{
public:
	DataExporter()			:	progressout(NULL), row(NULL), row_ptr(NULL), nulls_indicator(NULL) {}
	virtual void Init(boost::shared_ptr<Buffer> buffer, std::vector<ATI> source_deas, fields_t const& fields, std::vector<ATI>& result_deas);
	virtual ~DataExporter();
	void FlushBuffer();

	void SetProgressOut(ChannelOut* po)		{ progressout = po; }
	void ShowProgress(int no_eq);

	virtual void PutNull() = 0;
	virtual void PutText(const RCBString& str) = 0;
	virtual void PutBin(const RCBString& str) = 0;
	virtual void PutNumeric(_int64 num) = 0;
	virtual void PutDateTime(_int64 dt) = 0;
	virtual void PutRowEnd() = 0;

protected:
	int		cur_attr;
	std::vector<ATI> deas;
	std::vector<ATI> source_deas;
	fields_t _fields;

	boost::shared_ptr<Buffer>	buf;
	ChannelOut*		progressout;

	int		no_attrs;

	// the fields below should be moved to RCDEforBinIndicator
	char*	row;
	char*	row_ptr;
	int		max_row_size;
	int		nulls_indicator_len;
	char*	nulls_indicator;
};

#endif //_CORE_RCDATAEXP_H_

