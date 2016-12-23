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

#ifndef _DATA_EXPORTERTXT_H_
#define _DATA_EXPORTERTXT_H_

#include "exporter/DataExporter.h"
#include "system/IOParameters.h"

class DEforTxt : public DataExporter
{
	public:
		virtual void PutNull() {
			WriteNull();
			WriteValueEnd();
		}

		virtual void PutText(const RCBString& str);

		virtual void PutBin(const RCBString& str);

		virtual void PutNumeric(_int64 num);

		virtual void PutRowEnd();

	protected:
		DEforTxt(const IOParameters& iop);

		void WriteStringQualifier()		{ buf->WriteIfNonzero(str_q); }
		void WriteDelimiter()			{ buf->WriteIfNonzero(delim); }
		void WriteNull()				{ WriteString(nulls_str.c_str(), (int)nulls_str.length()); }

		//void WriteString(const char* str)			{ WriteString(str, (int)strlen(str)); }
		//void WriteString(const char* str, int len)	{ strncpy(buf->BufAppend((uint)len), str, (size_t)len); }
		void WriteString(const RCBString& str)			{ WriteString(str, (int)strlen(str)); }
		size_t WriteString(const RCBString& str, int len);
		void WriteChar(char c, uint repeat = 1)		{ memset(buf->BufAppend(repeat), c, repeat); }
		void WritePad(uint repeat)					{ WriteChar(' ', repeat); }

		void WriteValueEnd()
		{
			if(cur_attr == no_attrs - 1)
				cur_attr = 0;
			else {
				WriteDelimiter();
				cur_attr++;
			}
		}

		uchar	delim, str_q, esc;
		std::string nulls_str, escaped;
		CHARSET_INFO* destination_cs;
};

class RCDEforTxtVariable : public DEforTxt
{
public:
	RCDEforTxtVariable(const IOParameters& iop)
		:	DEforTxt(iop) {}

	virtual void PutDateTime(_int64 dt) {
		RCDateTime rcdt(dt, deas[cur_attr].Type());
		RCBString rcs = rcdt.ToRCString();
		WriteString(rcs);
		WriteValueEnd();
	}
};

#endif
