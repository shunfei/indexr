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


#include "DataExporterTxt.h"

DEforTxt::DEforTxt(const IOParameters& iop)
	:	delim(iop.Delimiter()[0]), str_q(iop.StringQualifier()), esc(iop.EscapeCharacter()), nulls_str(iop.NullsStr())
#ifndef PURE_LIBRARY
		, destination_cs(iop.CharsetInfoNumber() ? get_charset(iop.CharsetInfoNumber(), 0) : 0)
#else
		, destination_cs(0)
#endif
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#endif
}

void DEforTxt::PutText(const RCBString& str)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	WriteStringQualifier();
	int char_len = int(deas[cur_attr].GetCollation().collation->cset->numchars(deas[cur_attr].GetCollation().collation,
			str.val, str.val + str.len)); //len in chars
	int bytes_written = int(WriteString(str, str.len)); //len in bytes
	if((deas[cur_attr].Type() == RC_STRING) && (char_len < deas[cur_attr].CharLen()))
	// it can be necessary to change the WritePad implementation to something like:
	// collation->cset->fill(cs, copy->to_ptr+copy->from_length, copy->to_length-copy->from_length, ' ');
	// if ' ' (space) can have different codes.
	if(!destination_cs) { //export as binary
		WritePad(deas[cur_attr].Precision() - bytes_written);
	} else {
		WritePad(deas[cur_attr].CharLen() - char_len);
	}
	WriteStringQualifier();
	WriteValueEnd();
#endif

}

void DEforTxt::PutBin(const RCBString& str)
{
	int len = (int)strlen(str);
//if((rcdea[cur_attr].attrt == RC_BYTE) && (len < rcdea[cur_attr].size))
//	len = rcdea[cur_attr].size;
	if(len > 0) {
		char* hex = new char[len*2];
		Convert2Hex((const unsigned char*)str.val, len, hex, len*2, false);
		WriteString(RCBString(hex, len*2));
		delete[] hex;
	}
	WriteValueEnd();
}

void DEforTxt::PutNumeric(_int64 num)
{
	RCNum rcn(num, source_deas[cur_attr].Scale(), ATI::IsRealType(source_deas[cur_attr].Type()), source_deas[cur_attr].Type());
	RCBString rcs = rcn.ToRCString();
	WriteString(rcs);
	WriteValueEnd();
}

void DEforTxt::PutRowEnd()
{
	WriteString("\r\n", 2);
}


size_t DEforTxt::WriteString(const RCBString& str, int len)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	int res_len = 0;
	if(esc) {
		escaped.erase();
		for(int i=0; i< strlen(str); i++) {
			if(str[i] == str_q || (!str_q && str[i] == delim))
				escaped.append(1,esc);
			escaped.append(1,str[i]);
		}
		if(destination_cs) {
			int max_res_len = std::max(destination_cs->mbmaxlen * len + len, deas[cur_attr].GetCollation().collation->mbmaxlen * len + len);
			uint errors = 0;
			res_len = copy_and_convert(buf->BufAppend(max_res_len), max_res_len, destination_cs, escaped.c_str(), 
				uint32(escaped.length()), deas[cur_attr].GetCollation().collation, &errors);
			buf->SeekBack(max_res_len - res_len);
		} else {
			strncpy(buf->BufAppend(uint(escaped.length())), escaped.c_str(), escaped.length());
			res_len = len;
		}

	} else {
		if(destination_cs) {
			int max_res_len = std::max(destination_cs->mbmaxlen * len, deas[cur_attr].GetCollation().collation->mbmaxlen * len);
			uint errors = 0;
			res_len = copy_and_convert(buf->BufAppend(max_res_len), max_res_len, destination_cs, str.Value(), len, deas[cur_attr].GetCollation().collation, &errors);
			buf->SeekBack(max_res_len - res_len);
		} else {
			strncpy(buf->BufAppend((uint)len), str, len);
			res_len = len;
		}
	}
	return res_len;
#endif

}
