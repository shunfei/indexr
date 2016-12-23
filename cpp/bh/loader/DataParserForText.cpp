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

#include "types/DataConverter.h"
#include "core/tools.h"
#include "core/bintools.h"
#include "loader/NewValueSet.h"
#include "edition/loader/RCAttr_load.h"
#include "system/IOParameters.h"
#include "DataParserForText.h"
#include "types/ValueParserForText.h"

using namespace std;
using namespace boost;
using namespace boost::assign;

namespace
{

void PrepareKMPNext(DataParserForText::kmp_next_t& kmp_next, string const& pattern)
{
	kmp_next.resize(pattern.length() + 1);
	int b = kmp_next[0] = -1;
	for(int i = 1, lenpat(static_cast<int>(pattern.length())); i <= lenpat; ++i) {
		while((b > -1) && (pattern[b] != pattern[i - 1]))
			b = kmp_next[b];
		++b;
		kmp_next[i] = (pattern[i] == pattern[b]) ? kmp_next[b] : b;
	}
}

}

DataParserForText::DataParserForText(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr)
	:	DataParser(attrs, buffer, iop, mpr), qualifier_present(no_attr), prepared(false), eol(iop.LineTerminator()), delimiter(iop.Delimiter()),
	 	string_qualifier(iop.StringQualifier()), escape_char(iop.EscapeCharacter())
{
	ShiftBufferByBOM();
	for(int i = 0; i < no_attr; i++)
		qualifier_present[i].resize(mpr);

	for(int i = 0; i < no_attr; i++)
		if(atis[i].Type() != RC_STRING)
			value_sizes[i] = shared_ptr<vector<uint> >(new vector<uint>(max_parse_rows));

#ifndef PURE_LIBRARY
	cs_info = get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.CharsetInfoNumber(), 0);
	for(int i = 0; i < no_attr; i++)
		if(ATI::IsStringType(atis[i].Type()))
			atis[i].SetCollation(get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.ColumnsCollations()[i], 0));
#endif
	converted_values = 0;
	converted_values_size = 0;
	pos_in_converted_values = 0;

	PrepareKMPNext(kmp_next_delimiter, delimiter);
	if (string_qualifier) {
		enclose_delimiter = string((char*)&string_qualifier, 1) + delimiter;
		PrepareKMPNext(kmp_next_enclose_delimiter, enclose_delimiter);
	}
}

DataParserForText::~DataParserForText()
{
	free(converted_values);
	for(std::vector<char*>::iterator it = con_buffs.begin(); it != con_buffs.end(); it++)
			delete [] *it;
}

void DataParserForText::ReleaseCopies()
{
	while(!con_buffs.empty()) {
		delete[] con_buffs.back();
		con_buffs.pop_back();
	}
}

void DataParserForText::PrepareValuesPointers()
{
	if(oryg_vals_ptrs.size()) {
		if(cur_attr != 0) {
			for(int i = 0; i < no_prepared; i++) //restore original values pointers
				values_ptr[i] = oryg_vals_ptrs[i];
		}
		oryg_vals_ptrs.clear();
	}

	size_t opsbs = delimiter.size();
	for(int i = 0; i < no_prepared; i++) {
		if (cur_attr != 0 ) {
			values_ptr[i] += (*objs_sizes[cur_attr-1])[i] + opsbs ;
			if(qualifier_present[cur_attr - 1][i]) //previous column had string qualifier
				values_ptr[i]--; //undoing the following ++
		}
		if(string_qualifier && values_ptr[i][0] == string_qualifier) {
			values_ptr[i]++;
			qualifier_present[cur_attr][i]=true;
		} else
			qualifier_present[cur_attr][i]=false;
	}
}

void DataParserForText::FormatSpecificProcessing()
{
	if(ATI::IsBinType(cur_attr_type)) {
		for(int i = 0; i < no_prepared; i++) {
			int p = 0;
			uint os = GetObjSize(i);
			for(uint l = 0; l < os; l++) {
				int a = isalpha((uchar)values_ptr[i][l]) ? tolower(values_ptr[i][l]) - 'a' + 10 : values_ptr[i][l] - '0';
				int b = isalpha((uchar)values_ptr[i][l+1]) ? tolower(values_ptr[i][l+1]) - 'a' + 10 : values_ptr[i][l+1] - '0';
				values_ptr[i][p++] =  a * 16 + b;
				l++;
			}
		}
	} else if(ATI::IsTxtType(cur_attr_type)) {

		for(int i = 0; i < no_prepared; i++)
			ProcessEscChar(i);

#ifndef PURE_LIBRARY
		if(atis[cur_attr].CharsetInfo() != cs_info) {
			uint errors = 0;
			if(atis[cur_attr].CharsetInfo()->mbmaxlen <= cs_info->mbmaxlen) {
				for(int i = 0; i < no_prepared; i++)
					(*value_sizes[cur_attr])[i] = copy_and_convert(values_ptr[i], GetObjSize(i), atis[cur_attr].CharsetInfo(), values_ptr[i], GetValueSize(i), cs_info, &errors);
			} else {
				SetPosInConvertedValues(0);
				oryg_vals_ptrs.clear();
				vector<size_t> positions;
				for(int i = 0; i < no_prepared; i++) {
					uint size_before = GetValueSize(i);
					uint size_after = 0;
					size_t new_max_size = size_before * atis[cur_attr].CharsetInfo()->mbmaxlen;
					size_t pos = GetBufferForConvertedValues(new_max_size + 1);
					char* v_ptr = ConvertedValue(pos);
					positions.push_back(pos);
					size_after = copy_and_convert(v_ptr, (uint32)new_max_size, atis[cur_attr].CharsetInfo(), values_ptr[i], size_before, cs_info, &errors);
					*(v_ptr + size_after) = 0;
					(*value_sizes[cur_attr])[i] = size_after;
				}

				for(int i = 0; i < no_prepared; i++) {
					oryg_vals_ptrs.push_back(values_ptr[i]);
					values_ptr[i] = ConvertedValue(positions[i]);
				}
			}
		}
#endif
	}
}

/*void DataParserForText::GetCRLF()
{
	bool inside_string = false;
	char* ptr = buf_start;
	while(ptr != buf_end) {
		if(!inside_string) {
			if(string_qualifier && *ptr == string_qualifier)
				inside_string = 1;
			else if(*ptr == '\r' && *(ptr+1) == '\n') {
				eol = "\r\n";
				crlf = 2;
				break;
			} else if(*ptr == '\n') {
				eol = "\n";
				crlf = 1;
				break;
			}
		} else if(inside_string && string_qualifier && *ptr == string_qualifier) {
			if(*(ptr + 1) == delimiter[0] ||  *(ptr + 1) == '\n')
				inside_string = 0;
			else if((ptr + 2) <= buf_end && *(ptr + 1) == '\r' && *(ptr + 2) == '\n')
				inside_string = 0;
		}
		ptr++;
	}
	if (crlf==0) throw FormatRCException( BHERROR_DATA_ERROR, 1, -1 );
}

inline int DataParserForText::GetRowSize(char* row_ptr, int rno)
{
	row_incomplete = false;
	if(!prepared) {
		GetCRLF();
		prepared = true;
	}
	if(row_ptr != buf_end) {
		char* ptr = row_ptr;
		char* v_ptr = row_ptr;
		bool stop = false;
		bool inside_string = false;
		int no_attrs_tmp = 0;
		int tan = no_attr - 1;
		bool esc_char = false;
		bool new_value_beg = true;

		while(!stop) {
			if(esc_char) {
				new_value_beg = false;
				esc_char = false;
			} else if(escape_char && *ptr == escape_char &&
			    ATI::IsTxtType(atis[no_attrs_tmp].Type()) &&
			    !(escape_char == string_qualifier && new_value_beg) &&
			    (   !(escape_char == string_qualifier && !new_value_beg &&
				(((no_attrs_tmp != tan) && (*(ptr+1) == delimiter)) ||
				   ((no_attrs_tmp == tan) && IsEOR(ptr+1))
				))
                            ))
			{
				new_value_beg = false;
				esc_char = true;
			} else if(string_qualifier && *ptr == string_qualifier) {
				if(!inside_string) {
					inside_string = true;
				} else if ( ( ( ( ptr + 1 ) != buf_end ) && ( no_attrs_tmp != tan ) && ( *(ptr + 1) == delimiter ) )
						|| ( ( ( ptr + crlf ) < buf_end ) && ( no_attrs_tmp == tan ) && IsEOR( ptr + 1 ) ) ) {
					inside_string = false;
				} else if ( ( ( ( ptr + 1 ) != buf_end ) && ( no_attrs_tmp != tan ) ) || ( ( ( ptr + crlf ) < buf_end ) && ( no_attrs_tmp == tan ) ) )
					throw FormatRCException(BHERROR_DATA_ERROR, rno + 1, no_attrs_tmp + 1 );
				new_value_beg = false;
				esc_char = false;
			} else if(!inside_string) {
				if(((no_attrs_tmp != tan) && (*ptr == delimiter)) || ((no_attrs_tmp == tan) && IsEOR(ptr))) {
					(*objs_sizes[no_attrs_tmp])[rno] = (uint)(ptr - v_ptr);
					if(no_attrs_tmp != 0)
						(*objs_sizes[no_attrs_tmp])[rno]--;
					(*value_sizes[no_attrs_tmp])[rno] = (*objs_sizes[no_attrs_tmp])[rno];
					v_ptr = ptr;
					no_attrs_tmp++;
					new_value_beg = true;
					if(no_attrs_tmp == no_attr)
						break;
				} else if(IsEOR(ptr)) {
					if(string_qualifier || (no_attrs_tmp != tan ))
						throw FormatRCException(BHERROR_DATA_ERROR, rno + 1, no_attrs_tmp + 1 );
				}
				esc_char = false;
			}
			if((ptr + 1) == buf_end) {
				row_incomplete = true;
				error_value.first = rno + 1;
				error_value.second = no_attrs_tmp + 1;
				break;
			}
			ptr++;
		}
		if(no_attrs_tmp == no_attr)
			return (int)(ptr - row_ptr) + crlf;
	}
 	return -1;
}*/

inline void DataParserForText::GuessUnescapedEOL(const char* ptr)
{
	for( ; ptr < buf_end; ptr++) {
		if(escape_char && *ptr == escape_char) {
			if(ptr + 2 < buf_end && ptr[1] == '\r' && ptr[2] == '\n')
				ptr += 3;
			else
				ptr += 2;
		} else if(*ptr == '\n') {
			eol = "\n";
			break;
		} else if(*ptr == '\r' && ptr + 1 < buf_end && ptr[1] == '\n') {
			eol = "\r\n";
			break;
		}
	}
}

inline void DataParserForText::GuessUnescapedEOLWithEnclose(const char* ptr)
{
	for( ; ptr < buf_end; ptr++) {
		if(escape_char && *ptr == escape_char)
			ptr += 2;
		else if (string_qualifier && *ptr == string_qualifier) {
			if(ptr + 1 < buf_end && ptr[1] == '\n') {
				eol = "\n";
				break;
			}
			else if( ptr + 2 < buf_end && ptr[1] == '\r' && ptr[2] == '\n') {
				eol = "\r\n";
				break;
			}
		}
	}
}

inline bool DataParserForText::SearchUnescapedPattern(const char*& ptr, const string& pattern, const vector<int>& kmp_next)
{
	const char* c_pattern = pattern.c_str();
	size_t size = pattern.size();
	const char* search_end = buf_end;
	if(size == 1) {
		while(ptr < search_end && *ptr != *c_pattern) {
			if(escape_char && *ptr == escape_char)
				ptr += 2;
			else
				++ptr;
		}
	} else if(size == 2) {
		--search_end;
		while(ptr < search_end && (*ptr != *c_pattern || ptr[1] != c_pattern[1])) {
			if(escape_char && *ptr == escape_char)
				ptr += 2;
			else
				++ptr;
		}
	} else {
		int b = 0;
		for(; ptr < buf_end; ++ptr) {
			if(escape_char && *ptr == escape_char) {
				b = 0;
				++ptr;
			} else if(c_pattern[b] != *ptr) {
				if(b != 0) {
					b = kmp_next[b];
					while((b > -1) && (c_pattern[b] != *ptr))
						b = kmp_next[b];
					++b;
				}
			} else if(++b == size) {
				ptr -= size - 1;
				break;
			}
		}
	}
/*	else if(size == 3) {
		search_end -= 2;
		while(ptr < search_end && (*ptr != c_pattern[0] || *(ptr+1) != c_pattern[1] || *(ptr+2) != c_pattern[2])) {
			if(*ptr == escape_char)
				ptr += 2;
			else
				++ptr;
		}
	}
	else
		BHASSERT(0, (string("Unexpected pattern: '") + delimiter + "'").c_str());*/
	return (ptr < search_end);
}

inline bool TailsMatch(const char* s1, const char* s2, const size_t size)
{
	uint i = 1;
	while(i < size && s1[i] == s2[i])
		i++;
	return (i==size);
}

inline DataParserForText::SearchResult DataParserForText::SearchUnescapedPatternNoEOL(const char*& ptr, const string& pattern, const vector<int>& kmp_next)
{
	const char* c_pattern = pattern.c_str();
	size_t size = pattern.size();
	const char* c_eol = eol.c_str();
	size_t crlf = eol.size();
	const char* search_end = buf_end;
	if(size == 1) {
		while(ptr < search_end && *ptr != *c_pattern) {
			if(escape_char && *ptr == escape_char)
				ptr += 2;
			else if(*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
				return END_OF_LINE;
			else
				++ptr;
		}
	} else if(size == 2) {
		--search_end;
		while(ptr < search_end && (*ptr != *c_pattern || ptr[1] != c_pattern[1])) {
			if(escape_char && *ptr == escape_char)
				ptr += 2;
			else if(*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
				return END_OF_LINE;
			else
				++ptr;
		}
	} else {
		int b = 0;
		for ( ; ptr < buf_end; ++ ptr ) {
			if(escape_char && *ptr == escape_char) {
				b = 0;
				++ ptr;
			} else if(*ptr == *c_eol && ptr + crlf <= buf_end && TailsMatch(ptr, c_eol, crlf))
				return END_OF_LINE;
			else if( c_pattern[ b ] != *ptr ) {
				if( b != 0 ) {
					b = kmp_next[ b ];
					while( ( b > -1 ) && ( c_pattern[ b ] != *ptr ) )
						b = kmp_next[ b ];
					++ b;
				}
			} else if( ++ b == size ) {
				ptr -= size - 1;
				break;
			}
		}
	}
/*	else if(size == 3) {
		search_end -= 2;
		while(ptr < search_end && (*ptr != c_pattern[0] || *(ptr+1) != c_pattern[1] || *(ptr+2) != c_pattern[2])) {
			if(escape_char && *ptr == escape_char)
				ptr += 2;
			else if (*ptr == eol[0] && (crlf == 1 || *(ptr+1) == eol[1]))
				return END_OF_LINE;
			else
				++ptr;
		}
	}
	else
		BHASSERT(0, (string("Unexpected pattern: '") + delimiter + "'").c_str());*/
	return (ptr < search_end) ? PATTERN_FOUND : END_OF_BUFFER;
}

void DataParserForText::GetEOL()
{
	if(eol.size() == 0) {
		const char* ptr = buf_start;
		for(int col = 0; col < no_attr - 1; ++col) {
			if(string_qualifier && *ptr == string_qualifier) {
				if(!SearchUnescapedPattern(++ptr, enclose_delimiter, kmp_next_enclose_delimiter))
					throw FormatRCException(BHERROR_DATA_ERROR, 1, col + 1);
				++ptr;
			} else if(!SearchUnescapedPattern(ptr, delimiter, kmp_next_delimiter))
				throw FormatRCException(BHERROR_DATA_ERROR, 1, col + 1);
			ptr += delimiter.size();
		}
		if(string_qualifier && *ptr == string_qualifier)
			GuessUnescapedEOLWithEnclose(++ptr);
		else
			GuessUnescapedEOL(ptr);
	}
	if(eol.size()==0)
		throw FormatRCException(BHERROR_DATA_ERROR, 1, no_attr);
	PrepareKMPNext(kmp_next_eol, eol);
	if(string_qualifier) {
		enclose_eol = string((char*) &string_qualifier, 1) + eol;
		PrepareKMPNext(kmp_next_enclose_eol, enclose_eol);
	}
}

inline int DataParserForText::GetRowSize(char* row_ptr, int rno)
{
	if(!prepared) {
		GetEOL();
		prepared = true;
	}
	row_incomplete = false;
	if (row_ptr == buf_end)
		return -1;
	const char* ptr = row_ptr;
	for(int col = 0; col < no_attr - 1; ++col) {
		const char* val_beg = ptr;
		if(string_qualifier && *ptr == string_qualifier) {
			row_incomplete = !SearchUnescapedPattern(++ptr, enclose_delimiter, kmp_next_enclose_delimiter);
			++ptr;
		} else {
			SearchResult res = SearchUnescapedPatternNoEOL(ptr, delimiter, kmp_next_delimiter);
			if (res == END_OF_LINE)
				throw FormatRCException(BHERROR_DATA_ERROR, rno + 1, col + 1 );
			row_incomplete = (res == END_OF_BUFFER);
		}

		if (row_incomplete) {
			error_value.first = rno + 1;
			error_value.second = col + 1;
			break;
		}
		(*value_sizes[col])[rno] = (*objs_sizes[col])[rno] = (uint)(ptr - val_beg);
		ptr += delimiter.size();
	}
	if (!row_incomplete) {
		// the last column
		const char* val_beg = ptr;
		if(string_qualifier && *ptr == string_qualifier) {
			row_incomplete = !SearchUnescapedPattern(++ptr, enclose_eol, kmp_next_enclose_eol);
			++ptr;
		} else
			row_incomplete = !SearchUnescapedPattern(ptr, eol, kmp_next_eol);

		if (row_incomplete) {
			error_value.first = rno + 1;
			error_value.second = no_attr;
		} else {
			(*value_sizes[no_attr - 1])[rno] = (*objs_sizes[no_attr - 1])[rno] = (uint)(ptr - val_beg);
			ptr += eol.size();
		}
	}
	return row_incomplete ? -1 : (int)(ptr - row_ptr);
}

void DataParserForText::PrepareObjsSizes()
{
	objs_sizes_ptr = objs_sizes[cur_attr];
	for(int i = 0; i < no_prepared; i++) {
		if (string_qualifier ) {
			if(GetObjSize(i) && qualifier_present[cur_attr][i])
				(*value_sizes[cur_attr])[i] -= 2;
		}
		//process escape chars

		// eliminate trailing spaces
		if(ATI::IsCharType(cur_attr_type)) {
			for (char* c = values_ptr[i] + GetValueSize(i) - 1; c >= values_ptr[i]; c--) {
				if (*c == ' ')
					(*value_sizes[cur_attr])[i]--;
				else
					break;
			}
		}
	}
}

void DataParserForText::ProcessEscChar(int i)
{
	if(!IsNull(i)) {
		char* ptr = values_ptr[i];
		uint s = GetValueSize(i);
		uint j = 0;
		while (j < s && ptr[j] != escape_char)
			j++;
		if (j < s) {
			uint p = j;
			for( ; j < s; j++)	{
				if(ptr[j] == escape_char) {
					ptr[p] = TranslateEscapedChar(ptr[++j]);
					if ((*value_sizes[cur_attr])[i])
						(*value_sizes[cur_attr])[i]--;
				} else
					ptr[p] = ptr[j];
				p++;
			}
		}
	}
}

void DataParserForText::PrepareNulls()
{
	for(int i = 0; i < no_prepared; i++) {
		char* ptr = values_ptr[i];
		switch(GetObjSize(i)) {
		case 0:
			nulls[i] = 1;
			break;
		case 2:
			if(*ptr == '\\' && (*(ptr+1) == 'N' || *(ptr+1) == 'n'))
				nulls[i] = 1;
			else
				nulls[i] = 0;
			break;
		case 4:
			if(qualifier_present[cur_attr][i]) {
				if(*ptr == '\\' && *(ptr+1) == 'N')
					nulls[i] = 1;
				else
					nulls[i] = 0;
			}
			else if(strncasecmp(values_ptr[i], "NULL", 4) == 0)
				nulls[i] = 1;
			else
				nulls[i] = 0;
			break;
		default:
			nulls[i] = 0;
			break;
		}
	}

/*	if(!IsLastColumn()) {
		for(int i = 0; i < no_prepared; i++) {
			int rs = row_sizes[i];
			if(
				*values_ptr[i] == delimiter[0] ||
				(((values_ptr[i] + 5) < (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "NULL", 4) == 0
						&& (*(values_ptr[i] + 4) == delimiter[0])) ||
				(!qualifier_present[cur_attr][i] && ((values_ptr[i] + 3) < (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "\\N", 2) == 0
						&& (*(values_ptr[i] + 2) == delimiter[0])) ||
				(qualifier_present[cur_attr][i] && ((values_ptr[i] + 4) < (rows_ptr[i] + rs)) && strncmp(values_ptr[i], "\\N", 2) == 0
						&& (*(values_ptr[i] + 3) == delimiter[0]))
			)
				nulls[i] = 1;
			else
				nulls[i] = 0;
		}
	} else {
		for(int i = 0; i < no_prepared; i++) {
			int rs = row_sizes[i];
			if(
				IsEOR(values_ptr[i]) ||
				(((values_ptr[i] + 4 + crlf) == (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "NULL", 4) == 0
						&& IsEOR(values_ptr[i] + 4)) ||
				(!qualifier_present[cur_attr][i] && ((values_ptr[i] + 2 + crlf) == (rows_ptr[i] + rs)) && strncasecmp(values_ptr[i], "\\N", 2) == 0
						&& IsEOR(values_ptr[i] + 2)) ||
				(qualifier_present[cur_attr][i] && ((values_ptr[i] + 3 + crlf) == (rows_ptr[i] + rs)) && strncmp(values_ptr[i], "\\N", 2) == 0
						&& IsEOR(values_ptr[i] + 3))
			)
				nulls[i] = 1;
			else
				nulls[i] = 0;
		}
	}*/
}

bool DataParserForText::FormatSpecificDataCheck()
{

	if(ATI::IsTxtType(cur_attr_type)) {
		for(int i = 0; i < no_prepared; i++) {
#ifndef PURE_LIBRARY
			size_t char_len = atis[cur_attr].CharsetInfo()->cset->numchars(atis[cur_attr].CharsetInfo(), values_ptr[i], values_ptr[i] + GetValueSize(i));
#else
			size_t char_len = GetValueSize(i);
#endif
			if(!IsNull(i) && char_len > atis[cur_attr].CharLen()) {
				error_value.first = i + 1;
				error_value.second = cur_attr + 1;
				return false;
			}
		}
	} else if(ATI::IsBinType(cur_attr_type)) {
		for(int i = 0; i < no_prepared; i++) {
			if(!IsNull(i) && (GetObjSize(i)%2)) {
				error_value.first = i + 1;
				error_value.second = cur_attr + 1;
				return false;
			}
		}
	}
	return true;
}

uint DataParserForText::GetValueSize(int ono)
{
	if(ATI::IsBinType(CurrentAttributeType()))
		return (*value_sizes[cur_attr])[ono] / 2;
	else
		return (*value_sizes[cur_attr])[ono];

}

char* DataParserForText::GetObjPtr(int ono) const
{
	return oryg_vals_ptrs.size() ? oryg_vals_ptrs[ono] : values_ptr[ono];
}

char DataParserForText::TranslateEscapedChar(char c)
{
	static char in[] = {'0',  'b',  'n',  'r',  't',  char(26)};
	static char out[]= {'\0', '\b', '\n', '\r', '\t', char(26)};
	for(int i = 0; i < 6; i++)
		if(in[i] == c)
			return out[i];
	return c;
}

//bool DataParserForText::IsEOR(const char* ptr) const
//{
//	return (crlf == 2 && *ptr == '\r' && *(ptr + 1) == '\n') ||	(crlf == 1 && *ptr == '\n');
//}

/*bool DataParserForText::IsEOR(const char* ptr) const
{
	int i = 0;
	while(i < crlf && ptr[i] == eol[i])
		i++;
	return i == crlf;
}*/

void DataParserForText::ShiftBufferByBOM()
{
	// utf-8
	if((uchar)buf_start[0] == 0xEF && (uchar)buf_start[1] == 0xBB && (uchar)buf_start[2] == 0xBF) {
		buf_start += 3;
		buf_ptr += 3;
	} else if((uchar)buf_start[0] == 0xFE && (uchar)buf_start[1] == 0xFF || // UTF-16-BE
		(uchar)buf_start[0] == 0xFF && (uchar)buf_start[1] == 0xFE) {  // UTF-16-LE
		buf_start += 2;
		buf_ptr += 2;
	}
}

size_t DataParserForText::GetBufferForConvertedValues(size_t size)
{
	if(pos_in_converted_values + size > converted_values_size) {
		converted_values = (char*)realloc(converted_values, pos_in_converted_values + size);
		converted_values_size = pos_in_converted_values + size;
	}
	size_t tmp_pos = pos_in_converted_values;
	pos_in_converted_values += size;
	return  tmp_pos;
}

char ** DataParserForText::copy_values_ptr(int start_ono, int end_ono) {
#ifndef PURE_LIBRARY
	if( ATI::IsTxtType(cur_attr_type) && (atis[cur_attr].CharsetInfo()->mbmaxlen > cs_info->mbmaxlen) )
	{
		char **result = new char*[end_ono-start_ono];
		char *buf = new char[converted_values_size];
		memcpy(buf, converted_values, converted_values_size);
		con_buffs.push_back(buf);
		for( int i=0, j=start_ono; j<end_ono; i++,j++ )
			result[i] = values_ptr[j] - converted_values + buf;
			
		return result;		
	}
#endif
	return DataParser::copy_values_ptr(start_ono, end_ono);
}

bool DataParserForText::DoPreparedValuesHasToBeCoppied()
{
#ifndef PURE_LIBRARY
	return ATI::IsTxtType(cur_attr_type) && (atis[cur_attr].CharsetInfo()->mbmaxlen > cs_info->mbmaxlen);
#else
	return false;
#endif
}
