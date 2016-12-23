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
#include <limits>

#include "edition/local.h"
#include "ParsingStrategyText.h"
#include "types/ValueParserForText.h"
#include "loader/ValueCache.h"

using namespace std;

namespace
{

void PrepareKMPNext(ParsingStrategyText::kmp_next_t& kmp_next, string const& pattern)
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

ParsingStrategyText::ParsingStrategyText(const IOParameters& iop/*, int64 loader_start_time*/)
	:	ParsingStrategy(iop), prepared(false), eol(iop.LineTerminator()), delimiter(iop.Delimiter()),
	 	string_qualifier(iop.StringQualifier()), escape_char(iop.EscapeCharacter()), temp_buf(65536),
	 	tmp_string(ZERO_LENGTH_STRING, 0), parsing_functions(NoColumns())
{
#ifndef PURE_LIBRARY
	cs_info = get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.CharsetInfoNumber(), 0);
#endif
	ValueParserAutoPtr value_parser = DataFormat::GetDataFormat(iop.GetEDF())->CreateValueParser();
	for(int i = 0; i < NoColumns(); ++i) {
		if (ATI::IsStringType(GetATI(i).Type())) {
#ifndef PURE_LIBRARY
			GetATI(i).SetCollation(get_charset_IB((char*)iop.CharsetsDir().c_str(), iop.ColumnsCollations()[i], 0));
#endif
		} else
			parsing_functions[i] = value_parser->GetParsingFuntion(GetATI(i));
	}
	PrepareKMPNext(kmp_next_delimiter, delimiter);
	if (string_qualifier) {
		enclose_delimiter = string((char*)&string_qualifier, 1) + delimiter;
		PrepareKMPNext(kmp_next_enclose_delimiter, enclose_delimiter);
	}
}

ParsingStrategyText::~ParsingStrategyText()
{
}

int ParsingStrategyText::ParseHeader(const char* const buf, size_t size) const
{
	// get the length of BOM
	int hdr_len = 0;
	if((uchar)buf[0] == 0xEF && (uchar)buf[1] == 0xBB && (uchar)buf[2] == 0xBF) {
		hdr_len = 3;
	} else if((uchar)buf[0] == 0xFE && (uchar)buf[1] == 0xFF || // UTF-16-BE
		(uchar)buf[0] == 0xFF && (uchar)buf[1] == 0xFE) {  // UTF-16-LE
		hdr_len = 2;
	}
	return hdr_len;
}

inline void ParsingStrategyText::GuessUnescapedEOL(const char* ptr, const char* const buf_end)
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

inline void ParsingStrategyText::GuessUnescapedEOLWithEnclose(const char* ptr, const char* const buf_end)
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

inline bool ParsingStrategyText::SearchUnescapedPattern(const char*& ptr, const char* const buf_end, const string& pattern, const vector<int>& kmp_next)
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

inline ParsingStrategyText::SearchResult ParsingStrategyText::SearchUnescapedPatternNoEOL(const char*& ptr, const char* const buf_end, const string& pattern, const vector<int>& kmp_next)
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

void ParsingStrategyText::GetEOL(const char* const buf, const char* const buf_end)
{
	if(eol.size() == 0) {
		const char* ptr = buf;
		for(int col = 0; col < NoColumns() - 1; ++col) {
			if(string_qualifier && *ptr == string_qualifier) {
				if(!SearchUnescapedPattern(++ptr, buf_end, enclose_delimiter, kmp_next_enclose_delimiter))
					throw RCException("Unable to detect the line terminating sequence, please specify it explicitly.");
				++ptr;
			} else if(!SearchUnescapedPattern(ptr, buf_end, delimiter, kmp_next_delimiter))
				throw RCException("Unable to detect the line terminating sequence, please specify it explicitly.");
			ptr += delimiter.size();
		}
		if(string_qualifier && *ptr == string_qualifier)
			GuessUnescapedEOLWithEnclose(++ptr, buf_end);
		else
			GuessUnescapedEOL(ptr, buf_end);
	}
	if(eol.size()==0)
		throw RCException("Unable to detect the line terminating sequence, please specify it explicitly.");
	PrepareKMPNext(kmp_next_eol, eol);
	if(string_qualifier) {
		enclose_eol = string((char*) &string_qualifier, 1) + eol;
		PrepareKMPNext(kmp_next_enclose_eol, enclose_eol);
	}
}

ParsingStrategy::ParseResult ParsingStrategyText::GetRow(const char* const buf, size_t size, std::vector<boost::shared_ptr<ValueCache> >& record, uint& rowsize, ParseError& errorinfo)
{
	const char* buf_end = buf + size;
	if(!prepared) {
		GetEOL(buf, buf_end);
		prepared = true;
	}
	if (buf == buf_end)
		return ParsingStrategy::END_OF_BUFFER;
	const char* ptr = buf;
	bool row_incomplete = false;
	errorinfo.value = -1;
	for(int col = 0; col < NoColumns() - 1; ++col) {
		const char* val_beg = ptr;
		if(string_qualifier && *ptr == string_qualifier) {
			row_incomplete = !SearchUnescapedPattern(++ptr, buf_end, enclose_delimiter, kmp_next_enclose_delimiter);
			++ptr;
		} else {
			SearchResult res = SearchUnescapedPatternNoEOL(ptr, buf_end, delimiter, kmp_next_delimiter);
			if (res == END_OF_LINE) {
				ptr += eol.size();
				rowsize = uint(ptr - buf);
				errorinfo.value = col;
				return ERROR;
			}
			row_incomplete = (res == END_OF_BUFFER);
		}

		if (row_incomplete) {
			errorinfo.value = col;
			break;
		}
		try {
			GetValue(val_beg, ptr - val_beg, col, *record[col]);
		} catch (...) {
			if (errorinfo.value == -1)
				errorinfo.value = col;
		}
		ptr += delimiter.size();
	}
	if (!row_incomplete) {
		// the last column
		const char* val_beg = ptr;
		if(string_qualifier && *ptr == string_qualifier) {
			row_incomplete = !SearchUnescapedPattern(++ptr, buf_end, enclose_eol, kmp_next_enclose_eol);
			++ptr;
		} else
			row_incomplete = !SearchUnescapedPattern(ptr, buf_end, eol, kmp_next_eol);

		if (!row_incomplete) {
			try {
				GetValue(val_beg, ptr - val_beg, NoColumns() - 1, *record[NoColumns() - 1]);
			} catch (...) {
				if (errorinfo.value == -1)
					errorinfo.value = NoColumns() - 1;
			}
			ptr += eol.size();
		}
	}
	if (row_incomplete) {
		if (errorinfo.value == -1)
			errorinfo.value = NoColumns() - 1;
		return ParsingStrategy::END_OF_BUFFER;
	}
	rowsize = uint(ptr - buf);
	return errorinfo.value == -1 ? OK : ERROR;
}

char TranslateEscapedChar(char c)
{
	static char in[] = {'0',  'b',  'n',  'r',  't',  char(26)};
	static char out[]= {'\0', '\b', '\n', '\r', '\t', char(26)};
	for(int i = 0; i < 6; i++)
		if(in[i] == c)
			return out[i];
	return c;
}

void ParsingStrategyText::GetValue(const char* value_ptr, size_t value_size, ushort col, ValueCache& buffer)
{
	ATI& ati = GetATI(col);

	bool is_enclosed = false;
	if (string_qualifier && *value_ptr == string_qualifier) {
		// trim quotes
		++value_ptr;
		value_size -= 2;
		is_enclosed = true;
	}

	if (ATI::IsCharType(ati.Type())) {
		// trim spaces
		while (value_size > 0 && value_ptr[value_size - 1] == ' ')
			--value_size;
	}

	// check for null
	bool isnull = false;
	switch(value_size) {
	case 0:
		if(!is_enclosed)
			isnull = true;
		break;
	case 2:
		if(*value_ptr == '\\' && (value_ptr[1] == 'N' || (!is_enclosed && value_ptr[1] == 'n')))
			isnull = true;
		break;
	case 4:
		if(!is_enclosed && strncasecmp(value_ptr, "NULL", 4) == 0)
			isnull = true;
		break;
	default:
		break;
	}

	if(isnull)
		buffer.ExpectedNull(true);

	else if(ATI::IsBinType(ati.Type())) {
		// convert hexadecimal format to binary
		if(value_size % 2)
			throw FormatRCException(BHERROR_DATA_ERROR, 0, 0);
		char* buf = reinterpret_cast<char*>(buffer.Prepare(value_size/2));
		int p = 0;
		for(uint l = 0; l < value_size; l += 2) {
			int a = 0;
			int b = 0;
			if(isalpha((uchar)value_ptr[l])) {
				char c = tolower(value_ptr[l]);
				if(c < 'a' || c > 'f')
					throw FormatRCException(BHERROR_DATA_ERROR, 0, 0);
				a = c - 'a' + 10;
			} else
				a = value_ptr[l] - '0';
			if(isalpha((uchar)value_ptr[l+1])) {
				char c = tolower(value_ptr[l+1]);
				if(c < 'a' || c > 'f')
					throw FormatRCException(BHERROR_DATA_ERROR, 0, 0);
				b = c - 'a' + 10;
			} else
				b = value_ptr[l+1] - '0';
			buf[p++] =  (a << 4) | b;
		}
		buffer.ExpectedSize(int(value_size/2));

	} else if(ATI::IsTxtType(ati.Type())) {
		// process escape characters
		uint reserved = (uint)value_size * ati.CharsetInfo()->mbmaxlen;
		char* buf = reinterpret_cast<char*>(buffer.Prepare(reserved));
		int new_size = 0;
		for(int j = 0; j < value_size; j++) {
			if(value_ptr[j] == escape_char)
				buf[new_size] = TranslateEscapedChar(value_ptr[++j]);
			else
				buf[new_size] = value_ptr[j];
			new_size++;
		}
#ifndef PURE_LIBRARY
		if(ati.CharsetInfo() != cs_info) {
			// convert between charsets
			uint errors = 0;
			if(ati.CharsetInfo()->mbmaxlen <= cs_info->mbmaxlen)
				new_size = copy_and_convert(buf, reserved, ati.CharsetInfo(), buf, new_size, cs_info, &errors);
			else {
				if(new_size > temp_buf.size())
					temp_buf.resize(new_size);
				char* tmpbuf = &temp_buf[0];
				memcpy(tmpbuf, buf, new_size);
				new_size = copy_and_convert(buf, reserved, ati.CharsetInfo(), tmpbuf, new_size, cs_info, &errors);
			}
		}
		// check the value length
		size_t char_len = ati.CharsetInfo()->cset->numchars(ati.CharsetInfo(), buf, buf + new_size);
#else
		size_t char_len = new_size;
#endif
		if(char_len > ati.CharLen())
			throw FormatRCException(BHERROR_DATA_ERROR, 0, 0);

		buffer.ExpectedSize(new_size);

	} else {
		TemporalValueReplacement<char> tmpvr(const_cast<char&>(value_ptr[value_size]), 0);
		tmp_string.val = const_cast<char*>(value_ptr);
		tmp_string.len = (uint)value_size;
		if(parsing_functions[col](tmp_string, *reinterpret_cast<int64*>(buffer.Prepare(sizeof(int64)))) == BHRC_FAILD)
			throw FormatRCException(BHERROR_DATA_ERROR, 0, col); //TODO: throw appropriate exception
		buffer.ExpectedSize(sizeof(int64));
	}
}
