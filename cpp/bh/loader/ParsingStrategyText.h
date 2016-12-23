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

#ifndef PARSINPOLICYTEXT_H_
#define PARSINPOLICYTEXT_H_

#include <vector>

#include "edition/local_fwd.h"
#include "loader/ParsingStrategy.h"
#include "types/ValueParser.h"

class NewValuesSetBase;
class ValueCache;

class ParsingStrategyText: public ParsingStrategy
{
public:
	typedef std::vector<int> kmp_next_t;

	enum SearchResult	{ PATTERN_FOUND, END_OF_BUFFER, END_OF_LINE };

	ParsingStrategyText(const IOParameters& iop/*, int64 loader_start_time*/);
	virtual ~ParsingStrategyText();

	int ParseHeader(const char* const buf, size_t size) const;
	ParseResult GetRow(const char* const buf, size_t size, std::vector<boost::shared_ptr<ValueCache> >& values, uint& rowsize, ParseError& errorinfo);

private:
	bool		prepared;

	std::string eol;
	std::string	delimiter;
	uchar		string_qualifier;
	char		escape_char;
	std::string	enclose_delimiter;
	std::string	enclose_eol;

	kmp_next_t kmp_next_delimiter;
	kmp_next_t kmp_next_enclose_delimiter;
	kmp_next_t kmp_next_eol;
	kmp_next_t kmp_next_enclose_eol;

	CHARSET_INFO* cs_info;
	std::vector<char> temp_buf;

	RCBString tmp_string;
	std::vector<ParsingFunction> parsing_functions;

	void GuessUnescapedEOL(const char* ptr, const char* buf_end);
	void GuessUnescapedEOLWithEnclose(const char* ptr, const char* const buf_end);
	bool SearchUnescapedPattern(const char* &ptr, const char* const buf_end, const std::string& pattern, const std::vector<int>& kmp_next);
	SearchResult SearchUnescapedPatternNoEOL(const char* &ptr, const char* const buf_end, const std::string& pattern, const std::vector<int>& kmp_next);
	void GetEOL(const char* const buf, const char* const buf_end);

	void GetValue(const char* const value_ptr, size_t value_size, ushort col, ValueCache& value);
};

#endif
