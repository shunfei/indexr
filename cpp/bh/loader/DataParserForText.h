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

#ifndef _BHDATA_LOADER_H_
#define _BHDATA_LOADER_H_

#include <iostream>
#include <ctype.h>
#include <math.h>
#include "common/CommonDefinitions.h"
#include "compress/tools.h"
#include "common/bhassert.h"
#include "loader/DataParser.h"

class NewValuesSet;
class Buffer;

class DataParserForText : public DataParser
{
	friend class NewValuesSet;

	enum SearchResult	{ PATTERN_FOUND, END_OF_BUFFER, END_OF_LINE };

	void	ProcessEscChar(int i);

	std::vector<std::vector<bool> > qualifier_present;
	std::vector<char*> con_buffs;
	
public:
	typedef std::vector<int> kmp_next_t;

	DataParserForText(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr);
	virtual ~DataParserForText();
public:
	virtual int		GetRowSize(char* buf_ptr, int rno);
	virtual void	PrepareNulls();
	virtual void	PrepareObjsSizes();
	virtual void 	PrepareValuesPointers();
	virtual uint 	GetValueSize(int ono);
	virtual bool 	FormatSpecificDataCheck();
	virtual void	FormatSpecificProcessing();
    virtual void    ReleaseCopies();

public:
	char*	GetValue(int ono);
	void	GetEOL();
	//bool 	IsEOR(const char* ptr) const;

	bool	DoPreparedValuesHasToBeCoppied();

protected:
	virtual char* GetObjPtr(int ono) const;

public:
	uint**		no_esc_char;
	bool		prepared;
	std::string eol;
	//ushort	crlf;
	//char		delimiter;
	//char		delimiter_size;
	std::string	delimiter;
	uchar		string_qualifier;
	char		escape_char;

	char* 	converted_values;
	size_t	converted_values_size;
	size_t	pos_in_converted_values;

	char ** copy_values_ptr(int start_ono, int end_ono);

private:
	std::vector<char*> oryg_vals_ptrs;

	std::string		enclose_delimiter;
	std::string		enclose_eol;

	kmp_next_t kmp_next_delimiter;
	kmp_next_t kmp_next_enclose_delimiter;
	kmp_next_t kmp_next_eol;
	kmp_next_t kmp_next_enclose_eol;

private:
	
	/*! \brief The input text file can contain header information
	 * (BOM) describing byte ordering. The function moves the 
	 * \e buf_start pointer to the first byte after BOM
	// \return void 
	*/
	void	ShiftBufferByBOM();
	size_t	GetBufferForConvertedValues(size_t size);
	void 	SetPosInConvertedValues(size_t pos) { pos_in_converted_values = pos; };
	char*	ConvertedValue(size_t pos) { return converted_values + pos; }

	void GuessUnescapedEOL(const char* ptr);
	void GuessUnescapedEOLWithEnclose(const char* ptr);
	bool SearchUnescapedPattern(const char* &ptr, const std::string& pattern, const std::vector<int>& kmp_next);
	SearchResult SearchUnescapedPatternNoEOL(const char* &ptr, const std::string& pattern, const std::vector<int>& kmp_next);

	CHARSET_INFO* cs_info;

public:
	static char	TranslateEscapedChar(char c);
};


#endif //_CORE_BHDATA_LOADER_H_

