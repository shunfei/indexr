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

#include <string.h>
#include <sstream>
#include <algorithm>
#include <iterator>

#include "util/BHString.h"

using namespace std;

std::string add_dir_sep( std::string const& path_ )
{
	string path;
	char last( 0 );
	if ( ! path_.empty() ) {
		last = *path_.rbegin();
		path = path_;
	} else {
		path = ".";
	}
	if ( ( last != '/' ) && ( last != '\\' ) )
		path += "/";
	return ( path );
}


size_t strnlenib(const char* s, size_t size)
{
	char* pos0 = (char* )memchr(s, 0, size);
	size_t len = (!pos0) ? size : pos0 - s;
	return len;
}

/* Tested*/

std::string lowercase(std::string const& str_)
{
	string str;
	str.reserve( str_.length() + 1 );
	std::transform(str_.begin(), str_.end(), back_insert_iterator<string>( str ), static_cast<int(*)(int)>( tolower ) );
	return str;
}

std::vector<std::string> tokenize(std::string str, char ch)
{
	std::string token;
	std::istringstream iss(str);
	std::vector<std::string> tokens;
	while ( getline(iss, token, ch) )
		tokens.push_back(token);
	return tokens;
}

