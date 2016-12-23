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

#include <cassert>
#include <iostream>

#include "mysql_gate.h"

#include "domaininject/DomainInjection.h"
#include "domaininject/Concatenator.h"

#ifndef PURE_LIBRARY

int wildcmp(const DTCollation& collation, const char *str, const char *str_end, const char *wildstr,const char *wildend, int escape, int w_one, int w_many)
{
	 return collation.collation->coll->wildcmp(collation.collation, str, str_end,wildstr, wildend,	escape, w_one, w_many);
}

size_t strnxfrm(const DTCollation& collation, uchar* src, size_t src_len, const uchar* dest, size_t dest_len)
{
	return collation.collation->coll->strnxfrm(collation.collation, src, src_len, dest, dest_len);
}

longlong Item_func_is_decomposition_rule_correct::val_int()
{
	String value;
	return DomainInjectionManager::IsValid(std::string(args[0]->val_str(&value)->c_ptr_safe()));
}

const char* const InfobrightServerVersion()
{
	static std::string IB_VERSION ( CURRENT_SERVER_VERSION );
	static bool already_set = false;
	if(!already_set) {
		char const start[] = "IB_";
		char const* p( strstr( MYSQL_COMPILATION_COMMENT, start ) );

		static int const MAX_VER_LEN( 8 );
		static char ver[MAX_VER_LEN + 1] = "";
		if(p) {
			if(strlen(p) > (sizeof(start) - 1)) {
				p += (sizeof(start) - 1);
				for (int i(0); p[i] && ( i < MAX_VER_LEN ) && ( p[i] != '_' ); ++ i)
					ver[i] = p[i];
				if ( ver[0] )
					IB_VERSION = std::string(ver);
			}
		}
		already_set = true;
	}
	return IB_VERSION.c_str();
}

#else

const char* const InfobrightServerVersion()
{
	return CURRENT_SERVER_VERSION.c_str();
}

int wildcmp(const DTCollation& collation, const char *str,const char *str_end, const char *wildstr,const char *wildend, int escape,int w_one, int w_many)
{
	assert(!"NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
}

size_t strnxfrm(const DTCollation& collation, unsigned char* src, size_t src_len, const unsigned char* dest, size_t dest_len)
{
	assert(!"NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
}

#endif
