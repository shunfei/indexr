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

#include <time.h>
#ifdef __GNUC__
#if !defined(__sun__)   // solaris 10 does not have execinfo.h
#include <cxxabi.h>
#include <execinfo.h>
#endif
#endif /* #include __GNUC__ */

#include "core/tools.h"
#include "system/RCSystem.h"

using namespace std;
using namespace boost;

namespace bh
{

int const COORD::TABLE = 0;
int const COORD::COLUMN = 1;
int const COORD::DP = 2;

}

void MakeStringUnique(std::string& id)
{
	static IBMutex unique_string_mutex;
	static uint64 unique_string_generator_id = 0;

	char uid[64] = "";
	int64 pid = GetPid();
	time_t cur_time = time(NULL);
	uint64 localid = 0;
	{
		IBGuard localid_guard(unique_string_mutex);
		localid = ++unique_string_generator_id;
	}
	sprintf(uid, "%lld-%lld-%llu", pid, (int64)cur_time, localid);
	id += uid;
}

string demangle( char const* symbolName_ )
{
#ifdef __GNUC__
#if !defined(__sun__)
	int status = 0;
	string symbol;
	char* p( abi::__cxa_demangle( symbolName_, 0, 0, &status ) );
	if ( p )
		{
		symbol = p;
		::free( p );
		}
	return ( symbol );
#endif
#else /* #ifdef __GNUC__ */
	return ( "" );
#endif /* #else #ifdef __GNUC__ */
}

std::vector<std::string> get_stack( int maxDepth_ )
{
	vector<string> stack;
#ifdef _EXECINFO_H

	typedef shared_array<void*> pointers_t;
	pointers_t pointer( new void*[maxDepth_ + 1] );

	int size( backtrace( pointer.get(), maxDepth_ ) );
	char** strings( backtrace_symbols( pointer.get(), size ) );

	if ( maxDepth_ < size )
		size = maxDepth_;
	char* ptr = NULL;
	char* end = NULL;
	for ( int ctr( 0 ); ctr < size; ++ ctr ) {
		ptr = strchr( strings[ ctr ], '(' );
		if ( ptr ) {
			end = strchr( ptr, '+' );
			if ( end )
				(*end) = 0;
			string symbol( demangle( ptr + 1 ) );
			if ( ! symbol.empty() )
				stack.push_back( symbol );
			else if ( strings[ctr] )
				stack.push_back( strings[ctr] );
		}
	}
	::free( strings );
#endif /* #ifdef _EXECINFO_H */
	return ( stack );
}

void dump_stack( char const* file_, int line_, char const* function_, string const& msg_, int maxDepth_ )
{
	vector<string> stack;
	get_stack( maxDepth_ ).swap( stack );
	rclog << lock
		<< "--------------------------------------------------------------------------------\n"
			 "--- Stack dump from:\n"
			 "--- file:     " << file_ << "\n"
			 "--- line:     " << line_ << "\n"
			 "--- function: " << function_ << "\n"
			 "--- message:  " << msg_ << "\n"
			 "--------------------------------------------------------------------------------\n";
	for ( vector<string>::const_iterator it( stack.begin() ), end( stack.end() ); it != end; ++ it )
		rclog << *it << endl;
	rclog << "\n--------------------------------------------------------------------------------" << unlock;
	return;
}
